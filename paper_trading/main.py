"""
Main Runner — V7 Dual Funnel Paper Trading System
===================================================
Wires all 6 layers together with two independent funnels.

V6 INHERITED:
  - Two TokenScanner instances (BBUP funnel + BBDOWN funnel)
  - Each funnel has its own token-list query config, phase-driven polling
    cadence, and Full Focus slots
  - Each funnel subscribes its tokens on its own WS candle timeframe
  - Heartbeat task: periodic unconditional log + task health monitoring
  - Threaded DB: flush_writes, take_snapshots, rebuild_cycles run
    via run_in_executor() to never block the async event loop
  - Task death detection: heartbeat checks all critical tasks

V7 CHANGES:
  - Token overview fetch moves from Full promotion → Narrow promotion
    (data captured at the right funnel position for future filter analysis)
  - In-memory overview cache: fetched once at narrow, consumed once at Full
    (no double-fetch; defensive fallback fetches fresh on cache miss)
  - Cache eviction: pop-on-consume + periodic orphan sweep from _status_loop
"""

import asyncio
import logging
import signal
import sys
import time
from datetime import datetime, timezone
from typing import Dict, List, Optional

# Load a local .env into os.environ as early as possible (at import time, before
# any entry path reads DATA_PROVIDER_API_KEY). No-op when no .env exists, so a
# service-manager deployment (environment set by the unit) is unaffected. Both `python -m paper_trading run`
# (which imports this module) and `python main.py` are covered.
from dotenv import load_dotenv
load_dotenv()

from paper_trading.orchestrator.config import (
    SystemConfig, STRATEGY_CONFIGS, FunnelConfig,
)
from paper_trading.orchestrator.orchestrator import Orchestrator
from paper_trading.data_engine.engine import DataEngine
from paper_trading.data_engine.rest_client import (
    DataProviderRESTClient, ListQueryConfig, BBUP_LIST_CONFIG, BBDOWN_LIST_CONFIG,
)
from paper_trading.scanner.scanner import TokenScanner
from paper_trading.recording.scoring_collector import ScoringDataCollector
from paper_trading.core.models import Timeframe

logger = logging.getLogger(__name__)


# ═══════════════════════════════════════════════════════════════
# V7.1: WATCHDOG CONFIG + systemd sd_notify
# ═══════════════════════════════════════════════════════════════

# If the WS feed produces no messages for this long, the heartbeat forces
# a reconnect (layer behind websocket_client's own recv timeout).
WS_SILENCE_RECONNECT_S = 180.0
# Minimum gap between watchdog-forced reconnects, so we don't spam them.
WS_WATCHDOG_COOLDOWN_S = 120.0


def _sd_notify(state: str) -> None:
    """Send a notification to systemd via the NOTIFY_SOCKET, if present.

    No-op when not run under systemd (socket env var absent), so this is
    safe in foreground/smoke-test runs. Pure stdlib, no new dependency.

    Used for:
      - "READY=1"      once startup completes
      - "WATCHDOG=1"   periodic liveness ping from the heartbeat
    """
    addr = __import__("os").environ.get("NOTIFY_SOCKET")
    if not addr:
        return
    try:
        import socket
        # Abstract namespace socket starts with '@' → replace with NUL byte
        if addr.startswith("@"):
            addr = "\0" + addr[1:]
        with socket.socket(socket.AF_UNIX, socket.SOCK_DGRAM) as sock:
            sock.connect(addr)
            sock.sendall(state.encode("utf-8"))
    except Exception as e:
        logger.debug(f"sd_notify({state!r}) failed (ignored): {e}")


def _make_list_config(funnel_cfg: FunnelConfig) -> ListQueryConfig:
    """Build a ListQueryConfig from a FunnelConfig."""
    return ListQueryConfig(
        name=funnel_cfg.name,
        min_market_cap=funnel_cfg.min_market_cap,
        max_market_cap=funnel_cfg.max_market_cap,
        min_liquidity=funnel_cfg.min_liquidity,
        min_volume_5m_usd=funnel_cfg.min_volume_5m_usd,
    )


class PaperTradingSystem:
    """
    Top-level system with two independent funnels.

    BBUP funnel:   volume-pre-filtered tokens → BBUP WS timeframe → BBUP strategies
    BBDOWN funnel: broad tokens               → BBDOWN WS timeframe → BBDOWN strategies
    """

    def __init__(
        self,
        data_api_key: str = "",
        config: Optional[SystemConfig] = None,
    ):
        self.config = config or SystemConfig(data_api_key=data_api_key)

        # ── Layer 6: Orchestrator ──────────────────────────────
        self.orchestrator = Orchestrator(self.config)

        # ── Layer 2: Data Engine (shared WS + REST) ────────────
        self.data_engine = DataEngine(
            api_key=self.config.data_api_key,
            on_candle=self._on_candle,
            on_tick=self._on_tick,
            backfill_candles=self.config.backfill_candles,
        )

        # ── REST client (shared for both scanners) ─────────────
        self._scanner_rest = DataProviderRESTClient(
            api_key=self.config.data_api_key,
        )

        # ── Layer 1: Two Scanners ──────────────────────────────
        bbup_cfg = self.config.bbup_funnel
        self.scanner_bbup = TokenScanner(
            data_fetcher=self._scanner_rest,
            on_full_promotion=self._on_bbup_promoted,
            on_full_removal=self._on_bbup_removed,
            on_narrow_promotion=self._on_bbup_narrow,
            funnel_name="bbup",
            list_config=_make_list_config(bbup_cfg),
            wide_capacity=bbup_cfg.wide_capacity,
            narrow_capacity=bbup_cfg.narrow_capacity,
            full_capacity=bbup_cfg.full_capacity,
            list_poll_interval_minutes=bbup_cfg.list_poll_interval_minutes,
            narrow_scan_interval_minutes=bbup_cfg.narrow_scan_interval_minutes,
            sol_poll_interval_minutes=self.config.sol_poll_interval_minutes,
        )

        bbdown_cfg = self.config.bbdown_funnel
        self.scanner_bbdown = TokenScanner(
            data_fetcher=self._scanner_rest,
            on_full_promotion=self._on_bbdown_promoted,
            on_full_removal=self._on_bbdown_removed,
            on_narrow_promotion=self._on_bbdown_narrow,
            funnel_name="bbdown",
            list_config=_make_list_config(bbdown_cfg),
            wide_capacity=bbdown_cfg.wide_capacity,
            narrow_capacity=bbdown_cfg.narrow_capacity,
            full_capacity=bbdown_cfg.full_capacity,
            list_poll_interval_minutes=bbdown_cfg.list_poll_interval_minutes,
            narrow_scan_interval_minutes=bbdown_cfg.narrow_scan_interval_minutes,
            sol_poll_interval_minutes=self.config.sol_poll_interval_minutes,
        )

        # ── Scoring (passive) ─────────────────────────────────
        self.scoring = ScoringDataCollector(self.config.db_path)

        # ── State ─────────────────────────────────────────────
        self._running = False
        self._tasks: List[asyncio.Task] = []
        self._background_tasks: set = set()
        self._shutdown_event = asyncio.Event()
        self._last_heartbeat: float = 0

        # ── V7: Token overview cache ──────────────────────────
        # Populated on narrow promotion by _handle_narrow_promotion (T7),
        # consumed on Full promotion by _handle_promotion (T8) via _pop_overview,
        # swept for orphans by _status_loop (T9) via _sweep_orphan_overviews.
        # No lock needed — all access is from the event loop, every op is
        # a single atomic dict primitive (no awaits inside helpers).
        self._overview_cache: Dict[str, dict] = {}

        # ── V7.1: cache hit/miss telemetry (C2) ───────────────
        # Incremented in _pop_overview: hit when an overview was cached at
        # narrow and successfully consumed at Full, miss when Full had to
        # fall back to a fresh fetch. Logged in the STATUS line.
        self._cache_hits: int = 0
        self._cache_misses: int = 0

        # ── V7.1: WS watchdog state ───────────────────────────
        # If the WS feed goes silent longer than WS_SILENCE_RECONNECT_S,
        # the heartbeat forces a reconnect (defense-in-depth behind the
        # recv-timeout fix in websocket_client.listen). Guarded by a
        # cooldown so we don't spam reconnects.
        self._ws_watchdog_last_action: float = 0.0

    # ═══════════════════════════════════════════════════════════
    # CALLBACK WIRING — BBUP FUNNEL
    # ═══════════════════════════════════════════════════════════

    def _on_bbup_narrow(self, token_ca: str, ticker: str):
        # V7: fetch token_overview at narrow + cache for Full re-use.
        # Pattern mirrors _on_bbup_promoted — sync callback launches async task.
        task = asyncio.create_task(
            self._handle_narrow_promotion(token_ca, ticker, "bbup"),
            name=f"bbup_narrow_{token_ca[:8]}",
        )
        self._background_tasks.add(task)
        task.add_done_callback(self._task_done)

    def _on_bbup_promoted(self, token_ca: str, ticker: str):
        task = asyncio.create_task(
            self._handle_promotion(token_ca, ticker, "bbup"),
            name=f"bbup_promo_{token_ca[:8]}",
        )
        self._background_tasks.add(task)
        task.add_done_callback(self._task_done)

    def _on_bbup_removed(self, token_ca: str, reason: str):
        task = asyncio.create_task(
            self._handle_removal(token_ca, reason, "bbup"),
            name=f"bbup_remove_{token_ca[:8]}",
        )
        self._background_tasks.add(task)
        task.add_done_callback(self._task_done)

    # ═══════════════════════════════════════════════════════════
    # CALLBACK WIRING — BBDOWN FUNNEL
    # ═══════════════════════════════════════════════════════════

    def _on_bbdown_narrow(self, token_ca: str, ticker: str):
        # V7: fetch token_overview at narrow + cache for Full re-use.
        task = asyncio.create_task(
            self._handle_narrow_promotion(token_ca, ticker, "bbdown"),
            name=f"bbdown_narrow_{token_ca[:8]}",
        )
        self._background_tasks.add(task)
        task.add_done_callback(self._task_done)

    def _on_bbdown_promoted(self, token_ca: str, ticker: str):
        task = asyncio.create_task(
            self._handle_promotion(token_ca, ticker, "bbdown"),
            name=f"bbdown_promo_{token_ca[:8]}",
        )
        self._background_tasks.add(task)
        task.add_done_callback(self._task_done)

    def _on_bbdown_removed(self, token_ca: str, reason: str):
        task = asyncio.create_task(
            self._handle_removal(token_ca, reason, "bbdown"),
            name=f"bbdown_remove_{token_ca[:8]}",
        )
        self._background_tasks.add(task)
        task.add_done_callback(self._task_done)

    # ═══════════════════════════════════════════════════════════
    # SHARED HANDLERS
    # ═══════════════════════════════════════════════════════════

    def _task_done(self, task: asyncio.Task):
        """Callback for background tasks — log exceptions, remove from set."""
        self._background_tasks.discard(task)
        if task.cancelled():
            return
        exc = task.exception()
        if exc:
            logger.error(
                f"Background task {task.get_name()} failed: {exc}",
                exc_info=exc,
            )

    # ═══════════════════════════════════════════════════════════
    # V7: TOKEN OVERVIEW CACHE
    # ═══════════════════════════════════════════════════════════
    # The cache holds the (large) token-overview response between
    # narrow promotion (where it's fetched and logged as scoring snapshot)
    # and Full promotion (where it's re-used for the full_promotion snapshot,
    # avoiding a second paid fetch).
    #
    # Lifecycle:
    #   narrow promotion → _handle_narrow_promotion fetches + caches via _cache_overview
    #   Full promotion   → _handle_promotion consumes via _pop_overview
    #                      (cache miss → defensive fresh fetch)
    #   token leaves both funnels → _sweep_orphan_overviews evicts the entry
    #
    # All access is from the event loop. Operations are atomic dict primitives;
    # no lock needed. The defensive fallback at Full promotion handles the only
    # real race (promotion happens before fetch completes → cache miss).

    def _cache_overview(self, token_ca: str, overview_data: Optional[dict]) -> None:
        """Store an overview response for later consumption at Full promotion.

        None values are NOT cached — they're treated as fetch failures and
        leave the cache miss/hit semantics of _pop_overview unambiguous.
        """
        if overview_data is None:
            return
        self._overview_cache[token_ca] = overview_data

    def _pop_overview(self, token_ca: str) -> Optional[dict]:
        """Retrieve and remove cached overview. Returns None on miss.

        V7.1 (C2): records cache hit/miss telemetry for the STATUS line."""
        result = self._overview_cache.pop(token_ca, None)
        if result is None:
            self._cache_misses += 1
        else:
            self._cache_hits += 1
        return result

    def _peek_overview(self, token_ca: str) -> bool:
        """Check whether an overview is cached, without consuming it.
        Used for diagnostics; not used to gate any decision."""
        return token_ca in self._overview_cache

    def _evict_overview_if_orphaned(self, token_ca: str) -> bool:
        """Evict the cache entry if the token is no longer in any scanner's
        narrow or full bucket. Returns True if an eviction happened.

        Token is considered orphaned when it's absent from BOTH funnels'
        narrow buckets AND BOTH funnels' full buckets. A token alive in
        either funnel's narrow or full is still in-funnel; keep its cache.
        """
        # Cheap path: not in cache at all → nothing to do
        if token_ca not in self._overview_cache:
            return False

        in_bbup_narrow = self.scanner_bbup.funnel.buckets.narrow.get(token_ca) is not None
        in_bbup_full = self.scanner_bbup.funnel.buckets.full.get(token_ca) is not None
        in_bbdown_narrow = self.scanner_bbdown.funnel.buckets.narrow.get(token_ca) is not None
        in_bbdown_full = self.scanner_bbdown.funnel.buckets.full.get(token_ca) is not None

        still_alive = in_bbup_narrow or in_bbup_full or in_bbdown_narrow or in_bbdown_full
        if still_alive:
            return False

        self._overview_cache.pop(token_ca, None)
        return True

    def _sweep_orphan_overviews(self) -> int:
        """Walk the cache and evict every entry whose token has left both funnels.
        Returns the number of evictions. Intended to be called periodically
        from _status_loop (T9). Cheap (O(N) over cache size, typically <100 entries).
        """
        if not self._overview_cache:
            return 0
        # Snapshot keys to avoid mutation-during-iteration
        evicted = 0
        for ca in list(self._overview_cache.keys()):
            if self._evict_overview_if_orphaned(ca):
                evicted += 1
        return evicted

    def _on_candle(self, candle):
        self.orchestrator.on_candle_received(candle)
        self.scoring.log_candle(candle)

    def _on_tick(self, tick):
        self.orchestrator.on_tick_received(tick)

    async def _handle_narrow_promotion(self, token_ca: str, ticker: str, funnel_name: str):
        """V7: Narrow-promotion async handler (shared by both funnels).

        Three actions in order:
          1. Fetch the token overview (paid call) — captures rich short-window
             activity data (unique wallets, volume change, etc.) at the right
             funnel position for future filter analysis.
          2. Log a 'narrow_entry' scoring snapshot with the overview JSON attached.
             Skipped if the token has already left the narrow bucket between the
             sync callback firing and this task running (rare but possible — token
             might race to Full or get demoted/expelled).
          3. Cache the overview in self._overview_cache so the Full-promotion
             handler (T8) re-uses it without a second paid fetch.

        Race tolerance: looks for the token in narrow OR full (in case it
        already raced past us). If found nowhere, snapshot is skipped but the
        overview is still cached — the orphan sweep (T9) cleans up unused entries.

        Fetch failure tolerance: _cache_overview is a no-op when overview is None,
        so a failed fetch silently leaves the cache empty for this token. The
        Full-promotion handler's defensive fallback (T8) will then fetch fresh.
        """
        try:
            logger.info(
                f"[{funnel_name}] Token promoted to Narrow: {ticker} "
                f"({token_ca[:12]}...) — fetching overview"
            )

            # 1. Fetch overview (paid call)
            overview_data = await self._scanner_rest.fetch_token_overview(token_ca)

            # 2. Find the token to log the scoring snapshot.
            #    Look in narrow first (expected), then full (race-promoted case).
            scanner = self.scanner_bbup if funnel_name == "bbup" else self.scanner_bbdown
            token = (scanner.funnel.buckets.narrow.get(token_ca)
                     or scanner.funnel.buckets.full.get(token_ca))

            if token is not None:
                self.scoring.log_snapshot(
                    token, stage="narrow_entry",
                    overview_data=overview_data,
                )
            else:
                logger.debug(
                    f"[{funnel_name}] Token {token_ca[:12]}... left funnel before "
                    f"narrow snapshot could be logged (will not log)"
                )

            # 3. Cache for re-use at Full promotion.
            #    Refused if overview_data is None — defensive fallback at Full handles it.
            self._cache_overview(token_ca, overview_data)

            if overview_data is not None:
                logger.info(
                    f"[{funnel_name}] Narrow snapshot logged + overview cached: "
                    f"{ticker}"
                )
            else:
                logger.warning(
                    f"[{funnel_name}] Narrow overview fetch failed for {ticker} "
                    f"({token_ca[:12]}...) — Full promotion will fall back to fresh fetch"
                )
        except Exception as e:
            logger.error(
                f"[{funnel_name}] Failed to handle narrow promotion for "
                f"{ticker} ({token_ca[:12]}...): {e}"
            )

    async def _handle_promotion(self, token_ca: str, ticker: str, funnel_name: str):
        """Async handler for token promotion (shared by both funnels).

        V7: Consumes the overview from the in-memory cache populated by
        _handle_narrow_promotion (T7) at narrow promotion time. On cache
        miss, falls back to a fresh fetch (defensive — same cost as V6).

        Cache-miss conditions:
          1. Narrow-stage fetch returned None (API soft failure)
          2. Narrow-stage fetch raised an exception
          3. Race: narrow-stage task hadn't finished its await before this
             task started (very fast narrow→full promotion)

        All three are correctness-recoverable via the fallback. No data loss.
        """
        try:
            logger.info(f"[{funnel_name}] Token promoted to Full: {ticker} ({token_ca[:12]}...)")

            # V7: try cache first (populated by T7 narrow handler)
            overview_data = self._pop_overview(token_ca)
            if overview_data is None:
                # Cache miss: fetch fresh as defensive fallback (paid call)
                logger.warning(
                    f"[{funnel_name}] Overview cache miss at Full promotion for {ticker} "
                    f"({token_ca[:12]}...) — falling back to fresh fetch"
                )
                overview_data = await self._scanner_rest.fetch_token_overview(token_ca)
            else:
                logger.debug(
                    f"[{funnel_name}] Overview cache hit at Full promotion for {ticker} "
                    f"({token_ca[:12]}...) — no extra fetch"
                )

            # Log Full promotion snapshot
            scanner = self.scanner_bbup if funnel_name == "bbup" else self.scanner_bbdown
            token = scanner.funnel.buckets.full.get(token_ca)
            if token:
                self.scoring.log_snapshot(
                    token, stage="full_promotion",
                    overview_data=overview_data,
                )

            # Determine which timeframes to subscribe and backfill
            funnel_cfg = (self.config.bbup_funnel if funnel_name == "bbup"
                         else self.config.bbdown_funnel)
            ws_tf = funnel_cfg.ws_timeframe_minutes

            # Subscribe to WebSocket + fetch history
            history_5m, history_1m = await self.data_engine.add_token_with_backfill(
                token_ca, ticker
            )

            # Notify orchestrator (routes to funnel-specific strategies only)
            self.orchestrator.on_token_discovered(
                token_ca=token_ca,
                ticker=ticker,
                history_5m=history_5m if ws_tf == 5 else [],
                history_1m=history_1m if ws_tf == 1 else [],
                funnel_name=funnel_name,
            )

            logger.info(
                f"[{funnel_name}] Token fully wired: {ticker} — "
                f"5m history: {len(history_5m)} candles, "
                f"1m history: {len(history_1m)} candles"
            )

        except Exception as e:
            logger.error(f"[{funnel_name}] Failed to wire promoted token {ticker}: {e}")

    async def _handle_removal(self, token_ca: str, reason: str, funnel_name: str):
        """Async handler for token removal."""
        try:
            await self.data_engine.remove_token(token_ca)
            self.orchestrator.on_token_removed(token_ca, reason, funnel_name=funnel_name)
            logger.info(f"[{funnel_name}] Token removed: {token_ca[:12]}... reason={reason}")
        except Exception as e:
            logger.error(f"[{funnel_name}] Failed to remove token {token_ca[:12]}...: {e}")

    # ═══════════════════════════════════════════════════════════
    # MAIN RUN LOOP
    # ═══════════════════════════════════════════════════════════

    async def run(self):
        """Main entry point. Starts all components and blocks until shutdown."""
        self._running = True

        loop = asyncio.get_running_loop()
        for sig in (signal.SIGINT, signal.SIGTERM):
            loop.add_signal_handler(sig, self._signal_shutdown)

        try:
            # 1. Start orchestrator
            logger.info("=" * 60)
            logger.info("PAPER TRADING SYSTEM — DUAL FUNNEL — STARTING")
            logger.info("=" * 60)
            self.orchestrator.startup()
            self.scoring.open()

            # 2. Start data engine
            await self.data_engine.start()

            # 2b. A1: re-subscribe + backfill any open positions restored at
            # startup. Needs the data engine started (WS connected). Each
            # restored token is held but inert (orchestrator suppresses its
            # signals) until its REST backfill reseeds >= sma_period candles.
            restored = self.orchestrator.get_restored_tokens()
            if restored:
                logger.info(
                    f"A1: re-subscribing {len(restored)} restored "
                    f"open-position token(s) for backfill"
                )
                for (sname, ca, ticker) in restored:
                    try:
                        h5, h1 = await self.data_engine.add_token_with_backfill(
                            ca, ticker
                        )
                        self.orchestrator.seed_restored_token(sname, ca, h5, h1)
                    except Exception as e:
                        logger.error(
                            f"A1 re-subscribe failed for {ticker} "
                            f"({ca[:12]}...): {e}"
                        )

            # 3. Start BOTH scanners
            await self.scanner_bbup.start()
            await self.scanner_bbdown.start()

            # 4. Launch periodic tasks
            self._tasks = [
                asyncio.create_task(
                    self.data_engine.run(), name="data_engine_listen"),
                asyncio.create_task(
                    self._snapshot_loop(), name="snapshot_loop"),
                asyncio.create_task(
                    self._cycle_rebuild_loop(), name="cycle_rebuild"),
                asyncio.create_task(
                    self._status_loop(), name="status_log"),
                asyncio.create_task(
                    self._heartbeat_loop(), name="heartbeat"),
            ]

            logger.info("=" * 60)
            logger.info("ALL SYSTEMS RUNNING — DUAL FUNNEL")
            logger.info(f"  Strategies: {len(self.config.strategies)}")
            for fc in (self.config.bbup_funnel, self.config.bbdown_funnel):
                logger.info(f"  {fc.name.upper()} funnel: full={fc.full_capacity} slots, "
                            f"WS={fc.ws_timeframe_minutes}m")
            logger.info(f"  Heartbeat: {self.config.heartbeat_interval_seconds}s")
            logger.info("=" * 60)

            # V7.1: notify systemd that startup is complete (no-op outside
            # systemd). Must come AFTER tasks are launched so Type=notify
            # only considers us "started" once we're genuinely running.
            _sd_notify("READY=1")

            await self._shutdown_event.wait()

        except Exception as e:
            logger.error(f"System error: {e}", exc_info=True)
        finally:
            await self.shutdown()

    def _signal_shutdown(self):
        logger.info("Shutdown signal received...")
        self._running = False
        self._shutdown_event.set()

    async def shutdown(self):
        """Graceful shutdown of all components."""
        logger.info("=" * 60)
        logger.info("SHUTTING DOWN")
        logger.info("=" * 60)

        self._running = False

        # Cancel periodic tasks
        for task in self._tasks:
            task.cancel()
        for task in self._tasks:
            try:
                await task
            except asyncio.CancelledError:
                pass
        self._tasks.clear()

        # Stop both scanners
        for scanner in [self.scanner_bbup, self.scanner_bbdown]:
            try:
                await scanner.stop()
            except Exception as e:
                logger.error(f"Scanner shutdown error: {e}")

        # Stop data engine
        try:
            await self.data_engine.stop()
        except Exception as e:
            logger.error(f"Data engine shutdown error: {e}")

        # Close REST session
        try:
            await self._scanner_rest.close()
        except Exception as e:
            logger.error(f"REST client shutdown error: {e}")

        # Shutdown orchestrator (final snapshots + close DB)
        try:
            self.orchestrator.shutdown()
        except Exception as e:
            logger.error(f"Orchestrator shutdown error: {e}")

        # Close scoring collector
        try:
            self.scoring.close()
        except Exception as e:
            logger.error(f"Scoring collector shutdown error: {e}")

        logger.info("Shutdown complete.")

    # ═══════════════════════════════════════════════════════════
    # PERIODIC TASKS (threaded DB operations)
    # ═══════════════════════════════════════════════════════════

    async def _snapshot_loop(self):
        """Take portfolio snapshots via thread executor."""
        loop = asyncio.get_running_loop()
        while self._running:
            try:
                await asyncio.sleep(self.config.snapshot_interval_seconds)
                if not self._running:
                    break
                await loop.run_in_executor(
                    None, self.orchestrator.take_snapshots
                )
            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error(f"Snapshot error: {e}")

    async def _cycle_rebuild_loop(self):
        """Reconstruct trade cycles via thread executor."""
        loop = asyncio.get_running_loop()
        while self._running:
            try:
                await asyncio.sleep(self.config.cycle_rebuild_interval_seconds)
                if not self._running:
                    break
                await loop.run_in_executor(
                    None, self.orchestrator.rebuild_cycles
                )
            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error(f"Cycle rebuild error: {e}")

    async def _status_loop(self):
        """Log system status every 5 minutes.

        V7: Also runs the token-overview cache orphan sweep here. The sweep
        evicts cache entries for tokens that have left both funnels' narrow
        AND full buckets. Cheap (O(N) over cache size, typically <100 entries).
        Sweep failures are isolated from the status log via nested try/except.
        """
        while self._running:
            try:
                await asyncio.sleep(300)
                if not self._running:
                    break

                # V7: orphan sweep — isolated from the status-log body below
                # so a sweep failure doesn't suppress the status output.
                evicted = 0
                try:
                    evicted = self._sweep_orphan_overviews()
                except Exception as sweep_err:
                    logger.error(f"Overview cache sweep error: {sweep_err}")
                if evicted > 0:
                    logger.info(
                        f"Overview cache sweep: evicted {evicted} orphan "
                        f"entr{'y' if evicted == 1 else 'ies'} "
                        f"({len(self._overview_cache)} remaining)"
                    )

                # BBUP funnel status
                bbup_status = self.scanner_bbup.status()
                bbup_funnel = bbup_status.get("funnel", {})
                bbup_buckets = bbup_funnel.get("buckets", {})

                # BBDOWN funnel status
                bbdown_status = self.scanner_bbdown.status()
                bbdown_funnel = bbdown_status.get("funnel", {})
                bbdown_buckets = bbdown_funnel.get("buckets", {})

                engine_stats = self.data_engine.get_stats()
                # Credits: WS bytes + data-engine REST + the scanners' REST
                # client (token list, security, overview, SOL price).
                credits_total = (engine_stats.get("total_credits", 0)
                                 + self._scanner_rest.get_stats()["total_credits_consumed"])

                logger.info(
                    f"STATUS | "
                    f"BBUP: W={bbup_buckets.get('wide', {}).get('count', 0)}/"
                    f"{bbup_buckets.get('wide', {}).get('capacity', 0)} "
                    f"N={bbup_buckets.get('narrow', {}).get('count', 0)}/"
                    f"{bbup_buckets.get('narrow', {}).get('capacity', 0)} "
                    f"F={bbup_buckets.get('full', {}).get('count', 0)}/"
                    f"{bbup_buckets.get('full', {}).get('capacity', 0)} | "
                    f"BBDOWN: W={bbdown_buckets.get('wide', {}).get('count', 0)}/"
                    f"{bbdown_buckets.get('wide', {}).get('capacity', 0)} "
                    f"N={bbdown_buckets.get('narrow', {}).get('count', 0)}/"
                    f"{bbdown_buckets.get('narrow', {}).get('capacity', 0)} "
                    f"F={bbdown_buckets.get('full', {}).get('count', 0)}/"
                    f"{bbdown_buckets.get('full', {}).get('capacity', 0)} | "
                    f"Credits: {credits_total:.0f} | "
                    f"Tokens: {engine_stats.get('tracked_tokens', 0)} | "
                    f"Cache: {len(self._overview_cache)} "
                    f"(hit={self._cache_hits} miss={self._cache_misses})"
                )

            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error(f"Status log error: {e}")

    # ═══════════════════════════════════════════════════════════
    # HEARTBEAT (V6 — event loop health monitoring)
    # ═══════════════════════════════════════════════════════════

    async def _heartbeat_loop(self):
        """
        Unconditional periodic heartbeat. If this stops appearing in logs,
        the event loop itself is frozen.

        Also checks:
        - All named tasks are still alive
        - DB flush timing for slow operations
        - Tick queue saturation
        """
        interval = self.config.heartbeat_interval_seconds
        while self._running:
            try:
                await asyncio.sleep(interval)
                if not self._running:
                    break

                now = time.monotonic()
                self._last_heartbeat = now

                # Check task health
                dead_tasks = []
                for task in self._tasks:
                    if task.done() and not task.cancelled():
                        exc = task.exception() if not task.cancelled() else None
                        dead_tasks.append((task.get_name(), exc))

                if dead_tasks:
                    for name, exc in dead_tasks:
                        logger.critical(
                            f"HEARTBEAT: Task '{name}' is DEAD! "
                            f"Exception: {exc}"
                        )
                else:
                    # Normal heartbeat
                    orch_status = self.orchestrator.get_status()
                    ws_stats = self.data_engine.get_stats().get("websocket", {})
                    ws_silent_for = ws_stats.get("seconds_since_last_message", 0.0)

                    # V7.1 WATCHDOG: if the WS feed has gone silent past
                    # threshold, force a reconnect (defense-in-depth behind
                    # the recv-timeout fix). Cooldown-guarded to avoid spam.
                    if (
                        ws_silent_for > WS_SILENCE_RECONNECT_S
                        and (now - self._ws_watchdog_last_action) > WS_WATCHDOG_COOLDOWN_S
                    ):
                        logger.critical(
                            f"WATCHDOG: WS feed silent for {ws_silent_for:.0f}s "
                            f"(> {WS_SILENCE_RECONNECT_S:.0f}s) — forcing reconnect"
                        )
                        self._ws_watchdog_last_action = now
                        # Fire-and-forget; force_reconnect closes the socket,
                        # which makes the listen loop's recv() raise + reconnect.
                        t = asyncio.create_task(self.data_engine.force_ws_reconnect())
                        self._background_tasks.add(t)
                        t.add_done_callback(self._background_tasks.discard)

                    logger.info(
                        f"HEARTBEAT | alive | "
                        f"tasks={len(self._tasks)} | "
                        f"bg_tasks={len(self._background_tasks)} | "
                        f"positions={orch_status.get('total_open_positions', 0)} | "
                        f"trades={orch_status.get('trades_executed', 0)} | "
                        f"tick_q={ws_stats.get('tick_queue_size', 0)} | "
                        f"ws_age={ws_silent_for:.0f}s | "
                        f"db_flush={self.orchestrator.db.last_flush_ms:.0f}ms | "
                        f"db_rebuild={self.orchestrator.db.last_cycle_rebuild_ms:.0f}ms"
                    )

                # V7.1: tell systemd we're alive (no-op if not under systemd).
                # Placed OUTSIDE the dead-task branch so a dead task means we
                # STOP pinging → systemd WatchdogSec kills+restarts us.
                if not dead_tasks:
                    _sd_notify("WATCHDOG=1")

            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error(f"Heartbeat error: {e}")


# ═══════════════════════════════════════════════════════════════
# CLI ENTRY POINT
# ═══════════════════════════════════════════════════════════════

def setup_logging(level: str = "INFO", log_file: str = "paper_trading.log"):
    """Configure logging for the system."""
    fmt = "%(asctime)s [%(levelname)s] %(name)s: %(message)s"
    datefmt = "%H:%M:%S"

    handlers = [
        logging.StreamHandler(sys.stdout),
        logging.FileHandler(log_file, mode='a'),
    ]

    logging.basicConfig(
        level=getattr(logging, level.upper(), logging.INFO),
        format=fmt,
        datefmt=datefmt,
        handlers=handlers,
    )

    logging.getLogger("websockets").setLevel(logging.WARNING)
    logging.getLogger("aiohttp").setLevel(logging.WARNING)


def main():
    """CLI entry point."""
    import argparse

    parser = argparse.ArgumentParser(description="Paper Trading System")
    parser.add_argument("--api-key", type=str, default="",
                        help="Data-provider API key (or set DATA_PROVIDER_API_KEY env var)")
    parser.add_argument("--db", type=str, default="paper_trading.db",
                        help="SQLite database path")
    parser.add_argument("--log-level", type=str, default="INFO",
                        choices=["DEBUG", "INFO", "WARNING", "ERROR"])
    parser.add_argument("--log-file", type=str, default="paper_trading.log")
    args = parser.parse_args()

    import os
    api_key = args.api_key or os.environ.get("DATA_PROVIDER_API_KEY", "")
    if not api_key:
        print("ERROR: data-provider API key required.")
        print("  Use --api-key YOUR_KEY or set DATA_PROVIDER_API_KEY env var")
        sys.exit(1)

    setup_logging(args.log_level, args.log_file)

    config = SystemConfig(
        data_api_key=api_key,
        db_path=args.db,
        log_level=args.log_level,
        log_file=args.log_file,
    )

    system = PaperTradingSystem(config=config)

    try:
        asyncio.run(system.run())
    except KeyboardInterrupt:
        pass


if __name__ == "__main__":
    main()
