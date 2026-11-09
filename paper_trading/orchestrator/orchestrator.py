"""
Orchestrator (Layer 6) — V6 Dual Funnel
=========================================
Central coordinator. Owns strategies, portfolios, executor, DB.

V6 CHANGES:
  - Strategies are tagged with their funnel ("bbup" or "bbdown")
  - on_token_discovered/removed accept a funnel_name to route
    tokens only to strategies belonging to that funnel
  - DB operations (flush_writes, take_snapshots, rebuild_cycles)
    are wrapped in run_in_executor() by main.py — the orchestrator
    methods remain sync but are called from threads
  - Peak concurrent position count tracked per strategy
"""

import logging
import sys
from datetime import datetime, timezone
from typing import Dict, List, Optional

from paper_trading.config.params import param
from paper_trading.core.models import Candle, SignalType, Timeframe, TickData
from paper_trading.strategies.bbdown_strategy import BBDOWNStrategy
from paper_trading.strategies.bbup_strategy import BBUPStrategy
from paper_trading.execution.executor import (
    PaperExecutor, PortfolioManager, OpenPosition,
)
from paper_trading.execution.dispatcher import SignalDispatcher
from paper_trading.recording.database import DatabaseManager
from paper_trading.recording.metrics import MetricsCalculator
from paper_trading.orchestrator.config import SystemConfig, StrategyConfig

logger = logging.getLogger(__name__)

StrategyInstance = BBDOWNStrategy | BBUPStrategy

# A1 / Task 2.5: a restored position whose last_updated_timestamp is older than
# this is flagged stale (WS likely dropped for hours / token possibly rugged).
# The horizon is a "dropped for hours" floor that won't false-trip on a
# normal restart. Paper: log + flag only.
RESTORE_STALE_AGE_SECONDS = param("system.restore_stale_age_seconds")


class Orchestrator:
    """
    Central coordinator. Owns all components, routes all events.

    V6: Strategies are grouped by funnel. Token discovery/removal
    is routed only to the strategies belonging to the same funnel.
    """

    def __init__(self, config: SystemConfig = None):
        self.config = config or SystemConfig()
        self.running = False

        # Components
        self.strategies: Dict[str, StrategyInstance] = {}
        self.portfolios: Dict[str, PortfolioManager] = {}
        self.strategy_configs: Dict[str, StrategyConfig] = {}
        self.executor: Optional[PaperExecutor] = None
        self.dispatcher: Optional[SignalDispatcher] = None
        self.db: Optional[DatabaseManager] = None
        self.metrics: Optional[MetricsCalculator] = None

        # Routing: timeframe → list of strategy names
        self._timeframe_routes: Dict[int, List[str]] = {}

        # V6: Funnel → strategy names
        self._funnel_routes: Dict[str, List[str]] = {}

        # Stats
        self.candles_processed = 0
        self.trades_executed = 0
        self.startup_time: Optional[datetime] = None

        # A1: (strategy_name, token_ca, ticker) of positions restored at
        # startup, awaiting WS re-subscribe + backfill (done in main.run).
        self._restored_tokens: List[tuple] = []

    # ════════════════════════════════════════════════════════════
    # STARTUP
    # ════════════════════════════════════════════════════════════

    def startup(self):
        """Initialize all components in dependency order."""
        self.startup_time = datetime.now(timezone.utc)

        self._setup_logging()
        logger.info("=" * 60)
        logger.info("PAPER TRADING SYSTEM STARTING — V6 DUAL FUNNEL")
        logger.info(f"Mode: {self.config.mode}")
        logger.info(f"Strategies: {len(self.config.strategies)}")
        logger.info("=" * 60)

        # Database
        self.db = DatabaseManager(self.config.db_path)
        self.db.open()
        self.db.log_event("system_startup", self.startup_time,
                          details=f"V6 dual funnel, strategies={len(self.config.strategies)}")
        logger.info(f"Database opened: {self.config.db_path}")

        # Executor
        if self.config.mode == "paper":
            self.executor = PaperExecutor()
        else:
            raise ValueError(f"Mode '{self.config.mode}' not yet implemented")

        # Strategies + Portfolios
        self._create_strategies()
        logger.info(f"Created {len(self.strategies)} strategy instances")

        # Restore portfolio state from DB (cash + counters from snapshots)
        self._restore_portfolios()

        # A1: rebuild open positions from the durable open_positions table
        # (repopulates PortfolioManager.positions + reconstructs TokenState).
        # Must run AFTER _restore_portfolios (which sets cash) and AFTER
        # _create_strategies (token_states must exist to attach to).
        self._restore_open_positions()

        # A1: attach the DB so subsequent LIVE mutations persist synchronously.
        # Done AFTER restore so the direct repopulation above does not re-write
        # rows (restore reads; it must not write).
        for portfolio in self.portfolios.values():
            portfolio.attach_db(self.db)

        # Dispatcher
        self.dispatcher = SignalDispatcher(self.executor)
        for name, strategy in self.strategies.items():
            cfg = self.strategy_configs[name]
            self.dispatcher.register(
                strategy, self.portfolios[name],
                cfg.position_size, cfg.max_positions,
            )
        logger.info("Dispatcher wired")

        # Metrics
        self.metrics = MetricsCalculator(self.db)

        self.running = True
        logger.info("System ready — waiting for data")

    def _create_strategies(self):
        """Instantiate all strategy instances with their portfolios.

        V7: passes per-instance filter / cooldown overrides from StrategyConfig
        through to the strategy constructors. The ablation grid relies on this:
        each of the 9 instances picks up its specific regime here.
        """
        for cfg in self.config.strategies:
            if cfg.strategy_type == "BBDOWN":
                strategy = BBDOWNStrategy(
                    name=cfg.name,
                    sma_period=cfg.sma_period,
                    bb_multiplier=cfg.bb_multiplier,
                    position_size=cfg.position_size,
                    max_positions=cfg.max_positions,
                    max_market_cap=cfg.max_market_cap,
                    # ── V7 per-instance overrides ─────────────────
                    min_vol_window_usd=cfg.min_vol_window_usd,
                    max_market_cap_entry=cfg.max_market_cap_entry,
                    bbw_filter_enabled=cfg.bbw_filter_enabled,
                    bbw_min_pct=cfg.bbw_min_pct,
                    bbw_max_pct=cfg.bbw_max_pct,
                    bearish_exit_cooldown_minutes=cfg.bearish_exit_cooldown_minutes,
                )
            elif cfg.strategy_type == "BBUP":
                strategy = BBUPStrategy(
                    name=cfg.name,
                    sma_period=cfg.sma_period,
                    bb_multiplier=cfg.bb_multiplier,
                    position_size=cfg.position_size,
                    max_positions=cfg.max_positions,
                    max_market_cap=cfg.max_market_cap,
                    min_market_cap=cfg.min_market_cap,
                    # ── V7 per-instance overrides ─────────────────
                    sma_sl_cooldown_minutes=cfg.sma_sl_cooldown_minutes,
                    body_filter_enabled=cfg.body_filter_enabled,
                    body_ratio_min=cfg.body_ratio_min,
                    require_bullish_trigger=cfg.require_bullish_trigger,
                )
            else:
                raise ValueError(f"Unknown strategy type: {cfg.strategy_type}")

            portfolio = PortfolioManager(cfg.name, cfg.initial_balance)

            self.strategies[cfg.name] = strategy
            self.portfolios[cfg.name] = portfolio
            self.strategy_configs[cfg.name] = cfg

            # Timeframe routing
            tf = cfg.timeframe_minutes
            if tf not in self._timeframe_routes:
                self._timeframe_routes[tf] = []
            self._timeframe_routes[tf].append(cfg.name)

            # V6: Funnel routing
            funnel = cfg.funnel
            if funnel not in self._funnel_routes:
                self._funnel_routes[funnel] = []
            self._funnel_routes[funnel].append(cfg.name)

            logger.info(
                f"  {cfg.name}: {cfg.strategy_type} {cfg.timeframe_minutes}m "
                f"SMA={cfg.sma_period} BB={cfg.bb_multiplier} "
                f"max_pos={cfg.max_positions} funnel={funnel} "
                f"${cfg.initial_balance} portfolio"
            )
            # V7: second info line — explicit ablation regime per instance.
            # Guards against label-vs-config drift (the instance label names the
            # regime; this log confirms the actual values match).
            if cfg.strategy_type == "BBDOWN":
                logger.info(
                    f"    [regime] vol_window>=${cfg.min_vol_window_usd:,.0f}  "
                    f"mc_entry<=${cfg.max_market_cap_entry:,.0f}  "
                    f"bbw_filter={'ON' if cfg.bbw_filter_enabled else 'OFF'}"
                    f"{f' [{cfg.bbw_min_pct:.0f}-{cfg.bbw_max_pct:.0f}%]' if cfg.bbw_filter_enabled else ''}  "
                    f"bearish_cd={cfg.bearish_exit_cooldown_minutes:.0f}min"
                )
            elif cfg.strategy_type == "BBUP":
                logger.info(
                    f"    [regime] body_filter={'ON' if cfg.body_filter_enabled else 'OFF'}"
                    f"{f' [ratio>={cfg.body_ratio_min:.2f}, bullish_req={cfg.require_bullish_trigger}]' if cfg.body_filter_enabled else ''}  "
                    f"sma_sl_cd={cfg.sma_sl_cooldown_minutes:.0f}min"
                )

    def _restore_portfolios(self):
        """Restore portfolio state from DB snapshots (H3 fix)."""
        try:
            snapshots = self.db.get_latest_snapshots()
            if not snapshots:
                logger.info("No prior snapshots found — fresh start")
                return

            restored = 0
            for name, portfolio in self.portfolios.items():
                snap = snapshots.get(name)
                if snap is None:
                    continue
                portfolio.restore_from_snapshot(snap)
                restored += 1
                logger.info(
                    f"  {name}: restored cash=${portfolio.cash_balance:.2f} "
                    f"trades={portfolio.total_trades} cycles={portfolio.total_cycles} "
                    f"peak=${portfolio.peak_value:.2f}"
                )

            logger.info(
                f"Portfolios restored: {restored}/{len(self.portfolios)} "
                f"from prior snapshots"
            )
        except Exception as e:
            logger.warning(f"Portfolio restore failed (starting fresh): {e}")

    # ════════════════════════════════════════════════════════════
    # A1 — OPEN-POSITION RESTORE (Issue B fix)
    # ════════════════════════════════════════════════════════════

    def _restore_open_positions(self):
        """Rebuild open positions from the durable open_positions table.

        Repopulates PortfolioManager.positions (sizing is authoritative from
        the DB) and reconstructs the per-token TokenState (has_position + the
        sma_sl_pending / sma_partial_done exit flags). Indicator history is gone
        on restart, so each restored token is GATED (awaiting_backfill=True):
        held but inert until REST backfill reseeds it. The WS re-subscribe +
        backfill itself happens in PaperTradingSystem.run (needs the started
        data engine) via get_restored_tokens() / seed_restored_token().
        """
        self._restored_tokens = []
        try:
            rows = self.db.get_open_positions()
        except Exception as e:
            logger.warning(f"A1 open-position restore skipped (read failed): {e}")
            return

        if not rows:
            logger.info("A1: no open positions to restore")
            return

        now = datetime.now(timezone.utc)
        restored = 0
        for row in rows:
            name = row["strategy_name"]
            ca = row["token_ca"]
            portfolio = self.portfolios.get(name)
            strategy = self.strategies.get(name)
            if portfolio is None or strategy is None:
                logger.warning(
                    f"A1: open position for unknown strategy '{name}' "
                    f"(token {ca[:12]}...) — roster changed? skipping (row kept)"
                )
                continue

            ticker = self.db.get_ticker_for(name, ca) or ca[:8]
            try:
                entry_ts = datetime.fromisoformat(row["entry_timestamp"])
            except (ValueError, TypeError):
                entry_ts = now

            # ── Rebuild the portfolio position ──────────────────
            pos = OpenPosition(
                token_ca=ca,
                token_ticker=ticker,
                strategy_name=name,
                entry_price=row["entry_price"],
                entry_fill_price=row["entry_price"],
                token_quantity=row["tokens_held"],
                invested_usd=row["invested_usd"],
                entry_timestamp=entry_ts,
                entry_market_cap=row["entry_market_cap"],
            )
            pos.tokens_remaining = row["tokens_held"]
            pos.current_price = row["entry_price"]   # until first live tick
            portfolio.positions[ca] = pos

            # ── Rebuild the TokenState (history empty → gated) ──
            strategy.add_token(ca, ticker)
            state = strategy.token_states[ca]
            state.has_position = True
            state.entry_price = row["entry_price"]
            state.entry_timestamp = entry_ts
            state.entry_market_cap = row["entry_market_cap"]
            state.entry_candle_idx = 0
            state.candles_since_entry = 0
            state.position_tokens = row["tokens_held"]
            state.initial_tokens = row["tokens_held"]
            state.cycle_invested = row["invested_usd"]
            state.trade_high_price = row["entry_price"]
            state.trade_low_price = row["entry_price"]
            state.sma_partial_done = bool(row["sma_partial_done"])
            state.sma_sl_pending = bool(row["sma_sl_pending"])
            state.awaiting_backfill = True

            # ── Task 2.5: stale flag (log + mark only in paper) ─
            try:
                last_upd = datetime.fromisoformat(row["last_updated_timestamp"])
                age_s = (now - last_upd).total_seconds()
            except (ValueError, TypeError):
                age_s = 0.0
            if age_s > RESTORE_STALE_AGE_SECONDS:
                state.restored_stale = True
                logger.warning(
                    f"A1 STALE on restore: [{name}] {ticker} ({ca[:12]}...) "
                    f"last updated {age_s/3600:.1f}h ago "
                    f"(> {RESTORE_STALE_AGE_SECONDS/3600:.1f}h) — flagged for "
                    f"review (paper: no auto-exit; live build wires the exit)"
                )
                self.db.log_event(
                    "open_position_stale", now, name, ca,
                    f"age={age_s/3600:.1f}h > {RESTORE_STALE_AGE_SECONDS/3600:.1f}h",
                )

            self._restored_tokens.append((name, ca, ticker))
            restored += 1
            logger.info(
                f"A1 restored: [{name}] {ticker} ({ca[:12]}...) "
                f"tokens={row['tokens_held']:.4f} invested=${row['invested_usd']:.2f} "
                f"sma_sl_pending={bool(row['sma_sl_pending'])} "
                f"sma_partial_done={bool(row['sma_partial_done'])}"
                f"{' [STALE]' if state.restored_stale else ''}"
            )

        logger.info(
            f"A1: restored {restored} open position(s) — held + GATED until WS "
            f"re-subscribe + backfill completes"
        )

    def get_restored_tokens(self) -> List[tuple]:
        """A1: (strategy_name, token_ca, ticker) tuples restored at startup,
        for PaperTradingSystem.run to WS-re-subscribe + backfill."""
        return list(self._restored_tokens)

    def seed_restored_token(self, strategy_name: str, token_ca: str,
                            history_5m: List[Candle], history_1m: List[Candle]):
        """A1 (Task 2.4): seed REST-backfilled history into a restored token's
        TokenState and, once >= sma_period candles exist with valid indicators,
        clear the backfill gate so the position resumes trading — including
        firing a persisted SMA-SL exit on the next candle."""
        strategy = self.strategies.get(strategy_name)
        if strategy is None:
            return
        state = strategy.token_states.get(token_ca)
        if state is None:
            return

        cfg = self.strategy_configs.get(strategy_name)
        history = history_1m if (cfg and cfg.timeframe_minutes == 1) else history_5m
        if history:
            strategy.seed_token_history(token_ca, history)

        if state.candle_count >= strategy.sma_period and state.is_ready:
            state.awaiting_backfill = False
            logger.info(
                f"A1 backfill complete: [{strategy_name}] {state.token_ticker} "
                f"({token_ca[:12]}...) {state.candle_count} candles — trading resumes"
            )
        else:
            logger.warning(
                f"A1 backfill INSUFFICIENT: [{strategy_name}] "
                f"{state.token_ticker} ({token_ca[:12]}...) "
                f"{state.candle_count} candles (< SMA {strategy.sma_period}) — "
                f"position HELD, still gated (no signals)"
            )

    def _setup_logging(self):
        root = logging.getLogger()
        root.setLevel(getattr(logging, self.config.log_level, logging.INFO))

        if not root.handlers:
            ch = logging.StreamHandler(sys.stdout)
            ch.setLevel(logging.INFO)
            fmt = logging.Formatter(
                "%(asctime)s [%(levelname)s] %(name)s: %(message)s",
                datefmt="%H:%M:%S",
            )
            ch.setFormatter(fmt)
            root.addHandler(ch)

        if self.config.log_file:
            fh = logging.FileHandler(self.config.log_file)
            fh.setLevel(logging.DEBUG)
            fmt = logging.Formatter(
                "%(asctime)s [%(levelname)s] %(name)s: %(message)s",
            )
            fh.setFormatter(fmt)
            root.addHandler(fh)

    # ════════════════════════════════════════════════════════════
    # EVENT CALLBACKS
    # ════════════════════════════════════════════════════════════

    def on_candle_received(self, candle: Candle):
        """Route candle to strategies matching its timeframe."""
        if not self.running:
            return

        tf_minutes = 5 if candle.timeframe == Timeframe.FIVE_MIN else 1
        strategy_names = self._timeframe_routes.get(tf_minutes, [])

        for name in strategy_names:
            strategy = self.strategies[name]
            state = strategy.token_states.get(candle.token_ca)
            if state is None:
                continue

            # A1: a restored position is held but INERT until backfill
            # completes — do not feed it candles or let it emit signals.
            if state.awaiting_backfill:
                continue

            # A1: capture sma_sl_pending so a candle-driven change (close < SMA
            # → pending) that does NOT mutate the portfolio can still be
            # persisted below. None when not in a position.
            prev_sl_pending = state.sma_sl_pending if state.has_position else None

            signals = strategy.on_candle(candle)
            trades = self.dispatcher.process_signals(signals)

            for trade in trades:
                self.db.log_trade(trade)
                self.trades_executed += 1

                if trade.signal_type == "entry":
                    self.db.log_event(
                        "trade_entry", candle.timestamp,
                        name, candle.token_ca,
                        f"BUY {trade.token_quantity:.4f} @ ${trade.execution_price:.8f}",
                    )
                elif trade.signal_type.startswith("exit"):
                    self.db.log_event(
                        "trade_exit", candle.timestamp,
                        name, candle.token_ca,
                        f"{trade.signal_type}: net=${trade.net_usd:.2f}",
                    )

            # A1: persist an sma_sl_pending flip that happened without a cash
            # mutation. Only when the position still exists — a full exit in
            # process_signals already deleted the row (and persisted) via
            # close_position, in which case has_position is now False.
            if (state.has_position and prev_sl_pending is not None
                    and state.sma_sl_pending != prev_sl_pending):
                self.db.update_open_position_flags(
                    name, candle.token_ca, datetime.now(timezone.utc),
                    sma_sl_pending=int(state.sma_sl_pending),
                    sma_partial_done=int(state.sma_partial_done),
                )

        self.candles_processed += 1
        self.db.flush_writes()

    def on_tick_received(self, tick: TickData):
        """Route tick to strategies matching its timeframe."""
        if not self.running:
            return

        if tick.timeframe is not None:
            tf_minutes = 5 if tick.timeframe == Timeframe.FIVE_MIN else 1
            strategy_names = self._timeframe_routes.get(tf_minutes, [])
        else:
            strategy_names = list(self.strategies.keys())

        for name in strategy_names:
            strategy = self.strategies[name]
            state = strategy.token_states.get(tick.token_ca)
            if state is None:
                continue

            # A1: a restored position is held but INERT until backfill
            # completes — no entry/exit signals on stale indicator state.
            if state.awaiting_backfill:
                continue

            signals = strategy.on_tick(tick)

            # Update position price for portfolio valuation
            portfolio = self.portfolios[name]
            pos = portfolio.positions.get(tick.token_ca)
            if pos is not None:
                pos.current_price = tick.price

            trades = self.dispatcher.process_signals(signals)

            for trade in trades:
                self.db.log_trade(trade)
                self.trades_executed += 1

                if trade.signal_type == "entry":
                    self.db.log_event(
                        "trade_entry", tick.timestamp,
                        name, tick.token_ca,
                        f"BUY {trade.token_quantity:.4f} @ ${trade.execution_price:.8f} [TICK]",
                    )
                elif trade.signal_type.startswith("exit"):
                    self.db.log_event(
                        "trade_exit", tick.timestamp,
                        name, tick.token_ca,
                        f"{trade.signal_type}: net=${trade.net_usd:.2f} [TICK]",
                    )

        self.db.flush_writes()

    def on_token_discovered(
        self, token_ca: str, ticker: str,
        history_5m: List[Candle] = None,
        history_1m: List[Candle] = None,
        funnel_name: str = "",
    ):
        """
        Token entered Full Focus. Add to strategies belonging to the
        specified funnel only (V6: per-funnel routing).
        """
        if not self.running:
            return

        # V6: Route to funnel-specific strategies only
        if funnel_name and funnel_name in self._funnel_routes:
            target_names = self._funnel_routes[funnel_name]
        else:
            target_names = list(self.strategies.keys())

        for name in target_names:
            strategy = self.strategies[name]
            cfg = self.strategy_configs[name]
            strategy.add_token(token_ca, ticker)

            if cfg.timeframe_minutes == 5 and history_5m:
                strategy.seed_token_history(token_ca, history_5m)
            elif cfg.timeframe_minutes == 1 and history_1m:
                strategy.seed_token_history(token_ca, history_1m)

        self.db.log_event(
            "token_discovered", datetime.now(timezone.utc),
            token_ca=token_ca,
            details=f"ticker={ticker}, funnel={funnel_name}, "
                    f"added to {len(target_names)} strategies",
        )
        logger.info(
            f"[{funnel_name}] Token discovered: {ticker} ({token_ca[:12]}...) "
            f"→ {len(target_names)} strategies"
        )

    def on_token_removed(self, token_ca: str, reason: str = "expired",
                         funnel_name: str = ""):
        """
        Token leaving Full Focus. Remove from funnel-specific strategies.
        """
        if not self.running:
            return

        if funnel_name and funnel_name in self._funnel_routes:
            target_names = self._funnel_routes[funnel_name]
        else:
            target_names = list(self.strategies.keys())

        removed_from = 0
        flagged = 0
        for name in target_names:
            strategy = self.strategies[name]
            if strategy.remove_token(token_ca):
                removed_from += 1
            else:
                flagged += 1

        self.db.log_event(
            "token_removed", datetime.now(timezone.utc),
            token_ca=token_ca,
            details=f"reason={reason}, funnel={funnel_name}, "
                    f"removed={removed_from}, flagged={flagged}",
        )
        if flagged:
            logger.info(
                f"[{funnel_name}] Token {token_ca[:12]}... flagged for removal "
                f"({flagged} strategies have open positions)"
            )

    # ════════════════════════════════════════════════════════════
    # PERIODIC TASKS
    # ════════════════════════════════════════════════════════════

    def take_snapshots(self):
        """Capture portfolio state for all strategies."""
        now = datetime.now(timezone.utc)
        for name, portfolio in self.portfolios.items():
            self.db.log_snapshot(portfolio, now)

    def rebuild_cycles(self):
        """Reconstruct cycle records from trades."""
        total = self.db.reconstruct_cycles()

    # ════════════════════════════════════════════════════════════
    # STATUS & REPORTING
    # ════════════════════════════════════════════════════════════

    def get_status(self) -> dict:
        now = datetime.now(timezone.utc)
        uptime = (now - self.startup_time).total_seconds() if self.startup_time else 0

        strategy_summaries = []
        for name, strategy in self.strategies.items():
            portfolio = self.portfolios[name]
            summary = strategy.get_summary()
            summary.update({
                "cash": round(portfolio.cash_balance, 2),
                "total_value": round(portfolio.total_value, 2),
                "total_trades": portfolio.total_trades,
                "total_cycles": portfolio.total_cycles,
                "funnel": self.strategy_configs[name].funnel,
            })
            strategy_summaries.append(summary)

        total_positions = sum(s.get("open_positions", 0) for s in strategy_summaries)
        total_value = sum(s.get("total_value", 0) for s in strategy_summaries)

        return {
            "running": self.running,
            "mode": self.config.mode,
            "uptime_seconds": int(uptime),
            "candles_processed": self.candles_processed,
            "trades_executed": self.trades_executed,
            "total_open_positions": total_positions,
            "total_portfolio_value": round(total_value, 2),
            "initial_capital": sum(c.initial_balance for c in self.config.strategies),
            "strategies": strategy_summaries,
            "db_last_flush_ms": self.db.last_flush_ms if self.db else 0,
            "db_last_rebuild_ms": self.db.last_cycle_rebuild_ms if self.db else 0,
        }

    def print_status(self):
        status = self.get_status()
        logger.info("=" * 60)
        logger.info("SYSTEM STATUS")
        logger.info(f"  Uptime: {status['uptime_seconds']}s")
        logger.info(f"  Candles: {status['candles_processed']}")
        logger.info(f"  Trades: {status['trades_executed']}")
        logger.info(f"  Open positions: {status['total_open_positions']}")
        logger.info(f"  Portfolio: ${status['total_portfolio_value']:,.2f} "
                     f"/ ${status['initial_capital']:,.2f}")
        logger.info("-" * 60)
        for s in status["strategies"]:
            logger.info(
                f"  [{s.get('funnel','')}] {s['name']}: "
                f"{s.get('open_positions', 0)} pos (peak {s.get('peak_concurrent_positions', 0)}), "
                f"${s.get('cash', 0):.2f} cash, {s.get('total_cycles', 0)} cycles, "
                f"taken={s.get('entries_taken', 0)} skipped={s.get('entries_skipped', 0)}"
            )
        logger.info("=" * 60)

    # ════════════════════════════════════════════════════════════
    # SHUTDOWN
    # ════════════════════════════════════════════════════════════

    def shutdown(self):
        if not self.running:
            return

        logger.info("=" * 60)
        logger.info("SHUTDOWN INITIATED")
        logger.info("=" * 60)

        self.running = False
        now = datetime.now(timezone.utc)

        self.take_snapshots()
        logger.info("Final snapshots taken")

        self.rebuild_cycles()
        logger.info("Final cycle rebuild complete")

        self.print_status()

        self.db.log_event("system_shutdown", now,
                          details=f"candles={self.candles_processed}, trades={self.trades_executed}")

        self.db.close()
        logger.info("Database closed. Shutdown complete.")
