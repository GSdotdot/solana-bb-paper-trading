"""
Token Scanner (Layer 1 — Top Level) — Dual Funnel
==================================================
Coordinates token discovery via token-list polling, funnel management,
and data engine integration.

Each funnel gets its own TokenScanner with its own list-query config and its
own FunnelManager, so each strategy family uses its own server-side pre-filter
and sets its own polling cadence from its own phase.
The BBUP funnel uses a volume pre-filter; the BBDOWN funnel uses a broad query.

Data Fetcher Protocol:
  - fetch_token_list(config): poll token list with specific config
  - fetch_holder_data(token_ca): fetch top holder concentration for Narrow scan
  - fetch_sol_price(): get current SOL/USD price for fee calculations
"""

import asyncio
import logging
from typing import Callable, Dict, List, Optional, Protocol

from paper_trading.config.params import param
from paper_trading.scanner.token_model import ScannerToken, Tier, Launchpad
from paper_trading.scanner.buckets import BucketSet
from paper_trading.scanner.blacklist import BlacklistManager
from paper_trading.scanner.funnel_manager import FunnelManager, Phase
from paper_trading.scanner.filters import (
    PreFilterGate, WideScanFilters, NarrowScanFilters,
)

logger = logging.getLogger(__name__)


# ═══════════════════════════════════════════════════════════════
# DATA FETCHER PROTOCOL
# ═══════════════════════════════════════════════════════════════

class DataFetcher(Protocol):
    """Interface for API data sources. Swap MockDataFetcher for DataProviderRESTClient."""
    async def fetch_token_list(self, config=None) -> List[dict]: ...
    async def fetch_holder_data(self, token_ca: str) -> Optional[dict]: ...
    async def fetch_sol_price(self) -> Optional[float]: ...


class MockDataFetcher:
    """Test-only data fetcher that returns synthetic token-list data.
    Default values are arbitrary illustrations, not tuned thresholds."""

    def __init__(
        self,
        default_mcap: float = 123_000,
        default_liquidity: float = 25_000,
        default_volume_24h: float = 40_000,
        default_volume_5m: float = 4_000,
        default_holder_pct: float = 0.20,
        sol_price: float = 150.0,
    ):
        self.default_mcap = default_mcap
        self.default_liquidity = default_liquidity
        self.default_volume_24h = default_volume_24h
        self.default_volume_5m = default_volume_5m
        self.default_holder_pct = default_holder_pct
        self.sol_price = sol_price
        self._mock_tokens: List[dict] = []
        self.calls_list = 0
        self.calls_holder = 0
        self.calls_sol = 0

    def add_mock_token(self, token_ca: str, symbol: str = "MOCK", **overrides):
        data = {
            "address": token_ca,
            "symbol": symbol,
            "name": f"Mock {symbol}",
            "market_cap": overrides.get("market_cap", self.default_mcap),
            "liquidity": overrides.get("liquidity", self.default_liquidity),
            "volume_24h_usd": overrides.get("volume_24h_usd", self.default_volume_24h),
            "volume_5m_usd": overrides.get("volume_5m_usd", self.default_volume_5m),
            "volume_1h_usd": overrides.get("volume_1h_usd", 12_345),
            "price": overrides.get("price", 0.001),
            "holder": overrides.get("holder", 500),
            "unique_wallet_24h": overrides.get("unique_wallet_24h", 200),
            "trade_1h_count": overrides.get("trade_1h_count", 100),
            "last_trade_unix_time": overrides.get("last_trade_unix_time", 1700000000),
            "recent_listing_time": overrides.get("recent_listing_time", None),
            "price_change_1h_percent": overrides.get("price_change_1h_percent", 0.0),
            "price_change_24h_percent": overrides.get("price_change_24h_percent", 0.0),
            "volume_1h_change_percent": overrides.get("volume_1h_change_percent", 0.0),
            "volume_5m_change_percent": overrides.get("volume_5m_change_percent", 0.0),
        }
        self._mock_tokens.append(data)

    def clear_mock_tokens(self):
        self._mock_tokens.clear()

    async def fetch_token_list(self, config=None) -> List[dict]:
        self.calls_list += 1
        return list(self._mock_tokens)

    async def fetch_holder_data(self, token_ca: str) -> Optional[dict]:
        self.calls_holder += 1
        return {
            "top10_holder_pct": self.default_holder_pct,
            "top10_user_pct": self.default_holder_pct,
            "creator_pct": 0.001,
            "freeze_authority": None,
            "transfer_fee_enabled": None,
            "pre_market_holders": 0,
            "creation_time": None,
        }

    async def fetch_sol_price(self) -> Optional[float]:
        self.calls_sol += 1
        return self.sol_price


# ═══════════════════════════════════════════════════════════════
# TOKEN SCANNER
# ═══════════════════════════════════════════════════════════════

class TokenScanner:
    """
    Top-level scanner coordinator for one funnel.

    Each funnel gets its own TokenScanner instance with its own
    list_config for strategy-specific API filtering.

    Lifecycle:
      1. start() → launches polling loops
      2. list poll loop discovers tokens → routes through FunnelManager
      3. Narrow scan loop fetches holder data → evaluates
      4. SOL price loop updates fee calculation reference
      5. TTL loop cleans up expired tokens
      6. stop() → cancels all tasks
    """

    def __init__(
        self,
        data_fetcher: DataFetcher,
        on_full_promotion: Callable[[str, str], None],
        on_full_removal: Callable[[str, str], None],
        on_narrow_promotion: Callable[[str, str], None] = None,
        # Funnel identity
        funnel_name: str = "default",
        # Token-list query config (per-funnel)
        list_config=None,
        # Bucket config
        wide_capacity: int = param("funnels.defaults.wide_capacity"),
        narrow_capacity: int = param("funnels.defaults.narrow_capacity"),
        full_capacity: int = param("funnels.defaults.full_capacity"),
        wide_ttl: int = param("funnels.buckets.wide_ttl_seconds"),
        narrow_ttl: int = param("funnels.buckets.narrow_ttl_seconds"),
        full_ttl: int = param("funnels.buckets.full_ttl_seconds"),
        # Filter config
        min_mcap: float = param("scanner.wide.min_mcap"),
        max_mcap: float = param("scanner.wide.max_mcap"),
        min_liquidity_ratio: float = param("scanner.wide.min_liquidity_ratio"),
        holder_tolerance_pct: float = param("scanner.narrow.holder_tolerance_pct"),
        # Polling config
        list_poll_interval_minutes: float = param("funnels.defaults.list_poll_interval_minutes"),
        narrow_scan_interval_minutes: float = param("funnels.defaults.narrow_scan_interval_minutes"),
        sol_poll_interval_minutes: float = param("system.sol_poll_interval_minutes"),
        cooldown_hours: int = param("scanner.blacklist_cooldown_hours"),
    ):
        self.data_fetcher = data_fetcher
        self.funnel_name = funnel_name
        self.list_config = list_config  # V6: per-funnel API query config

        buckets = BucketSet(
            wide_capacity=wide_capacity, narrow_capacity=narrow_capacity,
            full_capacity=full_capacity, wide_ttl=wide_ttl,
            narrow_ttl=narrow_ttl, full_ttl=full_ttl,
        )
        blacklist = BlacklistManager(cooldown_hours=cooldown_hours)
        pre_filter = PreFilterGate(pumpfun_only=True)
        wide_filters = WideScanFilters(
            min_mcap=min_mcap, max_mcap=max_mcap,
            min_liquidity_ratio=min_liquidity_ratio,
        )
        narrow_filters = NarrowScanFilters(
            holder_tolerance_pct=holder_tolerance_pct,
        )

        self.funnel = FunnelManager(
            buckets=buckets, blacklist=blacklist, pre_filter=pre_filter,
            wide_filters=wide_filters, narrow_filters=narrow_filters,
            on_full_promotion=on_full_promotion,
            on_full_removal=on_full_removal,
            on_narrow_promotion=on_narrow_promotion,
        )

        self.list_poll_interval = list_poll_interval_minutes
        self.narrow_scan_interval = narrow_scan_interval_minutes
        self.sol_poll_interval = sol_poll_interval_minutes

        self._running = False
        self._scan_tasks: List[asyncio.Task] = []

    # ── Token-List Processing ────────────────────────────────

    async def run_list_poll(self):
        """
        Fetch token list using this funnel's config and process each token.
        """
        list_items = await self.data_fetcher.fetch_token_list(
            config=self.list_config,
        )
        if not list_items:
            return

        new_count = promoted = parked = rejected = 0
        for item in list_items:
            address = item.get("address", "")
            if not address:
                continue

            # In-memory PumpFun check before creating ScannerToken
            if not address.endswith("pump"):
                continue

            symbol = item.get("symbol", "")
            token = ScannerToken(token_ca=address, ticker=symbol)
            token.update_from_list(item)

            result = self.funnel.process_list_token(token)
            if result.passed:
                if result.failed_filter == "borderline":
                    parked += 1
                elif result.failed_filter == "":
                    promoted += 1
                new_count += 1
            else:
                rejected += 1

        logger.info(
            f"[{self.funnel_name}] list poll: {len(list_items)} items, "
            f"{new_count} processed, {promoted} to narrow, "
            f"{parked} parked, {rejected} rejected"
        )

    # ── Narrow Scan ──────────────────────────────────────────

    async def run_narrow_scan(self):
        """Fetch security data for tokens in Narrow that haven't been checked yet."""
        pending = self.funnel.get_narrow_tokens_needing_scan()
        if not pending:
            return

        for ca in pending:
            token = self.funnel.buckets.narrow.get(ca)
            if token is None:
                continue

            sec_data = await self.data_fetcher.fetch_holder_data(ca)
            if sec_data is not None:
                token.top10_holder_pct = sec_data.get("top10_holder_pct")
                token.top10_user_pct = sec_data.get("top10_user_pct")
                token.creator_pct = sec_data.get("creator_pct")
                token.freeze_authority = sec_data.get("freeze_authority")
                token.transfer_fee_enabled = sec_data.get("transfer_fee_enabled")
                token.pre_market_holder_count = sec_data.get("pre_market_holders", 0)

            self.funnel.evaluate_narrow_token(ca)

    # ── SOL Price ────────────────────────────────────────────

    async def run_sol_price_update(self):
        """Update SOL price for fee generation calculations."""
        price = await self.data_fetcher.fetch_sol_price()
        if price is not None and price > 0:
            self.funnel.wide_filters.set_sol_price(price)
            logger.debug(f"SOL price updated: ${price:.2f}")

    # ── TTL Cleanup ──────────────────────────────────────────

    def run_ttl_cleanup(self):
        self.funnel.enforce_ttls()
        self.funnel.blacklist.cleanup_expired()

    # ── Manual token injection (for testing) ─────────────────

    def on_new_token(self, token_ca: str, ticker: str = "",
                     launchpad: Launchpad = Launchpad.UNKNOWN,
                     list_data: Optional[dict] = None) -> bool:
        token = ScannerToken(token_ca=token_ca, ticker=ticker, launchpad=launchpad)
        if list_data:
            token.update_from_list(list_data)
        result = self.funnel.process_list_token(token)
        return result.passed

    # ── Async Lifecycle ──────────────────────────────────────

    async def start(self):
        """Start all polling loops for this funnel."""
        self._running = True
        # Initial SOL price fetch
        await self.run_sol_price_update()
        self._scan_tasks = [
            asyncio.create_task(
                self._list_poll_loop(),
                name=f"{self.funnel_name}_list_poll",
            ),
            asyncio.create_task(
                self._narrow_scan_loop(),
                name=f"{self.funnel_name}_narrow_scan",
            ),
            asyncio.create_task(
                self._sol_price_loop(),
                name=f"{self.funnel_name}_sol_price",
            ),
            asyncio.create_task(
                self._ttl_loop(),
                name=f"{self.funnel_name}_ttl",
            ),
        ]
        logger.info(f"[{self.funnel_name}] Token scanner started")

    async def stop(self):
        """Stop all polling loops."""
        self._running = False
        for task in self._scan_tasks:
            task.cancel()
            try:
                await task
            except asyncio.CancelledError:
                pass
        self._scan_tasks.clear()
        logger.info(f"[{self.funnel_name}] Token scanner stopped")

    async def _list_poll_loop(self):
        first_run = True
        while self._running:
            try:
                if first_run:
                    first_run = False
                else:
                    intervals = self.funnel.get_recommended_intervals()
                    await asyncio.sleep(intervals["list_poll_minutes"] * 60)
                    if not self._running:
                        break
                await self.run_list_poll()
            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error(f"[{self.funnel_name}] list poll error: {e}")

    async def _narrow_scan_loop(self):
        first_run = True
        while self._running:
            try:
                if first_run:
                    first_run = False
                else:
                    intervals = self.funnel.get_recommended_intervals()
                    await asyncio.sleep(intervals["narrow_scan_minutes"] * 60)
                    if not self._running:
                        break
                await self.run_narrow_scan()
            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error(f"[{self.funnel_name}] Narrow scan error: {e}")

    async def _sol_price_loop(self):
        while self._running:
            try:
                await asyncio.sleep(self.sol_poll_interval * 60)
                if not self._running:
                    break
                await self.run_sol_price_update()
            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error(f"[{self.funnel_name}] SOL price poll error: {e}")

    async def _ttl_loop(self):
        while self._running:
            try:
                await asyncio.sleep(60)
                if not self._running:
                    break
                self.run_ttl_cleanup()
            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error(f"[{self.funnel_name}] TTL cleanup error: {e}")

    # ── Status ───────────────────────────────────────────────

    def status(self) -> dict:
        return {
            "funnel_name": self.funnel_name,
            "running": self._running,
            "funnel": self.funnel.status(),
        }
