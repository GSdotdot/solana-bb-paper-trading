"""
Data Provider REST Client (Layer 2) — Dual Funnel
==================================================
Async HTTP client (aiohttp) for all REST operations against the market-data
provider. The provider's identity, base URL, endpoint paths, response field
names and per-call credit costs are redacted: they are loaded from
config (`data_provider.*`) and the environment (`DATA_PROVIDER_REST_URL`,
`DATA_PROVIDER_API_KEY`).

fetch_token_list() accepts a ListQueryConfig so each funnel can run its own
server-side query (BBUP = volume pre-filter, BBDOWN = broad). The provider
caps how many server-side filters one request may carry; the client tries
the richer query first and automatically falls back to the capped query.

Endpoint roles (each call costs API credits; costs tracked per call):
  1. token_list     — discovery polling                (WIDE stage input)
  2. token_security — holder concentration check       (NARROW stage, 1 call/token)
  3. price          — periodic SOL/USD for fee checks
  4. ohlcv          — historical candle backfill        (FULL stage seeding)
  5. token_overview — market-cap REST fallback

Credit accounting: every request adds its configured credit cost to
`total_credits_consumed` (charged once per logical request, as the provider
bills on receipt). Totals from every client are summed into the periodic
STATUS log line so usage can be
tracked against the monthly plan budget.
"""

import asyncio
import logging
import os
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Dict, List, Optional

from paper_trading.config.params import param
from paper_trading.core.models import Candle, Timeframe

logger = logging.getLogger(__name__)

# Provider details — never hard-coded (env + config)
API_BASE = os.environ.get("DATA_PROVIDER_REST_URL", "")
ENDPOINTS: Dict[str, str] = param("data_provider.endpoints")
CREDIT_COST: Dict[str, int] = param("data_provider.credit_cost")
FIELDS: Dict[str, str] = param("data_provider.response_fields")
AUTH_HEADER: str = param("data_provider.auth_header")
EXTRA_HEADERS: Dict[str, str] = param("data_provider.extra_headers")
MAX_SERVER_FILTERS = param("data_provider.max_server_filters")

# Public wrapped-SOL mint (a chain constant, not a personal address)
SOL_ADDRESS = "So11111111111111111111111111111111111111112"

# Retry config
MAX_RETRIES = 3
RETRY_BACKOFF_BASE = 1.0   # seconds
REQUEST_TIMEOUT = 15        # seconds


# ═══════════════════════════════════════════════════════════════
# TOKEN LIST QUERY CONFIG
# ═══════════════════════════════════════════════════════════════

@dataclass(frozen=True)
class ListQueryConfig:
    """
    Configuration for token-list polling (one per funnel).

    The provider limits the number of server-side filters per request.
    If the richer query (with the volume pre-filter) returns nothing or
    errors, the client falls back to the capped query automatically and
    moves the dropped filter client-side.
    """
    name: str = "default"                    # For logging ("bbup" / "bbdown")
    sort_by: str = "recent_listing_time"
    sort_type: str = "desc"
    limit: int = 100
    offset: int = 0
    # Server-side filters (values tuned privately):
    source: str = param("data_provider.launchpad_source_id")
    min_liquidity: float = param("funnels.defaults.min_liquidity")
    min_market_cap: float = param("funnels.defaults.min_market_cap")
    max_market_cap: float = param("funnels.defaults.max_market_cap")
    min_volume_5m_usd: float = 0             # 0 = not sent


# Preset configs for each funnel
BBUP_LIST_CONFIG = ListQueryConfig(
    name="bbup",
    min_volume_5m_usd=param("funnels.bbup.min_volume_5m_usd"),    # volume pre-filter
)

BBDOWN_LIST_CONFIG = ListQueryConfig(
    name="bbdown",
    min_volume_5m_usd=param("funnels.bbdown.min_volume_5m_usd"),  # broad query
)

DEFAULT_LIST_CONFIG = ListQueryConfig()


# ═══════════════════════════════════════════════════════════════
# BACKFILL TYPES
# ═══════════════════════════════════════════════════════════════

@dataclass
class BackfillRequest:
    """Parameters for a historical candle backfill."""
    token_ca: str
    timeframe: Timeframe
    candle_count: int = param("data_engine.backfill_candles")


@dataclass
class BackfillResult:
    """Result of a backfill request."""
    token_ca: str
    timeframe: Timeframe
    candles: List[Candle]
    credit_cost: int = 0
    success: bool = True
    error: str = ""


# ═══════════════════════════════════════════════════════════════
# REST CLIENT
# ═══════════════════════════════════════════════════════════════

class DataProviderRESTClient:
    """
    Production HTTP client for the market-data provider's REST API.

    fetch_token_list() accepts a per-funnel query config. The client tracks
    whether the richer filter set works and falls back automatically.
    """

    def __init__(self, api_key: str):
        self.api_key = api_key
        self.total_credits_consumed = 0
        self.total_requests = 0
        self.total_errors = 0
        self._session = None
        # Track whether the extended filter mode works per config name
        self._extended_filter_failed: Dict[str, bool] = {}

    def _headers(self) -> dict:
        headers = {AUTH_HEADER: self.api_key, "accept": "application/json"}
        headers.update(EXTRA_HEADERS or {})
        return headers

    async def _get_session(self):
        """Get or create the shared aiohttp session."""
        if self._session is None or self._session.closed:
            try:
                import aiohttp
            except ImportError:
                raise ImportError(
                    "aiohttp package required: pip install aiohttp"
                )
            timeout = aiohttp.ClientTimeout(total=REQUEST_TIMEOUT)
            self._session = aiohttp.ClientSession(
                headers=self._headers(),
                timeout=timeout,
            )
        return self._session

    async def close(self):
        """Close the HTTP session. Call on shutdown."""
        if self._session and not self._session.closed:
            await self._session.close()
            self._session = None

    async def _request(self, method: str, url: str, params: dict = None,
                       credit_cost: int = 0) -> Optional[dict]:
        """
        Execute an HTTP request with retry logic.

        Retries on:
          - 429 Too Many Requests (rate limit) — waits Retry-After or backoff
          - 5xx Server Error — exponential backoff
          - Connection errors — exponential backoff

        Returns parsed JSON response or None on failure.
        Credits are charged once per logical request (on first attempt),
        mirroring how the provider bills on request receipt.
        """
        session = await self._get_session()
        charged = False

        for attempt in range(MAX_RETRIES + 1):
            try:
                async with session.request(method, url, params=params) as resp:
                    self.total_requests += 1
                    if not charged:
                        self.total_credits_consumed += credit_cost
                        charged = True

                    if resp.status == 200:
                        data = await resp.json()
                        if data.get("success") is False:
                            logger.warning(
                                f"API returned success=false: {url} "
                                f"msg={data.get('message', 'unknown')}"
                            )
                            return None
                        return data

                    if resp.status == 429:
                        retry_after = resp.headers.get("Retry-After")
                        wait = float(retry_after) if retry_after else (RETRY_BACKOFF_BASE * (2 ** attempt))
                        logger.warning(
                            f"Rate limited (429). Waiting {wait:.1f}s "
                            f"(attempt {attempt + 1}/{MAX_RETRIES + 1})"
                        )
                        await asyncio.sleep(wait)
                        continue

                    if resp.status >= 500:
                        wait = RETRY_BACKOFF_BASE * (2 ** attempt)
                        logger.warning(
                            f"Server error {resp.status}. Waiting {wait:.1f}s "
                            f"(attempt {attempt + 1}/{MAX_RETRIES + 1})"
                        )
                        await asyncio.sleep(wait)
                        continue

                    # 4xx (other than 429) — don't retry
                    body = await resp.text()
                    logger.error(f"HTTP {resp.status}: {url} — {body[:200]}")
                    self.total_errors += 1
                    return None

            except asyncio.TimeoutError:
                wait = RETRY_BACKOFF_BASE * (2 ** attempt)
                logger.warning(
                    f"Request timeout: {url}. Waiting {wait:.1f}s "
                    f"(attempt {attempt + 1}/{MAX_RETRIES + 1})"
                )
                if not charged:
                    self.total_credits_consumed += credit_cost
                    charged = True
                await asyncio.sleep(wait)
                continue

            except Exception as e:
                if attempt < MAX_RETRIES:
                    wait = RETRY_BACKOFF_BASE * (2 ** attempt)
                    logger.warning(
                        f"Request error: {e}. Waiting {wait:.1f}s "
                        f"(attempt {attempt + 1}/{MAX_RETRIES + 1})"
                    )
                    await asyncio.sleep(wait)
                    continue
                logger.error(f"Request failed after {MAX_RETRIES + 1} attempts: {url} — {e}")
                self.total_errors += 1
                return None

        logger.error(f"Max retries exceeded: {url}")
        self.total_errors += 1
        return None

    @staticmethod
    def _items(data: Optional[dict]) -> List[dict]:
        return (data or {}).get("data", {}).get("items", [])

    # ── Token List (Discovery → WIDE stage) ──────────────────

    async def fetch_token_list(
        self, config: ListQueryConfig = None,
    ) -> List[dict]:
        """
        Fetch the token list with per-funnel server-side filters.

        If a volume pre-filter is configured, tries the extended filter set
        first; falls back to the capped set (dropping min_liquidity, which is
        then enforced client-side by the Wide filters) if the provider returns
        nothing or rejects the query.
        """
        cfg = config or DEFAULT_LIST_CONFIG
        url = f"{API_BASE}{ENDPOINTS['token_list']}"
        cost = CREDIT_COST["token_list"]

        params = {
            "sort_by": cfg.sort_by,
            "sort_type": cfg.sort_type,
            "offset": cfg.offset,
            "limit": cfg.limit,
            "source": cfg.source,
            "min_market_cap": cfg.min_market_cap,
            "max_market_cap": cfg.max_market_cap,
        }

        use_extended = (
            cfg.min_volume_5m_usd > 0
            and not self._extended_filter_failed.get(cfg.name, False)
        )

        if use_extended:
            params["min_liquidity"] = cfg.min_liquidity
            params["min_volume_5m_usd"] = cfg.min_volume_5m_usd

            items = self._items(await self._request("GET", url, params=params, credit_cost=cost))
            if items:
                logger.info(f"[{cfg.name}] Token list (extended filters): {len(items)} tokens")
                return items

            logger.warning(
                f"[{cfg.name}] Extended-filter query returned 0 tokens — "
                f"falling back to capped filter set (dropping min_liquidity)"
            )
            self._extended_filter_failed[cfg.name] = True
            params.pop("min_liquidity", None)
            data = await self._request("GET", url, params=params, credit_cost=cost)
        elif cfg.min_volume_5m_usd > 0:
            params["min_volume_5m_usd"] = cfg.min_volume_5m_usd
            data = await self._request("GET", url, params=params, credit_cost=cost)
        else:
            params["min_liquidity"] = cfg.min_liquidity
            data = await self._request("GET", url, params=params, credit_cost=cost)

        if data is None:
            return []

        items = self._items(data)
        mode = "capped fallback" if self._extended_filter_failed.get(cfg.name) else "standard"
        logger.info(f"[{cfg.name}] Token list ({mode}): {len(items)} tokens")
        return items

    # ── Token Security (NARROW stage) ────────────────────────

    async def fetch_holder_data(self, token_ca: str) -> Optional[dict]:
        """Fetch token security data including holder concentration (paid call)."""
        url = f"{API_BASE}{ENDPOINTS['token_security']}"
        data = await self._request("GET", url, params={"address": token_ca},
                                   credit_cost=CREDIT_COST["token_security"])
        if data is None:
            return None

        sec = data.get("data", {})
        if not sec:
            logger.debug(f"No security data for {token_ca[:12]}...")
            return None

        f = FIELDS
        result = {
            "top10_holder_pct": sec.get(f["top10_holder_pct"]),
            "top10_user_pct": sec.get(f["top10_user_pct"]),
            "creator_pct": sec.get(f["creator_pct"]),
            "freeze_authority": sec.get(f["freeze_authority"]),
            "transfer_fee_enabled": sec.get(f["transfer_fee_enabled"]),
            "pre_market_holders": len(sec.get(f["pre_market_holders"]) or []),
            "creation_time": sec.get(f["creation_time"]),
        }

        logger.debug(
            f"Security data: {token_ca[:12]}... "
            f"top10={result['top10_holder_pct']}, top10_user={result['top10_user_pct']}"
        )
        return result

    # ── SOL Price ────────────────────────────────────────────

    async def fetch_sol_price(self) -> Optional[float]:
        """Fetch current SOL/USD price (used by the Wide fee-generation check)."""
        url = f"{API_BASE}{ENDPOINTS['price']}"
        data = await self._request("GET", url, params={"address": SOL_ADDRESS},
                                   credit_cost=CREDIT_COST["price"])
        if data is None:
            return None

        value = data.get("data", {}).get("value")
        if value is None:
            logger.warning("SOL price response missing 'value' field")
            return None

        price = float(value)
        logger.debug(f"SOL price: ${price:.2f}")
        return price

    # ── Historical Backfill (OHLCV) ──────────────────────────

    async def fetch_ohlcv(
        self, token_ca: str, timeframe: Timeframe,
        candle_count: int = param("data_engine.backfill_candles"),
    ) -> BackfillResult:
        """Fetch historical OHLCV candles for a token (credit cost scales with size)."""
        interval_secs = 60 if timeframe == Timeframe.ONE_MIN else 300
        chart_type = "1m" if timeframe == Timeframe.ONE_MIN else "5m"

        now = int(datetime.now(timezone.utc).timestamp())
        time_from = now - (candle_count * interval_secs)

        credit_cost = self._estimate_ohlcv_credits(candle_count)

        url = f"{API_BASE}{ENDPOINTS['ohlcv']}"
        params = {
            "address": token_ca,
            "type": chart_type,
            "time_from": time_from,
            "time_to": now,
        }

        data = await self._request("GET", url, params=params, credit_cost=credit_cost)
        if data is None:
            return BackfillResult(
                token_ca=token_ca, timeframe=timeframe, candles=[],
                credit_cost=credit_cost, success=False, error="API request failed",
            )

        candles = [
            self._parse_ohlcv_item(token_ca, timeframe, item)
            for item in self._items(data)
        ]

        logger.info(f"Backfill: {token_ca[:12]}... {chart_type} × {len(candles)} candles")

        return BackfillResult(
            token_ca=token_ca, timeframe=timeframe, candles=candles,
            credit_cost=credit_cost, success=True,
        )

    @staticmethod
    def _estimate_ohlcv_credits(candle_count: int) -> int:
        """Size-tiered credit cost: list of [max_candles, credits] from config."""
        tiers = CREDIT_COST["ohlcv_tiers"]
        for max_candles, credits in tiers:
            if candle_count <= max_candles:
                return credits
        return tiers[-1][1]

    @staticmethod
    def _parse_ohlcv_item(token_ca: str, timeframe: Timeframe,
                           item: dict) -> Candle:
        f = FIELDS
        ts = datetime.fromtimestamp(item.get(f["candle_time"], 0), tz=timezone.utc)
        return Candle(
            token_ca=token_ca,
            timeframe=timeframe,
            timestamp=ts,
            open=item.get(f["candle_open"], 0),
            high=item.get(f["candle_high"], 0),
            low=item.get(f["candle_low"], 0),
            close=item.get(f["candle_close"], 0),
            volume=item.get(f["candle_volume"], 0),
            market_cap=0,
        )

    # ── Market Cap Polling (REST fallback) ───────────────────

    async def fetch_market_cap(self, token_ca: str) -> Optional[float]:
        """Fetch market cap via REST. Fallback only."""
        overview = await self.fetch_token_overview(token_ca)
        if overview is None:
            return None
        return overview.get(FIELDS["overview_market_cap"]) or overview.get("market_cap")

    async def fetch_token_overview(self, token_ca: str) -> Optional[dict]:
        """Fetch full token overview (market-cap fallback and scoring snapshots)."""
        url = f"{API_BASE}{ENDPOINTS['token_overview']}"
        data = await self._request("GET", url, params={"address": token_ca},
                                   credit_cost=CREDIT_COST["token_overview"])
        if data is None:
            return None

        overview = data.get("data", {})
        if not overview:
            return None

        logger.debug(f"Token overview: {token_ca[:12]}...")
        return overview

    async def fetch_market_caps_batch(
        self, token_cas: List[str],
    ) -> Dict[str, float]:
        """Fetch market caps for multiple tokens via individual calls."""
        results = {}
        for ca in token_cas:
            mcap = await self.fetch_market_cap(ca)
            if mcap is not None:
                results[ca] = mcap
        return results

    # ── Stats ────────────────────────────────────────────────

    def get_stats(self) -> dict:
        return {
            "total_credits_consumed": round(self.total_credits_consumed, 2),
            "total_requests": self.total_requests,
            "total_errors": self.total_errors,
        }
