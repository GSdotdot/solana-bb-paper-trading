"""
Scanner Token Model
====================
Tracks a token's journey through the funnel tiers.

Data is populated primarily from token list response at discovery time.
Progressive accumulation:
  Discovery (token-list):  address, ticker, mcap, liquidity, volume, price, holder count,
                          launchpad info block (graduated, progress, source, creator, pool reserves)
  Wide Evaluation:  fee generation check, liquidity/mcap ratio (computed in-memory)
  Narrow Scan:     security data via the token-security endpoint (holder concentration, freeze, etc.)
  Full Focus:      promoted to orchestrator, token_overview snapshot for scoring

Source filtering: PumpFun only for paper trading. Extensible to other launchpads later.
"""

from dataclasses import dataclass, field
from datetime import datetime, timezone
from enum import Enum
from typing import Optional

from paper_trading.config.params import param


class Tier(Enum):
    """Current tier in the funnel."""
    CANDIDATE = "candidate"     # Just discovered, awaiting evaluation
    WIDE = "wide"               # Borderline — close to qualifying, parked for re-evaluation
    NARROW = "narrow"           # Passed Wide evaluation, awaiting safety checks
    FULL = "full"               # Active monitoring via WebSocket
    EXPELLED = "expelled"       # Removed (failed filter, TTL, blacklisted)


class Launchpad(Enum):
    """Detected token launchpad."""
    PUMPFUN = "pumpfun"
    BONKFUN = "bonkfun"         # Deferred — not used in paper trading
    BAGS = "bags"               # Deferred — not used in paper trading
    UNKNOWN = "unknown"


@dataclass
class ScannerToken:
    """
    A token being tracked through the scanner funnel.

    Most data fields are populated at discovery time from token list response.
    Only holder concentration data requires an additional API call (at Narrow scan).
    """

    # ── Identity (set at discovery) ──────────────────────────
    token_ca: str
    ticker: str = ""
    name: str = ""
    launchpad: Launchpad = Launchpad.UNKNOWN

    # ── Tier tracking ────────────────────────────────────────
    tier: Tier = Tier.CANDIDATE
    discovered_at: datetime = field(default_factory=lambda: datetime.now(timezone.utc))
    promoted_at: Optional[datetime] = None      # Last tier promotion time
    tier_entry_time: datetime = field(default_factory=lambda: datetime.now(timezone.utc))

    # ── token list data (populated at discovery) ─────────
    price: Optional[float] = None
    market_cap: Optional[float] = None
    liquidity: Optional[float] = None           # = TVL
    volume_5m_usd: Optional[float] = None
    volume_1h_usd: Optional[float] = None
    volume_24h_usd: Optional[float] = None
    holder_count: Optional[int] = None
    unique_wallet_24h: Optional[int] = None
    trade_1h_count: Optional[int] = None
    recent_listing_time: Optional[datetime] = None
    last_trade_unix_time: Optional[int] = None

    # ── Price change data (from token list, for scoring) ──────
    price_change_1h_pct: Optional[float] = None
    price_change_24h_pct: Optional[float] = None

    # ── Volume change data (from token list, for scoring) ─────
    volume_1h_change_pct: Optional[float] = None
    volume_5m_change_pct: Optional[float] = None

    # ── Launchpad info (from token list — zero extra credits) ──
    launchpad_source: Optional[str] = None           # launchpad source id
    graduated: Optional[bool] = None            # Bonding curve → DEX graduation
    graduated_time: Optional[int] = None        # Unix timestamp of graduation
    progress_percent: Optional[float] = None    # Bonding curve progress (0-100)
    launchpad_creator: Optional[str] = None          # Creator address (never exported in samples)
    pool_address: Optional[str] = None          # Bonding curve pool address
    real_sol_reserves: Optional[float] = None   # Actual SOL in pool
    real_token_reserves: Optional[float] = None # Actual tokens in pool
    platform_id: Optional[str] = None           # Launchpad program ID

    # ── Buy/sell pressure (from token list, for scoring) ──────
    buy_24h: Optional[int] = None
    sell_24h: Optional[int] = None
    volume_buy_24h_usd: Optional[float] = None
    volume_sell_24h_usd: Optional[float] = None
    trade_24h_count: Optional[int] = None

    # ── Wide evaluation results (computed in-memory) ─────────
    liquidity_ratio: Optional[float] = None     # liquidity / market_cap
    estimated_fees_sol: Optional[float] = None   # volume_24h * fee_rate / sol_price
    fee_check_passed: Optional[bool] = None
    wide_eval_done: bool = False

    # ── Narrow Scan data (from token_security API call) ────
    top10_holder_pct: Optional[float] = None        # Includes LP/program accounts
    top10_user_pct: Optional[float] = None          # May exclude LP — better for our check
    creator_pct: Optional[float] = None             # Creator's current holdings
    freeze_authority: Optional[str] = None          # Non-null = honeypot risk
    transfer_fee_enabled: Optional[bool] = None     # Fee-on-transfer = honeypot
    pre_market_holder_count: int = 0                # Early holders (sniper signal)
    narrow_scan_done: bool = False

    # ── Legacy fields (kept for compatibility, skipped for PumpFun paper trading)
    mint_authority_revoked: Optional[bool] = None
    freeze_authority_revoked: Optional[bool] = None
    creator_address: Optional[str] = None
    creation_time: Optional[datetime] = None
    safety_score: Optional[float] = None

    # ── Scanner metadata ─────────────────────────────────────
    wide_scan_count: int = 0        # Times evaluated at Wide level
    narrow_scan_count: int = 0      # Times scanned at Narrow level
    list_appearances: int = 0         # Times seen in token list results
    expiry_reason: str = ""         # Why it was expelled

    def promote_to(self, new_tier: Tier):
        """Move token to a higher tier."""
        now = datetime.now(timezone.utc)
        self.tier = new_tier
        self.promoted_at = now
        self.tier_entry_time = now

    def expel(self, reason: str):
        """Remove token from the funnel."""
        self.tier = Tier.EXPELLED
        self.expiry_reason = reason

    @property
    def age_seconds(self) -> float:
        """Time since discovery."""
        return (datetime.now(timezone.utc) - self.discovered_at).total_seconds()

    @property
    def time_in_tier_seconds(self) -> float:
        """Time spent in current tier."""
        return (datetime.now(timezone.utc) - self.tier_entry_time).total_seconds()

    @property
    def is_pumpfun(self) -> bool:
        """Check if token address ends with 'pump' — PumpFun identifier."""
        return self.token_ca.endswith("pump")

    def update_from_list(self, list_data: dict):
        """
        Populate token fields from a token list response item.
        Called when the token is first discovered or re-seen in list polling.
        Parses both the flat fields and the nested launchpad info block.
        """
        self.list_appearances += 1
        self.name = list_data.get("name", self.name)
        self.ticker = list_data.get("symbol", self.ticker)
        self.price = list_data.get("price", self.price)
        self.market_cap = list_data.get("market_cap", self.market_cap)
        self.liquidity = list_data.get("liquidity", self.liquidity)
        self.volume_5m_usd = list_data.get("volume_5m_usd", self.volume_5m_usd)
        self.volume_1h_usd = list_data.get("volume_1h_usd", self.volume_1h_usd)
        self.volume_24h_usd = list_data.get("volume_24h_usd", self.volume_24h_usd)
        self.holder_count = list_data.get("holder", self.holder_count)
        self.unique_wallet_24h = list_data.get("unique_wallet_24h", self.unique_wallet_24h)
        self.trade_1h_count = list_data.get("trade_1h_count", self.trade_1h_count)
        self.price_change_1h_pct = list_data.get("price_change_1h_percent", self.price_change_1h_pct)
        self.price_change_24h_pct = list_data.get("price_change_24h_percent", self.price_change_24h_pct)
        self.volume_1h_change_pct = list_data.get("volume_1h_change_percent", self.volume_1h_change_pct)
        self.volume_5m_change_pct = list_data.get("volume_5m_change_percent", self.volume_5m_change_pct)
        self.last_trade_unix_time = list_data.get("last_trade_unix_time", self.last_trade_unix_time)

        # Buy/sell pressure
        self.buy_24h = list_data.get("buy_24h", self.buy_24h)
        self.sell_24h = list_data.get("sell_24h", self.sell_24h)
        self.volume_buy_24h_usd = list_data.get("volume_buy_24h_usd", self.volume_buy_24h_usd)
        self.volume_sell_24h_usd = list_data.get("volume_sell_24h_usd", self.volume_sell_24h_usd)
        self.trade_24h_count = list_data.get("trade_24h_count", self.trade_24h_count)

        rlt = list_data.get("recent_listing_time")
        if rlt is not None:
            try:
                self.recent_listing_time = datetime.fromtimestamp(int(rlt), tz=timezone.utc)
            except (ValueError, TypeError, OSError):
                pass

        # Parse launchpad info block (present in token-list endpoint)
        info = list_data.get(param("data_provider.response_fields.launchpad_info_block"))
        if info:
            self.launchpad_source = info.get("source", self.launchpad_source)
            self.graduated = info.get("graduated", self.graduated)
            self.graduated_time = info.get("graduated_time", self.graduated_time)
            self.progress_percent = info.get("progress_percent", self.progress_percent)
            self.launchpad_creator = info.get("creator", self.launchpad_creator)
            self.platform_id = info.get("platform_id", self.platform_id)
            ct = info.get("creation_time")
            if ct is not None:
                try:
                    self.creation_time = datetime.fromtimestamp(int(ct), tz=timezone.utc)
                except (ValueError, TypeError, OSError):
                    pass

            pool = info.get("pool")
            if pool:
                self.pool_address = pool.get("address", self.pool_address)
                try:
                    self.real_sol_reserves = float(pool.get("real_sol_reserves", 0))
                except (ValueError, TypeError):
                    pass
                try:
                    self.real_token_reserves = float(pool.get("real_token_reserves", 0))
                except (ValueError, TypeError):
                    pass

            # Set launchpad from source
            source = info.get("source", "")
            if source == param("data_provider.launchpad_source_id"):
                self.launchpad = Launchpad.PUMPFUN
