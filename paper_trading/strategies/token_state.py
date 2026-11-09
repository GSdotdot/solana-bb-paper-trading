"""
Token State
===========
Tracks everything a strategy instance needs to know about a single token.
One TokenState per (strategy_instance, token) pair.

Contains:
- Price/volume/MC history ring buffer
- Computed indicators
- Position state (entry, partial exits, trade tracking)
- BBDOWN-specific: 2-stage exit state, MC hard stop levels
- BBUP-specific: SMA SL 2-step pending flag
- Cooldowns and cycle counters
"""

from dataclasses import dataclass, field
from datetime import datetime, timedelta
from typing import List, Optional

from paper_trading.config.params import param
from paper_trading.core.models import Candle, IndicatorSnapshot
from paper_trading.core import indicators as ind


@dataclass
class TokenState:
    """Complete state for one token within one strategy instance."""

    token_ca: str
    token_ticker: str = ""

    # ── Price/Volume History (ring buffer, most recent at [-1]) ──
    close_prices: List[float] = field(default_factory=list)
    high_prices: List[float] = field(default_factory=list)
    low_prices: List[float] = field(default_factory=list)
    open_prices: List[float] = field(default_factory=list)
    volumes: List[float] = field(default_factory=list)
    market_caps: List[float] = field(default_factory=list)
    timestamps: List[datetime] = field(default_factory=list)
    max_history: int = 250

    # ── Current Indicators ──────────────────────────────────────
    indicators: Optional[IndicatorSnapshot] = None

    # ── Position State ──────────────────────────────────────────
    has_position: bool = False
    entry_price: float = 0.0            # Actual fill price at detection
    entry_timestamp: Optional[datetime] = None
    entry_candle_idx: int = 0           # Index in history at entry
    entry_market_cap: float = 0.0
    candles_since_entry: int = 0
    position_tokens: float = 0.0        # Current token quantity held
    initial_tokens: float = 0.0         # Tokens at entry (before partials)
    cycle_invested: float = 0.0         # Total USD in this cycle

    # Trade tracking (for MFE/MAE and analysis)
    trade_high_price: float = 0.0
    trade_low_price: float = float('inf')
    candle_volumes_in_trade: List[float] = field(default_factory=list)

    # ── BBDOWN-Specific ─────────────────────────────────────────
    # 2-stage exit tracking
    sma_partial_done: bool = False          # Has the partial SMA exit fired?
    sma_partial_price: float = 0.0
    sma_partial_tokens_sold: float = 0.0
    sma_partial_timestamp: Optional[datetime] = None

    # 2nd SMA cross-below detection (bearish exit after partial)
    prev_candle_closed_below_sma: bool = False

    # MC Hard Stop (only for entries below a market-cap ceiling)
    hard_stop_mc: float = 0.0               # MC threshold for liquidation
    hard_stop_active: bool = False

    # Post-hard-stop recovery
    hard_stop_recovery_required: bool = False
    hard_stop_recovery_earliest: Optional[datetime] = None  # + recovery wait
    hard_stop_recovery_mc_threshold: float = param("strategies.bbdown.hard_stop.recovery_mc")  # Must exceed

    # ── BBUP-Specific ───────────────────────────────────────────
    # SMA Stop Loss 2-step: candle closes below SMA → pending flag →
    # next candle sells at OPEN
    sma_sl_pending: bool = False

    # BBUP phantom cycle prevention: after a BBUP cycle completes,
    # block re-entry until a tick is observed where price < SMA.
    # This prevents instant re-entry when price is still above SMA
    # after a UBB exit (the "phantom cycle" bug).
    # Set True on position close, cleared when tick < SMA observed.
    bbup_awaiting_cross_below: bool = False

    # ── Shared Cooldowns & Cycle Tracking ───────────────────────
    cooldown_until: Optional[datetime] = None       # Per-exit cooldowns
    cycles_completed: int = 0
    max_cycles_cooldown_until: Optional[datetime] = None

    # Scanner integration
    flagged_for_removal: bool = False

    # ── A1 (V7.1): open-position persistence / restore state ────
    # Set True for a position rebuilt from the open_positions table at startup.
    # While True the orchestrator suppresses ALL signal processing for this
    # (strategy, token): the position is HELD but inert until REST backfill
    # reseeds >= sma_period candles and indicators are valid again. This is the
    # Task 2.4 backfill gate — a restored token must not fire on stale state.
    awaiting_backfill: bool = False
    # Set True if, at restore, the row's last_updated_timestamp is older than
    # a staleness horizon (WS likely dropped for hours / token possibly
    # rugged). Paper mode only logs + flags it.
    restored_stale: bool = False

    # V5: Tick entry lock — prevents duplicate entry signals before
    # notify_entry_filled() sets has_position=True.
    # Exit path does NOT need a lock: close_position() sets has_position=False
    # immediately, and the has_position guard prevents duplicate exits.
    # H4 fix: separated from a single tick_signal_fired that was blocking
    # same-candle entry→exit sequences.
    tick_entry_fired: bool = False

    # ════════════════════════════════════════════════════════════
    # HISTORY MANAGEMENT
    # ════════════════════════════════════════════════════════════

    @property
    def is_ready(self) -> bool:
        """Indicators have been computed at least once."""
        return self.indicators is not None

    @property
    def candle_count(self) -> int:
        return len(self.close_prices)

    @property
    def current_market_cap(self) -> float:
        return self.market_caps[-1] if self.market_caps else 0.0

    @property
    def previous_close(self) -> Optional[float]:
        """Close price of the candle before the most recent one."""
        if len(self.close_prices) >= 2:
            return self.close_prices[-2]
        return None

    @property
    def previous_sma(self) -> Optional[float]:
        """SMA at the previous candle. Needed for BBUP cross detection."""
        # We don't store historical indicators, so we can't get this directly.
        # Instead, the strategy checks prev_close vs current SMA as a proxy,
        # or we track it explicitly. See note in BBUP strategy.
        return None

    def add_candle(self, candle: Candle):
        """Append new candle and trim history if needed."""
        self.close_prices.append(candle.close)
        self.high_prices.append(candle.high)
        self.low_prices.append(candle.low)
        self.open_prices.append(candle.open)
        self.volumes.append(candle.volume)
        self.market_caps.append(candle.market_cap)
        self.timestamps.append(candle.timestamp)

        # Trim ring buffer
        if len(self.close_prices) > self.max_history:
            trim = len(self.close_prices) - self.max_history
            self.close_prices = self.close_prices[trim:]
            self.high_prices = self.high_prices[trim:]
            self.low_prices = self.low_prices[trim:]
            self.open_prices = self.open_prices[trim:]
            self.volumes = self.volumes[trim:]
            self.market_caps = self.market_caps[trim:]
            self.timestamps = self.timestamps[trim:]
            # Adjust entry_candle_idx if in a position
            if self.has_position:
                self.entry_candle_idx = max(0, self.entry_candle_idx - trim)

        # Update trade tracking if in position
        if self.has_position:
            self.candles_since_entry += 1
            self.candle_volumes_in_trade.append(candle.volume)
            if candle.high > self.trade_high_price:
                self.trade_high_price = candle.high
            if candle.low < self.trade_low_price:
                self.trade_low_price = candle.low

    def update_indicators(
        self, sma_period: int, bb_multiplier: float,
        rsi_period: int = param("strategies.rsi_period"),
        vol_avg_period: int = param("strategies.volume_avg_candles")
    ):
        """Recompute indicators from current history."""
        self.indicators = ind.compute_all(
            close_prices=self.close_prices,
            volumes=self.volumes,
            sma_period=sma_period,
            bb_multiplier=bb_multiplier,
            rsi_period=rsi_period,
            vol_avg_period=vol_avg_period,
            market_cap=self.current_market_cap,
        )

    def seed_history(self, candles: List[Candle]):
        """
        Backfill historical candles (Option B warm-up).
        Called when token enters Full Focus to avoid waiting for warmup.
        Candles must be in chronological order (oldest first).
        """
        for c in candles:
            self.close_prices.append(c.close)
            self.high_prices.append(c.high)
            self.low_prices.append(c.low)
            self.open_prices.append(c.open)
            self.volumes.append(c.volume)
            self.market_caps.append(c.market_cap)
            self.timestamps.append(c.timestamp)

    # ════════════════════════════════════════════════════════════
    # POSITION LIFECYCLE
    # ════════════════════════════════════════════════════════════

    def open_position(
        self, price: float, tokens: float, invested: float,
        timestamp: datetime, market_cap: float
    ):
        """Record position entry."""
        self.has_position = True
        self.entry_price = price
        self.entry_timestamp = timestamp
        self.entry_candle_idx = self.candle_count - 1
        self.entry_market_cap = market_cap
        self.candles_since_entry = 0
        self.position_tokens = tokens
        self.initial_tokens = tokens
        self.cycle_invested = invested
        self.trade_high_price = price
        self.trade_low_price = price
        self.candle_volumes_in_trade = []

        # Reset exit-tracking state
        self.sma_partial_done = False
        self.sma_partial_price = 0.0
        self.sma_partial_tokens_sold = 0.0
        self.sma_partial_timestamp = None
        self.prev_candle_closed_below_sma = False
        self.sma_sl_pending = False

    def close_position(self):
        """Reset position state after full exit."""
        self.has_position = False
        self.entry_price = 0.0
        self.entry_timestamp = None
        self.entry_market_cap = 0.0
        self.candles_since_entry = 0
        self.position_tokens = 0.0
        self.initial_tokens = 0.0
        self.cycle_invested = 0.0
        self.trade_high_price = 0.0
        self.trade_low_price = float('inf')
        self.candle_volumes_in_trade = []
        self.sma_partial_done = False
        self.sma_partial_price = 0.0
        self.sma_partial_tokens_sold = 0.0
        self.sma_partial_timestamp = None
        self.prev_candle_closed_below_sma = False
        self.sma_sl_pending = False
        self.hard_stop_active = False
        self.hard_stop_mc = 0.0
        self.cycles_completed += 1
        self.tick_entry_fired = False

    def record_partial_sell(self, tokens_sold: float, price: float, timestamp: datetime):
        """Record a partial exit (BBDOWN partial at SMA)."""
        self.sma_partial_done = True
        self.sma_partial_price = price
        self.sma_partial_tokens_sold = tokens_sold
        self.sma_partial_timestamp = timestamp
        self.position_tokens -= tokens_sold

    # ════════════════════════════════════════════════════════════
    # COOLDOWN MANAGEMENT
    # ════════════════════════════════════════════════════════════

    def set_cooldown(self, minutes: float, from_time: datetime):
        """Set a cooldown period. Only extends, never shortens."""
        new_until = from_time + timedelta(minutes=minutes)
        if self.cooldown_until is None or new_until > self.cooldown_until:
            self.cooldown_until = new_until

    def is_in_cooldown(self, current_time: datetime) -> bool:
        """Check if token is currently in cooldown."""
        if self.cooldown_until and current_time < self.cooldown_until:
            return True
        if self.max_cycles_cooldown_until and current_time < self.max_cycles_cooldown_until:
            return True
        return False

    def set_hard_stop_recovery(self, from_time: datetime):
        """After MC hard stop: require a wait period + market-cap recovery."""
        self.hard_stop_recovery_required = True
        self.hard_stop_recovery_earliest = from_time + timedelta(
            hours=param("strategies.bbdown.hard_stop.recovery_hours"))

    def can_enter_after_hard_stop(self, current_time: datetime, current_mc: float) -> bool:
        """Check if hard stop recovery conditions are met."""
        if not self.hard_stop_recovery_required:
            return True
        if self.hard_stop_recovery_earliest and current_time < self.hard_stop_recovery_earliest:
            return False
        if current_mc < self.hard_stop_recovery_mc_threshold:
            return False
        # Both conditions met — clear the flag
        self.hard_stop_recovery_required = False
        self.hard_stop_recovery_earliest = None
        return True

    # ════════════════════════════════════════════════════════════
    # BBDOWN MC HARD STOP CALCULATION
    # ════════════════════════════════════════════════════════════

    def calculate_hard_stop_mc(self, entry_mc: float):
        """
        Proportional MC hard stop, piecewise-linear in entry market cap.

        The schedule is a list of segments
            [entry_mc_lo, entry_mc_hi, stop_mc_lo, stop_mc_hi]
        and the stop is linearly interpolated inside the matching segment.
        Entries below the first segment use its lowest stop; entries at or
        above `max_entry_mc` get no hard stop.      [all breakpoints: XXXX]
        """
        max_entry_mc = param("strategies.bbdown.hard_stop.max_entry_mc")
        schedule = param("strategies.bbdown.hard_stop.schedule")

        if entry_mc >= max_entry_mc:
            self.hard_stop_active = False
            self.hard_stop_mc = 0.0
            return

        self.hard_stop_active = True

        for mc_lo, mc_hi, stop_lo, stop_hi in schedule:
            if entry_mc <= mc_hi:
                if entry_mc < mc_lo:
                    # Below segment minimum — use its lowest stop
                    self.hard_stop_mc = stop_lo
                else:
                    proportion = (entry_mc - mc_lo) / (mc_hi - mc_lo)
                    self.hard_stop_mc = stop_lo + proportion * (stop_hi - stop_lo)
                return
        # Above the last segment but below max_entry_mc: use its highest stop
        self.hard_stop_mc = schedule[-1][3]

    @property
    def avg_volume_in_trade(self) -> float:
        """Average candle volume during this trade."""
        if not self.candle_volumes_in_trade:
            return 0.0
        return sum(self.candle_volumes_in_trade) / len(self.candle_volumes_in_trade)
