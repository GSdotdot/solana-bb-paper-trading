"""
Core Data Models & Enums
========================
Pure data containers used by every layer of the system.
No business logic lives here — only structure and types.
"""

from dataclasses import dataclass, field
from datetime import datetime
from enum import Enum
from typing import Optional, List


# ═══════════════════════════════════════════════════════════════════
# ENUMS
# ═══════════════════════════════════════════════════════════════════

class StrategyType(Enum):
    BBDOWN = "bbdown"
    BBUP = "bbup"


class Timeframe(Enum):
    ONE_MIN = "1m"
    FIVE_MIN = "5m"


class SignalType(Enum):
    NO_ACTION = "no_action"
    ENTRY = "entry"
    EXIT_UBB = "exit_ubb"              # BBDOWN: 2nd stage UBB exit / BBUP: UBB take-profit
    EXIT_SMA_PARTIAL = "exit_sma_partial"  # BBDOWN only: partial sell at SMA
    EXIT_SMA_FULL = "exit_sma_full"    # BBDOWN: 2nd SMA cross-below 100% / BBUP: SMA stop loss
    EXIT_TIME = "exit_time"            # BBUP only: legacy time exit
    EXIT_HARD_STOP = "exit_hard_stop"  # BBDOWN only: MC hard stop
    EXIT_SHUTDOWN = "exit_shutdown"     # System graceful shutdown


# ═══════════════════════════════════════════════════════════════════
# TICK DATA (live WebSocket price update)
# ═══════════════════════════════════════════════════════════════════

@dataclass
class TickData:
    """A single live price update from the WebSocket stream.
    Used for real-time entry/exit detection (not candle-close-based).
    """
    token_ca: str
    price: float
    market_cap: float
    timestamp: datetime
    timeframe: 'Timeframe' = None    # Which chart subscription produced this tick
    volume_usd: float = 0.0         # Running candle volume in USD


class ExitReason(Enum):
    UBB_TARGET = "ubb_target"
    SMA_PARTIAL = "sma_partial"        # BBDOWN partial at SMA
    SMA_BEARISH = "sma_bearish"        # BBDOWN 2nd SMA cross-below
    SMA_STOP_LOSS = "sma_stop_loss"    # BBUP SMA SL (2-step)
    TIME_EXIT = "time_exit"
    HARD_STOP = "hard_stop"
    FORCED_CLOSE = "forced_close"      # End of data, shutdown, max cycles


class CyclePattern(Enum):
    """Cycle outcome classification."""
    # BBDOWN patterns
    BBDOWN_HARD_STOP = "hard_stop"
    BBDOWN_SMA_UBB = "sma_then_ubb"         # Best case: partial at SMA + rest at UBB
    BBDOWN_SMA_BEARISH = "sma_then_bearish"  # partial at SMA + rest at 2nd SMA exit
    BBDOWN_UBB_DIRECT = "ubb_direct"         # Edge case: UBB reached before SMA
    # BBUP patterns
    BBUP_UBB = "ubb_exit"                    # Winner: reached upper band
    BBUP_TIME = "time_exit"                  # Neutral: legacy timeout
    BBUP_SMA_SL = "sma_sl_exit"             # Loser: SMA stop loss
    # Shared
    FORCED = "forced"


# ═══════════════════════════════════════════════════════════════════
# CANDLE
# ═══════════════════════════════════════════════════════════════════

@dataclass
class Candle:
    """Single OHLCV candle with optional market cap."""
    token_ca: str
    timeframe: Timeframe
    timestamp: datetime
    open: float
    high: float
    low: float
    close: float
    volume: float           # USD volume
    market_cap: float = 0.0 # Required for BBDOWN hard stops & slippage tiers


# ═══════════════════════════════════════════════════════════════════
# INDICATOR SNAPSHOT
# ═══════════════════════════════════════════════════════════════════

@dataclass
class IndicatorSnapshot:
    """All computed indicators at a point in time. Attached to signals/trades."""
    sma: float = 0.0
    bb_upper: float = 0.0
    bb_lower: float = 0.0
    bb_width_pct: float = 0.0      # (upper - lower) / sma * 100
    rsi: float = 0.0
    volume_avg_n: float = 0.0      # rolling avg volume over the volume window (USD)
    current_close: float = 0.0
    current_volume: float = 0.0
    current_market_cap: float = 0.0


# ═══════════════════════════════════════════════════════════════════
# SIGNAL
# ═══════════════════════════════════════════════════════════════════

@dataclass
class Signal:
    """Emitted by strategy engine. Consumed by execution engine."""
    signal_type: SignalType
    strategy_name: str
    token_ca: str
    timestamp: datetime
    price: float                    # Price at detection (actual WebSocket price)
    fill_price: float = 0.0        # Suggested fill price (may differ from detection)
    sell_pct: float = 1.0           # What fraction to sell (partial fraction for BBDOWN)
    indicators: Optional[IndicatorSnapshot] = None
    reason: str = ""

    @property
    def is_actionable(self) -> bool:
        return self.signal_type != SignalType.NO_ACTION

    @property
    def is_entry(self) -> bool:
        return self.signal_type == SignalType.ENTRY

    @property
    def is_exit(self) -> bool:
        return self.signal_type not in (SignalType.NO_ACTION, SignalType.ENTRY)
