"""
Execution Engine (Layer 4)
==========================
Handles trade execution, fee/slippage modeling, and portfolio management.

Components:
  - PortfolioManager: Per-strategy cash balance, position tracking, equity snapshots
  - BaseExecutor: Abstract interface (paper vs live)
  - PaperExecutor: Simulated execution with realistic fee/slippage model

Friction model (fee + slippage + priority fee; values in config → friction):
  - DEX fee:      DEX_FEE_PCT of trade amount              (XXXX)
  - Priority fee: PRIORITY_FEE_USD flat per transaction    (XXXX)
  - Slippage:     market-cap-tiered, applied in both directions
                  MC <  SLIPPAGE_THRESHOLD_MC → SLIPPAGE_BELOW  (XXXX)
                  MC >= SLIPPAGE_THRESHOLD_MC → SLIPPAGE_ABOVE  (XXXX)

Fill Price Logic:
  - Entries/exits fill at signal.fill_price (indicator crossing level)
  - Slippage applied ON TOP of fill_price
  - Buy: you pay MORE → fill_price * (1 + slippage)
  - Sell: you receive LESS → fill_price * (1 - slippage)

Integration:
  Strategy emits Signal → Executor processes it → calls strategy.notify_*()
"""

from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Dict, List, Optional, Tuple
import logging

from paper_trading.config.params import param
from paper_trading.core.models import (
    Signal, SignalType, IndicatorSnapshot, StrategyType
)

logger = logging.getLogger(__name__)


# ═══════════════════════════════════════════════════════════════
# FEE / SLIPPAGE CONSTANTS
# ═══════════════════════════════════════════════════════════════

DEX_FEE_PCT = param("friction.dex_fee_pct")                    # tuned privately
PRIORITY_FEE_USD = param("friction.priority_fee_usd")          # tuned privately
SLIPPAGE_THRESHOLD_MC = param("friction.slippage_threshold_mc")  # tier boundary
SLIPPAGE_BELOW = param("friction.slippage_below")              # MC below boundary
SLIPPAGE_ABOVE = param("friction.slippage_above")              # MC at/above boundary


def get_slippage(market_cap: float) -> float:
    """2-tier slippage model based on market cap."""
    if market_cap < SLIPPAGE_THRESHOLD_MC:
        return SLIPPAGE_BELOW
    return SLIPPAGE_ABOVE


# ═══════════════════════════════════════════════════════════════
# TRADE RECORD
# ═══════════════════════════════════════════════════════════════

@dataclass
class TradeRecord:
    """
    Complete record of a single execution (one buy or one sell).
    Multiple TradeRecords make up a cycle.
    The recording layer (Layer 5) persists these to SQLite.
    """
    # Identification
    trade_id: str = ""
    strategy_name: str = ""
    token_ca: str = ""
    token_ticker: str = ""
    signal_type: str = ""           # SignalType value

    # Timing
    timestamp: datetime = None

    # Pricing
    fill_price: float = 0.0         # Indicator-level price (before slippage)
    execution_price: float = 0.0    # After slippage
    candle_close: float = 0.0       # Reference price (signal.price)
    market_cap: float = 0.0

    # Sizing
    usd_amount: float = 0.0         # Gross USD (buy amount or sell proceeds pre-fee)
    token_quantity: float = 0.0

    # Costs
    slippage_pct: float = 0.0
    slippage_usd: float = 0.0
    dex_fee_usd: float = 0.0
    priority_fee_usd: float = 0.0
    total_cost_usd: float = 0.0     # All fees combined

    # Net
    net_usd: float = 0.0            # What you actually spent (buy) or received (sell)

    # Portfolio state after this trade
    cash_after: float = 0.0
    positions_after: int = 0

    # Indicators at time of trade
    indicators: Optional[IndicatorSnapshot] = None
    reason: str = ""

    # Sell-specific
    sell_pct: float = 0.0           # partial fraction for BBDOWN partial, 1.0 for full


# ═══════════════════════════════════════════════════════════════
# PORTFOLIO MANAGER
# ═══════════════════════════════════════════════════════════════

@dataclass
class OpenPosition:
    """Tracks a single open position within a portfolio."""
    token_ca: str
    token_ticker: str
    strategy_name: str
    entry_price: float              # Execution price (after slippage)
    entry_fill_price: float         # Indicator-level price (before slippage)
    token_quantity: float
    invested_usd: float             # Total USD spent (including all fees)
    entry_timestamp: datetime = None
    entry_market_cap: float = 0.0

    # Running state
    current_price: float = 0.0
    unrealized_pnl: float = 0.0

    # Partial exit tracking (BBDOWN)
    tokens_remaining: float = 0.0   # Decremented on partial sells
    partial_proceeds: float = 0.0   # Accumulated proceeds from partial sells

    def __post_init__(self):
        self.tokens_remaining = self.token_quantity
        self.current_price = self.entry_price


class PortfolioManager:
    """
    Manages one strategy instance's simulated portfolio.
    Each strategy instance gets its own PortfolioManager.
    """

    def __init__(self, strategy_name: str, initial_balance: float = param("strategies.defaults.initial_balance")):
        self.strategy_name = strategy_name
        self.cash_balance = initial_balance
        self.initial_balance = initial_balance
        self.positions: Dict[str, OpenPosition] = {}  # token_ca → position

        # Tracking
        self.total_trades = 0
        self.total_cycles = 0
        self.peak_value = initial_balance
        self.total_fees_paid = 0.0
        self.total_slippage_cost = 0.0

        # A1: DB handle for crash-durable open-position persistence. Attached
        # by the orchestrator AFTER restore (so restore's direct repopulation
        # does not re-write rows). None during backtests / unit use → mutations
        # then behave exactly as before (no persistence side effects).
        self._db = None

    def attach_db(self, db):
        """A1: enable synchronous open-position persistence on mutations."""
        self._db = db

    @property
    def open_position_count(self) -> int:
        return len(self.positions)

    @property
    def deployed_capital(self) -> float:
        # Cost basis still at risk: partial-exit proceeds are already in cash.
        # (Fixed during the showcase review: summing the full invested_usd
        # double-counted BBDOWN partial proceeds in total_value while a
        # partially-exited position was open. Cycles/PF were never affected.)
        return sum(p.invested_usd - p.partial_proceeds for p in self.positions.values())

    @property
    def unrealized_pnl(self) -> float:
        pnl = 0.0
        for p in self.positions.values():
            # Rough estimate: current tokens * current price vs invested
            gross = p.tokens_remaining * p.current_price
            # Subtract estimated sell costs
            slip = get_slippage(p.entry_market_cap)
            net_est = gross * (1 - slip) * (1 - DEX_FEE_PCT) - PRIORITY_FEE_USD
            pnl += net_est - (p.invested_usd - p.partial_proceeds)
        return pnl

    @property
    def total_value(self) -> float:
        return self.cash_balance + self.deployed_capital + self.unrealized_pnl

    @property
    def max_drawdown_pct(self) -> float:
        if self.peak_value <= 0:
            return 0.0
        return (self.peak_value - self.total_value) / self.peak_value * 100

    def can_open_position(self, cost: float, max_positions: int = param("strategies.defaults.max_positions")) -> bool:
        if self.open_position_count >= max_positions:
            return False
        if self.cash_balance < cost:
            return False
        return True

    def has_position(self, token_ca: str) -> bool:
        return token_ca in self.positions

    def open_position(self, position: OpenPosition, total_cost: float):
        """Deduct cost from cash, register position."""
        self.cash_balance -= total_cost
        self.positions[position.token_ca] = position
        self.total_trades += 1
        # A1: persist synchronously. A fresh entry always has both exit flags
        # clear (the prior cycle's state was reset on close/open).
        if self._db is not None:
            self._db.upsert_open_position(
                position, portfolio=self, ts=datetime.now(timezone.utc),
                sma_sl_pending=0, sma_partial_done=0,
            )

    def record_partial_sell(self, token_ca: str, tokens_sold: float, net_proceeds: float):
        """Update position after a partial sell (BBDOWN partial exit at SMA)."""
        pos = self.positions.get(token_ca)
        if pos is None:
            return
        pos.tokens_remaining -= tokens_sold
        pos.partial_proceeds += net_proceeds
        self.cash_balance += net_proceeds
        self.total_trades += 1
        # A1: a partial sell IS the sma_partial_done transition — persist the
        # reduced tokens_held + the flag synchronously.
        if self._db is not None:
            self._db.upsert_open_position(
                pos, portfolio=self, ts=datetime.now(timezone.utc),
                sma_sl_pending=0, sma_partial_done=1,
            )

    def close_position(self, token_ca: str, net_proceeds: float):
        """Full exit: add proceeds to cash, remove position."""
        pos = self.positions.pop(token_ca, None)
        if pos is None:
            return
        self.cash_balance += net_proceeds
        self.total_trades += 1
        self.total_cycles += 1
        # Update peak
        val = self.total_value
        if val > self.peak_value:
            self.peak_value = val
        # A1: a full exit deletes the durable row synchronously.
        if self._db is not None:
            self._db.delete_open_position(
                self.strategy_name, token_ca,
                portfolio=self, ts=datetime.now(timezone.utc),
            )

    def snapshot(self) -> dict:
        return {
            "strategy_name": self.strategy_name,
            "cash_balance": round(self.cash_balance, 4),
            "open_positions": self.open_position_count,
            "deployed_capital": round(self.deployed_capital, 4),
            "total_value": round(self.total_value, 4),
            "total_trades": self.total_trades,
            "total_cycles": self.total_cycles,
            "peak_value": round(self.peak_value, 4),
            "total_fees_paid": round(self.total_fees_paid, 4),
        }

    def restore_from_snapshot(self, snap: dict):
        """
        Restore portfolio counters from a DB snapshot row.
        Called on startup to maintain continuity across restarts.

        Restores: cash_balance, total_trades, total_cycles, peak_value,
                  total_fees_paid, total_slippage.

        A1 / Issue B FIX: cash restores from the persisted **cash_balance**, NOT
        from total_value. The old code set cash = total_value (cash + unrealized)
        and wiped the positions dict — that promoted phantom unrealized PnL into
        real cash and orphaned the open buy, so cash drifted UP on every restart
        of a Restart=always service.

        Now open positions are persisted in the open_positions table and rebuilt
        separately (Orchestrator._restore_open_positions), so cash must stay the
        bare cash_balance; deployed + unrealized are recomputed from the restored
        positions. The snapshot written atomically at the last mutation keeps
        cash_balance consistent with those positions, so post-restore the
        identity total_value == cash + deployed + unrealized holds.
        """
        # Cash = persisted cash only (positions are restored separately).
        self.cash_balance = snap.get("cash_balance", self.initial_balance)
        self.total_trades = snap.get("total_trades", 0)
        self.total_cycles = snap.get("total_cycles", 0)
        self.peak_value = snap.get("peak_value", self.initial_balance)
        self.total_fees_paid = snap.get("total_fees_paid", 0.0)
        self.total_slippage_cost = snap.get("total_slippage", 0.0)
        # Cleared here; Orchestrator._restore_open_positions repopulates from
        # the open_positions table immediately after this call.
        self.positions.clear()


# ═══════════════════════════════════════════════════════════════
# BASE EXECUTOR (interface)
# ═══════════════════════════════════════════════════════════════

class BaseExecutor(ABC):
    """
    Abstract executor interface.
    PaperExecutor simulates fills. LiveExecutor (future) submits real txns.
    """

    @abstractmethod
    def execute_entry(
        self, signal: Signal, portfolio: PortfolioManager,
        position_size: float, max_positions: int, token_ticker: str,
    ) -> Optional[TradeRecord]:
        """Execute a buy. Returns TradeRecord or None if rejected."""
        ...

    @abstractmethod
    def execute_exit(
        self, signal: Signal, portfolio: PortfolioManager,
        token_ticker: str,
    ) -> Optional[TradeRecord]:
        """Execute a sell (full or partial). Returns TradeRecord or None."""
        ...


# ═══════════════════════════════════════════════════════════════
# PAPER EXECUTOR
# ═══════════════════════════════════════════════════════════════

class PaperExecutor(BaseExecutor):
    """
    Simulated execution engine for paper trading.
    
    Buy math:
      buy_amount      = position_size
      dex_fee         = buy_amount * DEX_FEE_PCT
      net_for_tokens  = buy_amount - dex_fee
      execution_price = fill_price * (1 + slippage)
      tokens          = net_for_tokens / execution_price
      total_cost      = buy_amount + PRIORITY_FEE_USD

    Sell math:
      gross = tokens * fill_price * (1 - slippage)
      dex_fee = gross * DEX_FEE_PCT
      net = gross - dex_fee - PRIORITY_FEE_USD
    """

    def __init__(self):
        self._trade_counter = 0

    def _next_trade_id(self, strategy_name: str) -> str:
        """Generate unique trade ID using timestamp + counter.
        Survives restarts without collisions."""
        import time
        self._trade_counter += 1
        ts = int(time.time() * 1000) % 10_000_000  # millisecond precision, 7 digits
        return f"{strategy_name}_T{ts}_{self._trade_counter:04d}"

    # ── BUY ──────────────────────────────────────────────────

    def execute_entry(
        self, signal: Signal, portfolio: PortfolioManager,
        position_size: float, max_positions: int, token_ticker: str,
    ) -> Optional[TradeRecord]:

        market_cap = signal.indicators.current_market_cap if signal.indicators else 0
        slippage_pct = get_slippage(market_cap)

        # Cost calculation
        buy_amount = position_size
        dex_fee = buy_amount * DEX_FEE_PCT
        net_for_tokens = buy_amount - dex_fee
        execution_price = signal.fill_price * (1 + slippage_pct)  # Fill + slippage
        tokens = net_for_tokens / execution_price
        total_cost = buy_amount + PRIORITY_FEE_USD

        # Slippage cost in USD
        slippage_usd = net_for_tokens - (net_for_tokens / (1 + slippage_pct))

        # Can we afford it?
        if not portfolio.can_open_position(total_cost, max_positions):
            logger.debug(
                f"[{signal.strategy_name}] Entry rejected: "
                f"cash=${portfolio.cash_balance:.2f}, positions={portfolio.open_position_count}"
            )
            return None

        # Create position
        position = OpenPosition(
            token_ca=signal.token_ca,
            token_ticker=token_ticker,
            strategy_name=signal.strategy_name,
            entry_price=execution_price,
            entry_fill_price=signal.fill_price,
            token_quantity=tokens,
            invested_usd=total_cost,
            entry_timestamp=signal.timestamp,
            entry_market_cap=market_cap,
        )

        portfolio.open_position(position, total_cost)
        portfolio.total_fees_paid += dex_fee + PRIORITY_FEE_USD
        portfolio.total_slippage_cost += slippage_usd

        trade = TradeRecord(
            trade_id=self._next_trade_id(signal.strategy_name),
            strategy_name=signal.strategy_name,
            token_ca=signal.token_ca,
            token_ticker=token_ticker,
            signal_type=signal.signal_type.value,
            timestamp=signal.timestamp,
            fill_price=signal.fill_price,
            execution_price=execution_price,
            candle_close=signal.price,
            market_cap=market_cap,
            usd_amount=buy_amount,
            token_quantity=tokens,
            slippage_pct=slippage_pct,
            slippage_usd=slippage_usd,
            dex_fee_usd=dex_fee,
            priority_fee_usd=PRIORITY_FEE_USD,
            total_cost_usd=dex_fee + PRIORITY_FEE_USD + slippage_usd,
            net_usd=-total_cost,  # V7.1 FIX: a BUY is cash OUT → negative
            cash_after=portfolio.cash_balance,
            positions_after=portfolio.open_position_count,
            indicators=signal.indicators,
            reason=signal.reason,
        )

        logger.info(
            f"[{signal.strategy_name}] BUY {token_ticker}: "
            f"{tokens:.4f} tokens @ ${execution_price:.8f} "
            f"(fill=${signal.fill_price:.8f} + {slippage_pct*100:.1f}% slip) "
            f"| cost=${total_cost:.2f} | cash=${portfolio.cash_balance:.2f}"
        )

        return trade

    # ── SELL ─────────────────────────────────────────────────

    def execute_exit(
        self, signal: Signal, portfolio: PortfolioManager,
        token_ticker: str,
    ) -> Optional[TradeRecord]:

        pos = portfolio.positions.get(signal.token_ca)
        if pos is None:
            logger.warning(
                f"[{signal.strategy_name}] Exit signal for {token_ticker} "
                f"but no position found"
            )
            return None

        market_cap = signal.indicators.current_market_cap if signal.indicators else pos.entry_market_cap
        slippage_pct = get_slippage(market_cap)

        # How many tokens to sell
        tokens_to_sell = pos.tokens_remaining * signal.sell_pct

        # Sell math
        execution_price = signal.fill_price * (1 - slippage_pct)
        gross_proceeds = tokens_to_sell * execution_price
        dex_fee = gross_proceeds * DEX_FEE_PCT
        net_proceeds = gross_proceeds - dex_fee - PRIORITY_FEE_USD

        # Slippage cost
        gross_no_slip = tokens_to_sell * signal.fill_price
        slippage_usd = gross_no_slip - gross_proceeds

        # Track fees
        portfolio.total_fees_paid += dex_fee + PRIORITY_FEE_USD
        portfolio.total_slippage_cost += slippage_usd

        is_partial = signal.sell_pct < 1.0

        if is_partial:
            portfolio.record_partial_sell(signal.token_ca, tokens_to_sell, net_proceeds)
        else:
            portfolio.close_position(signal.token_ca, net_proceeds)

        trade = TradeRecord(
            trade_id=self._next_trade_id(signal.strategy_name),
            strategy_name=signal.strategy_name,
            token_ca=signal.token_ca,
            token_ticker=token_ticker,
            signal_type=signal.signal_type.value,
            timestamp=signal.timestamp,
            fill_price=signal.fill_price,
            execution_price=execution_price,
            candle_close=signal.price,
            market_cap=market_cap,
            usd_amount=gross_proceeds,
            token_quantity=tokens_to_sell,
            slippage_pct=slippage_pct,
            slippage_usd=slippage_usd,
            dex_fee_usd=dex_fee,
            priority_fee_usd=PRIORITY_FEE_USD,
            total_cost_usd=dex_fee + PRIORITY_FEE_USD + slippage_usd,
            net_usd=net_proceeds,
            cash_after=portfolio.cash_balance,
            positions_after=portfolio.open_position_count,
            indicators=signal.indicators,
            reason=signal.reason,
            sell_pct=signal.sell_pct,
        )

        action = "PARTIAL SELL" if is_partial else "SELL"
        logger.info(
            f"[{signal.strategy_name}] {action} {token_ticker}: "
            f"{tokens_to_sell:.4f} tokens @ ${execution_price:.8f} "
            f"(fill=${signal.fill_price:.8f} - {slippage_pct*100:.1f}% slip) "
            f"| net=${net_proceeds:.2f} | cash=${portfolio.cash_balance:.2f}"
        )

        return trade
