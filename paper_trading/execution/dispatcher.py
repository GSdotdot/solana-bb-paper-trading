"""
Signal Dispatcher
=================
Routes signals from strategies through the executor and calls
the appropriate strategy callbacks after execution.

This is the integration bridge between:
  - Strategy Engine (Layer 3): emits Signal objects
  - Execution Engine (Layer 4): processes buy/sell
  - Strategy callbacks: notify_entry_filled, notify_partial_exit_filled, etc.

Usage:
    dispatcher = SignalDispatcher(executor)
    dispatcher.register(strategy, portfolio)
    
    # On each candle:
    signals = strategy.on_candle(candle)
    trades = dispatcher.process_signals(signals)
"""

from typing import Dict, List, Optional, Tuple, Union
import logging

from paper_trading.config.params import param
from paper_trading.core.models import Signal, SignalType
from paper_trading.execution.executor import (
    BaseExecutor, PortfolioManager, TradeRecord
)
from paper_trading.strategies.bbdown_strategy import BBDOWNStrategy
from paper_trading.strategies.bbup_strategy import BBUPStrategy

logger = logging.getLogger(__name__)

StrategyInstance = Union[BBDOWNStrategy, BBUPStrategy]


class SignalDispatcher:
    """
    Processes signals and coordinates between strategies, executor, and portfolios.
    """

    def __init__(self, executor: BaseExecutor):
        self.executor = executor
        # strategy_name → (strategy_instance, portfolio, position_size, max_positions)
        self._registry: Dict[str, Tuple[StrategyInstance, PortfolioManager, float, int]] = {}

    def register(
        self, strategy: StrategyInstance, portfolio: PortfolioManager,
        position_size: float = param("strategies.defaults.position_size"),
        max_positions: int = param("strategies.defaults.max_positions"),
    ):
        """Register a strategy + its portfolio for signal processing."""
        self._registry[strategy.name] = (strategy, portfolio, position_size, max_positions)

    def process_signals(self, signals: List[Signal]) -> List[TradeRecord]:
        """
        Process a batch of signals. Returns list of executed trades.
        Handles entry, partial exit, and full exit with proper callbacks.
        """
        trades = []
        for signal in signals:
            if not signal.is_actionable:
                continue

            reg = self._registry.get(signal.strategy_name)
            if reg is None:
                logger.warning(f"Signal from unregistered strategy: {signal.strategy_name}")
                continue

            strategy, portfolio, pos_size, max_pos = reg

            if signal.is_entry:
                trade = self._handle_entry(signal, strategy, portfolio, pos_size, max_pos)
            elif signal.is_exit:
                trade = self._handle_exit(signal, strategy, portfolio)
            else:
                continue

            if trade is not None:
                trades.append(trade)

        return trades

    def _handle_entry(
        self, signal: Signal, strategy: StrategyInstance,
        portfolio: PortfolioManager, position_size: float, max_positions: int,
    ) -> Optional[TradeRecord]:
        """Execute entry and notify strategy."""
        # Get ticker from strategy's token state
        state = strategy.token_states.get(signal.token_ca)
        ticker = state.token_ticker if state else signal.token_ca[:8]

        trade = self.executor.execute_entry(
            signal, portfolio, position_size, max_positions, ticker,
        )
        if trade is None:
            return None

        # Notify strategy of fill.
        # invested is a POSITIVE magnitude for the position ledger
        # (TokenState.cycle_invested / cost basis). trade.net_usd is the SIGNED
        # cash-ledger figure — negative for a BUY since the V7.1 Issue-A fix
        # (executor.py:392) — so pass abs(). This matches the cycle table's
        # entry_invested = abs(entry.net_usd) (database.py:391); a negative
        # cycle_invested would corrupt any pnl_pct that divides by it.
        mc = signal.indicators.current_market_cap if signal.indicators else 0
        strategy.notify_entry_filled(
            token_ca=signal.token_ca,
            fill_price=trade.execution_price,
            tokens=trade.token_quantity,
            invested=abs(trade.net_usd),
            timestamp=signal.timestamp,
            market_cap=mc,
        )
        return trade

    def _handle_exit(
        self, signal: Signal, strategy: StrategyInstance,
        portfolio: PortfolioManager,
    ) -> Optional[TradeRecord]:
        """Execute exit (partial or full) and notify strategy."""
        state = strategy.token_states.get(signal.token_ca)
        ticker = state.token_ticker if state else signal.token_ca[:8]

        trade = self.executor.execute_exit(signal, portfolio, ticker)
        if trade is None:
            return None

        is_partial = signal.sell_pct < 1.0

        if is_partial:
            # BBDOWN partial SMA exit
            if hasattr(strategy, 'notify_partial_exit_filled'):
                strategy.notify_partial_exit_filled(
                    token_ca=signal.token_ca,
                    tokens_sold=trade.token_quantity,
                    price=trade.execution_price,
                    timestamp=signal.timestamp,
                )
        else:
            # Full exit
            strategy.notify_full_exit_filled(signal.token_ca)

        return trade

    def get_all_snapshots(self) -> List[dict]:
        """Get portfolio snapshots for all registered strategies."""
        snapshots = []
        for name, (_, portfolio, _, _) in self._registry.items():
            snapshots.append(portfolio.snapshot())
        return snapshots
