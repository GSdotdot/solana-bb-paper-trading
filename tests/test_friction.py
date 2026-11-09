"""
Friction model: fee + slippage + priority fee (execution/executor.py).

Values come from tests/test_config.yaml — ILLUSTRATIVE, not the tuned ones.
The tests check the *structure* of the cost model: tier selection, the buy
and sell arithmetic, and that a flat-price round trip loses exactly the
modelled friction.
"""
from datetime import datetime, timezone
from math import isclose

from paper_trading.config.params import param
from paper_trading.core.models import IndicatorSnapshot, Signal, SignalType
from paper_trading.execution.executor import PaperExecutor, PortfolioManager, get_slippage

FEE = param("friction.dex_fee_pct")
PRIO = param("friction.priority_fee_usd")
THR = param("friction.slippage_threshold_mc")
BELOW = param("friction.slippage_below")
ABOVE = param("friction.slippage_above")
SIZE = param("strategies.defaults.position_size")
T0 = datetime(2026, 1, 1, tzinfo=timezone.utc)
CA = "TOKEN_A_pump"


def _signal(kind, price, mc):
    ind = IndicatorSnapshot(sma=price, bb_upper=price * 1.1, bb_lower=price * 0.9,
                            current_close=price, current_market_cap=mc)
    return Signal(signal_type=kind, strategy_name="T", token_ca=CA, timestamp=T0,
                  price=price, fill_price=price, indicators=ind)


def test_slippage_is_tiered_by_market_cap():
    assert get_slippage(THR - 1) == BELOW
    assert get_slippage(THR) == ABOVE


def test_buy_applies_fee_slippage_and_priority_fee():
    pf = PortfolioManager("T")
    start = pf.cash_balance
    price, mc = 0.002, THR / 2
    trade = PaperExecutor().execute_entry(_signal(SignalType.ENTRY, price, mc), pf, SIZE, 5, "TKN")

    exec_price = price * (1 + BELOW)
    assert isclose(trade.execution_price, exec_price)
    assert isclose(trade.token_quantity, SIZE * (1 - FEE) / exec_price)
    assert isclose(pf.cash_balance, start - (SIZE + PRIO))
    assert trade.net_usd < 0          # a buy is cash out


def test_flat_price_round_trip_loses_exactly_the_modelled_friction():
    pf = PortfolioManager("T")
    start = pf.cash_balance
    price, mc = 0.002, THR * 2        # upper slippage tier
    ex = PaperExecutor()
    buy = ex.execute_entry(_signal(SignalType.ENTRY, price, mc), pf, SIZE, 5, "TKN")
    ex.execute_exit(_signal(SignalType.EXIT_UBB, price, mc), pf, "TKN")

    tokens = buy.token_quantity
    proceeds = tokens * price * (1 - ABOVE) * (1 - FEE) - PRIO
    expected_pnl = proceeds - (SIZE + PRIO)
    assert isclose(pf.cash_balance - start, expected_pnl, rel_tol=1e-9)
    assert pf.cash_balance < start    # friction alone never makes money
    assert CA not in pf.positions


def test_total_value_does_not_double_count_partial_exit_proceeds():
    """After a BBDOWN partial exit, the proceeds sit in cash; the open
    remainder must be valued once (mark-to-market net of exit costs)."""
    pf = PortfolioManager("T")
    price, mc = 0.002, THR / 2
    ex = PaperExecutor()
    ex.execute_entry(_signal(SignalType.ENTRY, price, mc), pf, SIZE, 5, "TKN")
    part = _signal(SignalType.EXIT_SMA_PARTIAL, price, mc)
    part.sell_pct = param("strategies.bbdown.partial_sell_pct")
    ex.execute_exit(part, pf, "TKN")

    pos = pf.positions[CA]
    mark = pos.current_price            # marked at last price (entry exec price until a tick arrives)
    remaining_net = pos.tokens_remaining * mark * (1 - BELOW) * (1 - FEE) - PRIO
    assert isclose(pf.total_value, pf.cash_balance + remaining_net, rel_tol=1e-9)
