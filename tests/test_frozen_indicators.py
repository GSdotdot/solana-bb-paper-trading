"""
Frozen-indicator / tick-based entry design (the lookahead-bias fix).

Band levels are computed ONLY from closed candles. Live ticks are compared
against those frozen levels and can never move them; a signal fires the
moment a tick crosses a frozen level and fills at that level.

Synthetic prices; parameters from tests/test_config.yaml (ILLUSTRATIVE).
"""
from datetime import datetime, timedelta, timezone

from paper_trading.core.models import Candle, SignalType, TickData, Timeframe
from paper_trading.strategies.bbdown_strategy import BBDOWNStrategy
from paper_trading.strategies.bbup_strategy import BBUPStrategy

T0 = datetime(2026, 1, 1, tzinfo=timezone.utc)
MC = 123_000.0
CLOSES = [1.00, 1.10, 0.95, 1.08, 0.97, 1.06, 0.98, 1.05, 0.96]   # oscillating, last close low


def _candles(ca, tf, closes, start=0):
    out = []
    for i, c in enumerate(closes):
        o = c * 0.99
        out.append(Candle(token_ca=ca, timeframe=tf,
                          timestamp=T0 + timedelta(minutes=start + i),
                          open=o, high=max(o, c) * 1.01, low=min(o, c) * 0.99,
                          close=c, volume=4_321.0, market_cap=MC))
    return out


def _tick(ca, price, minute):
    return TickData(token_ca=ca, price=price, market_cap=MC,
                    timestamp=T0 + timedelta(minutes=minute, seconds=30))


def _feed(strategy, candles):
    for c in candles:
        strategy.on_candle(c)


# ── BBDOWN: tick below frozen LBB → entry filled AT the LBB ──────────

def test_bbdown_enters_on_tick_through_frozen_lower_band():
    ca = "TOKEN_C_pump"
    s = BBDOWNStrategy(name="BBDOWN_T")
    s.add_token(ca, "C")
    _feed(s, _candles(ca, Timeframe.FIVE_MIN, CLOSES))
    state = s.token_states[ca]
    frozen = state.indicators
    lbb = frozen.bb_lower

    assert s.on_tick(_tick(ca, lbb * 1.01, 10)) == []           # above LBB: nothing
    sigs = s.on_tick(_tick(ca, lbb * 0.90, 10))                 # pierces LBB
    assert len(sigs) == 1 and sigs[0].signal_type == SignalType.ENTRY
    assert sigs[0].fill_price == lbb                            # fill at the level, not the tick

    # Ticks never move the bands: indicators are identical after the ticks.
    assert state.indicators is frozen and state.indicators.bb_lower == lbb
    # One entry per candle interval (tick entry lock).
    assert s.on_tick(_tick(ca, lbb * 0.80, 10)) == []


def test_bands_only_change_when_a_candle_closes():
    ca = "TOKEN_D_pump"
    s = BBDOWNStrategy(name="BBDOWN_T")
    s.add_token(ca, "D")
    _feed(s, _candles(ca, Timeframe.FIVE_MIN, CLOSES))
    before = s.token_states[ca].indicators
    for m in range(5):                                          # a burst of extreme ticks
        s.on_tick(_tick(ca, 10.0, 20 + m))
    assert s.token_states[ca].indicators is before
    _feed(s, _candles(ca, Timeframe.FIVE_MIN, [1.02], start=len(CLOSES)))
    assert s.token_states[ca].indicators is not before          # new close → new frozen levels


# ── BBUP: cross-up vs frozen SMA, fill at SMA, phantom-cycle guard ──

def test_bbup_cross_up_fills_at_frozen_sma_and_guard_blocks_phantom_reentry():
    ca = "TOKEN_E_pump"
    s = BBUPStrategy(name="BBUP_T", body_filter_enabled=False, sma_sl_cooldown_minutes=0)
    s.add_token(ca, "E")
    _feed(s, _candles(ca, Timeframe.ONE_MIN, CLOSES))
    ind = s.token_states[ca].indicators
    assert CLOSES[-1] < ind.sma                                 # last close below SMA

    assert s.on_tick(_tick(ca, ind.sma * 0.99, 10)) == []        # not crossed yet
    sigs = s.on_tick(_tick(ca, ind.sma * 1.01, 10))
    assert len(sigs) == 1 and sigs[0].signal_type == SignalType.ENTRY
    assert sigs[0].fill_price == ind.sma
    s.notify_entry_filled(ca, ind.sma, 1.0, 10.0, T0, MC)

    # Take profit at the frozen upper band
    exit_sigs = s.on_tick(_tick(ca, ind.bb_upper * 1.001, 11))
    assert exit_sigs and exit_sigs[0].signal_type == SignalType.EXIT_UBB
    s.notify_full_exit_filled(ca)

    # New candle resets the per-candle lock; price is still above SMA, so the
    # phantom-cycle guard must block re-entry until a tick below SMA is seen.
    _feed(s, _candles(ca, Timeframe.ONE_MIN, [0.97], start=len(CLOSES)))
    sma2 = s.token_states[ca].indicators.sma
    assert s.on_tick(_tick(ca, sma2 * 1.05, 12)) == []
    assert s.token_states[ca].bbup_awaiting_cross_below
    s.on_tick(_tick(ca, sma2 * 0.95, 12))                       # genuine cross below
    assert not s.token_states[ca].bbup_awaiting_cross_below
