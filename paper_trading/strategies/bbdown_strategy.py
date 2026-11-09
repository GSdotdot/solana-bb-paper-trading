"""
BBDOWN Strategy — Per-Instance Configurable Filters
===================================================
Mean reversion strategy: buy at lower BB touch, exit in 2 stages.

Per-instance parameters (constructor; values from config, redacted here):
  - min_vol_window_usd:           rolling-window USD volume floor
  - max_market_cap_entry:     entry market-cap ceiling
  - bbw_filter_enabled:       band-width window filter on/off (ablation axis)
  - bbw_min_pct / max_pct:    window used only when the filter is enabled
  - bearish_exit_cooldown_minutes: re-entry cooldown (0 disables)

The baseline ablation instance runs the previous-generation values side by
side with the current ones inside the same paper run (A/B under one regime).

FROZEN-INDICATOR / TICK-BASED design (on_tick):
  Band levels (LBB, SMA, UBB) are frozen at the last CLOSED candle. Each live
  tick is only compared against those frozen levels, so no signal can depend
  on information from a candle that has not finished yet.
  Entry:            live price <= LBB + filters  → fill at LBB level
  SMA partial exit: live price >= SMA            → sell a partial fraction at SMA
  UBB full exit:    live price ~>= UBB           → fill at UBB level
  MC hard stop:     live market_cap <= stop      → fill at live price

CANDLE-BASED (on_candle):
  2nd SMA cross-below: candle CLOSES below SMA → sets flag →
    next candle sells remaining at OPEN.

Cooldowns: bearish_exit_cooldown_minutes after 2nd SMA exit (per-instance);
           time + market-cap recovery requirement after a hard stop.
"""

from datetime import datetime
from typing import Dict, List, Optional
from collections import deque
import logging

from paper_trading.config.params import param
from paper_trading.core.models import (
    Candle, Signal, SignalType, IndicatorSnapshot, StrategyType, TickData
)
from paper_trading.core import indicators as ind
from paper_trading.strategies.token_state import TokenState

logger = logging.getLogger(__name__)

# ── Structural constants (NOT per-instance — these define algorithm shape) ──
NEAR_UBB_THRESHOLD = param("strategies.bbdown.near_ubb_threshold")   # proximity that counts as a UBB touch
VOLUME_AVG_CANDLES = param("strategies.volume_avg_candles")           # volume averaging window
PARTIAL_SELL_PCT = param("strategies.bbdown.partial_sell_pct")        # fraction sold at SMA


class BBDOWNStrategy:
    """
    Single BBDOWN strategy instance with fixed SMA/BB parameters.
    All filter thresholds and the bearish-exit cooldown are per-instance
    constructor params (driven by StrategyConfig in orchestrator/config.py).
    """

    def __init__(
        self,
        name: str,
        sma_period: int = param("strategies.bbdown.sma_period"),
        bb_multiplier: float = param("strategies.bbdown.bb_multiplier"),
        position_size: float = param("strategies.defaults.position_size"),
        max_positions: int = param("strategies.bbdown.max_positions"),
        max_market_cap: float = param("strategies.defaults.max_market_cap"),
        # ── per-instance filter / cooldown params ──────────────
        min_vol_window_usd: float = param("strategies.defaults.bbdown.min_vol_window_usd"),
        max_market_cap_entry: float = param("strategies.defaults.bbdown.max_market_cap_entry"),
        bbw_filter_enabled: bool = False,
        bbw_min_pct: float = param("strategies.defaults.bbdown.bbw_min_pct"),
        bbw_max_pct: float = param("strategies.defaults.bbdown.bbw_max_pct"),
        bearish_exit_cooldown_minutes: float = param("strategies.defaults.bbdown.bearish_exit_cooldown_minutes"),
    ):
        self.name = name
        self.strategy_type = StrategyType.BBDOWN
        self.sma_period = sma_period
        self.bb_multiplier = bb_multiplier
        self.position_size = position_size
        self.max_positions = max_positions
        # Note: max_market_cap is legacy/display only for BBDOWN.
        # Entry decisions use max_market_cap_entry (gate 3).
        self.max_market_cap = max_market_cap

        # ── per-instance filter / cooldown state ──────────────
        self.min_vol_window_usd = min_vol_window_usd
        self.max_market_cap_entry = max_market_cap_entry
        self.bbw_filter_enabled = bbw_filter_enabled
        self.bbw_min_pct = bbw_min_pct
        self.bbw_max_pct = bbw_max_pct
        self.bearish_exit_cooldown_minutes = bearish_exit_cooldown_minutes

        self.token_states: Dict[str, TokenState] = {}

        # V6: Peak concurrent position tracking
        self.peak_concurrent_positions: int = 0

        # V6: Skip tracking
        self.entries_skipped: int = 0
        self.entries_taken: int = 0
        self._skip_log: deque = deque(maxlen=500)   # V7.1: O(1) bounded

    # ════════════════════════════════════════════════════════════
    # TOKEN MANAGEMENT
    # ════════════════════════════════════════════════════════════

    def add_token(self, token_ca: str, ticker: str = ""):
        if token_ca not in self.token_states:
            self.token_states[token_ca] = TokenState(
                token_ca=token_ca, token_ticker=ticker
            )

    def remove_token(self, token_ca: str) -> bool:
        state = self.token_states.get(token_ca)
        if state is None:
            return True
        if state.has_position:
            state.flagged_for_removal = True
            return False
        del self.token_states[token_ca]
        return True

    def seed_token_history(self, token_ca: str, candles: List[Candle]):
        state = self.token_states.get(token_ca)
        if state is None:
            return
        state.seed_history(candles)
        state.update_indicators(self.sma_period, self.bb_multiplier)

    @property
    def open_position_count(self) -> int:
        return sum(1 for s in self.token_states.values() if s.has_position)

    def _update_peak_positions(self):
        current = self.open_position_count
        if current > self.peak_concurrent_positions:
            self.peak_concurrent_positions = current

    # ════════════════════════════════════════════════════════════
    # LIVE TICK PROCESSING
    # ════════════════════════════════════════════════════════════

    def on_tick(self, tick: TickData) -> List[Signal]:
        """Process a live WebSocket price tick."""
        state = self.token_states.get(tick.token_ca)
        if state is None:
            return []

        if not state.is_ready:
            return []

        finalized_ind = state.indicators
        if finalized_ind is None:
            return []

        finalized_ind = IndicatorSnapshot(
            sma=finalized_ind.sma,
            bb_upper=finalized_ind.bb_upper,
            bb_lower=finalized_ind.bb_lower,
            bb_width_pct=finalized_ind.bb_width_pct,
            rsi=finalized_ind.rsi,
            volume_avg_n=finalized_ind.volume_avg_n,
            current_close=tick.price,
            current_volume=finalized_ind.current_volume,
            current_market_cap=tick.market_cap,
        )

        if state.has_position:
            exit_signal = self._check_tick_exits(state, tick, finalized_ind)
            if exit_signal is not None:
                return [exit_signal]
            return []

        if state.tick_entry_fired:
            return []

        entry_signal = self._check_tick_entry(state, tick, finalized_ind)
        if entry_signal is not None:
            state.tick_entry_fired = True
            self.entries_taken += 1
            self._update_peak_positions()
            return [entry_signal]

        return []

    # ════════════════════════════════════════════════════════════
    # TICK-BASED EXIT LOGIC
    # ════════════════════════════════════════════════════════════

    def _check_tick_exits(
        self, state: TokenState, tick: TickData, live_ind: IndicatorSnapshot
    ) -> Optional[Signal]:
        """
        Check exit conditions in priority order:
        1. MC Hard Stop
        2. SMA partial
        3. UBB full exit
        """
        # ── 1. MC HARD STOP ──────────────────────────────────────
        if state.hard_stop_active and tick.market_cap > 0:
            if tick.market_cap <= state.hard_stop_mc:
                logger.info(
                    f"[{self.name}] HARD STOP on {state.token_ticker}: "
                    f"MC ${tick.market_cap:,.0f} <= stop ${state.hard_stop_mc:,.0f}"
                )
                signal = Signal(
                    signal_type=SignalType.EXIT_HARD_STOP,
                    strategy_name=self.name,
                    token_ca=state.token_ca,
                    timestamp=tick.timestamp,
                    price=tick.price,
                    fill_price=tick.price,
                    sell_pct=1.0,
                    indicators=live_ind,
                    reason=f"MC hard stop: {tick.market_cap:,.0f} <= {state.hard_stop_mc:,.0f}",
                )
                state.set_hard_stop_recovery(tick.timestamp)
                state.close_position()
                return signal

        # ── Check if SMA partial exit already done ───────────────
        if not state.sma_partial_done:
            # Edge case: UBB reached BEFORE SMA touch — force both
            if tick.price >= live_ind.bb_upper * (1 - NEAR_UBB_THRESHOLD):
                logger.info(
                    f"[{self.name}] UBB before SMA on {state.token_ticker}: "
                    f"price {tick.price:.8f} >= UBB {live_ind.bb_upper:.8f}, "
                    f"forcing SMA partial first"
                )
                signal = Signal(
                    signal_type=SignalType.EXIT_SMA_PARTIAL,
                    strategy_name=self.name,
                    token_ca=state.token_ca,
                    timestamp=tick.timestamp,
                    price=tick.price,
                    fill_price=live_ind.bb_upper,
                    sell_pct=PARTIAL_SELL_PCT,
                    indicators=live_ind,
                    reason=f"SMA partial (forced by UBB): price >= UBB {live_ind.bb_upper:.8f}",
                )
                return signal

            # Normal SMA partial: live price crosses above SMA
            if tick.price >= live_ind.sma:
                logger.info(
                    f"[{self.name}] SMA partial exit on {state.token_ticker}: "
                    f"price {tick.price:.8f} >= SMA {live_ind.sma:.8f}"
                )
                signal = Signal(
                    signal_type=SignalType.EXIT_SMA_PARTIAL,
                    strategy_name=self.name,
                    token_ca=state.token_ca,
                    timestamp=tick.timestamp,
                    price=tick.price,
                    fill_price=live_ind.sma,
                    sell_pct=PARTIAL_SELL_PCT,
                    indicators=live_ind,
                    reason=f"SMA partial: price {tick.price:.8f} >= SMA {live_ind.sma:.8f}",
                )
                return signal

        else:
            # SMA partial already done — check UBB full exit
            if tick.price >= live_ind.bb_upper * (1 - NEAR_UBB_THRESHOLD):
                logger.info(
                    f"[{self.name}] UBB exit on {state.token_ticker}: "
                    f"price {tick.price:.8f} >= UBB {live_ind.bb_upper:.8f}"
                )
                signal = Signal(
                    signal_type=SignalType.EXIT_UBB,
                    strategy_name=self.name,
                    token_ca=state.token_ca,
                    timestamp=tick.timestamp,
                    price=tick.price,
                    fill_price=live_ind.bb_upper,
                    sell_pct=1.0,
                    indicators=live_ind,
                    reason=f"UBB exit: price {tick.price:.8f} near UBB {live_ind.bb_upper:.8f}",
                )
                state.close_position()
                return signal

        return None

    # ════════════════════════════════════════════════════════════
    # TICK-BASED ENTRY LOGIC (with environment filter)
    # ════════════════════════════════════════════════════════════

    def _check_tick_entry(
        self, state: TokenState, tick: TickData, live_ind: IndicatorSnapshot
    ) -> Optional[Signal]:
        """
        Entry conditions (all must pass; thresholds redacted → config):
        1. Not in cooldown
        2. Not in hard stop recovery
        3. tick.market_cap <= max_market_cap_entry          [XXXX]
        4. Max positions not reached
        5. rolling avg volume >= min_vol_window_usd             [XXXX]
        6. Live price <= frozen LBB (mean-reversion entry trigger)
        7. (Optional) BBW within [XXXX, XXXX) — only if bbw_filter_enabled

        Skip logging:
          BBW failures are logged with reason "bbw_low" / "bbw_high" — but only
          AFTER LBB touch is confirmed, to avoid log spam from non-signal tokens.
          The vol filter uses silent return on failure (earlier pattern;
          if vol_low logging is wanted in future, defer behind LBB check like BBW).
        """
        now = tick.timestamp

        # Gate 1: Cooldown
        if state.is_in_cooldown(now):
            return None

        # Gate 2: Hard stop recovery
        if not state.can_enter_after_hard_stop(now, tick.market_cap):
            return None

        # Gate 3: Market cap ceiling (per-instance)
        if tick.market_cap > self.max_market_cap_entry or tick.market_cap <= 0:
            return None

        # Gate 4: Position limit
        if self.open_position_count >= self.max_positions:
            return None

        # Gate 5: Volume filter (per-instance threshold)
        # Computed from finalized candles. Silent return on failure.
        if len(state.volumes) >= VOLUME_AVG_CANDLES:
            pre_entry_vols = state.volumes[-VOLUME_AVG_CANDLES:]
            avg_vol = sum(pre_entry_vols) / len(pre_entry_vols)
        elif len(state.volumes) >= 1:
            avg_vol = sum(state.volumes) / len(state.volumes)
        else:
            return None

        if avg_vol < self.min_vol_window_usd:
            return None

        # Gate 6: LBB touch — live price pierces below Lower BB
        if tick.price > live_ind.bb_lower:
            return None

        # ── LBB touched! ────────────────────────────────────────────────
        # Past this point, any rejection is a "genuine LBB signal got
        # filtered out" event — worth logging.

        # Gate 7: BBW environment filter (per-instance, toggleable ablation axis)
        if self.bbw_filter_enabled:
            if live_ind.bb_width_pct < self.bbw_min_pct:
                self._log_skip(state, tick, live_ind, avg_vol, "bbw_low",
                               f"BBW {live_ind.bb_width_pct:.1f}% below window")
                return None

            if live_ind.bb_width_pct >= self.bbw_max_pct:
                self._log_skip(state, tick, live_ind, avg_vol, "bbw_high",
                               f"BBW {live_ind.bb_width_pct:.1f}% above window")
                return None

        # ── ALL GATES PASSED → ENTRY SIGNAL ──────────────────
        logger.info(
            f"[{self.name}] ENTRY on {state.token_ticker}: "
            f"price {tick.price:.8f} <= LBB {live_ind.bb_lower:.8f} | "
            f"MC ${tick.market_cap:,.0f} | VolNavg ${avg_vol:,.0f} | "
            f"BBW {live_ind.bb_width_pct:.1f}%"
        )

        return Signal(
            signal_type=SignalType.ENTRY,
            strategy_name=self.name,
            token_ca=state.token_ca,
            timestamp=tick.timestamp,
            price=tick.price,
            fill_price=live_ind.bb_lower,
            indicators=live_ind,
            reason=(
                f"LBB entry: price {tick.price:.8f} <= LBB {live_ind.bb_lower:.8f} | "
                f"BB width {live_ind.bb_width_pct:.1f}% | VolN ${avg_vol:,.0f}"
            ),
        )

    def _log_skip(
        self, state: TokenState, tick: TickData,
        live_ind: IndicatorSnapshot, avg_vol: float,
        skip_reason: str, detail: str,
    ):
        """Log a skipped entry with full indicator values."""
        self.entries_skipped += 1

        skip_record = {
            "token_ca": state.token_ca,
            "ticker": state.token_ticker,
            "strategy_name": self.name,
            "timestamp": tick.timestamp.isoformat(),
            "skip_reason": skip_reason,
            "detail": detail,
            "price": tick.price,
            "market_cap": tick.market_cap,
            "sma": live_ind.sma,
            "bb_upper": live_ind.bb_upper,
            "bb_lower": live_ind.bb_lower,
            "bb_width_pct": live_ind.bb_width_pct,
            "rsi": live_ind.rsi,
            "volume_avg_n": avg_vol,
        }
        self._skip_log.append(skip_record)
        # V7.1: deque(maxlen=500) self-bounds — no manual trim needed.

        logger.info(
            f"[{self.name}] ENTRY SKIPPED on {state.token_ticker}: "
            f"{detail} | VolN ${avg_vol:,.0f} | MC ${tick.market_cap:,.0f}"
        )

    def get_skip_log(self) -> List[dict]:
        return list(self._skip_log)

    # ════════════════════════════════════════════════════════════
    # CANDLE-BASED PROCESSING (finalized candles only)
    # ════════════════════════════════════════════════════════════

    def on_candle(self, candle: Candle) -> List[Signal]:
        """Process a finalized candle."""
        state = self.token_states.get(candle.token_ca)
        if state is None:
            return [self._no_action(candle)]

        state.tick_entry_fired = False

        state.add_candle(candle)
        state.update_indicators(self.sma_period, self.bb_multiplier)

        if not state.is_ready:
            return [self._no_action(candle)]

        if state.has_position and state.sma_partial_done:
            # 2nd SMA cross-below EXECUTION (flag from previous candle)
            if state.prev_candle_closed_below_sma:
                logger.info(
                    f"[{self.name}] 2nd SMA bearish exit on {state.token_ticker}: "
                    f"prev close < SMA, selling at open {candle.open:.8f}"
                )
                signal = Signal(
                    signal_type=SignalType.EXIT_SMA_FULL,
                    strategy_name=self.name,
                    token_ca=state.token_ca,
                    timestamp=candle.timestamp,
                    price=candle.open,
                    fill_price=candle.open,
                    sell_pct=1.0,
                    indicators=state.indicators,
                    reason=f"2nd SMA cross-below: sell remaining at open {candle.open:.8f}",
                )
                # Cooldown is per-instance; 0 disables it entirely.
                if self.bearish_exit_cooldown_minutes > 0:
                    state.set_cooldown(self.bearish_exit_cooldown_minutes, candle.timestamp)
                state.close_position()
                return [signal]

            # 2nd SMA cross-below DETECTION
            self._update_sma_cross_below_tracking(state, candle)

        return [self._no_action(candle)]

    def _update_sma_cross_below_tracking(self, state: TokenState, candle: Candle):
        if not state.sma_partial_done:
            state.prev_candle_closed_below_sma = False
            return
        ind_snap = state.indicators
        if ind_snap is None:
            return
        state.prev_candle_closed_below_sma = (candle.close < ind_snap.sma)

    # ════════════════════════════════════════════════════════════
    # EXECUTION CALLBACKS
    # ════════════════════════════════════════════════════════════

    def notify_entry_filled(
        self, token_ca: str, fill_price: float, tokens: float,
        invested: float, timestamp: datetime, market_cap: float
    ):
        state = self.token_states.get(token_ca)
        if state is None:
            return
        state.open_position(fill_price, tokens, invested, timestamp, market_cap)
        state.calculate_hard_stop_mc(market_cap)
        self._update_peak_positions()

    def notify_partial_exit_filled(
        self, token_ca: str, tokens_sold: float, price: float, timestamp: datetime
    ):
        state = self.token_states.get(token_ca)
        if state is None:
            return
        state.record_partial_sell(tokens_sold, price, timestamp)

    def notify_full_exit_filled(self, token_ca: str):
        state = self.token_states.get(token_ca)
        if state is None:
            return
        if state.has_position:
            state.close_position()
        if state.flagged_for_removal:
            del self.token_states[token_ca]

    # ════════════════════════════════════════════════════════════
    # HELPERS
    # ════════════════════════════════════════════════════════════

    def _no_action(self, candle: Candle) -> Signal:
        return Signal(
            signal_type=SignalType.NO_ACTION,
            strategy_name=self.name,
            token_ca=candle.token_ca,
            timestamp=candle.timestamp,
            price=candle.close,
        )

    def get_summary(self) -> dict:
        return {
            "name": self.name,
            "type": "BBDOWN",
            "sma": self.sma_period,
            "bb": self.bb_multiplier,
            "tokens_tracked": len(self.token_states),
            "tokens_ready": sum(1 for s in self.token_states.values() if s.is_ready),
            "open_positions": self.open_position_count,
            "peak_concurrent_positions": self.peak_concurrent_positions,
            "entries_taken": self.entries_taken,
            "entries_skipped": self.entries_skipped,
            "positions_with_partial": sum(
                1 for s in self.token_states.values()
                if s.has_position and s.sma_partial_done
            ),
        }
