"""
BBUP Strategy — Per-Instance Body Filter + Cooldown Toggle
==========================================================
Momentum strategy: buy on SMA cross-up, target upper BB, stop at SMA.

Per-instance parameters (constructor; values from config, redacted here):
  - body_filter_enabled:      trigger-candle body filter on/off (ablation axis)
  - body_ratio_min:           body/range ratio threshold            [XXXX]
  - require_bullish_trigger:  trigger candle must close > open
  - sma_sl_cooldown_minutes:  re-entry cooldown after SMA stop (0 disables)

The baseline ablation instances disable the body filter to A/B against the
current settings inside the same paper run.

NOTE: The `bbup_awaiting_cross_below` flag stays unchanged regardless of
cooldown setting — it is correctness (phantom-cycle bug fix), not tuning.

FROZEN-INDICATOR / TICK-BASED design (on_tick):
  Indicators are frozen at the last CLOSED candle; live ticks only test
  whether price has crossed those frozen levels.
  Entry: live price crosses above frozen SMA
         + band-width gate + RSI gate + volume gate     [thresholds: XXXX]
         + volume environment window                     [XXXX, XXXX)
         + optional trigger-candle body filter
         → fill at SMA level
  UBB exit: live price >= UBB → fill at UBB level

CANDLE-BASED (on_candle):
  SMA SL: candle CLOSES below SMA → pending → next candle sell at OPEN
  History + indicator refresh from finalized data.

Phantom cycle prevention: after any exit, block re-entry until
a tick below SMA is observed (genuine new cross-up needed).
"""

from datetime import datetime, timedelta
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

# Filter thresholds (tuned privately — see config.example.yaml → strategies.bbup)
BB_WIDTH_MIN_PCT = param("strategies.bbup.bb_width_min_pct")
MIN_RSI_ENTRY = param("strategies.bbup.min_rsi_entry")
MIN_AVG_VOLUME_USD = param("strategies.bbup.min_avg_volume_usd")     # base volume gate
VOLUME_AVG_CANDLES = param("strategies.volume_avg_candles")

# Environment filter: avg volume must sit inside a window
ENV_MIN_VOLUME_USD = param("strategies.bbup.env_min_volume_usd")
ENV_MAX_VOLUME_USD = param("strategies.bbup.env_max_volume_usd")

# Cycle limits
MAX_CYCLES_PER_TOKEN = param("strategies.bbup.max_cycles_per_token")
MAX_CYCLES_COOLDOWN_HOURS = param("strategies.bbup.max_cycles_cooldown_hours")


class BBUPStrategy:
    """
    Single BBUP strategy instance with fixed SMA/BB parameters.
    SMA SL cooldown and trigger-candle body filter are per-instance
    constructor params (driven by StrategyConfig in orchestrator/config.py).
    """

    def __init__(
        self,
        name: str,
        sma_period: int = param("strategies.bbup.sma_period"),
        bb_multiplier: float = param("strategies.bbup.bb_multiplier"),
        position_size: float = param("strategies.defaults.position_size"),
        max_positions: int = param("strategies.bbup.max_positions"),
        max_market_cap: float = param("strategies.defaults.max_market_cap"),
        min_market_cap: float = param("strategies.defaults.min_market_cap"),
        # ── per-instance filter / cooldown params ──────────────
        sma_sl_cooldown_minutes: float = param("strategies.defaults.bbup.sma_sl_cooldown_minutes"),
        body_filter_enabled: bool = True,
        body_ratio_min: float = param("strategies.defaults.bbup.body_ratio_min"),
        require_bullish_trigger: bool = True,
    ):
        self.name = name
        self.strategy_type = StrategyType.BBUP
        self.sma_period = sma_period
        self.bb_multiplier = bb_multiplier
        self.position_size = position_size
        self.max_positions = max_positions
        self.max_market_cap = max_market_cap
        self.min_market_cap = min_market_cap

        # ── per-instance filter / cooldown state ──────────────
        self.sma_sl_cooldown_minutes = sma_sl_cooldown_minutes
        self.body_filter_enabled = body_filter_enabled
        self.body_ratio_min = body_ratio_min
        self.require_bullish_trigger = require_bullish_trigger

        self.token_states: Dict[str, TokenState] = {}

        # V6: Peak concurrent position tracking
        self.peak_concurrent_positions: int = 0

        # V6: Skip tracking
        self.entries_skipped: int = 0
        self.entries_taken: int = 0
        self._skip_log: deque = deque(maxlen=500)   # Recent skips for export (V7.1: O(1) bounded)

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
        """Track high-water mark for concurrent positions."""
        current = self.open_position_count
        if current > self.peak_concurrent_positions:
            self.peak_concurrent_positions = current

    # ════════════════════════════════════════════════════════════
    # LIVE TICK PROCESSING (V5 — real-time entry/exit detection)
    # ════════════════════════════════════════════════════════════

    def on_tick(self, tick: TickData) -> List[Signal]:
        """
        Process a live WebSocket price tick. Checks entry and exit
        conditions using FINALIZED indicators (from last closed candle).
        """
        state = self.token_states.get(tick.token_ca)
        if state is None:
            return []

        if not state.is_ready:
            return []

        finalized_ind = state.indicators
        if finalized_ind is None:
            return []

        # Update market cap in the snapshot for downstream use
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

        # If in position: check tick-based exits
        if state.has_position:
            exit_signal = self._check_tick_exits(state, tick, finalized_ind)
            if exit_signal is not None:
                return [exit_signal]
            return []

        # Not in position: check tick-based entry
        if state.tick_entry_fired:
            return []

        # Phantom cycle prevention
        if state.bbup_awaiting_cross_below:
            if finalized_ind.sma > 0 and tick.price < finalized_ind.sma:
                state.bbup_awaiting_cross_below = False
                logger.debug(
                    f"[{self.name}] Cross-below observed on {state.token_ticker}: "
                    f"price {tick.price:.8f} < SMA {finalized_ind.sma:.8f} — "
                    f"re-entry unlocked"
                )
            else:
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
        """UBB exit: live price >= upper BB -> sell at UBB level."""
        if tick.price >= live_ind.bb_upper:
            logger.info(
                f"[{self.name}] UBB EXIT on {state.token_ticker}: "
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
                reason=f"UBB exit: price {tick.price:.8f} >= UBB {live_ind.bb_upper:.8f}",
            )
            state.close_position()
            state.bbup_awaiting_cross_below = True
            return signal

        return None

    # ════════════════════════════════════════════════════════════
    # TICK-BASED ENTRY LOGIC (with trigger-candle body filter)
    # ════════════════════════════════════════════════════════════

    def _check_tick_entry(
        self, state: TokenState, tick: TickData, live_ind: IndicatorSnapshot
    ) -> Optional[Signal]:
        """
        Entry conditions (all must pass; thresholds redacted → config):
        1. Not in cooldown
        2. Max cycles not reached
        3. Market cap within [XXXX, XXXX]
        4. Max positions not reached
        5. SMA cross-up: last finalized close < SMA AND live price > SMA
        6. BB width >= XXXX
        7. RSI >= XXXX
        8. rolling avg volume >= XXXX
        9. ENVIRONMENT FILTER: passes if avg volume within [XXXX, XXXX)
       10. BODY FILTER (optional): trigger candle body_ratio >= body_ratio_min
           AND (if require_bullish_trigger) close > open
           Reads from state.{open,high,low,close}_prices[-1] — last finalized candle.
        """
        now = tick.timestamp

        # Gate 1: Cooldown
        if state.is_in_cooldown(now):
            return None

        # Gate 2: Max cycles
        if state.cycles_completed >= MAX_CYCLES_PER_TOKEN:
            if state.max_cycles_cooldown_until is None:
                state.max_cycles_cooldown_until = now + timedelta(
                    hours=MAX_CYCLES_COOLDOWN_HOURS
                )
            return None

        # Gate 3: Market cap range
        if not (self.min_market_cap <= tick.market_cap <= self.max_market_cap):
            return None

        # Gate 4: Position limit
        if self.open_position_count >= self.max_positions:
            return None

        # Gate 5: SMA Cross-Up detection
        if len(state.close_prices) < 1:
            return None

        last_finalized_close = state.close_prices[-1]
        finalized_sma = live_ind.sma

        if last_finalized_close >= finalized_sma:
            return None

        if tick.price <= finalized_sma:
            return None

        # Gate 6: BB width (finalized)
        if live_ind.bb_width_pct < BB_WIDTH_MIN_PCT:
            return None

        # Gate 7: RSI (finalized)
        if live_ind.rsi < MIN_RSI_ENTRY:
            return None

        # Gate 8: Volume (from finalized candles)
        if len(state.volumes) >= VOLUME_AVG_CANDLES:
            pre_entry_vols = state.volumes[-VOLUME_AVG_CANDLES:]
            avg_vol = sum(pre_entry_vols) / len(pre_entry_vols)
        elif len(state.volumes) >= 1:
            avg_vol = sum(state.volumes) / len(state.volumes)
        else:
            return None

        if avg_vol < MIN_AVG_VOLUME_USD:
            return None

        # ── Gate 9: ENVIRONMENT FILTER ────────────────────────
        # All prior gates passed (genuine signal). Now check if
        # the environment is right for profitable execution.
        if avg_vol < ENV_MIN_VOLUME_USD:
            self._log_skip(state, tick, live_ind, avg_vol, "vol_low",
                           f"avg_vol ${avg_vol:,.0f} below window")
            return None

        if avg_vol >= ENV_MAX_VOLUME_USD:
            self._log_skip(state, tick, live_ind, avg_vol, "vol_high",
                           f"avg_vol ${avg_vol:,.0f} above window")
            return None

        # ── Gate 10: TRIGGER-CANDLE BODY FILTER ───────────────
        # Inspects the LAST FINALIZED candle (state.{open,high,low,close}_prices[-1])
        # — i.e. the candle whose close caused the SMA cross-up to be valid.
        # Mechanical interpretation: a doji / long-wick trigger is a fakeout;
        # a decisive full-body bullish trigger is what momentum continuation needs.
        if self.body_filter_enabled:
            # We're past Gate 5 (SMA cross-up), so close_prices[-1] is guaranteed
            # to exist. Defensive check anyway for robustness.
            if (len(state.open_prices) < 1 or len(state.close_prices) < 1
                    or len(state.high_prices) < 1 or len(state.low_prices) < 1):
                return None

            trigger_open = state.open_prices[-1]
            trigger_high = state.high_prices[-1]
            trigger_low = state.low_prices[-1]
            trigger_close = state.close_prices[-1]

            trigger_range = trigger_high - trigger_low
            trigger_body = abs(trigger_close - trigger_open)
            body_ratio = (trigger_body / trigger_range) if trigger_range > 0 else 0.0
            is_bullish = trigger_close > trigger_open

            # Check body ratio first (more fundamental — "is this decisive at all?")
            if body_ratio < self.body_ratio_min:
                self._log_skip(state, tick, live_ind, avg_vol, "body_ratio_low",
                               f"body_ratio {body_ratio:.2f} below minimum")
                return None

            # Then check direction (only if a bullish trigger is required)
            if self.require_bullish_trigger and not is_bullish:
                self._log_skip(state, tick, live_ind, avg_vol, "body_not_bullish",
                               f"trigger close {trigger_close:.8f} <= open {trigger_open:.8f}")
                return None

        # ── ALL GATES PASSED → ENTRY SIGNAL ──────────────────
        logger.info(
            f"[{self.name}] ENTRY on {state.token_ticker}: "
            f"SMA cross-up | last_close {last_finalized_close:.8f} < "
            f"SMA {finalized_sma:.8f} < "
            f"live_price {tick.price:.8f} | RSI {live_ind.rsi:.1f} | "
            f"MC ${tick.market_cap:,.0f} | VolN ${avg_vol:,.0f} | "
            f"BBW {live_ind.bb_width_pct:.1f}%"
        )

        return Signal(
            signal_type=SignalType.ENTRY,
            strategy_name=self.name,
            token_ca=state.token_ca,
            timestamp=tick.timestamp,
            price=tick.price,
            fill_price=live_ind.sma,
            indicators=live_ind,
            reason=(
                f"SMA cross-up: {last_finalized_close:.8f} < SMA {live_ind.sma:.8f} "
                f"< {tick.price:.8f} | RSI {live_ind.rsi:.1f} | "
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
            f"{detail} | BBW {live_ind.bb_width_pct:.1f}% | "
            f"MC ${tick.market_cap:,.0f}"
        )

    def get_skip_log(self) -> List[dict]:
        """Return recent skip log for export/analysis."""
        return list(self._skip_log)

    # ════════════════════════════════════════════════════════════
    # CANDLE-BASED PROCESSING (finalized candles only)
    # ════════════════════════════════════════════════════════════

    def on_candle(self, candle: Candle) -> List[Signal]:
        """
        Process a finalized candle. With tick-based detection, this ONLY handles:
        1. Resetting the tick_signal_fired lock for the new candle
        2. Updating history buffers with the finalized OHLCV
        3. Refreshing indicators from clean finalized data
        4. SMA SL execution (pending from previous candle -> sell at OPEN)
        5. SMA SL confirmation (close < SMA -> set pending for next candle)
        """
        state = self.token_states.get(candle.token_ca)
        if state is None:
            return [self._no_action(candle)]

        # Reset tick entry lock for new candle interval
        state.tick_entry_fired = False

        # Update history
        state.add_candle(candle)
        state.update_indicators(self.sma_period, self.bb_multiplier)

        if not state.is_ready:
            return [self._no_action(candle)]

        # If in position: check candle-based SMA SL
        if state.has_position:
            # SMA SL EXECUTION (from previous candle's pending flag)
            if state.sma_sl_pending:
                logger.info(
                    f"[{self.name}] SMA SL EXIT on {state.token_ticker}: "
                    f"pending from prev candle, fill at OPEN {candle.open:.8f}"
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
                    reason=f"SMA SL: prev close < SMA, sell at open {candle.open:.8f}",
                )
                # Cooldown is per-instance; 0 disables it entirely.
                # bbup_awaiting_cross_below is set regardless — that flag is
                # correctness (phantom-cycle fix), not part of the cooldown.
                if self.sma_sl_cooldown_minutes > 0:
                    state.set_cooldown(self.sma_sl_cooldown_minutes, candle.timestamp)
                state.close_position()
                state.bbup_awaiting_cross_below = True
                return [signal]

            # SMA SL CONFIRMATION (set pending for next candle)
            self._check_sma_sl_confirmation(state, candle)

        return [self._no_action(candle)]

    def _check_sma_sl_confirmation(self, state: TokenState, candle: Candle):
        """SMA SL step 1: candle CLOSES below SMA → set pending flag."""
        ind_snap = state.indicators
        if ind_snap is None:
            return

        if state.candles_since_entry < 1:
            return

        if candle.close < ind_snap.sma:
            state.sma_sl_pending = True
            logger.debug(
                f"[{self.name}] SMA SL PENDING on {state.token_ticker}: "
                f"close {candle.close:.8f} < SMA {ind_snap.sma:.8f}"
            )

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
        self._update_peak_positions()

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
            "type": "BBUP",
            "sma": self.sma_period,
            "bb": self.bb_multiplier,
            "tokens_tracked": len(self.token_states),
            "tokens_ready": sum(1 for s in self.token_states.values() if s.is_ready),
            "open_positions": self.open_position_count,
            "peak_concurrent_positions": self.peak_concurrent_positions,
            "entries_taken": self.entries_taken,
            "entries_skipped": self.entries_skipped,
            "sma_sl_pending": sum(
                1 for s in self.token_states.values()
                if s.has_position and s.sma_sl_pending
            ),
        }
