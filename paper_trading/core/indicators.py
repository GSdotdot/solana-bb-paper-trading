"""
Technical Indicators
====================
Stateless math functions for SMA, Bollinger Bands, RSI, volume averages.
Given same inputs → always same outputs. No side effects.

All functions expect lists with most recent value at index [-1].
Return None when insufficient data.
"""

import math
from typing import Optional, Tuple, List

from paper_trading.config.params import param
from paper_trading.core.models import IndicatorSnapshot


def sma(prices: List[float], period: int) -> Optional[float]:
    """Simple Moving Average over last `period` values."""
    if len(prices) < period:
        return None
    return sum(prices[-period:]) / period


def rolling_std(prices: List[float], period: int) -> Optional[float]:
    """Population standard deviation over last `period` values."""
    if len(prices) < period:
        return None
    window = prices[-period:]
    mean = sum(window) / period
    variance = sum((p - mean) ** 2 for p in window) / period
    return math.sqrt(variance)


def bollinger_bands(
    prices: List[float], period: int, multiplier: float
) -> Optional[Tuple[float, float, float, float]]:
    """
    Returns (middle, upper, lower, width_pct) or None.
    
    middle    = SMA(period)
    upper     = SMA + multiplier * stddev
    lower     = SMA - multiplier * stddev  
    width_pct = (upper - lower) / middle * 100
    """
    middle = sma(prices, period)
    if middle is None or middle <= 0:
        return None
    std = rolling_std(prices, period)
    if std is None:
        return None

    upper = middle + multiplier * std
    lower = middle - multiplier * std
    width_pct = (upper - lower) / middle * 100.0
    return (middle, upper, lower, width_pct)


def rsi(prices: List[float], period: int) -> Optional[float]:
    """
    RSI using Wilder's smoothing. Needs period + 1 prices minimum.
    Returns value 0-100 or None.
    """
    if len(prices) < period + 1:
        return None

    changes = [prices[i] - prices[i - 1] for i in range(1, len(prices))]

    # Seed with simple average over first `period` changes
    avg_gain = sum(max(c, 0) for c in changes[:period]) / period
    avg_loss = sum(abs(min(c, 0)) for c in changes[:period]) / period

    # Wilder's smoothing for the rest
    for c in changes[period:]:
        avg_gain = (avg_gain * (period - 1) + max(c, 0)) / period
        avg_loss = (avg_loss * (period - 1) + abs(min(c, 0))) / period

    if avg_loss == 0:
        return 100.0
    rs = avg_gain / avg_loss
    return 100.0 - (100.0 / (1.0 + rs))


def volume_avg(volumes: List[float], period: int) -> Optional[float]:
    """Average volume over last `period` candles. None if insufficient data."""
    if len(volumes) < period:
        return None
    return sum(volumes[-period:]) / period


def compute_all(
    close_prices: List[float],
    volumes: List[float],
    sma_period: int,
    bb_multiplier: float,
    rsi_period: int = None,
    vol_avg_period: int = None,
    market_cap: float = 0.0,
) -> Optional[IndicatorSnapshot]:
    """
    Compute full indicator snapshot for current candle.
    Returns None if insufficient data for BB calculation (the critical one).
    RSI returns 50.0 as neutral default if not enough data yet.
    """
    if rsi_period is None:
        rsi_period = param("strategies.rsi_period")
    if vol_avg_period is None:
        vol_avg_period = param("strategies.volume_avg_candles")

    bb = bollinger_bands(close_prices, sma_period, bb_multiplier)
    if bb is None:
        return None
    bb_mid, bb_up, bb_low, bb_width = bb

    rsi_val = rsi(close_prices, rsi_period)
    if rsi_val is None:
        rsi_val = 50.0  # Neutral default

    vol = volume_avg(volumes, vol_avg_period)
    if vol is None:
        vol = 0.0

    return IndicatorSnapshot(
        sma=bb_mid,
        bb_upper=bb_up,
        bb_lower=bb_low,
        bb_width_pct=bb_width,
        rsi=rsi_val,
        volume_avg_n=vol,
        current_close=close_prices[-1] if close_prices else 0.0,
        current_volume=volumes[-1] if volumes else 0.0,
        current_market_cap=market_cap,
    )


# NOTE: compute_live() was removed in V5.1.
# It was the V5.0 approach that appended the live tick price to history
# before computing indicators, causing LBB to chase price downward
# (dynamic indicator distortion). V5.1 uses indicators FROZEN at the last
# closed candle instead — see strategies' on_tick() methods and
# docs/lookahead_bias.md.
