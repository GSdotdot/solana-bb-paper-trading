"""
System Configuration — Ablation Roster
=======================================
Single source of truth for the runtime configuration objects.

Every tuned number (indicator periods, band multipliers, timeframes, volume
floors, market-cap gates, cooldowns, sizing, funnel capacities, polling
intervals) is loaded from YAML via `param()` — see config.example.yaml.
In the public showcase those values are redacted placeholders (XXXX).

Architecture:
  - Dual independent funnels, one per strategy family:
      BBUP funnel   — volume-pre-filtered discovery, own WS timeframe
      BBDOWN funnel — broad discovery, own WS timeframe
    Each funnel sets its own polling cadence from its own phase
    (Phase-dependent polling cadence lives in funnel_manager.py.)
  - A roster of strategy instances arranged as an ABLATION GRID: instances of
    the same family share entry/exit logic and differ along one or two named
    axes, so pair-wise comparisons isolate the effect of each change.

ABLATION GRID (instance names are neutral labels; values in config):

BBDOWN — 5 instances:
  BBDOWN_A  previous-generation baseline (old volume floor, old MC ceiling,
            band-width filter ON, bearish-exit cooldown ON)
  BBDOWN_B  current filter stack, cooldown ON
  BBDOWN_C  current filter stack, cooldown OFF          (cooldown ablation)
  BBDOWN_D  current stack minus MC ceiling, cooldown OFF (MC-ceiling ablation)
  BBDOWN_E  current stack + band-width filter, cooldown OFF (BBW ablation)

BBUP — 4 instances:
  BBUP_A    previous-generation baseline (body filter OFF, cooldown ON)
  BBUP_B    body filter ON,  cooldown ON
  BBUP_C    body filter ON,  cooldown OFF               (cooldown ablation)
  BBUP_D    body filter OFF, cooldown OFF               (cooldown ablation)
"""

from dataclasses import dataclass, field
from typing import List

from paper_trading.config.params import param


def _p(key: str):
    return param(key)


@dataclass(frozen=True)
class StrategyConfig:
    """Config for one strategy instance.

    Defaults come from `strategies.defaults` in the YAML; each roster entry
    overrides only the fields its ablation axis changes.
    """
    # ── Identity ───────────────────────────────────────────────────
    name: str
    strategy_type: str          # "BBDOWN" or "BBUP"
    sma_period: int
    bb_multiplier: float
    timeframe_minutes: int
    funnel: str = ""            # "bbup" or "bbdown" — which funnel feeds this

    # ── Common execution config (sizing) ──────────────────────────
    position_size: float = _p("strategies.defaults.position_size")
    max_positions: int = _p("strategies.defaults.max_positions")
    initial_balance: float = _p("strategies.defaults.initial_balance")
    max_market_cap: float = _p("strategies.defaults.max_market_cap")   # BBUP MC gate ceiling
    min_market_cap: float = _p("strategies.defaults.min_market_cap")   # BBUP MC gate floor

    # ── BBDOWN per-instance parameters ────────────────────────────
    min_vol_window_usd: float = _p("strategies.defaults.bbdown.min_vol_window_usd")
    """Rolling-window USD volume floor for BBDOWN entry."""

    max_market_cap_entry: float = _p("strategies.defaults.bbdown.max_market_cap_entry")
    """BBDOWN entry market-cap ceiling."""

    bbw_filter_enabled: bool = False
    """BBDOWN band-width window filter (ablation axis)."""

    bbw_min_pct: float = _p("strategies.defaults.bbdown.bbw_min_pct")
    """BBDOWN band-width lower bound when filter enabled."""

    bbw_max_pct: float = _p("strategies.defaults.bbdown.bbw_max_pct")
    """BBDOWN band-width upper bound when filter enabled."""

    bearish_exit_cooldown_minutes: float = _p("strategies.defaults.bbdown.bearish_exit_cooldown_minutes")
    """BBDOWN post-bearish-exit re-entry cooldown. 0 disables."""

    # ── BBUP per-instance parameters ──────────────────────────────
    sma_sl_cooldown_minutes: float = _p("strategies.defaults.bbup.sma_sl_cooldown_minutes")
    """BBUP post-SMA-stop re-entry cooldown. 0 disables.
    Note: bbup_awaiting_cross_below flag is correctness, NOT this cooldown."""

    body_filter_enabled: bool = True
    """BBUP trigger-candle body filter (ablation axis)."""

    body_ratio_min: float = _p("strategies.defaults.bbup.body_ratio_min")
    """BBUP body filter — minimum body/range ratio."""

    require_bullish_trigger: bool = True
    """BBUP body filter — trigger candle must close > open."""


@dataclass(frozen=True)
class FunnelConfig:
    """Config for one independent funnel."""
    name: str                           # "bbup" or "bbdown"
    wide_capacity: int = _p("funnels.defaults.wide_capacity")
    narrow_capacity: int = _p("funnels.defaults.narrow_capacity")
    full_capacity: int = _p("funnels.defaults.full_capacity")
    # NOTE: the two per-funnel interval fields below are passed to the scanner
    # but are informational only: the live polling loops take their cadence
    # from FunnelManager.get_recommended_intervals() (phase-driven, per funnel).
    list_poll_interval_minutes: float = _p("funnels.defaults.list_poll_interval_minutes")
    narrow_scan_interval_minutes: float = _p("funnels.defaults.narrow_scan_interval_minutes")
    # Token-list API query params (server-side pre-filter)
    min_market_cap: float = _p("funnels.defaults.min_market_cap")
    max_market_cap: float = _p("funnels.defaults.max_market_cap")
    min_liquidity: float = _p("funnels.defaults.min_liquidity")
    min_volume_5m_usd: float = 0        # 0 = not sent to API
    # WebSocket candle timeframe for this funnel's tokens
    ws_timeframe_minutes: int = _p("funnels.defaults.ws_timeframe_minutes")


# ═══════════════════════════════════════════════════════════════
# FUNNEL CONFIGS — one independent funnel per strategy family
# ═══════════════════════════════════════════════════════════════

def _funnel(name: str) -> FunnelConfig:
    k = f"funnels.{name}"
    return FunnelConfig(
        name=name,
        full_capacity=_p(f"{k}.full_capacity"),
        list_poll_interval_minutes=_p(f"{k}.list_poll_interval_minutes"),
        narrow_scan_interval_minutes=_p(f"{k}.narrow_scan_interval_minutes"),
        min_volume_5m_usd=_p(f"{k}.min_volume_5m_usd"),
        ws_timeframe_minutes=_p(f"{k}.ws_timeframe_minutes"),
    )


BBUP_FUNNEL_CONFIG = _funnel("bbup")
BBDOWN_FUNNEL_CONFIG = _funnel("bbdown")


# ═══════════════════════════════════════════════════════════════
# ABLATION ROSTER — 9 STRATEGY INSTANCES
# ═══════════════════════════════════════════════════════════════
# All instances of a family share one band multiplier so that the filter and
# cooldown changes are not confounded by a multiplier sweep.

def _bbdown(name: str, **axes) -> StrategyConfig:
    return StrategyConfig(
        name=name,
        strategy_type="BBDOWN",
        sma_period=_p("strategies.bbdown.sma_period"),
        bb_multiplier=_p("strategies.bbdown.bb_multiplier"),
        timeframe_minutes=_p("strategies.bbdown.timeframe_minutes"),
        max_positions=_p("strategies.bbdown.max_positions"),
        funnel="bbdown",
        **axes,
    )


def _bbup(name: str, **axes) -> StrategyConfig:
    return StrategyConfig(
        name=name,
        strategy_type="BBUP",
        sma_period=_p("strategies.bbup.sma_period"),
        bb_multiplier=_p("strategies.bbup.bb_multiplier"),
        timeframe_minutes=_p("strategies.bbup.timeframe_minutes"),
        max_positions=_p("strategies.bbup.max_positions"),
        funnel="bbup",
        **axes,
    )


_BASE = "strategies.bbdown.baseline"   # previous-generation values
_CUR = "strategies.bbdown.current"     # current-generation values

STRATEGY_CONFIGS: List[StrategyConfig] = [
    # ─── BBDOWN — 5 instances ─────────────────────────────────
    _bbdown("BBDOWN_A",                     # previous-generation baseline
            min_vol_window_usd=_p(f"{_BASE}.min_vol_window_usd"),
            max_market_cap_entry=_p(f"{_BASE}.max_market_cap_entry"),
            bbw_filter_enabled=True,
            bearish_exit_cooldown_minutes=_p(f"{_BASE}.bearish_exit_cooldown_minutes")),
    _bbdown("BBDOWN_B",                     # current stack, cooldown ON
            min_vol_window_usd=_p(f"{_CUR}.min_vol_window_usd"),
            max_market_cap_entry=_p(f"{_CUR}.max_market_cap_entry"),
            bbw_filter_enabled=False,
            bearish_exit_cooldown_minutes=_p(f"{_CUR}.bearish_exit_cooldown_minutes")),
    _bbdown("BBDOWN_C",                     # current stack, cooldown OFF
            min_vol_window_usd=_p(f"{_CUR}.min_vol_window_usd"),
            max_market_cap_entry=_p(f"{_CUR}.max_market_cap_entry"),
            bbw_filter_enabled=False,
            bearish_exit_cooldown_minutes=0.0),
    _bbdown("BBDOWN_D",                     # MC ceiling relaxed, cooldown OFF
            min_vol_window_usd=_p(f"{_CUR}.min_vol_window_usd"),
            max_market_cap_entry=_p(f"{_BASE}.max_market_cap_entry"),
            bbw_filter_enabled=False,
            bearish_exit_cooldown_minutes=0.0),
    _bbdown("BBDOWN_E",                     # band-width filter kept, cooldown OFF
            min_vol_window_usd=_p(f"{_CUR}.min_vol_window_usd"),
            max_market_cap_entry=_p(f"{_CUR}.max_market_cap_entry"),
            bbw_filter_enabled=True,
            bearish_exit_cooldown_minutes=0.0),

    # ─── BBUP — 4 instances ───────────────────────────────────
    _bbup("BBUP_A",                         # previous-generation baseline
          body_filter_enabled=False,
          sma_sl_cooldown_minutes=_p("strategies.bbup.cooldown_minutes")),
    _bbup("BBUP_B",                         # body filter ON, cooldown ON
          body_filter_enabled=True,
          require_bullish_trigger=True,
          sma_sl_cooldown_minutes=_p("strategies.bbup.cooldown_minutes")),
    _bbup("BBUP_C",                         # body filter ON, cooldown OFF
          body_filter_enabled=True,
          require_bullish_trigger=True,
          sma_sl_cooldown_minutes=0.0),
    _bbup("BBUP_D",                         # body filter OFF, cooldown OFF
          body_filter_enabled=False,
          sma_sl_cooldown_minutes=0.0),
]


@dataclass(frozen=True)
class SystemConfig:
    """Top-level system configuration."""

    # Mode
    mode: str = "paper"

    # Database (relative path)
    db_path: str = "paper_trading.db"

    # Strategy instances
    strategies: List[StrategyConfig] = field(default_factory=lambda: list(STRATEGY_CONFIGS))

    # Funnel configs
    bbup_funnel: FunnelConfig = field(default_factory=lambda: BBUP_FUNNEL_CONFIG)
    bbdown_funnel: FunnelConfig = field(default_factory=lambda: BBDOWN_FUNNEL_CONFIG)

    # Recording cadence
    snapshot_interval_seconds: int = _p("system.snapshot_interval_seconds")
    cycle_rebuild_interval_seconds: int = _p("system.cycle_rebuild_interval_seconds")

    # Graceful shutdown timeout
    shutdown_timeout_seconds: int = 30

    # Logging (relative path)
    log_level: str = "INFO"
    log_file: str = "paper_trading.log"

    # ── Shared scanner config ─────────────────────────────────
    sol_poll_interval_minutes: float = _p("system.sol_poll_interval_minutes")

    # ── Data Engine config ────────────────────────────────────
    data_api_key: str = ""              # read from env DATA_PROVIDER_API_KEY
    websocket_reconnect_delay_seconds: int = _p("data_engine.ws_reconnect_delay_seconds")
    backfill_candles: int = _p("data_engine.backfill_candles")

    # ── Heartbeat ─────────────────────────────────────────────
    heartbeat_interval_seconds: int = _p("system.heartbeat_interval_seconds")


# Default config instance
DEFAULT_CONFIG = SystemConfig()
