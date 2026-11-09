"""
Three-stage discovery funnel: WIDE → NARROW → FULL FOCUS, and the
phase-driven polling cadence that controls API credit spend.

Synthetic tokens; thresholds from tests/test_config.yaml (ILLUSTRATIVE).
"""
from paper_trading.config.params import param
from paper_trading.scanner.buckets import BucketSet
from paper_trading.scanner.funnel_manager import FunnelManager, Phase
from paper_trading.scanner.token_model import ScannerToken

SOURCE = param("data_provider.launchpad_source_id")
SOL_USD = 150.0


def _token(n, mcap=123_000, liq=24_000, vol5m=4_321, vol24=43_210):
    t = ScannerToken(token_ca=f"TOKEN_{n}_pump", ticker=f"T{n}")
    t.launchpad_source = SOURCE
    t.market_cap, t.liquidity = mcap, liq
    t.volume_5m_usd, t.volume_24h_usd = vol5m, vol24
    return t


def _funnel(**caps):
    fm = FunnelManager(buckets=BucketSet(**caps) if caps else None)
    fm.wide_filters.set_sol_price(SOL_USD)
    return fm


def test_wide_pass_goes_straight_to_narrow_without_paid_calls():
    fm = _funnel()
    res = fm.process_list_token(_token(1))
    assert res.passed
    assert fm.buckets.narrow.get("TOKEN_1_pump") is not None


def test_fee_generation_failure_is_a_permanent_blacklist():
    fm = _funnel()
    res = fm.process_list_token(_token(2, vol24=10))
    assert not res.passed and res.permanent_blacklist
    assert fm.blacklist.is_blacklisted("TOKEN_2_pump")


def test_borderline_mcap_is_parked_in_wide_for_reevaluation():
    fm = _funnel()
    below = param("scanner.wide.min_mcap") * (param("scanner.borderline.below_min_factor") + 0.1)
    fm.process_list_token(_token(3, mcap=below, liq=below * 0.2))
    assert fm.buckets.wide.get("TOKEN_3_pump") is not None


def test_narrow_holder_check_promotes_demotes_or_blacklists():
    fm = _funnel()
    ceiling = param("scanner.narrow.holder_tiers")[0][1]
    tol = param("scanner.narrow.holder_tolerance_pct")
    for n, pct in ((4, ceiling * 0.5), (5, ceiling + tol / 2), (6, ceiling + tol * 5)):
        fm.process_list_token(_token(n))
        fm.buckets.narrow.get(f"TOKEN_{n}_pump").top10_holder_pct = pct
        fm.evaluate_narrow_token(f"TOKEN_{n}_pump")
    assert fm.buckets.full.get("TOKEN_4_pump") is not None      # pass → FULL (WS feed)
    assert fm.buckets.wide.get("TOKEN_5_pump") is not None      # near-fail → demoted
    assert fm.blacklist.is_blacklisted("TOKEN_6_pump")          # hard fail → blacklisted


def test_polling_cadence_follows_funnel_phase():
    pi = "funnels.phase_intervals"
    fm = _funnel(wide_capacity=2, narrow_capacity=2, full_capacity=1)
    assert fm.get_recommended_intervals() == dict(param(f"{pi}.initial_fill"))

    fm.process_list_token(_token(7))                           # → narrow → full
    fm.buckets.narrow.get("TOKEN_7_pump").top10_holder_pct = 0.01
    fm.evaluate_narrow_token("TOKEN_7_pump")
    fm.process_list_token(_token(8))                           # stays in narrow
    below = param("scanner.wide.min_mcap") * 0.9
    fm.process_list_token(_token(9, mcap=below, liq=below * 0.2))   # parked in wide

    assert fm.get_recommended_intervals() == dict(param(f"{pi}.maintenance"))
    assert fm.phase == Phase.MAINTENANCE                       # steady state → back off

    fm.buckets.narrow.remove("TOKEN_8_pump")                   # pipeline drains
    iv = fm.get_recommended_intervals()
    assert fm.phase == Phase.REFILL
    assert iv["narrow_scan_minutes"] == param(f"{pi}.refill")["narrow_scan_minutes_critical"]
