"""
A1 open-position persistence — deterministic acceptance harness
================================================================
Exercises the REAL code paths (DatabaseManager, PaperExecutor,
PortfolioManager, SignalDispatcher, strategy notify_*, and the new
Orchestrator open-position restore) without any network access:

  1. open_positions schema present
  2. kill-9: open a position, drop the DB connection mid-flight WITHOUT a
     graceful shutdown (buffered writes lost — true crash semantics), restart
     a fresh Orchestrator on the same DB file → cash + position match EXACTLY,
     token is queued for WS re-subscribe, and the restored token is GATED
     (no signal fires) until backfill reseeds >= sma_period candles
  3. partial-exit variant (sma_partial_done=1) restores, then the full exit
     fires correctly post-restore + backfill
  4. no cash drift: total_value after restore ~= before (Issue B fixed), and a
     drawdown query over portfolio_snapshots returns sane numbers

What this does NOT cover: the live WS re-subscribe itself (needs the provider
API + a live market). That path is wired (get_restored_tokens →
add_token_with_backfill → seed_restored_token) and the seed/gate half is
exercised here with synthetic candles.

All parameters come from tests/test_config.yaml — ILLUSTRATIVE values, not the
tuned ones. Prices, market caps and volumes below are synthetic.

Run:  pytest tests/
"""

import os
import sqlite3
import tempfile
from datetime import datetime, timezone, timedelta

from paper_trading.config.params import param
from paper_trading.orchestrator.config import SystemConfig, StrategyConfig
from paper_trading.orchestrator.orchestrator import Orchestrator
from paper_trading.core.models import (
    Signal, SignalType, IndicatorSnapshot, Candle, Timeframe, TickData,
)

T0 = datetime(2026, 6, 4, 12, 0, 0, tzinfo=timezone.utc)
CA_SIMPLE = "TOKEN_A_pump"      # pseudonymous test identifiers
CA_PARTIAL = "TOKEN_B_pump"
SMA_UP = param("strategies.bbup.sma_period")
SMA_DOWN = param("strategies.bbdown.sma_period")
EPS = 1e-9

_checks = []


def check(label, ok, detail=""):
    _checks.append(ok)
    mark = "PASS" if ok else "FAIL"
    print(f"  [{mark}] {label}" + (f"  — {detail}" if detail else ""))


def make_config(db_path, log_path):
    bbup = StrategyConfig(
        name="BBUP_TEST", strategy_type="BBUP", sma_period=SMA_UP,
        bb_multiplier=param("strategies.bbup.bb_multiplier"),
        timeframe_minutes=1, funnel="bbup",
        sma_sl_cooldown_minutes=0, body_filter_enabled=False,
    )
    bbdown = StrategyConfig(
        name="BBDOWN_TEST", strategy_type="BBDOWN", sma_period=SMA_DOWN,
        bb_multiplier=param("strategies.bbdown.bb_multiplier"),
        timeframe_minutes=5, funnel="bbdown",
    )
    return SystemConfig(
        strategies=[bbup, bbdown], db_path=db_path,
        log_file=log_path, log_level="WARNING",
    )


def open_position(orch, strat_name, ca, fill_price, mc=123_000.0):
    """Drive a real entry through the dispatcher → executor → PortfolioManager
    (persists open_positions) → strategy.notify_entry_filled."""
    strat = orch.strategies[strat_name]
    pf = orch.portfolios[strat_name]
    strat.add_token(ca, "TKN")
    ind = IndicatorSnapshot(
        sma=fill_price, bb_upper=fill_price * 1.2, bb_lower=fill_price * 0.8,
        bb_width_pct=33.0, rsi=50.0, volume_avg_n=1234.0,
        current_close=fill_price, current_market_cap=mc,
    )
    sig = Signal(
        signal_type=SignalType.ENTRY, strategy_name=strat_name, token_ca=ca,
        timestamp=T0, price=fill_price, fill_price=fill_price,
        indicators=ind, reason="harness entry",
    )
    return orch.dispatcher._handle_entry(
        sig, strat, pf,
        param("strategies.defaults.position_size"),
        param("strategies.defaults.max_positions"),
    )


def rising_candles(ca, tf, n, base, start_min):
    """n finalized candles, gently rising → close stays above SMA (no SMA-SL)."""
    out = []
    for i in range(n):
        c = base * (1.0 + 0.02 * i)
        out.append(Candle(
            token_ca=ca, timeframe=tf, timestamp=T0 + timedelta(minutes=start_min + i),
            open=c * 0.999, high=c * 1.01, low=c * 0.99, close=c,
            volume=1234.0, market_cap=123_000.0,
        ))
    return out


def crash(orch):
    """Simulate kill-9: drop the sqlite connection WITHOUT graceful shutdown,
    so buffered (un-flushed) writes are lost. Committed open_positions +
    snapshots survive. Releases the file handle for the next session."""
    orch.db.conn.close()


def main():
    tmp = tempfile.mkdtemp(prefix="a1_accept_")
    db_path = os.path.join(tmp, "paper_trading.db")
    log_path = os.path.join(tmp, "a1.log")
    print(f"\nDB: {db_path}\n")

    # ── TEST 0: schema ────────────────────────────────────────
    print("TEST 0 — open_positions schema")
    o = Orchestrator(make_config(db_path, log_path)); o.startup()
    schema = o.db.conn.execute(
        "SELECT sql FROM sqlite_master WHERE type='table' AND name='open_positions'"
    ).fetchone()
    check("open_positions table exists", schema is not None)
    if schema:
        s = schema[0]
        for col in ("strategy_name", "token_ca", "entry_timestamp", "entry_price",
                    "tokens_held", "invested_usd", "entry_market_cap",
                    "sma_partial_done", "sma_sl_pending", "last_updated_timestamp"):
            check(f"column {col}", col in s)
        check("PK (strategy_name, token_ca)",
              "PRIMARY KEY (strategy_name, token_ca)" in s)

    # ── TEST 1: kill-9 with a simple BBUP position ────────────
    print("\nTEST 1 — kill-9 restore (simple position)")
    trade = open_position(o, "BBUP_TEST", CA_SIMPLE, fill_price=0.001)
    pf1 = o.portfolios["BBUP_TEST"]
    cash_pre = pf1.cash_balance
    tv_pre = pf1.total_value
    pos1 = pf1.positions[CA_SIMPLE]
    tokens_pre, invested_pre = pos1.tokens_remaining, pos1.invested_usd
    print(f"    pre-kill: cash=${cash_pre:.6f}  total_value=${tv_pre:.6f}  "
          f"tokens={tokens_pre:.6f}  invested=${invested_pre:.4f}")
    rows = o.db.get_open_positions("BBUP_TEST")
    check("open_positions has 1 row after entry", len(rows) == 1)

    crash(o)

    # restart
    o2 = Orchestrator(make_config(db_path, log_path)); o2.startup()
    pf2 = o2.portfolios["BBUP_TEST"]
    st2 = o2.strategies["BBUP_TEST"].token_states.get(CA_SIMPLE)
    check("cash matches EXACTLY across restart",
          abs(pf2.cash_balance - cash_pre) < EPS,
          f"pre=${cash_pre:.6f} post=${pf2.cash_balance:.6f}")
    check("position restored", CA_SIMPLE in pf2.positions)
    if CA_SIMPLE in pf2.positions:
        p = pf2.positions[CA_SIMPLE]
        check("tokens_held matches EXACTLY", abs(p.tokens_remaining - tokens_pre) < EPS,
              f"{p.tokens_remaining:.6f}")
        check("invested_usd matches EXACTLY", abs(p.invested_usd - invested_pre) < EPS,
              f"${p.invested_usd:.4f}")
    check("STATUS shows >=1 open position",
          o2.get_status()["total_open_positions"] >= 1,
          f"count={o2.get_status()['total_open_positions']}")
    check("token queued for WS re-subscribe",
          any(t[1] == CA_SIMPLE for t in o2.get_restored_tokens()))
    check("restored token is GATED (awaiting_backfill)",
          st2 is not None and st2.awaiting_backfill)

    # no signal fires while gated: a tick at/above UBB would normally exit
    tv_after = pf2.total_value
    trades_before = o2.trades_executed
    o2.on_tick_received(TickData(
        token_ca=CA_SIMPLE, price=0.01, market_cap=123_000.0,
        timestamp=T0 + timedelta(minutes=1), timeframe=Timeframe.ONE_MIN,
    ))
    check("NO signal fires while gated (UBB-level tick ignored)",
          o2.trades_executed == trades_before and CA_SIMPLE in pf2.positions)

    # backfill: seed >= sma_period candles → gate clears
    o2.seed_restored_token("BBUP_TEST", CA_SIMPLE, [],
                           rising_candles(CA_SIMPLE, Timeframe.ONE_MIN, SMA_UP + 1, 0.001, 0))
    check("gate clears after backfill (>= SMA candles)",
          st2 is not None and not st2.awaiting_backfill,
          f"candles={st2.candle_count if st2 else 0}")

    # ── TEST 3 (uses Test 1 numbers): no cash drift ───────────
    print("\nTEST 3 — no cash drift across restart")
    check("total_value not inflated (Issue B fixed)",
          tv_after <= tv_pre + 1e-6,
          f"pre=${tv_pre:.6f} post=${tv_after:.6f}")
    check("total_value ~= pre-kill (cash + restored position)",
          abs(tv_after - tv_pre) < 1e-6,
          f"delta=${abs(tv_after - tv_pre):.2e}")

    # ── TEST 2: partial-exit variant ──────────────────────────
    print("\nTEST 2 — partial-exit (sma_partial_done=1) restore + full exit")
    open_position(o2, "BBDOWN_TEST", CA_PARTIAL, fill_price=0.002)
    pfb = o2.portfolios["BBDOWN_TEST"]
    posb = pfb.positions[CA_PARTIAL]
    tokens_full = posb.tokens_remaining
    # real partial mutation (persists sma_partial_done=1, reduced tokens_held)
    part = param("strategies.bbdown.partial_sell_pct")
    pfb.record_partial_sell(CA_PARTIAL, tokens_full * part, net_proceeds=10.0)
    o2.strategies["BBDOWN_TEST"].token_states[CA_PARTIAL].record_partial_sell(
        tokens_full * part, 0.002, T0)
    cash_pre_p = pfb.cash_balance
    tokens_after_partial = pfb.positions[CA_PARTIAL].tokens_remaining
    prow = o2.db.get_open_positions("BBDOWN_TEST")[0]
    check("partial persisted: sma_partial_done=1", prow["sma_partial_done"] == 1)
    check("partial persisted: tokens_held reduced",
          abs(prow["tokens_held"] - tokens_after_partial) < EPS)

    crash(o2)

    o3 = Orchestrator(make_config(db_path, log_path)); o3.startup()
    pfb3 = o3.portfolios["BBDOWN_TEST"]
    stb3 = o3.strategies["BBDOWN_TEST"].token_states.get(CA_PARTIAL)
    check("partial position restored", CA_PARTIAL in pfb3.positions)
    check("sma_partial_done restored True", stb3 is not None and stb3.sma_partial_done)
    check("partial cash matches EXACTLY", abs(pfb3.cash_balance - cash_pre_p) < EPS,
          f"${pfb3.cash_balance:.6f}")
    check("partial position GATED on restore",
          stb3 is not None and stb3.awaiting_backfill)

    # backfill the 5m strategy (history_5m is the correct arg) → gate clears
    o3.seed_restored_token("BBDOWN_TEST", CA_PARTIAL,
                           rising_candles(CA_PARTIAL, Timeframe.FIVE_MIN, SMA_DOWN + 2, 0.002, 0),
                           [])
    check("partial gate clears after correct (5m) backfill",
          stb3 is not None and not stb3.awaiting_backfill,
          f"candles={stb3.candle_count if stb3 else 0}")

    # fire the FULL exit through the REAL dispatcher → executor → close_position
    # path (the next actionable signal post-restore), not a direct PM call.
    strat_b = o3.strategies["BBDOWN_TEST"]
    cash_before_exit = pfb3.cash_balance
    tokens_held_now = pfb3.positions[CA_PARTIAL].tokens_remaining
    exit_ind = IndicatorSnapshot(
        sma=0.002, bb_upper=0.0024, bb_lower=0.0016, bb_width_pct=33.0,
        rsi=50.0, volume_avg_n=1234.0, current_close=0.002,
        current_market_cap=123_000.0,
    )
    exit_sig = Signal(
        signal_type=SignalType.EXIT_SMA_FULL, strategy_name="BBDOWN_TEST",
        token_ca=CA_PARTIAL, timestamp=T0 + timedelta(minutes=60), price=0.002,
        fill_price=0.002, sell_pct=1.0, indicators=exit_ind, reason="harness full exit",
    )
    exit_trade = o3.dispatcher._handle_exit(exit_sig, strat_b, pfb3)
    check("full exit fires post-restore via dispatcher (trade produced)",
          exit_trade is not None and exit_trade.signal_type == "exit_sma_full")
    check("full exit removed the position", CA_PARTIAL not in pfb3.positions)
    check("full exit deleted durable row",
          len(o3.db.get_open_positions("BBDOWN_TEST")) == 0)
    if exit_trade is not None:
        check("full exit credited cash by net proceeds",
              abs(pfb3.cash_balance - (cash_before_exit + exit_trade.net_usd)) < 1e-6,
              f"cash=${pfb3.cash_balance:.6f} net=${exit_trade.net_usd:.4f}")
        check("full exit sold the remaining (post-partial) tokens",
              abs(exit_trade.token_quantity - tokens_held_now) < EPS)

    # ── drawdown query (peak vs latest total_value per strategy) ──
    print("\nDrawdown check (peak vs latest total_value per strategy)")
    con = sqlite3.connect(db_path); con.row_factory = sqlite3.Row
    dd = con.execute("""
        WITH latest AS (
            SELECT strategy_name, total_value, peak_value,
                   ROW_NUMBER() OVER (PARTITION BY strategy_name
                                      ORDER BY id DESC) rn
            FROM portfolio_snapshots
        )
        SELECT strategy_name, total_value, peak_value,
               ROUND((peak_value - total_value) / peak_value * 100, 4) AS drawdown_pct
        FROM latest WHERE rn = 1 ORDER BY strategy_name
    """).fetchall()
    con.close()
    sane = True
    for r in dd:
        print(f"    {r['strategy_name']}: total=${r['total_value']:.4f} "
              f"peak=${r['peak_value']:.4f} drawdown={r['drawdown_pct']}%")
        if r["total_value"] > r["peak_value"] + 1e-6 or r["drawdown_pct"] < -1e-6:
            sane = False
    check("drawdown sane (no upward drift; total_value <= peak)", sane)

    # ── summary ───────────────────────────────────────────────
    passed = sum(1 for c in _checks if c)
    print(f"\n{'='*56}\nA1 ACCEPTANCE: {passed}/{len(_checks)} checks passed")
    print("="*56)
    return 0 if passed == len(_checks) else 1


def test_a1_acceptance():
    """pytest entry point: every check in the harness must pass."""
    assert main() == 0


if __name__ == "__main__":
    raise SystemExit(main())
