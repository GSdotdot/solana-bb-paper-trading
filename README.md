# Solana Microcap Paper-Trading System: Bollinger Band Strategies

## 1. What it is

An automated, event-driven **paper-trading system** for newly launched Solana microcap tokens
(PumpFun launchpad). It runs 24/7 on a Linux VPS as a systemd service (`Type=notify`, watchdog pings,
automatic restart; the unit file is not published). It discovers tokens through a market-data
provider's API with a three-stage, credit-aware funnel, streams live prices over WebSocket, and
trades two Bollinger Band strategy families: **BBDOWN** (mean reversion) and **BBUP** (momentum).
Each family runs as a grid of ablation instances. Every fill goes through an explicit friction model
(DEX fee, market-cap-tiered slippage, priority fee), and every trade, cycle and portfolio snapshot is
recorded in SQLite for later analysis. No real capital was ever deployed.

> **Code written with AI assistance; strategy design, filters and validation methodology are my own.**

## 2. Architecture

```
                         ┌───────────── market-data provider (REST + WebSocket) ─────────────┐
                         │                                                                    │
 ┌─ [1] SCANNER ─────────┴────── one independent funnel per strategy family ──────────────┐  │
 │  token list ──► WIDE ────────────► NARROW ─────────────────► FULL FOCUS                 │  │
 │  (poll)         list data only     1 paid security call      live WS feed               │  │
 │                 zero extra credits holder concentration      (only survivors)           │  │
 │  cadence: INITIAL_FILL (aggressive) → MAINTENANCE (back off) → REFILL (speed up)        │  │
 └───────────────────────────────────────────────────────────────┬────────────────────────┘  │
                                                                 │ promote / remove           │
 ┌─ [2] DATA ENGINE ─────────────────────────────────────────────▼────────────────────────┐  │
 │  WS subscriptions · REST backfill · candle assembler · receive-timeout reconnect      │◄─┘
 │  tick queue (non-blocking) · market-cap stream · credit accounting                     │
 └───────────────┬───────────────────────────────────────────────┬───────────────────────┘
                 │ closed candles                                │ live ticks
 ┌─ [3] STRATEGIES ▼───────────────────────────────────────────────▼──────────────────────┐
 │  on_candle: update history, FREEZE SMA / BB levels    on_tick: tick vs frozen levels    │
 │  BBDOWN ×5 instances (mean reversion, 2-stage exit, market-cap hard stop)               │
 │  BBUP   ×4 instances (SMA cross-up, upper-band target, SMA stop)        → Signals       │
 └───────────────────────────────────────────────┬────────────────────────────────────────┘
 ┌─ [4] EXECUTION ───────────────────────────────▼────────────────────────────────────────┐
 │  dispatcher → PaperExecutor: fill at level, + slippage (MC-tiered) + DEX fee + priority │
 │  PortfolioManager per instance · open positions persisted synchronously (crash-safe)    │
 └───────────────────────────────────────────────┬────────────────────────────────────────┘
 ┌─ [5] RECORDING ───────────────────────────────▼────────────────────────────────────────┐
 │  SQLite (buffered writes): trades · cycles · portfolio_snapshots · open_positions      │
 │  ohlcv_archive · scoring snapshots (passive, never traded on) · Excel/CSV export        │
 └──────────────────────────────────────────────────────────────────────────────────────────┘
 ┌─ [6] ORCHESTRATOR ─────────────────────────────────────────────────────────────────────┐
 │  routes events by funnel & timeframe · restores portfolios + open positions on restart  │
 │  heartbeat · dead-task detection · WS-silence watchdog · systemd READY / WATCHDOG pings │
 └──────────────────────────────────────────────────────────────────────────────────────────┘
```

| Layer | Package | Responsibility |
|---|---|---|
| 1 Scanner | `paper_trading/scanner` | Token discovery. **Wide** filters (age, market cap, short-window volume, liquidity ratio, fee generation) use data already in the token list, at no extra credit cost. **Narrow** runs one paid security call per token (holder concentration, with a tolerance band that demotes instead of blacklisting). Promotion to Narrow also triggers one token-overview fetch, logged for research and cached so it is reused at Full. **Full Focus** survivors get a WebSocket feed. Each funnel keeps its own phase-driven polling cadence, which is the main lever on monthly API-credit spend. |
| 2 Data engine | `paper_trading/data_engine` | Async REST client (retry, rate-limit handling, per-call credit tracking, server-side filter-cap fallback). WebSocket client (batched subscriptions, reconnect, receive timeout, candle assembly with no explicit close event). Tick queue that decouples the socket loop from strategy work. |
| 3 Strategies | `paper_trading/strategies` | **Frozen-indicator / tick-based design**: bands are computed only from closed candles and live ticks are tested against them (see `docs/lookahead_bias.md`). BBDOWN: lower-band entry, partial exit at SMA, rest at upper band or on a confirmed SMA breakdown, plus a market-cap hard stop. BBUP: SMA cross-up entry with environment filters, upper-band target, two-step SMA stop. Phantom-cycle guard blocks instant re-entry. |
| 4 Execution | `paper_trading/execution` | Signal routing and simulated fills. The friction model is fee + slippage + priority fee, applied on both legs. Paper and live paths are kept strictly separate. This repository contains **no** live-trading or signing code. |
| 5 Recording | `paper_trading/recording` | SQLite with buffered writes; snapshots and cycle rebuilds run in a thread pool. Cycle reconstruction, metrics (win rate, profit factor, drawdown, equity curves), a passive scoring collector and Excel/CSV export. |
| 6 Orchestrator | `paper_trading/orchestrator`, `main.py` | Wires the layers together, owns the ablation roster, restores state after a crash, and supervises health (heartbeat, watchdog, systemd integration). |

## 3. Research journey

1. **Backtest.** The two strategy ideas and an initial scoring model came from backtests on
   historical candles. The backtests were encouraging enough to justify a forward test.
2. **Candle-close paper trading.** The first live engine evaluated strategies when each candle
   closed. Results across every instance looked very strong, *too* strong for microcaps after fees.
3. **Discovery of a lookahead bias.** Tracing individual fills showed that the engine decided at
   candle close but filled at band levels touched *earlier inside* that candle, using the bar's
   high and low, which are unknowable until it closes. Every ambiguity resolved in the trader's
   favour. Full write-up: [`docs/lookahead_bias.md`](docs/lookahead_bias.md).
4. **Rebuild on live ticks.** The engine was rebuilt around WebSocket ticks. A first attempt that
   fed ticks into the indicator history made the bands chase price and was discarded. The final
   design freezes indicators at the last closed candle and fires only on observable crossings.
   Rebuilding it also meant fixing the reliability problems that tick-rate processing exposed: an
   event-loop freeze, crash recovery, and process supervision.
5. **Results under honest execution.** With realistic fills and friction, the **profit factor fell
   from about 2.5 to below 0.5**. Later iterations added filters and environment gates, tested them
   on fresh data, and did not restore a robust edge. The conclusion was not to deploy capital. The
   lasting output is the infrastructure and the validation discipline.

## 4. Methodology

**Friction modelling.** Every simulated fill is taken at an observable price level, then adjusted for
slippage (tiered by market cap, applied against the trader on both legs), a DEX fee and a flat
priority fee. Results were always labelled with the friction assumption that produced them, and the
go-live criterion was set under a deliberately conservative flat-slippage assumption, not the most
favourable one.

**Cross-validation across datasets.**
- Every candidate change was run as an **ablation grid**: instances that differ along one axis
  (volume floor, market-cap ceiling, band-width filter, cooldown, trigger-candle body filter) run
  **side by side in the same live window**, so comparisons share one market regime.
- Findings from one paper-trading window were checked against an **independent earlier window**,
  and against **two sub-periods** inside each window. A change had to point the same way in both.
- Before each new sample, **pass/fail thresholds were written down in advance**. Post-hoc filter
  results were treated as upper bounds, because a filter tuned on a dataset always looks better on
  that same dataset.
- Statistical significance and effect size were judged separately. Large effects on small samples
  were **instrumented and monitored, not deployed**.
- An OHLCV replay engine (separate tool, not included here) reproduced live cycles from recorded
  candles, as a check on post-hoc analysis where intra-candle paths allowed it.

**Survivorship bias in trade accounting.** Performance was first measured over *closed* cycles only.
Positions that never closed silently dropped out: tokens that collapsed inside a single candle
gapped straight through a close-based stop, and some positions were lost when the process restarted.
This is the same mechanism as survivorship bias in fund-performance studies (dead funds leave the
index, so the index looks better than the population). The fix was to judge strategies on **true
PnL = realised + open positions marked to market**, and to persist open positions so that they
survive a crash (`tests/test_a1_persistence.py`). The mean-reversion strategy went from acceptable
to clearly losing, and its ranking against the momentum strategy flipped. The momentum strategy,
whose stop fires reliably, was barely affected.

**Failed experiments (documented so they are not repeated).**
- Live indicators (ticks appended to history): the bands chased price, so the approach was discarded.
- Time-to-live tweaks for the Full Focus stage: pausing the TTL while positions were open let
  persistent losers keep cycling.
- A band-width "environment" window that looked strong in one window was judged overfit and kept
  only as an ablation arm.
- A trigger-candle body filter that met its pre-registered target post-hoc failed on the fresh
  sample, starving entries without improving them.
- Several plausible discriminators showed **no** separation between winning and losing tokens:
  holder concentration, liquidity ratio and creator share. Holder concentration remains a safety
  gate, not a source of edge.
- Whether a faster timeframe would have caught intra-candle collapses could not be tested by replay,
  because raw ticks were not stored. That limitation is now a design requirement.

## 5. Note on redactions

> **Tuned parameters, thresholds and data-provider details are redacted; placeholders mark where they apply.**

* Every tuned value is loaded from YAML through `paper_trading/config/params.py`.
  [`config.example.yaml`](config.example.yaml) lists **every** parameter with a comment on what it
  controls. Each value is the placeholder `XXXX`. Real values live in an ignored `config.local.yaml`.
* With only the example config, every module imports and the design can be explored, but using a
  redacted value raises `RedactedParameterError`. Nothing silently runs on made-up numbers.
* The data provider is referred to generically. Its name, URLs, endpoint paths, WebSocket message
  formats, key response fields and per-call credit costs are config or environment values. Generic
  request and response field names (e.g. `market_cap`, `volume_5m_usd`) remain in the code.
* Strategy instances use neutral labels (`BBDOWN_A…E`, `BBUP_A…D`) instead of names that encoded
  their parameters.

## Reading the code: tags in comments

Comments keep short tags from the build history, because they explain *why* code looks the way it does:

| Tag | Meaning |
|---|---|
| `V4` … `V7.1` | system generation (V4 = candle-close engine, V5+ = tick engine, V7.1 = hardening release) |
| `A1` | crash-durable open-position persistence and restore |
| `Issue A` / `Issue B` | cash-ledger sign fix for buys / cash drift across restarts |
| `T7`–`T9`, `C2`, `Task 2.x` | work items for the narrow-stage overview cache, cache telemetry and restore gating |
| `H4`, `M3` | earlier review findings (separate entry/exit locks; removal of live indicators) |

## Repository layout

```
paper_trading/            six-layer system (scanner, data_engine, strategies, execution,
                          recording, orchestrator) + core models/indicators + config loader
tests/                    unit + acceptance tests (run on ILLUSTRATIVE values in tests/test_config.yaml)
docs/lookahead_bias.md    the execution-model finding in detail
sample_data/              small real extract, pseudonymised and reduced (see its README)
config.example.yaml       every tuned parameter, redacted, with comments
.env.example              environment variable names (no values)
```

## Running the tests

```bash
pip install -r requirements.txt
pytest tests/
```

The tests point the loader at `tests/test_config.yaml`, which holds **illustrative values that are not
the tuned parameters**. They exercise the real code paths with synthetic data and make no network
calls:

- the friction arithmetic
- frozen-indicator tick entries and the phantom-cycle guard
- funnel routing (wide → narrow → full, demotion, blacklisting) and phase-driven polling cadence
- crash-safe position restore
- a check that the public config contains only placeholders

Running the live system requires a private `config.local.yaml` and data-provider credentials, which
are not included.
