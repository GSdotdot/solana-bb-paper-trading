# Sample data

A small, real extract from one paper-trading run that ran **both strategy families** (BBUP and
BBDOWN, nine ablation instances). It shows the schema and the kind of data recorded. It is
deliberately **not** enough to rebuild the indicator parameters, the position sizing, or the
identity of any traded token.

Rows are real recorded values unless a transformation is listed below. No values were invented.

> **Do not read profitability from `cycles.csv`.** Closed cycles overstate BBDOWN in particular.
> Some of the sampled BBDOWN instances net out positive on closed cycles, but positions that
> never closed are missing from this table. Tokens that collapsed inside a single candle gapped
> through the close-based stop and stayed open, so they never appear as cycles. This is the
> survivorship bias described in the main README (§4, Methodology), and the reason strategies were
> judged on **true PnL = realised + open positions marked to market**, never on closed cycles alone.
> The marked-to-market view is `portfolio_snapshots.csv`.

## Files

### `candles.csv`: closed OHLCV candles (`ohlcv_archive` table)

| column | meaning |
|---|---|
| `token` | pseudonym (`TOKEN_S`, …), consistent across all files |
| `timeframe` | `1m` or `5m` |
| `timestamp_utc` | candle open time |
| `open`/`high`/`low`/`close` | USD price, as recorded |
| `volume_usd` | candle volume in USD (base volume × close) |
| `market_cap` | market cap from the token-stats stream, USD |

25 consecutive candles for each of two different tokens, one per timeframe (duplicate stream rows
removed). Both tokens reached the Full Focus stage of the funnel but were **never traded** in this run,
so their candles cannot be lined up with any cycle to recover band levels.

### `cycles.csv`: completed trade cycles (`cycles` table, reduced)

| column | meaning |
|---|---|
| `cycle_id` | renumbered `CYC_001…` (original ids embedded parameter-bearing names) |
| `strategy_name` | neutral ablation label (see `paper_trading/orchestrator/config.py`) |
| `token` | pseudonym |
| `entry_hour_utc` / `exit_hour_utc` | **rounded down to the hour** |
| `exit_type` | `exit_ubb`, `exit_sma_full`, `exit_hard_stop` |
| `had_partial` | 1 if a BBDOWN partial exit at the SMA happened first |
| `pnl_pct` | net PnL % of the cycle **after** fees, slippage and priority fees |
| `duration_seconds` | entry → final exit |

16 BBDOWN and 16 BBUP cycles, **stratified by exit type** in proportion to that family's
population, so hard stops, upper-band exits and SMA exits appear at roughly their real frequency.
Cycles of `BBDOWN_A` and `BBUP_A` are deliberately left out (see below).

### `portfolio_snapshots.csv`: per-instance equity over time (`portfolio_snapshots` table, reduced)

| column | meaning |
|---|---|
| `strategy_name` | `BBDOWN_A` and `BBUP_A` |
| `timestamp_utc` | first snapshot in each hour |
| `open_positions`, `total_trades`, `total_cycles` | counters |
| `equity_index` | portfolio value (cash + open positions marked to market, net of exit costs), **indexed to 100 at start** |
| `drawdown_pct` | `(running peak − value) / running peak` over the rows shown |

20 hourly rows per instance.

**Correction applied.** Reconciling these snapshots against the trade ledger exposed a valuation bug
in the recorded `total_value`. While a BBDOWN position was open after its partial exit, the partial
proceeds were counted twice: once in cash, and again in the position's value. The affected rows are
written here with the corrected value: recorded value minus the partial proceeds of positions still
open. Before writing, the extraction checks that the cash rebuilt from the trades matches every
snapshot's cash exactly and that open-position counts agree.

The bug is fixed in `paper_trading/execution/executor.py` (`deployed_capital`) and covered by
`tests/test_friction.py::test_total_value_does_not_double_count_partial_exit_proceeds`. Cycles, PnL %
and profit factor are computed from trades and were never affected.

### `ticks.csv`: schema only, no rows

Live ticks (`TickData`: token, timestamp, timeframe, price, market cap, running candle volume) were
processed in memory and **never persisted**. The file holds only the header, and no tick values were
invented. Not storing ticks turned out to be a real limitation of the research (see
`docs/lookahead_bias.md` §7).

## What was removed and why

| removed | reason |
|---|---|
| real token addresses and tickers | identity; replaced by pseudonyms |
| original strategy names, trade/cycle ids | they encoded indicator periods and band multipliers |
| entry/exit/partial fill and execution prices | a fill sits exactly at a band level, so it could be used to back-solve the parameters |
| USD amounts (invested, proceeds, costs, cash, fees, slippage) | they reveal position size and the friction values |
| cycles of the two instances whose equity is shown | equity steps matched against cycle PnL % would reveal position size relative to balance |
| exact timestamps on cycles | could identify the traded token on-chain |
| entry market caps on cycles | their per-instance range would hint at the market-cap ceilings being ablated |
| `trades`, `events`, `scoring_snapshots`, `entry_conditions` tables | free-text reasons quote thresholds; snapshots hold creator addresses and raw provider payloads |
| `mfe`/`mae`, `candles_in_trade` columns | never populated in this build |
