# The lookahead bias, and how the execution model was rebuilt

This note explains the most important finding in the project. The early paper results looked
far too good. Tracing why showed a lookahead bias in the way fills were simulated. Fixing it
removed the edge.

## 1. The original design: decisions on candle close

The first paper-trading engine called strategies only when a candle **closed**. Entries and exits
checked that candle's high and low against the Bollinger Bands, and booked the fill **at the band
level** (simplified excerpt from the old BBDOWN strategy; values and filters omitted):

```python
# OLD — evaluated once per CLOSED candle
def on_candle(self, candle):
    state.add_candle(candle)
    ind = state.update_indicators(...)          # bands now include this candle

    # Entry: "the bar's low pierced the lower band"
    if candle.low < ind.bb_lower:
        return Signal(ENTRY,
                      price=candle.close,        # decision time = candle close
                      fill_price=ind.bb_lower)   # ...but filled at the band level

    # Exit: "the bar's high reached the SMA"
    if candle.high >= ind.sma:
        return Signal(EXIT_SMA_PARTIAL, fill_price=ind.sma)
```

## 2. Why that is lookahead

* **The low and high are only known after the bar closes.** The signal is generated at close time
  but filled at a price from *earlier inside* the bar. The simulator acts on information it could
  not have had when the price was actually at that level.
* **It assumes price traded at the band.** If price gapped straight through the lower band and kept
  falling, the old engine still bought exactly at the band, the best price in the bar.
* **Intra-bar ordering is unknown.** When an entry level and an exit level were both touched in one
  bar, the engine had no way to know which came first and resolved it favourably.
* **The bands themselves included the bar being traded.** The level being "crossed" was computed
  from a candle that had not finished when the cross supposedly happened.

Each of these errs in the trader's favour, and they compound. The equity curves looked excellent.

## 3. How it was found

The results were implausibly strong for microcap mean reversion and momentum after fees. Tracing
individual fills against the candle stream showed that entry prices were **never actually observable
at decision time**. The engine's fills were systematically the best case.

## 4. First fix attempt, which failed: live indicators

The first rebuild switched to live WebSocket ticks and **appended each tick to the price history**
before recomputing the bands. This created a different distortion. When a low tick entered the
history, the SMA and lower band moved down with it, so the band *chased* price and entries became
much harder to trigger. The more extreme the move, the further the band ran away. Removed.

## 5. The final design: frozen indicators, tick-based detection

```python
# NEW — indicators frozen at the last CLOSED candle
def on_candle(self, candle):                     # only place indicators change
    state.add_candle(candle)
    state.update_indicators(...)                 # SMA / LBB / UBB frozen until next close
    state.tick_entry_fired = False               # one entry per candle interval

def on_tick(self, tick):                         # every live price update
    ind = state.indicators                       # frozen levels from the last close
    if tick.price <= ind.bb_lower:               # observable crossing, right now
        return Signal(ENTRY, price=tick.price, fill_price=ind.bb_lower)
```

* Levels come only from **closed** candles and cannot be moved by ticks
  (`tests/test_frozen_indicators.py` checks this).
* A signal fires the moment an observable tick crosses a frozen level, and the fill is that level.
* The friction model (DEX fee, market-cap-tiered slippage, priority fee) is applied **on top** of
  every fill.
* Exits that require a confirmed close (e.g. a stop on "close below SMA") are executed at the
  **next candle's open**, never at the closing price that triggered them.

**Residual optimism, disclosed.** Two fills in the as-run code are still slightly generous. Both
favour the strategy, so they can only make results look better, and the no-edge conclusion holds
with them in:

* The BBUP entry fires when a tick is already *above* the frozen SMA but fills *at* the SMA. A gap
  through the SMA is filled at a price that was never printed.
* The BBDOWN upper-band exit (and the forced partial) fires when the tick is within a small tolerance
  *below* the upper band but fills *at* the band.

A stricter model fills at the worse of the level and the observed tick: `max(level, tick)` for buys,
`min(level, tick)` for sells. Slippage is applied on top in all cases.

## 6. Outcome

Under honest execution the **profit factor fell from about 2.5 to below 0.5**. Neither strategy
showed an edge, so no real capital was deployed. The value of the exercise was the execution
model itself, and the discipline of not trusting a result until the fills are observable.

## 7. What a future version would change

Ticks were consumed live but **never persisted**. So later questions such as "would a 1-minute
timeframe have caught the intra-candle collapses a 5-minute stop missed?" could not be answered by
replay, only inferred. Any future build should store the raw tick stream.
