# Kalshi 15-minute commodities × Hyperliquid feed: prelim (2026-11-13)

**Question.** Can a fair value computed from the streamed Hyperliquid HIP-3 price feed make money taking liquidity in Kalshi's
15-minute commodity up/down markets?

**Short answer: not with the plain version.** The Kalshi book moves about 5 s *before* the HL oracle. Kalshi's own mid is
better calibrated than the HL fair value at every horizon. Every threshold and latency tested loses money after fees.
The details and possible next steps are below.

> Feed note: Markets.xyz (Kinetiq, dex `mkts`) lists no commodities. Its only live markets are US500, USTECH, SMALL2000,
> USBOND and BVIV. The commodity perps are on **trade[XYZ]** (dex `xyz`), so that is the feed used here, from the
> recorder's `activeAssetCtx` stream.

## Data
| | |
|---|---|
| Kalshi | `KXWTI15M KXBRENT15M KXNATGAS15M KXGOLD15M KXSILVER15M KXPLATINUM15M KXPALLADIUM15M KXCOPPER15M`: settlements, plus the live `ticker` stream (best bid/ask) |
| HL | `xyz:` `CL BRENTOIL NATGAS GOLD SILVER PLATINUM PALLADIUM COPPER`, `asset_ctx` (oracle / mid, ~1–2 s), on a 1 s grid with no look-ahead |
| Window | 2026-11-01 → 11-12, 6,709 settled windows. **Train** 11-01..11-06, **test** 11-07..11-12 (the 7th–8th are a weekend) |
| Source | `../HyperliquidData/{hl,kalshi}.duckdb` (override with `PARAGON_DATA_ROOT`) |

## Contract mechanics (from `rules_primary`)
YES pays $1 if the **Pyth 1-min candle close** at window close ≥ the strike. The strike is the previous window's
settlement value, so each window is an at-the-money digital at open. That gives a model price:

`fair = Φ( (ln S_t + b̂ − ln K) / sqrt(σ²·τ + s_b²) )`

- `S_t`: the HL price.
- `b̂`: the median HL→Pyth log basis over the last 8 settled windows.
- `σ`: trailing 30-min realised volatility from 10 s returns.
- `s_b`: the RMS of recent basis-forecast errors.

## Results

**1. HL tracks the settlement source well.** The HL **oracle** at exactly `close_ms` matches Pyth best. Its median basis-forecast
error is 0.8 bp, against a typical 15-min move of 7–16 bp, and HL + b̂ calls the outcome correctly **96%** of the time.
The oracle beats the perp mid; offsets of ±30/60 s are worse.

The basis differs a lot by series, and the 8-window rolling median absorbs it:
- Gold and silver: under 0.5 bp of error.
- Copper: a constant offset of about 125 bp (probably a different reference contract).
- Natgas: the basis ramps 0→80 bp during the day and then resets.
(`out/basis_by_series.csv`, top-left panel of `out/prelim.png`.)

**2. Kalshi is already better calibrated than the model** (Brier score, lower is better, `out/calibration.csv`):

| time to close | model (HL oracle) | Kalshi mid |
|---|---|---|
| < 1 min | 0.097 | **0.065** |
| 1–3 min | 0.109 | **0.091** |
| 3–7 min | 0.138 | **0.125** |
| 7–15 min | 0.200 | **0.191** |

Turning off `s_b` or using the perp mid instead of the oracle makes the model worse (`diag.log`). Kalshi also wins
on every series in the last minute.

**3. Kalshi leads HL.** The correlation of 5 s changes, Kalshi mid vs HL fair, peaks at **0.52 when HL lags by 5 s**. It is 0.25
at the same time and about 0.02 when HL leads by 5 s. A fair-vs-mid gap does not close: after a 20¢ gap, the Kalshi mid
moves less than 0.5¢ over the next 60 s (`out/convergence.csv`). Kalshi market makers are almost certainly quoting off
Pyth or CME directly, and HL's oracle updates later (one Hyperliquid block, plus the validator oracle cadence).

**4. Backtest.** One taker entry per window, at the first signal where `edge − fee > thr`. The entry fills only if the quote
is still there after the latency delay, and is held to settlement. The fee is `0.07·P·(1−P)`.

| latency | thr | train $/contract (t) | test $/contract (t) |
|---|---|---|---|
| 0 s | 0.00 | −0.027 (−3.4) | −0.021 (−2.1) |
| 1 s | 0.03 | −0.028 (−3.0) | −0.016 (−1.4) |
| 1 s | 0.08 *(best on train)* | −0.017 (−1.8) | −0.006 (−0.6) |
| 3 s | 0.05 | −0.022 (−2.3) | −0.032 (−2.8) |

The full grid is in `out/backtest_grid.csv`. In the test period, WTI, copper and gold were slightly positive
(+1.8¢ to +2.6¢ per contract). None of them is significant (|t| < 1), and on train they were negative, so treat that as noise.

## Conclusion

Run plainly, streaming HL's oracle into an option-pricing fair value gives **no edge**. It is a slower copy of the price
Kalshi traders already use, and the model's large apparent "edges" are model error rather than stale Kalshi quotes.

## Directions that could still work (in rough order of promise)
1. **Use the HL order book, not the oracle.** On a fast move, the `xyz` perp book (`book5` at 0.5 s, plus trades) can
   reprice ahead of the oracle. The test: rebuild the lead-lag in §3 using microprice and trade flow at 100–500 ms
   resolution.
2. **Go to the source.** Stream Pyth directly (Hermes SSE), since it is the settlement feed. The thing to beat then is
   the Kalshi book's reaction time, not HL's.
3. **Make markets instead of taking.** Kalshi is well calibrated but wide in places. Quoting around a Pyth-based fair
   value and skewing on HL book pressure targets the spread and avoids paying taker fees. This needs the `book` /
   `book_delta` data (queue position, fills) rather than `ticker`.
4. **Stale strikes and settlement edge cases.** The natgas and copper basis jumps, together with the 24 s settlement lag,
   are where the model and Kalshi disagree most. Check whether Kalshi misprices the window after a basis jump.
5. **Larger sample.** This covers 12 days and 1 regime. Re-run after a few more weeks of recordings.

## Caveats
- Quotes come from Kalshi's `ticker` channel (top of book), not the full L2. Size at the touch is not checked, so
  fills are optimistic at size.
- Fees use 0.07·P(1−P) with no per-order rounding to the cent, which assumes lots of roughly 100 or more.
- HL and Kalshi timestamps are our receive times. Both collectors run on the same machine, so they share a clock.
- The alignment (`oracle`, +0 s) and the threshold were chosen on train only.

## Files
- `prelim.py`: the whole pipeline (alignment → calibration → lead-lag → backtest). It runs in about 20 s.
- `diag.py`: model variants, lead-lag cross-correlation, last-minute Brier by series.
- `run.log`, `diag.log`: console output of the runs above.
- `out/`: CSVs, `summary.json`, `prelim.png`.

```bash
cd ParagonArchitectGPUs
.venv/Scripts/python.exe Krish/Paragon15minMarketProject/prelim.py [--start 2026-11-01 --end 2026-11-12 --split 2026-11-07]
cd Krish/Paragon15minMarketProject && ../../.venv/Scripts/python.exe diag.py
```
