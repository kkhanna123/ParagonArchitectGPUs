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

---

# Follow-up: market-making during the CME break, latency, and liquidity rewards (`cme_break_mm.py`)

**Question.** While CME is shut, stream the HL `xyz` fair into Kalshi, make markets when the books widen, and collect
liquidity rewards on top. Does that make money? How much latency can it tolerate?

**Short answer: no.**
- **During the break:** the HL fair carries no information about the Pyth settlement price, and the break PnL is noise.
- **Normal hours:** making markets only pays if your price arrives **at least about 3 s before the HL feed**. About 5 s
  ahead earns roughly 1.5¢/contract. HL can't give you that lead, because it is the slow feed.
- **Liquidity rewards:** the programs on these series ended on 2026-10-28.

## When CME is down, Kalshi mostly is too
- **Weekends:** no windows from Friday 17:00 ET to Sunday 18:00 ET.
- **Daily 17:00–18:00 ET break:** only **WTI, Brent and NatGas** keep listing, 4 windows each. That is 96 windows over the
  sample, about 12 a day. Metals and copper pause.
- **Pyth keeps moving through the break:** only 2–4% of break windows settle exactly at their strike. The HL oracle also
  moves, but independently of Pyth.

## What the break looks like (2026-11-01..11-12)
| | CME break | normal | first 30 min after reopen |
|---|---|---|---|
| median Kalshi spread | **2¢** | 1¢ | 2¢ |
| books wider than 5¢ | 14% | 7% | 14% |
| median size at the touch | 15 | 49 | 18 |
| HL→Pyth basis-forecast error (MAD) | **9.2 bp** | 0.8 bp | 2.7 bp |
| Brier score, HL model | **0.326** (worse than a coin flip, 0.25) | 0.164 | 0.219 |
| Brier score, Kalshi mid | 0.187 | 0.155 | 0.186 |
| corr(Δ Kalshi mid, Δ HL fair 5 s earlier) | **≈0.00** | 0.53 | 0.37 |

The books do widen, but the HL fair is uninformative during the break: the basis error is larger than the typical
15-minute move. Kalshi stays reasonably calibrated, so whoever is making those markets has a better source than HL.

## Maker simulation
The simulation replays every Kalshi trade:
- We quote `fair ± h` on the cent grid and never cross the book. A trade fills us when its price reaches our quote.
- Each fill takes up to 100 contracts, and every position is held to settlement.
- There is no maker fee (sensitivity is left in the code).
- `L` is how far behind the HL feed our quote is. Negative `L` means we see the price |L| s **before** our HL recorder
  does, which stands in for a faster feed (Pyth Hermes, CME/ICE direct).

The tables below use the "touch" fill rule; the "strict" rule is in `mm.log` / `out/mm_sim.csv`.

Normal hours, $/contract (t-stat over windows):

| h \ L | −5 s | −3 s | −1 s | 0 s | +2 s |
|---|---|---|---|---|---|
| 1¢ | **+0.012** (2.8) | −0.000 (0.0) | −0.006 | −0.007 (−2.0) | −0.009 (−2.8) |
| 3¢ | **+0.015** (2.9) | +0.001 (0.2) | −0.007 | −0.009 (−2.2) | −0.011 (−2.9) |

- **Break-even is L ≈ −3 s.** You need to see the price about 3 s before the HL recorder does, and about 5 s before
  for a real edge. This matches the finding that Kalshi leads HL by about 5 s.
- The negative-L rows use HL's own future prices as the "faster feed". That is perfect foresight of a lagging feed, so
  they are an **upper bound**.
- Your Kalshi order round trip also has to fit inside the lead.

**CME break:** +1.5 to +4.6¢/contract at every latency, but **t ≈ 0.25–0.74**.
- Only 46% of windows are positive.
- The top 5 windows account for more than 100% of the total. The other windows lose money net.
- Inventory swings of ±60k contracts show it is taking directional positions on an uninformative fair.
- The PnL doesn't change with latency, which confirms the HL feed isn't driving it.
- Treat it as **noise, not edge**.

## Liquidity rewards
Kalshi's `/incentive_programs` endpoint shows the history:
- Every 15-min commodity series had a `series_lip` program paying **$20 per window** ($50 for Brent).
- Target size 300 contracts, 50% discount factor.
- **All of them ended on 2026-10-28**; gold, silver and WTI ended on 2026-09-19. None were live during this sample.

If the program came back:
- Resting 300 contracts at the inside would be most of the touch (median touch size is 15–49), so roughly **$17–19 per
  window**.
- During the break that is only about 12 windows a day, or **about $230/day**.
- In normal hours the ceiling is about $9k/day across all series.
- The rewards don't cover adverse selection at L = 0. That costs about 0.7–0.9¢/contract, so the rewards break even
  only if fills stay below about 2.5k contracts per window. The simulation fills about 12k per window at the inside.

## What would be required
1. **A price source at least 3–5 s ahead of HL's oracle:** Pyth Hermes (the settlement feed) or CME/ICE direct, plus
   sub-second Kalshi order entry. Test it directly by recording Pyth next to Kalshi and re-running this script with Pyth
   as `S_t`.
2. **For the break specifically:** find what Kalshi's makers price off when CME is shut. Candidates are ICE Brent, which
   trades through the CME break, and Pyth's own break-time publishers. That source is the fair; HL isn't.
3. **Watch `/incentive_programs`.** If a LIP comes back, quoting at a 2–3¢ half-spread with a fast fair is the one setup
   where spread plus rewards could clear adverse selection.

## Caveats
- There is no queue model: "touch" assumes we're first at our price, so fill volume is optimistic. Read the $/contract
  numbers, not $/day.
- The break sample is small: 96 windows, 3 series, 9 weekdays.
- LIP shares are a rough estimate from top-of-book size only.

Files: `cme_break_mm.py`, `mm.log`, `out/mm_sim.csv`, `out/mm_market_state.csv`, `out/mm_summary.json`.
Run time is about 9 minutes.
