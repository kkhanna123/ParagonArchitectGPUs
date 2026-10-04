# Data Availability Report — trade[XYZ] (Hyperliquid HIP-3)

*Compiled 2026-10-04. Every source marked ✅ was queried live while writing this report. ⚠️ = works with caveats, 🔑 = free but needs a key or account, 💰 = paid.*

> **Naming:** `trades.xyz` returns an empty page. The venue is **trade.xyz** ("Trade[XYZ]"), a perpetuals exchange deployed **on Hyperliquid** through HIP-3 ("builder-deployed perps"). Its markets live under the dex name **`xyz`** with symbols like `xyz:CL`, `xyz:GOLD`, `xyz:NVDA`. All data comes from **Hyperliquid's public API**.

Companion report: [`../architect/report.md`](../architect/report.md). Its §2–4 cover the free *off-venue* data (cloud GPU prices, Yahoo/FRED/EIA/CFTC, EDGAR) that we'd pair against these perps. That isn't repeated here.

---

## 0. TL;DR

| Need | Source | Granularity | History | Cost |
|---|---|---|---|---|
| Order book | WS `l2Book` / REST `l2Book` ✅ | **20 levels/side**, with size **and order count** per level, every block (~0.5s) | Live only | Free, no key |
| Top of book | WS `bbo` ✅ | Every change | Live only | Free |
| Trades | WS `trades` ✅ | **Every trade**, with side, size, ms timestamp, tx hash, **both wallet addresses** | Live; REST gives last 10 | Free |
| Mark / oracle / funding / OI | WS `activeAssetCtx` ✅ | Every update | Live | Free |
| Candles | REST `candleSnapshot` ✅ | 1m … 1M | **Last ~5000 candles per interval**: 1m ≈ 3.5 days, 1h ≈ 7 months, 1d = since listing | Free |
| Funding history | REST `fundingHistory` ✅ | **Hourly** rate + premium | Since listing (500 rows/call, paginate) | Free |
| Historical L2 snapshots + asset contexts | S3 `hyperliquid-archive` ⚠️ | L2 snapshots (hourly files), daily asset-context CSVs | Archive (updated ~monthly, gaps possible) | AWS requester-pays (cents–dollars) 🔑 |
| Historical **fills** (tick-level) | S3 `hl-mainnet-node-data` ⚠️ | Every fill, by block | Archive | AWS requester-pays 🔑 |
| Per-wallet history | REST `userFills`, `userFunding`, `clearinghouseState` ✅ | Every fill / position per address | Rolling window per address | Free |

**Main takeaways vs. Architect**
1. **Free and no account needed.** Every live endpoint works anonymously. There's no KYC, no sandbox and no fake prices, and the data is **real production data today**.
2. **Free historical tick data exists:** Hyperliquid publishes historical **fills and L2 snapshots on S3**. That is the thing we couldn't get for AX. It costs only AWS transfer fees (requester-pays), and we need an AWS account to read it.
3. **Real liquidity in the markets we care about:** S&P 500 (~$37M/day), crude (~$14M), Brent (~$11M), SK Hynix, Micron, DRAM, Intel (~$9–14M each), gold, silver. Open interest runs into the hundreds of millions on the top markets.
4. **No live GPU compute market.** Both `xyz:H100` and `para:H100` (on the Paragon dex) are **delisted**, their last marks were $2.60 and $2.56/GPU-hr, and their candles and funding are no longer served. For GPU exposure here we'd use the memory/semis proxies (`DRAM`, `MU`, `SKHX`, `SKHY`, `SNDK`, `NVDA`, `CRWV`, `NBIS`).
5. **Every trade shows both wallet addresses**, so we can do **flow and wallet-level analysis**: who's informed, who gets liquidated, whale positioning. That's impossible on a regular exchange.

---

## 1. Access & limits

| | |
|---|---|
| REST | `POST https://api.hyperliquid.xyz/info` with JSON body `{"type": ..., "dex": "xyz"}` or `{"coin": "xyz:CL"}` |
| WebSocket | `wss://api.hyperliquid.xyz/ws` → `{"method":"subscribe","subscription":{"type":"l2Book","coin":"xyz:CL"}}` |
| Auth | **None** for market data. A wallet signature is only needed to *trade* |
| Python SDK | `hyperliquid-python-sdk` (official) |
| REST rate limit | **1200 weight/min per IP**. `l2Book` = 2; most info calls = 20; `candleSnapshot` costs +1 per 60 candles returned; `fundingHistory`/`recentTrades`/`userFills` cost +1 per 20 rows |
| WS limits | 10 connections, **1000 subscriptions**, 2000 sent msgs/min per IP |

All HIP-3 dexes share the same endpoints. `{"type":"perpDexs"}` lists them: `xyz`, `flx` (Felix), `vntl` (Ventuals), `hyna`, `km`/`mkts` (Kinetiq), `abcd`, `cash`, **`para` (Paragon)**, `io`.

---

## 2. What each endpoint gives (verified)

### 2.1 Live (websocket) — real-time, tick-level
| Channel | Payload | Notes |
|---|---|---|
| `l2Book` | `{coin, time(ms), levels:[bids[20], asks[20]]}`, each level `{px, sz, n}` where `n` = **number of orders** | Full snapshot each block. Use `nSigFigs` (2–5) to get aggregated, wider books. Not true L3 (no order IDs), but order counts per level |
| `bbo` | best bid/ask with size and order count | Lightest-weight quote stream |
| `trades` | `{side, px, sz, time, hash, tid, users:[buyer, seller]}` | Wallet addresses on **every** trade |
| `activeAssetCtx` | funding, open interest, oracle, mark, mid, **impact bid/ask**, premium, 24h volume | ~1/s. Gives oracle-vs-mark basis directly |
| `candle` | live OHLCV + trade count for any interval | |
| `allMids` | mids for every coin | Useful for cross-asset snapshots |

Sample from a 20 s test on `xyz:CL`, `xyz:GOLD` and `xyz:SP500`: 4 book snapshots, 10 trade batches, 20 asset-context updates, 4 BBO, 8 candle updates.

### 2.2 REST snapshots & history
| Request | Result (tested 2026-10-04) |
|---|---|
| `metaAndAssetCtxs` `dex:"xyz"` | 130 markets (110 live, 20 delisted) with max leverage, size decimals, growth-mode flag + live mark/oracle/funding/OI/volume |
| `l2Book` | 20 levels/side (e.g. `xyz:CL` best bid 91.122 ×1.288 (1 order) / ask 91.123 ×14.5 (3 orders)) |
| `recentTrades` | Last 10 trades only, so not a history source |
| `candleSnapshot` | ~5000 candles max: `xyz:CL` **1m: 2026-10-01 → now**, **1h: 2026-03-10 → now**, **1d: 2026-01-06 → now** (listing). Same for GOLD/SP500 |
| `fundingHistory` | **Hourly** `fundingRate` + `premium`, 500 rows per call. `xyz:CL` from 2026-01-06 |
| Delisted coins (`xyz:H100`, `para:H100`) | Candles and funding return **empty**. History is gone from the API; S3 may still have it |

### 2.3 Historical archives (S3, requester-pays — needs an AWS account)
Per Hyperliquid's docs (not yet pulled by us; anonymous access returns 403 "Requester Pays"):

| Bucket / path | Content |
|---|---|
| `s3://hyperliquid-archive/market_data/<date>/<hour>/l2Book/<coin>.lz4` | L2 book snapshots, hourly files |
| `s3://hyperliquid-archive/asset_ctxs/<date>.csv.lz4` | Daily files of asset context (mark, oracle, funding, OI) |
| `s3://hl-mainnet-node-data/node_fills_by_block` | **Every fill**, API format, which is the tick-level trade history |
| `…/node_trades`, `node_fills` | Legacy trade/fill formats |
| `…/misc_events_by_block` | Funding payments, transfers and other non-trade events |
| `…/explorer_blocks`, `replica_cmds` | Raw blocks / every L1 transaction (orders and cancels), i.e. full order-flow reconstruction if we go deep |

Caveats from the docs: updated roughly monthly, "data may be missing", no candles or spot data on S3. **Still to verify: whether HIP-3 (`xyz:`) coins are included.**

---

## 3. trade[XYZ] universe (live, 2026-10-04 — 110 markets)

Volumes and OI are the 24h snapshot at time of writing.

**Commodities** (external price 23/5 from spot for precious metals; for energy and copper a **rolling basket of CME futures**, 5th–10th business day, 20%/day)
| Symbol | Underlying | Max lev | 24h vol | OI |
|---|---|---|---|---|
| `xyz:CL` | WTI crude (CME CL, currently X6 → Z6 roll 10/07–10/13) | 20x | $14.1M | $134M |
| `xyz:BRENTOIL` | Brent (ICE) | 20x | $10.9M | $170M |
| `xyz:NATGAS` | Henry Hub | 10x | $1.1M | $8M |
| `xyz:HO` | NY Harbor ULSD (diesel) | 10x | $0.4M | $1.6M |
| `xyz:GOLD` | Spot gold | 25x | $2.4M | **$279M** |
| `xyz:SILVER` | Spot silver | 25x | $3.1M | $145M |
| `xyz:PLATINUM` / `xyz:PALLADIUM` | Spot | 20x | $0.08M / $0.03M | $3M / $2.7M |
| `xyz:COPPER` | COMEX HG (Z6 → H7) | 20x | $0.4M | $13M |
| *Delisted:* `URANIUM, ALUMINIUM, CORN, WHEAT, TTF` | | | | |

**Equity indices / ETFs**: `SP500` ($37M/day, $349M OI), `XYZ100` (their Nasdaq-100-style index, $21M), `JP225`, `KR200`, `EWY`, `EWJ`, `EWT`, `EWZ`, `SMH`, `SOXL`, `XLE`, `TLT`, `XBI`, `URNM`, `KORU`, `MAGS`.

**AI / semis / memory** (our GPU-adjacent complex): `NVDA` ($4.2M), `AMD`, `AVGO`, `TSM`, `ASML`, `ARM`, `INTC` ($11M), `MU` ($9M), `SKHX` + `SKHY` (SK Hynix, $14M + $1.7M), `SMSN` (Samsung), `SNDK`, `WDC`, `KIOXIA`, `CXMT`, `GIGADEV`, **`DRAM` ($10M/day, a DRAM-price market)**, `MRVL`, `QCOM`, `AMAT`, `LITE`, `AAOI`, `DELL`.
**Compute / neocloud**: `CRWV` (CoreWeave), `NBIS` (Nebius), `IREN`, `ORCL`, `CRCL`, `BE` (Bloom, data-center power), `GEV`.
**Hyperscalers / mega-caps**: `MSFT, GOOGL, AMZN, META, AAPL, TSLA, NFLX, PLTR`, …
**Pre-IPO perps**: `SPCX` (SpaceX, $3.4M), `CBRS` ($26M), `ZHIPU`, `MINIMAX`, `UNITREE`, `SHEIN`, `OURA`.
**FX**: `EUR`, `JPY`, `GBP` (50x).
**Delisted:** `H100` (GPU compute), `VIX`, `DXY`, `VOL`, `KRW`, `NIFTY`, `IBOV`, `LRCX`, `GLW`, and others.

**Events (HIP-4 outcome contracts)**: trade[XYZ] also runs prediction-style markets (IPO-by-date, sports). The team publishes a read-only feed (`codeberg.org/hl-fan/hip4-markets`).

### Oracle / pricing mechanics (what the perps actually track)
- **Oracle** = external fair price while the underlying market is open. When it's closed (weekends, overnight for stocks), the oracle **drifts via a 30-min EMA of the impact-price difference**, so the perp book itself drives the price.
- **External price** = frozen at the last external close while the market is closed. Comparing it with the oracle measures the **weekend/overnight implied move**.
- **Mark** blends the oracle with the book's basis (150 s EMA).
- **Funding** is hourly, anchored to the oracle. The baseline rate seen on most markets is 0.000625%/hr.

### Fees
HIP-3 standard fees are **2× Hyperliquid's base**, split 50/50 between Hyperliquid and trade[XYZ]. Under **growth mode** (most non-crypto, non-gold markets) fees drop ≥90%: base tier **0.009% taker / 0.003% maker**, going to **0% maker** at >$500M 14-day volume. Gold is excluded from growth mode.

---

## 4. Arb / research ideas this data supports

| Idea | Legs | Data |
|---|---|---|
| **Perp vs. CME futures basis** | `xyz:CL` vs CL=F (X6/Z6 basket), `BRENTOIL` vs BZ=F, `COPPER` vs HG=F, `NATGAS` vs NG=F | xyz WS + Yahoo 1m / FRED (architect report §3). The oracle uses a *known* roll basket, so we can replicate it exactly |
| **WTI–Brent spread on-chain** | `xyz:CL` vs `xyz:BRENTOIL` | Both on xyz, so it's one venue and one margin account |
| **Weekend / overnight price discovery** | Oracle vs. frozen external price, then Monday open of CME/NYSE | `activeAssetCtx` + Yahoo. Does the perp's weekend drift predict the Monday gap? |
| **Gold/silver ratio, precious-metals RV** | `GOLD`, `SILVER`, `PLATINUM`, `PALLADIUM` | xyz WS |
| **Cross-dex same-asset spreads** | e.g. `xyz:AVGO` 356.77 vs `para:AVGO` 354.57 (−0.6%); `xyz:DRAM` vs `io:DRAM`; `xyz:SNDK` vs `io:SNDK` (io has *more* volume); `NBIS`, `CRWD`, `UNITREE` | Same API, different `dex`. Snapshot on 2026-10-04 found 11 overlapping live symbols |
| **Memory-cycle RV** | `DRAM` vs `MU`, `SKHX`/`SKHY`, `SMSN`, `SNDK`, `KIOXIA`, `CXMT` | xyz WS + candles |
| **GPU-proxy basket vs. GPU spot rental prices** | `NVDA`, `CRWV`, `NBIS`, `IREN` + memory names vs. Vast/RunPod/Shadeform H100 $/hr | xyz + our spot collector (architect report §2) |
| **Funding-rate carry** | Hourly funding vs. borrow/carry of the underlying | `fundingHistory` (hourly since listing) |
| **Wallet-flow signals** | Trades tagged with wallet addresses → identify informed wallets / liquidations | `trades` WS + `userFills` + S3 `node_fills_by_block` |
| **Liquidation cascades** | OI drops + aggressive prints at 20–50x leverage | `activeAssetCtx` OI + trades |

---

## 5. Gaps & risks
- **No GPU compute market** (H100 perps delisted on both xyz and para). Compute exposure here is via proxies only.
- **Candle history is shallow** (~5000 bars): 1-minute bars cover only ~3.5 days, so **record forward** or use S3.
- **S3 coverage of HIP-3 markets is unverified**, and it's requester-pays (needs AWS credentials; small transfer cost).
- **Venue/regulatory risk**: HIP-3 perps are on-chain and unregulated; access restrictions may apply by jurisdiction (US persons). This matters for *trading* only; reading public data is unaffected. Check before trading.
- **Oracle risk**: the oracle is produced by trade[XYZ]'s relayer. Off-hours prices are internal (driven by the book), so "basis" on weekends partly measures the oracle model.
- **Thin long tail**: many markets trade <$100k/day; real depth is concentrated in the top ~25.

## 6. Recommended next steps
1. **Point our recorder at Hyperliquid.** Our `recorder/` design (raw JSONL.gz → parquet) carries over directly: subscribe `l2Book` + `trades` + `activeAssetCtx` for the commodity, semis/memory and index markets (well under the 1000-subscription cap). Unlike AX this is **real data from day one, free**.
2. **Backfill now** while the windows exist: all `candleSnapshot` intervals and full `fundingHistory` for every live xyz market. The 1-minute bars roll off after ~3.5 days.
3. **Set up an AWS account** (free tier is fine) to test `hl-mainnet-node-data/node_fills_by_block` for `xyz:` coins. If they're present, we get **free historical tick trades**.
4. Build the first study: **`xyz:CL` vs CME CL basket basis** (cleanest, highest-volume commodity leg), then weekend oracle drift vs. Monday gaps.
