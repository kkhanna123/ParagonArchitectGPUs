# Data Availability Report — GPU Compute, Energy, Metals & Equities

*Compiled 2026-10-04. Every source marked ✅ was queried live while writing this report. ⚠️ = works with caveats, 🔑 = free but needs a key or account, 💰 = paid.*

The goal is to find out what free data we can pull to research **GPU compute vs. commodities (oil/gas, metals) vs. equities** relationships and arbitrages, and at what **granularity** we can get it.

---

## 0. TL;DR

| Need | Best free source | Granularity | History |
|---|---|---|---|
| GPU compute futures (H100/H200/B200/B300) | **Architect AX API** 🔑 | L1/L2/**L3** book, trades, candles 1s→1d | From listing (2026); see §1.5 |
| Oil / metals / equity **perps** (same venue) | **Architect AX API** 🔑 | Same as above, + funding-rate history | From listing |
| Spot GPU rental prices | Vast.ai ✅, RunPod ✅, Shadeform ✅, Azure ✅, AWS ✅, Oracle ✅ | Live snapshots (we build history by polling) | None free → **start a collector now** |
| GPU price **indices** (the settlement benchmarks) | Compute Desk (AX), Silicon Data (CME) | Daily | 💰 Not free |
| CME futures (CL, BZ, NG, HG, GC, SI, ALI) | Yahoo chart API ✅ / Architect brokerage 🔑 | 1m (7d), 5m (60d), 1h (2y), 1d (2000+) | See §3 |
| Spot oil/gas/metals benchmarks | FRED ✅, EIA ✅ | Daily (oil, gas), monthly (base metals) | 1986+ |
| Equities (NVDA, AMD, TSM, CRWV…) | Yahoo ✅ / AX perps 🔑 | 1m → 1d | Decades (daily) |
| Positioning | CFTC COT ✅ | Weekly | 1986+ |
| Fundamentals | SEC EDGAR XBRL ✅, Epoch AI ✅, FRED semis ✅ | Quarterly / event | Long |

**Main takeaways**
1. **Architect's API covers almost everything on one clock.** AX lists GPU compute futures *and* WTI, gold, silver, copper, aluminum, oil/gas ETF perps, and semiconductor/hyperscaler equity perps. That is the ideal venue for cross-asset relative value: same margin system, same funding mechanics, and one data feed.
2. **The GPU *index* data is not free.** Compute Desk (settles AX) and Silicon Data (settles CME's new H100/B200 futures, launching **2026-10-05**) are both paid. Our free stand-in is **spot rental prices scraped from cloud and marketplace APIs**. We should start logging these immediately because none of them provide history.
3. **AX history is only as deep as the listings.** The REST `/trades` endpoint returns just the **last 100 trades**, so for tick-level history we must **record the websocket ourselves** from day one.

---

## 1. Architect (primary venue)

Architect has two separate products, each with its own API:

| | **AX — Architect Exchange** | **Architect Brokerage** |
|---|---|---|
| What | Exchange for perpetual and dated futures on traditional assets (regulated in Bermuda by the BMA) | Futures broker routing to **CME** etc. |
| Docs | https://docs.architect.exchange (index: `/llms.txt`, OpenAPI at `/openapi/api-gateway.json`) | https://docs.architect.co |
| API | REST `https://gateway.architect.exchange/api` + WS `wss://gateway.architect.exchange/md/ws` | `architect-py` / Rust SDK (gRPC) |
| Sandbox | `https://gateway.sandbox.architect.exchange/api` (34 instruments live vs. 46 in prod) | Paper trading on an unverified account |
| Auth | `POST /authenticate` with api_key + secret → bearer token | API key from the Architect UI |
| Cost of data | Included with an account | Unverified/paper: **delayed** data, free. Verified (KYC + deposit): **live CME L1 + L2 at no extra charge**. Plus = $100/mo (API trading, algos) |
| Rate limit | 100 req/s per account | See docs `/concepts/rate-limiting` |
| Hosting | AWS eu-west-2 (London) | — |

> ⚠️ AX docs say *"We are not yet onboarding individual accounts"*. We need an **institutional / omnibus** account or a sandbox key. Confirm what access the pod has.

### 1.1 Public without a key ✅
- `GET /api/health`
- `GET /api/instruments` — full contract specs: tick size, margin, contract size, **underlying benchmark**, funding schedule, holiday calendar, roll composition. A snapshot is saved in the scratchpad; re-pull it anytime.

Everything else returns `401 no auth presented`.

### 1.2 Market data with a key 🔑 — what level we get

| Endpoint | Level of data |
|---|---|
| `GET /book?symbol=&level=2\|3` | **L2** (aggregated per price) or **L3** (individual order quantities per level) snapshot |
| WS `md/ws` `subscribe` `level: LEVEL_1 / LEVEL_2 / LEVEL_3 / TRADES` | Streaming book diffs + trades + ticker. The ticker includes a **live estimated funding rate** (`ef`) on symbols with an index feed |
| WS `subscribe_candles` | Streaming candles |
| `GET /candles` | OHLC + **buy volume / sell volume** split; widths `1s, 5s, 1m, 5m, 15m, 1h, 1d`; time-range query in ns |
| `GET /bbo-candles` | Candles of best bid/offer (good for spread and liquidity history) |
| `GET /trades` | Last **≤100** trades (price, qty, aggressor side). **Not a history endpoint** |
| `GET /tickers`, `/ticker` | Snapshot of last, bid, ask, volume etc. |
| `GET /funding-rates` | **Historical funding**: rate, amount, **benchmark price**, settlement price. Cursor-paged, unbounded range. This gives us the benchmark (e.g. CME WTI settle, WMR gold fix) for free |
| `GET /upcoming-special-settlements` | Dividends and corporate actions passed through to equity perps |
| Liquidity-program endpoints | Maker-reward accruals and score snapshots (relevant if we quote) |

Timestamps are seconds (`ts`) plus nanoseconds (`tn`).

### 1.3 AX instrument universe (live as of 2026-10-04, 46 instruments)

**Compute (12) — dated futures, cash-settled, 730 GPU-hours/contract (≈1 GPU-month), quoted in $/GPU-hour, tick 0.001, 20% IM / 15% MM**
| Product | Expiries | Benchmark |
|---|---|---|
| `NVDA-H100-*` | 2026-SEP, 2026-DEC, 2027-MAR | Compute Desk NVIDIA H100 GPU Index |
| `NVDA-H200-*` | same | Compute Desk H200 Index |
| `NVDA-B200-*` | same | Compute Desk B200 Index |
| `NVDA-B300-*` | same | Compute Desk B300 Index |

→ This gives a **term structure per chip** and **cross-generation spreads** (H100 vs H200 vs B200 vs B300), which are the core GPU trades.

**Energy**
| Symbol | Underlying benchmark | Notes |
|---|---|---|
| `WTIOIL-PERP` | CME WTI (CL) daily settle | 1 bbl/contract; rolls 80/20 across months (currently CLX6 → CLZ6), with the composition published in `additional_product_specs` |
| `USO-PERP`, `BNO-PERP`, `UNG-PERP` | Arca official close of the ETF | Oil, Brent and nat-gas ETF perps |

**Metals**
| Symbol | Benchmark |
|---|---|
| `XAU-PERP`, `XAU-2026-SEP`, `XAU-2026-DEC` | WMR gold daily close (perp **and** dated, so basis/term trades are possible) |
| `XAG-PERP` | WMR silver close |
| `XCU-PERP` | COMEX copper (HG) settle, rolling (HGZ6 → HGH7) |
| `XAL-PERP` | LME aluminum (AH) settle |

**Equities (19)** — benchmark is the trailing 30-min TWAP of the stock; funding is paid 13×/day during US hours; perps trade 23/7.
AI/semis complex: `NVDA, AMD, AVGO, TSM, ASML, ARM, MU, SKHY (SK Hynix), SNDK, INTC`.
Hyperscalers (GPU buyers): `MSFT, GOOGL, AMZN, META`. Other: `AAPL, TSLA, SPCX (SpaceX)`. Index ETFs: `SPY, QQQ`.

**FX / Rates**: `EURUSD, JPYUSD, MXNUSD, BRLUSD` perps; `UST10Y-PERP` (MVOTR10 index).

### 1.4 Architect Brokerage → CME data
`architect-py` provides L1/L2 book snapshots and diff streams, trades, streaming candles (down to 1s) and **historical candles** for CME products, e.g. `"GC 20250626 CME Future/USD"`. This is our route to *real-time* CME CL/BZ/NG/HG/GC/SI to price against the AX perps. Paper accounts are **delayed**; live data needs a verified account.

### 1.5 Gaps / to-do on Architect
- [ ] Get API key (sandbox first) and test how far back `/candles` actually goes. The docs don't state a retention limit.
- [ ] Stand up a **WS recorder** (L2/L3 + trades) for all compute + commodity + semis symbols → parquet. This is the only way to get tick history.
- [ ] Backfill `/funding-rates` for every perp. It returns the **benchmark price series** too, which is free settle data.
- [ ] Ask Architect whether Compute Desk index values are available to account holders (they are the settlement index).

---

## 2. GPU compute — spot & fundamentals (free)

None of the free sources keep price history, so **we must poll and store**. Suggested cadence: 5–15 min.

| Source | Status | What / level | Notes |
|---|---|---|---|
| **Vast.ai** `console.vast.ai/api/v0/bundles/` | ✅ no key | Per-offer marketplace listing: GPU model, count, $/hr total, **min bid** (interruptible), reliability, location, host specs | Returns ~64 offers per call, so query per `gpu_name` to cover the book. This is the closest thing to a free "order book" for GPU rental |
| **RunPod** GraphQL `api.runpod.io/graphql` | ✅ no key | Per GPU type: lowest on-demand ($/hr) and spot/bid price, secure vs. community cloud, VRAM | Includes AMD MI300X/MI350X |
| **Shadeform** `api.shadeform.ai/v1/instances/types` | ✅ no key | **Aggregator** across many neoclouds (Lambda, Crusoe, etc.): instance type, GPU, $/hr, availability per region | Best single free cross-provider snapshot |
| **Azure Retail Prices** `prices.azure.com/api/retail/prices` | ✅ no key | Every SKU × region: on-demand, **spot**, reserved; includes `effectiveStartDate` | Has a *dated* price history (e.g. ND H100 v5 spot changes). 277 rows for ND96isr_H100_v5 alone |
| **AWS Pricing bulk** `pricing.us-east-1.amazonaws.com/offers/v1.0/aws/AmazonEC2/...` | ✅ no key | On-demand/reserved for p4d/p5/p5e/p6 etc. | ~480 MB per region file; use the `/index.json` per region |
| **AWS Spot Advisor** `spot-bid-advisor.s3.amazonaws.com/spot-advisor-data.json` | ✅ no key | Savings % and interruption bucket per instance | e.g. p5.48xlarge us-east-1: 57% savings |
| **AWS Spot price history** (`DescribeSpotPriceHistory`) | 🔑 any free AWS account | **90 days** of spot price per instance/AZ, at event-level timestamps | Best free *historical* GPU price series. Needs AWS creds (no CLI installed yet) |
| **GCP Cloud Billing Catalog API** | 🔑 free API key | SKUs + prices for A3/A4 (H100/B200) | Old public pricelist JSON is dead (404) |
| **Oracle Cloud** `apexapps.oracle.com/.../cetools/api/v1/products` | ✅ no key | List prices incl. GPU shapes (A10, L40S, H100…) | |
| Lambda Cloud API | 🔑 free account | Instance types + availability | Returns 401 without a key |
| TensorDock | ❌ | Old v0 endpoint is gone | Skip |
| **Compute Desk** (DESKH100/H200/B200/B300, GX Hopper/Blackwell) | 💰 | Daily index based on private transactions — **AX settlement index** | Website charts only |
| **Silicon Data** (SDH100RT, SDB200RT, SDA100RT, MI300X…) | 💰 (7-day trial) | Daily — **CME GPU futures settlement index** (CME H100/B200 launch 2026-10-05) | Via portal, API, Kaiko |

**Fundamentals / supply side**
| Source | Status | Level |
|---|---|---|
| **Epoch AI** `epoch.ai/data/ml_hardware.csv`, `gpu_clusters.csv`, `all_ai_models.csv` | ✅ | 178 accelerators (FLOPs, memory, release price); 482 GPU clusters (H100-equivalents, owner, country, date) |
| **SEC EDGAR XBRL** `data.sec.gov/api/xbrl/...` | ✅ (set a User-Agent) | Quarterly revenue, capex, PP&E for NVDA, AMD, CRWV, MSFT, META, GOOGL, AMZN, ORCL. Hyperscaler **capex** is a key demand signal |
| **FRED** `IPG3344S` (semis industrial production), `PCU33443344` (semis PPI) | ✅ | Monthly, 1972+ / 1984+ |
| TSMC monthly revenue (investor site) | ✅ (scrape) | Monthly, leading indicator |

---

## 3. Energy & metals (free)

| Source | Status | Instruments | Granularity / history |
|---|---|---|---|
| **Yahoo chart API** `query1.finance.yahoo.com/v8/finance/chart/{sym}` | ✅ (use raw HTTP + browser UA; installed `yfinance 0.1.55` is too old and hangs, so upgrade it) | `CL=F` WTI, `BZ=F` Brent, `NG=F` Henry Hub, `HO=F`, `RB=F`, `HG=F` copper, `GC=F` gold, `SI=F` silver, `PL=F`, `PA=F`, `ALI=F` aluminum | **1m: last 7d**, **5m: last 60d**, **1h: last 2y**, **1d: back to 2000** (use `period1/period2`, because `range=max` downsamples to monthly). Front-month continuous only, not individual contract months |
| **FRED** | ✅ no key (CSV) | `DCOILWTICO` (WTI, 1986+), `DCOILBRENTEU` (Brent, 1987+), `DHHNGSP` (Henry Hub, 1997+) — **daily** | Spot through 2026-09-29 |
| | | `PCOPPUSDM`, `PALUMUSDM`, `PNICKUSDM` (IMF copper/alu/nickel) — **monthly** 1992+ | LBMA gold series was removed from FRED; use GC=F or AX funding benchmark |
| **EIA API v2** `api.eia.gov/v2/` | ✅ (`DEMO_KEY` works; get a free key for volume) | Spot prices (WTI, Brent, products, natgas), **weekly inventories**, production, refinery runs, **electricity prices** | Daily/weekly/monthly; 5000 rows per call |
| **CFTC Commitments of Traders** `publicreporting.cftc.gov` (Socrata) | ✅ no key | Positioning for every CFTC market (crude, natgas, copper, gold…) | Weekly, latest 2026-09-29 |
| **World Bank Pink Sheet** (CMO historical xlsx) | ✅ | ~70 commodities incl. oil, gas, coal, metals | Monthly, 1960+ |
| **AX `/funding-rates`** | 🔑 | Daily CME WTI settle, COMEX HG settle, LME AH settle, WMR gold/silver fix (returned as `benchmark_price`) | Daily since listing |
| Architect brokerage CME feed | 🔑 | Real-time L1/L2 on actual CME contract months | Real-time; historical candles per SDK |
| Stooq | ❌ | Now behind a JS proof-of-work wall | Skip |
| LME official | 💰 | — | Use AX XAL benchmark or FRED monthly instead |

**Power / electricity** (GPU cost input): EIA electricity data ✅. ISO real-time LMPs (ERCOT and PJM, which cover major data-center hubs) are free from the ISO websites; not yet tested.

---

## 4. Equities (free)

| Source | Status | Level |
|---|---|---|
| **Yahoo chart API** | ✅ | NVDA 1m for last ~7d; daily decades; CRWV (CoreWeave) since IPO 2025-03-28. Also NBIS, ORCL, SMCI, VRT, VST/CEG (power) |
| **AX equity perps** | 🔑 | L1–L3 + trades + funding for the 19 names in §1.3, 23/7 (includes overnight/weekend price discovery the cash market lacks) |
| SEC EDGAR | ✅ | Filings + XBRL fundamentals |

---

## 5. Arb / relative-value ideas this data supports

| Idea | Legs | Data needed |
|---|---|---|
| **Perp vs. benchmark basis** | AX `WTIOIL-PERP` vs. CME CL settle; `XAU-PERP` vs. WMR fix; `XCU-PERP` vs. HG | AX book + `/funding-rates` (benchmark included) + Yahoo/brokerage CME |
| **Perp vs. ETF perp** | `WTIOIL-PERP` vs `USO-PERP`; `BNO-PERP` vs. Brent; `UNG-PERP` vs NG=F | All on AX, so a clean same-venue spread |
| **Gold perp vs. dated** | `XAU-PERP` vs `XAU-2026-DEC` | Implied financing vs. funding paid |
| **GPU term structure** | H100 SEP/DEC/MAR, etc. | AX compute book |
| **GPU generation spread** | H100 vs H200 vs B200 vs B300 (per $/FLOP using Epoch specs) | AX + Epoch `ml_hardware.csv` |
| **GPU futures vs. spot rental** | AX `NVDA-H100-*` vs. Vast/RunPod/Shadeform/Azure spot H100 $/hr | AX + **our spot collector** |
| **AX vs. CME compute futures** | AX (Compute Desk index) vs. CME H100/B200 (Silicon Data index) from 2026-10-05 | AX + brokerage CME feed; index-methodology mismatch is the edge *and* the risk |
| **GPU vs. energy cost** | GPU $/hr vs. natgas/power (electricity is the main opex) | AX + EIA/FRED/NG=F |
| **GPU vs. equities** | H100 price vs. `NVDA-PERP`, `CRWV`, hyperscaler perps; memory names (`MU`, `SKHY`, `SNDK`) vs. Blackwell pricing | AX + Yahoo |
| **Copper/aluminum vs. AI capex** | `XCU`/`XAL` vs. data-center buildout (EDGAR capex, Epoch clusters) | AX + EDGAR + Epoch |

---

## 6. Recommended next steps
1. **Get Architect credentials** (AX sandbox → prod; brokerage for CME) and confirm candle-history depth.
2. **Start collectors now**, because history only exists from when we begin: (a) AX WS L2/L3 + trades for all compute, energy, metals and semis symbols; (b) GPU spot snapshots from Vast, RunPod, Shadeform and Azure every 5–15 min.
3. **Backfill free history**: FRED/EIA daily commodities, Yahoo 1h (2y) and 1d, AX `/funding-rates`, CFTC COT, Epoch, EDGAR capex.
4. Upgrade `yfinance` (`pip install -U yfinance`) or call the Yahoo chart endpoint directly.
5. Decide whether a Silicon Data trial or Compute Desk access is worth it to get the true settlement indices.
