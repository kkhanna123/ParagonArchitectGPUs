# Next Steps

*Last updated 2026-10-04. Details: [`Data/tradexyz/report.md`](Data/tradexyz/report.md), [`Data/architect/report.md`](Data/architect/report.md).*

## trade[XYZ] / Hyperliquid (primary)

1. **Point the recorder at Hyperliquid**
   - [ ] Add a Hyperliquid source to `recorder/` (same raw JSONL.gz → parquet design as the AX recorder).
   - [ ] WS `wss://api.hyperliquid.xyz/ws`, no auth. Subscribe to `l2Book`, `trades` and `activeAssetCtx` for:
     - Commodities: `xyz:CL`, `BRENTOIL`, `NATGAS`, `HO`, `GOLD`, `SILVER`, `PLATINUM`, `PALLADIUM`, `COPPER`
     - Memory / semis / compute: `DRAM`, `MU`, `SKHX`, `SKHY`, `SMSN`, `SNDK`, `KIOXIA`, `NVDA`, `AMD`, `TSM`, `CRWV`, `NBIS`, `IREN`
     - Indices: `SP500`, `XYZ100`
     - Cross-dex overlaps: `para:` / `io:` versions of `AVGO`, `DRAM`, `SNDK`, `NBIS`, `CRWD`, `UNITREE`
   - [ ] Stay under the limits: 1000 subscriptions and 10 connections per IP.
   - [ ] Run on an always-on machine (not a laptop that sleeps).

2. **Backfill now** (the windows roll off)
   - [ ] `candleSnapshot` for every live xyz market at 1m, 5m, 15m, 1h, 1d. Each call returns only the last ~5000 bars, so 1m covers about 3.5 days.
   - [ ] Full hourly `fundingHistory` for every live market (500 rows/call, paginate by `startTime`).
   - [ ] Re-run the 1m backfill every couple of days until the recorder is live.

3. **Check the S3 archive for free historical ticks**
   - [ ] Create a free AWS account and set up credentials (`aws configure`).
   - [ ] Check `s3://hl-mainnet-node-data/node_fills_by_block` and `s3://hyperliquid-archive/market_data/` (requester-pays) for `xyz:` coins.
   - [ ] If present: estimate transfer cost, then backfill fills and L2 snapshots for the markets above.
   - [ ] Also check for any history of the delisted `xyz:H100` / `para:H100` GPU perps.

4. **First studies**
   - [ ] `xyz:CL` vs. the CME CL roll basket (rebuild the oracle from the published 5th–10th business-day roll), then do Brent, copper and nat gas the same way.
   - [ ] Weekend/overnight oracle drift vs. the Monday CME/NYSE opening gap.
   - [ ] Cross-dex same-asset spreads (xyz vs. para vs. io).
   - [ ] Memory-cycle relative value: `DRAM` vs. `MU` / `SKHX` / `SMSN` / `SNDK`.
   - [ ] Wallet-flow analysis using the buyer/seller addresses on trades.

5. **Supporting free data** (see architect report §2–4)
   - [ ] GPU spot-rental collector: Vast.ai, RunPod, Shadeform, Azure, every 5–15 min.
   - [ ] Commodity benchmarks: Yahoo (1m/1h/1d), FRED, EIA, CFTC COT.
   - [ ] Upgrade `yfinance` (`pip install -U yfinance`) or call the Yahoo chart API directly.
   - [ ] Ask the UChicago library about WRDS/TAQ for equity tick history.

6. **Before trading** (not needed for data)
   - [ ] Confirm jurisdiction/eligibility for trading HIP-3 perps.

## Architect / AX (paused)

- [x] Sandbox key in `.env` (gitignored); recorder + parquet converter built and tested against the sandbox.
- [ ] If we come back: email sales@architect.co as the Paragon pod for production / data-only access. Free accounts are sandbox-only, and the sandbox prices are synthetic.
- [ ] With production access: run `python -m recorder.ax_recorder --env prod`, ideally on AWS eu-west-2 (London).
