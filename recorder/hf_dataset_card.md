---
pretty_name: Paragon market data (trade[XYZ] / Hyperliquid HIP-3 + Kalshi)
license: other
tags:
- finance
- market-data
- prediction-markets
---

# Paragon market data

The Paragon pod's market recordings, mirrored daily from the collector
([ParagonArchitectGPUs](https://github.com/kkhanna123/ParagonArchitectGPUs), `recorder/`).
**Private, for ParagonGlobal members only.** Exchange data is subject to the venues' terms; do not redistribute.

## Layout
Parquet, hive-partitioned (the repo root mirrors `<PARAGON_DATA_ROOT>/parquet`; `hf_pull` puts it back there), prices and sizes stored as int64 scaled by 1e8 (the DuckDB views un-scale them).

| path | contents | cadence |
|---|---|---|
| `hl/book5/date=D/dex=X/sym=S/` | 5-level L2 | ~0.5 s |
| `hl/book/…` | 20-level L2 with order counts | ~5 s |
| `hl/trades/…` | every trade, with buyer/seller wallets | tick |
| `hl/asset_ctx/…` | mark, oracle, mid, funding, OI, impact prices | ~1–2 s |
| `kalshi/book_delta/date=D/series=S/` | lossless book feed (snapshots + deltas) | tick |
| `kalshi/book/…` | top-5 YES/NO bids per change | tick |
| `kalshi/trades/…`, `ticker/…`, `markets/…` | trades, best bid/ask, contract metadata | tick |
| `hl_hist/…`, `kalshi_hist/…` | REST backfills: candles, funding, settled markets + outcomes | daily refresh |

One commit per UTC day; days appear the morning after (US Central).

## Pull
```bash
git clone https://github.com/kkhanna123/ParagonArchitectGPUs && cd ParagonArchitectGPUs
python -m venv .venv && .venv/Scripts/pip install -r requirements.txt   # Linux/mac: .venv/bin/pip
hf auth login                                                           # read token
python -m recorder.hf_pull --dest D:/ParagonData \
    --tables kalshi/trades,kalshi/ticker,hl/asset_ctx --start 2026-11-01 --end 2026-11-12 --hist
set PARAGON_DATA_ROOT=D:/ParagonData                                    # export on Linux/mac
```
Without `--tables/--start/--end` it pulls everything (tens of GB). `hf_pull` rebuilds `hl.duckdb` and
`kalshi.duckdb` in the destination, so the views and `Krish/Paragon15minMarketProject/*` work unchanged.
