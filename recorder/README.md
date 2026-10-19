# AX market-data recorder

Records the Architect AX websocket feed (L1/L2/L3 order book, trades, ticker, candles) to disk, and converts the recordings to parquet.
AX has no historical tick endpoint, so **this recording is our only tick history**.

## Setup
```bash
uv venv -p 3.12 .venv && uv pip install -p .venv -r requirements.txt
```
Keys go in `.env` at the repo root (gitignored):
```
ARCHITECT_API_KEY=...
ARCHITECT_API_SECRET=...
```

## Record
```bash
.venv/bin/python -m recorder.ax_recorder --env sandbox                 # every symbol, LEVEL_3
.venv/bin/python -m recorder.ax_recorder --env sandbox --candles 1s,1m  # + candle streams
.venv/bin/python -m recorder.ax_recorder --env prod --symbols NVDA-H100-2026-DEC,WTIOIL-PERP,XAU-PERP
```
- Output: `Data/raw/ax/<env>/<YYYY-MM-DD>/<HH>.jsonl.gz` (UTC hours). Each line has `rx_ns` (local receive time) plus the raw message, so nothing is lost.
- `connected` and `disconnected` event lines mark gaps.
- Reconnects automatically with backoff. It also reconnects if the feed goes silent for 20s (heartbeats arrive every ~5s).
- Stop with Ctrl-C or SIGTERM. Files are flushed and closed cleanly.
- Going to production only changes `--env prod`. Our current free key is **sandbox-only**.

## Convert
```bash
.venv/bin/python -m recorder.ax_to_parquet --env sandbox            # all days
.venv/bin/python -m recorder.ax_to_parquet --env sandbox --date 2026-10-04 --depth 20
```
Writes `Data/parquet/ax/<env>/{book,trades,tickers,candles,events}/<date>.parquet`.
- `book`: top-N levels per side, with price, qty, and **order count** (`*_n_i`, L3 only).
- `exch_ns`: exchange timestamp. `rx_ns`: our receive timestamp.

## Notes from testing (sandbox, 2026-10-04)
- Book messages arrive as **full snapshots** (`st: true`) on each change. The converter also handles diffs (`st: false`, qty 0 = delete) in case production sends them.
- Median exchange→local latency is ~88 ms from Chicago, because the servers are in AWS eu-west-2 (London). Initial snapshots carry the time of each book's last change, so drop them when measuring latency.
- Raw size is tiny on the sandbox (~28 KB/min for all 33 symbols). Production will be larger.
- Sandbox prices are synthetic, so use the sandbox for plumbing only.

## Tests
```bash
.venv/bin/python -m pytest tests -q
```

---

# Hyperliquid recorder (Markets.xyz, trade[XYZ] and other HIP-3 dexes)

Same design as the AX recorder: raw messages go to gzipped JSONL, and a converter turns them into
per-day parquet. Hyperliquid market data is public, so **no keys are needed**. The data is real
from day one.

## Setup
Same `.venv` and `requirements.txt` as above. On Windows, run `recorder\windows\setup.bat` once.
It makes `.venv` with plain `python -m venv` and needs no `uv`.

## Record
```bash
.venv/bin/python -m recorder.hl_recorder                     # Markets.xyz (Kinetiq): every live market
.venv/bin/python -m recorder.hl_recorder --coins US500,USTECH
# trade[XYZ], the steps.md list:
.venv/bin/python -m recorder.hl_recorder --dex xyz --coins CL,BRENTOIL,NATGAS,HO,GOLD,SILVER,PLATINUM,PALLADIUM,COPPER,DRAM,MU,SKHX,SKHY,SMSN,SNDK,KIOXIA,NVDA,AMD,TSM,CRWV,NBIS,IREN,SP500,XYZ100
# cross-dex overlaps: one recorder per dex, run side by side
.venv/bin/python -m recorder.hl_recorder --dex para --coins AVGO,DRAM,SNDK,NBIS,CRWD,UNITREE
```
On Windows, use `recorder\windows\record.bat` with the same arguments.

- **Output:** `Data/raw/hyperliquid/<dex>/<YYYY-MM-DD>/<HH>.jsonl.gz`, in the same format as AX: `{"rx_ns", "msg"}` lines plus
  `started` / `connected` / `disconnected` / `markets_changed` / `stopped` event lines.
- **Channels:** `l2Book` (all 20 levels, with order counts), `trades` (with **buyer and seller wallets**),
  `activeAssetCtx` (mark, oracle, funding, OI, impact prices) and `bbo`. A REST market snapshot
  (`metaAndAssetCtxs`) is also written every 60s.
- **Market list:** `--dex auto` (the default) picks Markets.xyz's live dex. Kinetiq also has an old, empty `km` dex
  next to the live `mkts`. The market list is re-checked every minute: new listings are subscribed and delistings
  dropped, with new sockets opened *before* old ones close so there's no gap.
- **Keeping the connection alive:** pings every 30s, and a reconnect with backoff if a socket goes silent for 60s.
  Subscriptions are split 100 per socket (Hyperliquid's limit is 1000 subs / 10 sockets per IP).
- **Crashes and outages:** the recorder restarts by itself after a crash or a network outage, with backoff from 5s
  to 5 min. Ctrl-C or SIGTERM stops it cleanly; Ctrl-Break also works on Windows. Only one recorder can write to a
  given folder at a time.
- **Automatic conversion:** each finished UTC day is converted to parquet automatically, 10 min after midnight
  (`--no-convert` turns this off). Days missed while it was off are caught up at startup.
- **Log:** `Data/raw/hyperliquid/recorder_<dex>.log` (rotating). Live health: `Data/raw/hyperliquid/<dex>/status.json`.

## Convert
```bash
.venv/bin/python -m recorder.hl_to_parquet                           # every dex/day not converted yet
.venv/bin/python -m recorder.hl_to_parquet --dex xyz --date today --depth 10
```
Writes `Data/parquet/hyperliquid/<dex>/{book,bbo,trades,ctx,meta,events}/<date>.parquet`:
- `book`: `bid_px_0`/`bid_qty_0`/`bid_n_0` … (level 0 = best). Defaults to 5 levels; the raw files keep all 20,
  so re-run with `--depth` for more.
- `trades`: `px qty side tid hash buyer seller`. Trades that Hyperliquid re-sends on every resubscribe are
  de-duplicated.
- `ctx`: `mark_px oracle_px mid_px funding open_interest premium day_ntl_vlm prev_day_px impact_bid_px impact_ask_px`.
  `funding` is the hourly rate.
- `meta`: one row per market per snapshot, including delisted ones (`live = false`).
- `exch_ns` is the exchange timestamp, `rx_ns` ours. Hyperliquid stamps in ms, stored here as ns.

The converter streams, so memory stays flat (~300 MB) however busy the day is. It also reads hours whose last
gzip member was cut off by a crash or power cut. `gzip.open` fails on those; this keeps everything up to the
damaged byte.

## Use
```python
from recorder.hl_load import load, mid_series
book = load("book", "USTECH", start="2026-10-18")              # dex="mkts" by default
cl   = load("trades", "CL", start="2026-10-18 14:00", dex="xyz")
px   = mid_series("US500", freq="1s")   # bid/ask/mid on a 1s grid; gaps over 5 min come back as NaN
```
`load` reads the parquet files, and reads today's (not yet converted) raw files directly, so it always includes
the latest data.

## Check
```bash
.venv/bin/python -m recorder.hl_check --status    # running? last message and latency per market
.venv/bin/python -m recorder.hl_check             # last 24h: coverage and gaps per market, crossed books,
                                                  # duplicate trades, wallet coverage, latency, disconnects
```

## Notes (tested 2026-10-11 on mkts)
- Only 5 of Kinetiq's 24 markets are live: US500 (tracks SPY, not the index), USTECH (QQQ), SMALL2000, USBOND, BVIV.
  The rest are delisted. trade[XYZ] (`--dex xyz`) is far bigger; see `Data/tradexyz/report.md`.
- Exchange-to-Chicago latency on mkts was ~0.3–0.7 s (Hyperliquid stamps each block, roughly every 0.5 s).
- Don't let the machine sleep. The recorder can't fill a gap, but `hl_check` will show it.

## Tests
`.venv/bin/python -m pytest tests -q` runs both recorders' tests. The Hyperliquid tests run against a local fake
server and cover reconnects, new listings, crash restart, Ctrl-C, the folder lock, crash-damaged files and the
loaders.
