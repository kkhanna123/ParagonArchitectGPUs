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
