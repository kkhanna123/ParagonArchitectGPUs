# Permanent collection — operations

Three long-lived processes, all started from the per-user Startup folder:

| process | what it does | lock |
|---|---|---|
| `run_collector.cmd` → `hl_xyz_recorder --watchlist all --connections 2` | trade[XYZ]: both L2 book streams, trades and asset contexts for the 50-market `all` watchlist | `<raw>\.hl.lock` |
| `run_kalshi_collector.cmd` → `recorder.kalshi.recorder` | Kalshi: books, trades, tickers and contract metadata for every family in `recorder/kalshi/adapters.py` | `<raw>\.kalshi.lock` |
| `run_daily_runner.cmd` → `daily_runner.py --at 00:40` | once a day, for both venues: convert → backfill → refresh catalog → prune raw | `<raw>\.daily_runner.lock` |

Locations come from `paths.cmd`:

| | variable | default |
|---|---|---|
| parquet + `hl.duckdb` + `kalshi.duckdb` | `PARAGON_DATA_ROOT` | `%USERPROFILE%\OneDrive\Desktop\Paragon\HyperliquidData` |
| raw JSONL (appended every few seconds, kept out of OneDrive) | `PARAGON_RAW_ROOT` | `C:\ParagonData\raw` |
| logs | `LOGDIR` | `C:\ParagonData\logs` |
| Kalshi API key (`KALSHI_API_KEY_ID`, `KALSHI_PRIVATE_KEY_PATH`) | `KALSHI_ENV_FILE` | `PolymarketStratsautomated\.env` |

A variable already set in the environment wins, **including a user-level one** — check
`[Environment]::GetEnvironmentVariable('PARAGON_DATA_ROOT','User')` if data lands somewhere
unexpected.

## Kalshi

Same pipeline and file layout as trade[XYZ], partitioned by series instead of dex/sym:
`parquet/kalshi/<table>/date=<D>/series=<S>/part.parquet`, catalogued in `kalshi.duckdb`.

| table | contents |
|---|---|
| `book` | top 5 YES bids and NO bids per change (YES ask = 1 − best NO bid; `book_top` does it) |
| `book_delta` | the lossless book feed: snapshot levels + every delta, rebuildable to full depth |
| `trades`, `ticker` | every trade; every changed best bid/ask/last/volume/OI |
| `markets` | contract metadata (strikes, times, status, result) whenever it changes |
| `settlements`, `outcomes` | backfill: every settled market, and per event the real-world value it settled on (EOD commodity price, observed temperature, AAA gas price, fed funds rate) |
| `candles` | backfill: per-market candles (1-minute; hourly for the fed series) |

**Adding markets:** add an `Adapter(...)` to `ADAPTERS` in `recorder/kalshi/adapters.py` and
restart the Kalshi collector. Find series tickers with
`python -m recorder.kalshi.adapters --search "<regex>"`. Rolling series (15-minute) want
`refresh_s=60` and a `lead_s` so each new market is subscribed before it opens.

The websocket needs the API key; REST (backfill, discovery) works without it. The recorder
reconnects on a sequence gap in the book feed, since a lost delta corrupts every later level.

## Why the Startup folder and not a scheduled task

Task Scheduler registration is **denied on this machine**: `Register-ScheduledTask` and
`schtasks /create` both return `Access is denied` even unelevated for a per-user logon task,
which points at policy rather than missing elevation. So autostart is a shim at

```
%APPDATA%\Microsoft\Windows\Start Menu\Programs\Startup\ParagonXYZCollector.vbs
```

which launches the three `.cmd` wrappers hidden. The shim only points at the repo, so edits to the
scripts take effect without reinstalling it.

**The one real cost of this:** it starts at **logon**, not at boot. After a reboot, nothing is
collected while the machine sits at the lock screen. The 0.5s book cannot be backfilled, so that
window is lost permanently. If task registration is ever permitted, run

```powershell
powershell -ExecutionPolicy Bypass -File recorder\service\install_tasks.ps1   # from an elevated shell
```

which registers a boot-triggered, restart-on-failure, runs-while-logged-out pair instead. Then
delete the Startup shim.

## Resilience, in layers

1. **Reconnect** — the recorder reconnects with exponential backoff, and also reconnects if a
   connection goes quiet for 30s (it pings every 45s; the server drops a socket silent for 60s).
2. **Restart** — `run_collector.cmd` is a loop, so the recorder exiting for any reason restarts
   it after 30s.
3. **Single instance** — both processes take an exclusive lock on a file keyed to their output
   directory. Logging in twice, or double-clicking the shim, is refused with a message naming
   the holding pid rather than duplicating the feed. That matters here: two collectors would
   mean duplicate rows and 12 websockets against a 10-per-IP limit.
4. The collector uses **6 of the 10** allowed websockets (2 shards × 3 roles), leaving headroom
   for ad-hoc queries and a second recorder.

## Checking on it

```powershell
# is it running?
Get-CimInstance Win32_Process -Filter "Name='python.exe'" |
  Where-Object { $_.CommandLine -like "*recorder.*" } |
  Select-Object ProcessId, CommandLine

# how many sockets to the venue?
$p = (Get-CimInstance Win32_Process -Filter "Name='python.exe'" |
      Where-Object { $_.CommandLine -like "*hl_xyz_recorder*" }).ProcessId
Get-NetTCPConnection -State Established | Where-Object { $p -contains $_.OwningProcess }

# logs
Get-Content C:\ParagonData\logs\collector-*.log -Tail 20
Get-Content C:\ParagonData\logs\kalshi-*.log -Tail 20
Get-Content C:\ParagonData\logs\maintenance-*.log -Tail 40
```

```bash
# what is on disk, and the growth rate
.venv/Scripts/python -m recorder.hl_db --report
.venv/Scripts/python -m recorder.kalshi.db --report

# gaps: every connect/disconnect the recorder logged
.venv/Scripts/python -m recorder.hl_db --sql "select event, count(*) from events group by 1"
```

## Stopping it

```powershell
# this session only
Get-CimInstance Win32_Process -Filter "Name='python.exe'" |
  Where-Object { $_.CommandLine -like "*recorder.*" } |
  ForEach-Object { Stop-Process -Id $_.ProcessId -Force }
# ...and the cmd.exe restart loops, or they will start it again in 30s
Get-CimInstance Win32_Process -Filter "Name='cmd.exe'" |
  Where-Object { $_.CommandLine -like "*run_collector*" -or $_.CommandLine -like "*run_kalshi_collector*" -or $_.CommandLine -like "*run_daily_runner*" } |
  ForEach-Object { Stop-Process -Id $_.ProcessId -Force }

# permanently
Remove-Item "$([Environment]::GetFolderPath('Startup'))\ParagonXYZCollector.vbs"
```

Stop the restart loops **before** the python processes, or the loop will relaunch them.

## The daily job

`daily_runner.py` holds the single definition of the steps; `daily_maintenance.cmd` is a
one-shot wrapper around it for manual runs and for the Task Scheduler path. Run it by hand with

```bash
.venv/Scripts/python -m recorder.service.daily_runner --once
```

Order matters and is deliberate:

1. **convert** — raw JSONL → parquet. Skips days whose parquet is already newer than their raw
   files, so a 30-day retention window does not mean re-converting 30 days every night. The
   in-progress hour always counts as stale, which is correct; reading a gzip file that is still
   being appended to stops at the last intact record rather than failing.
2. **backfill** — re-fetch candles and funding for the core watchlist. This is not optional
   upkeep: `candleSnapshot` only serves ~5000 bars, so the 1-minute window is ~3.5 days deep and
   **unrecoverable** once it rolls off. Runs merge into existing files, so a daily run is what
   accumulates 1m history past anything the API will ever return.
3. **catalog** — refresh the DuckDB views so new partitions are visible.
4. **retention** — delete raw JSONL older than 30 days. Parquet is never touched, and a raw day
   with no parquet is never deleted.
5. **kx_convert / kx_backfill / kx_catalog / kx_retention** — the same four for Kalshi. The
   backfill adds newly settled markets (with outcome values) and their candles; the first run
   used `--history` to walk Kalshi's archive as well.

A failing step is logged and the remaining steps still run.
