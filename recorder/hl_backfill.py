"""Backfill the Hyperliquid REST history that rolls off, before it does.

Three things, all free and keyless:
  candles   `candleSnapshot` per coin per interval. Only ~5000 bars come back per call, so
            1m covers ~3.5 days, 5m ~17 days, 1h ~7 months, 1d since listing. Re-run every
            couple of days until the live recorder has taken over, or the 1m gap is permanent.
  funding   `fundingHistory` hourly rate + premium since listing, 500 rows per call, paginated
            forward by `startTime`.
  universe  a dated snapshot of `metaAndAssetCtxs` per dex, so we keep a record of what was
            listed, its leverage and size decimals, and the delistings as they happen.

Output (parquet, one file per coin per interval so re-runs just overwrite):
  <out>/candles/interval=<iv>/dex=<dex>/sym=<SYM>/part.parquet
  <out>/funding/dex=<dex>/sym=<SYM>/part.parquet
  <out>/universe/date=<YYYY-MM-DD>/part.parquet

Rate limit: 1200 weight/min per IP. candleSnapshot costs 20 + 1 per 60 bars (so ~104 for a
5000-bar page) and fundingHistory 20 + 1 per 20 rows (~45 per 500-row page), so the pacer below
is a weight budget rather than a request count.

    python -m recorder.hl_backfill                       # every live market, every interval
    python -m recorder.hl_backfill --intervals 1m        # just the window that expires fastest
    python -m recorder.hl_backfill --coins xyz:CL --no-funding
"""
import argparse
import logging
import time
from datetime import datetime, timezone
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq

from recorder.hl_common import assign_tiers, coin_parts, data_root, fetch_universe, info, to_scaled

log = logging.getLogger("hl_backfill")

INTERVALS = ("1m", "5m", "15m", "1h", "1d")
MS = {"1m": 60_000, "5m": 300_000, "15m": 900_000, "1h": 3_600_000, "4h": 14_400_000,
      "1d": 86_400_000}
MAX_BARS = 5000      # the server's hard cap per candleSnapshot response
FUNDING_PAGE = 500   # rows per fundingHistory response

CANDLE_SCHEMA = pa.schema([
    ("open_ms", pa.int64()), ("close_ms", pa.int64()), ("open", pa.int64()), ("high", pa.int64()),
    ("low", pa.int64()), ("close", pa.int64()), ("volume", pa.int64()), ("n_trades", pa.int64()),
])
# Rates arrive with up to 10 decimals, past the 1e8 price scale, so they stay doubles.
FUNDING_SCHEMA = pa.schema([
    ("time_ms", pa.int64()), ("funding_rate", pa.float64()), ("premium", pa.float64()),
])
UNIVERSE_SCHEMA = pa.schema([
    ("coin", pa.string()), ("dex", pa.string()), ("delisted", pa.bool_()),
    ("max_leverage", pa.int32()), ("sz_decimals", pa.int32()), ("day_ntl_vlm", pa.float64()),
    ("open_interest_usd", pa.float64()), ("mark_px", pa.float64()),
])


class WeightBudget:
    """Sleeps to keep REST weight under the documented 1200/min per IP, with margin."""

    def __init__(self, per_min: int = 900):
        self.per_min = per_min
        self.spent = 0.0
        self.window_start = time.monotonic()

    def charge(self, weight: float):
        now = time.monotonic()
        if now - self.window_start >= 60:
            self.spent, self.window_start = 0.0, now
        if self.spent + weight > self.per_min:
            nap = 60 - (now - self.window_start)
            if nap > 0:
                log.info("weight budget reached; sleeping %.0fs", nap)
                time.sleep(nap)
            self.spent, self.window_start = 0.0, time.monotonic()
        self.spent += weight


def _part(out: Path, *parts: str) -> Path:
    p = out.joinpath(*parts) / "part.parquet"
    p.parent.mkdir(parents=True, exist_ok=True)
    return p


def _day(ms: int) -> str:
    return datetime.fromtimestamp(ms / 1000, timezone.utc).strftime("%Y-%m-%d")


def _sym_parts(coin: str):
    dex, sym = coin_parts(coin)
    return "dex=%s" % dex, "sym=%s" % sym


def fetch_candles(coin: str, interval: str, budget: WeightBudget, pages: int = 1) -> list:
    """Newest bars, oldest first, walking back `pages` windows at most.

    Paging back barely helps, which is the whole reason this has to be re-run: measured on
    xyz:CL 1m on 2026-10-04, page 1 returned 5001 bars back to 10-01 04:41 and page 2 only
    31 more, stopping dead at 10-01 04:11. The server keeps a fixed window per interval and
    will not serve past it at any startTime, so 1-minute data older than ~3.5 days is simply
    gone. The loop stops as soon as a short page comes back, so pages>1 costs one extra call.
    """
    step = MS[interval]
    end = int(time.time() * 1000)
    rows, seen = [], set()
    for _ in range(pages):
        start = end - MAX_BARS * step
        budget.charge(20 + MAX_BARS / 60)
        got = info({"type": "candleSnapshot",
                    "req": {"coin": coin, "interval": interval, "startTime": start, "endTime": end}})
        if not got:
            break
        for c in got:
            if c["t"] in seen:
                continue
            seen.add(c["t"])
            rows.append({"open_ms": c["t"], "close_ms": c["T"], "open": to_scaled(c["o"]),
                         "high": to_scaled(c["h"]), "low": to_scaled(c["l"]),
                         "close": to_scaled(c["c"]), "volume": to_scaled(c["v"]),
                         "n_trades": c.get("n")})
        oldest = min(c["t"] for c in got)
        if len(got) < MAX_BARS or oldest >= end:
            break
        end = oldest
    rows.sort(key=lambda r: r["open_ms"])
    return rows


def fetch_funding(coin: str, budget: WeightBudget, start_ms: int | None = None) -> list:
    """Full hourly funding history, paginating forward. 500 rows per page = ~21 days."""
    start = start_ms if start_ms is not None else 1_600_000_000_000  # before any HIP-3 listing
    rows, seen = [], set()
    while True:
        budget.charge(20 + FUNDING_PAGE / 20)
        got = info({"type": "fundingHistory", "coin": coin, "startTime": start})
        if not got:
            break
        new = [f for f in got if f["time"] not in seen]
        for f in new:
            seen.add(f["time"])
            premium = f.get("premium")
            rows.append({"time_ms": f["time"], "funding_rate": float(f["fundingRate"]),
                         "premium": None if premium is None else float(premium)})
        newest = max(f["time"] for f in got)
        if not new or newest <= start:
            break
        start = newest + 1
        if len(got) < FUNDING_PAGE:
            break
    rows.sort(key=lambda r: r["time_ms"])
    return rows


def write(rows: list, schema: pa.Schema, path: Path) -> int:
    if not rows:
        return 0
    pq.write_table(pa.Table.from_pylist(rows, schema=schema), path,
                   compression="zstd", compression_level=9, version="2.6")
    return len(rows)


def schema_matches(path: Path, schema: pa.Schema) -> bool:
    """Whether an existing parquet file has exactly the columns and types we write today."""
    if not path.exists():
        return False
    got = pq.ParquetFile(path).schema_arrow
    return got.names == schema.names and all(got.field(f.name).type == f.type for f in schema)


def merge_write(rows: list, schema: pa.Schema, path: Path, key: str) -> tuple:
    """Union new rows into an existing file, newest value winning per `key`.

    This is what turns a repeated backfill into real history: `candleSnapshot` only ever
    returns the last ~5000 bars, so re-running every couple of days and merging accumulates
    1-minute data well past the 3.5-day window the API exposes. It also lets the final,
    revised version of a still-open bar replace the partial one.

    A file whose schema does not match `schema` is **discarded rather than merged**. Mixing
    representations here is silent and badly wrong: when `funding_rate` changed from a 1e8-scaled
    int to a double, merging coerced the old ints into floats and carried the scaling with them,
    so a -0.001 funding rate read back as -100221. The rows are re-fetchable, so dropping them
    is cheap; keeping them is a corrupt table.
    """
    # `key` is one column name or a tuple of them (a composite key such as (ticker, end_ms)).
    kf = (lambda r: tuple(r[k] for k in key)) if isinstance(key, tuple) else (lambda r: r[key])
    merged = {}
    if path.exists():
        if schema_matches(path, schema):
            for r in pq.read_table(path).to_pylist():
                merged[kf(r)] = r
        else:
            log.warning("%s was written under a different schema; discarding its rows", path)
    before = len(merged)
    for r in rows:
        merged[kf(r)] = r
    out = sorted(merged.values(), key=kf)
    write(out, schema, path)
    return len(out), len(out) - before


def existing_max(path: Path, key: str, schema: pa.Schema | None = None) -> int | None:
    """Largest `key` already stored, so a paginated fetch can resume instead of restarting.

    Returns None when the file's schema is stale, because `merge_write` will discard those rows
    and resuming past them would leave a hole instead of refetching them.
    """
    if not path.exists() or (schema is not None and not schema_matches(path, schema)):
        return None
    stats = pq.ParquetFile(path).metadata
    best = None
    for rg in range(stats.num_row_groups):
        for c in range(stats.num_columns):
            col = stats.row_group(rg).column(c)
            if col.path_in_schema == key and col.statistics is not None:
                v = col.statistics.max
                best = v if best is None else max(best, v)
    if best is None:  # statistics were not written; fall back to reading the column
        best = max(pq.read_table(path, columns=[key])[key].to_pylist(), default=None)
    return best


def snapshot_universe(universe: list, out: Path) -> Path:
    day = datetime.now(timezone.utc).strftime("%Y-%m-%d")
    path = _part(out, "universe", "date=%s" % day)
    write(universe, UNIVERSE_SCHEMA, path)
    return path


def main(argv=None):
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    p = argparse.ArgumentParser(description=__doc__,
                                formatter_class=argparse.RawDescriptionHelpFormatter)
    p.add_argument("--coins", help="comma-separated; default = every live market on xyz/para/io")
    p.add_argument("--n-fast", type=int, default=56,
                   help="how many markets count as priority when ordering a full run")
    p.add_argument("--intervals", default=",".join(INTERVALS))
    p.add_argument("--pages", type=int, default=1, help="candleSnapshot pages to walk back per interval")
    p.add_argument("--no-candles", action="store_true")
    p.add_argument("--no-funding", action="store_true")
    p.add_argument("--weight-per-min", type=int, default=900, help="REST budget (cap is 1200)")
    p.add_argument("--out", type=Path, help="default: <PARAGON_DATA_ROOT>/parquet/hl_hist")
    a = p.parse_args(argv)
    out = a.out or data_root() / "parquet" / "hl_hist"
    intervals = [i for i in a.intervals.split(",") if i]
    for iv in intervals:
        if iv not in MS:
            raise SystemExit("unknown interval %r; known: %s" % (iv, ", ".join(MS)))

    universe = fetch_universe()
    log.info("universe snapshot -> %s", snapshot_universe(universe, out))
    if a.coins:
        coins = [c.strip() for c in a.coins.split(",") if c.strip()]
    else:
        # A full run is rate-limit bound at roughly a coin a minute, so order it by what we
        # would miss most: the markets the research plan names, then the rest by volume.
        # An interrupted run has then already secured the data that matters.
        live = [m for m in universe if not m["delisted"]]
        tiers = assign_tiers(live, n_fast=a.n_fast, min_vlm=0.0)
        coins = [m["coin"] for m in sorted(
            live, key=lambda m: (tiers.get(m["coin"]) != "fast", -m["day_ntl_vlm"]))]
    log.info("backfilling %d coins | candles=%s funding=%s", len(coins),
             "no" if a.no_candles else ",".join(intervals), not a.no_funding)

    budget = WeightBudget(a.weight_per_min)
    totals = {"candles": 0, "funding": 0}
    for i, coin in enumerate(coins, 1):
        try:
            dexp, symp = _sym_parts(coin)
        except ValueError as e:
            log.warning("skipping %s", e)
            continue
        got = []
        if not a.no_candles:
            for iv in intervals:
                try:
                    rows = fetch_candles(coin, iv, budget, pages=a.pages)
                except Exception as e:
                    log.warning("%s candles %s failed: %s", coin, iv, e)
                    continue
                if not rows:
                    continue
                total, added = merge_write(rows, CANDLE_SCHEMA,
                                           _part(out, "candles", "interval=%s" % iv, dexp, symp),
                                           "open_ms")
                totals["candles"] += added
                span = "%s..%s" % (_day(rows[0]["open_ms"]), _day(rows[-1]["open_ms"]))
                got.append("%s:%d+%d(%s)" % (iv, total, added, span))
        if not a.no_funding:
            try:
                path = _part(out, "funding", dexp, symp)
                last = existing_max(path, "time_ms", FUNDING_SCHEMA)
                # Resume a few hours before the last stored row rather than from listing.
                rows = fetch_funding(coin, budget,
                                     start_ms=last - 3 * 3_600_000 if last else None)
                if rows:
                    total, added = merge_write(rows, FUNDING_SCHEMA, path, "time_ms")
                    totals["funding"] += added
                    got.append("funding:%d+%d" % (total, added))
            except Exception as e:
                log.warning("%s funding failed: %s", coin, e)
        log.info("[%d/%d] %-18s %s", i, len(coins), coin, " ".join(got) or "(empty)")
    log.info("done: %d candle rows, %d funding rows -> %s", totals["candles"], totals["funding"], out)


if __name__ == "__main__":
    main()
