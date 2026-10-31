"""The local database: a DuckDB catalog of views over the parquet files.

DuckDB reads parquet directly, so the catalog holds no copy of the data -- it is a few hundred
KB of view definitions over <PARAGON_DATA_ROOT>/parquet. That matters at this scale: ingesting
into native tables would double the disk bill and lose the hive partition pruning that makes
single-symbol queries cheap.

Every view un-scales prices and sizes back to DOUBLE and exposes `dex`/`sym`/`date` from the
partition path plus a `coin` column ('xyz:CL') to match the API's naming:

  book5                                     5-level L2 at ~0.5s   (live)
  book                                      20-level L2 at ~5.4s  (live)
  trades, asset_ctx, bbo, events            live recordings (recorder.hl_xyz_to_parquet)
  candles, funding, universe                REST backfill (recorder.hl_backfill)
  book_top                                  top of book + spread + microprice, off book5

    python -m recorder.hl_db --init                   # build / refresh the catalog
    python -m recorder.hl_db --report                 # storage used, per table and per day
    python -m recorder.hl_db --sql "select count(*) from book where sym='CL'"
    python -m recorder.hl_db --retention 30           # drop raw .jsonl.gz older than N days
"""
import argparse
import logging
import shutil
import sys
from datetime import date, datetime, timedelta, timezone
from pathlib import Path

import duckdb

from recorder.hl_common import SCALE, data_root, raw_root

log = logging.getLogger("hl_db")

DEPTH = 20   # the 5.4s stream
DEPTH5 = 5   # the 0.5s stream


def book_columns(depth: int = DEPTH) -> list:
    cols = ["rx_ns", "exch_ms", "prev_ms"]
    for side in ("bid", "ask"):
        for i in range(depth):
            cols += ["%s_px_%d" % (side, i), "%s_sz_%d" % (side, i), "%s_n_%d" % (side, i)]
    return cols


def book_scaled(depth: int = DEPTH) -> set:
    return {"%s_%s_%d" % (s, f, i) for s in ("bid", "ask") for f in ("px", "sz")
            for i in range(depth)}


# (parquet subdirectory, columns in the file, which of them are 1e8-scaled int64s).
# The scaled set is listed out rather than matched by name: `open_ms` and `close_ms` are
# millisecond timestamps sitting next to `open`/`close` prices, and `day_ntl_vlm` is a scaled
# notional whose name shares nothing with the price columns.
VIEWS = {
    "book": ("parquet/hl/book", book_columns(DEPTH), book_scaled(DEPTH)),
    "book5": ("parquet/hl/book5", book_columns(DEPTH5), book_scaled(DEPTH5)),
    "trades": ("parquet/hl/trades",
               ["rx_ns", "exch_ms", "tid", "side", "px", "sz", "buyer", "seller", "hash"],
               {"px", "sz"}),
    "asset_ctx": ("parquet/hl/asset_ctx",
                  ["rx_ns", "funding", "open_interest", "prev_day_px", "day_ntl_vlm",
                   "day_base_vlm", "premium", "oracle_px", "mark_px", "mid_px",
                   "impact_bid", "impact_ask"],
                  # `funding` and `premium` are already doubles on disk (10-decimal rates).
                  {"open_interest", "prev_day_px", "day_ntl_vlm", "day_base_vlm",
                   "oracle_px", "mark_px", "mid_px", "impact_bid", "impact_ask"}),
    "bbo": ("parquet/hl/bbo",
            ["rx_ns", "exch_ms", "bid_px", "bid_sz", "bid_n", "ask_px", "ask_sz", "ask_n"],
            {"bid_px", "bid_sz", "ask_px", "ask_sz"}),
    "events": ("parquet/hl/events", ["rx_ns", "event", "detail"], set()),
    "candles": ("parquet/hl_hist/candles",
                ["open_ms", "close_ms", "open", "high", "low", "close", "volume", "n_trades"],
                {"open", "high", "low", "close", "volume"}),
    "funding": ("parquet/hl_hist/funding", ["time_ms", "funding_rate", "premium"], set()),
}


def _select(cols, scaled) -> list:
    """Column expressions that divide the int64 storage back out to a real price."""
    return ["%s / %d.0 AS %s" % (n, SCALE, n) if n in scaled else n for n in cols]

# `dex` and `sym` come from the partition path, so a view has to name them explicitly;
# `coin` rebuilds the API's 'xyz:CL' spelling for joins against the universe snapshots.
PARTITION_COLS = {"book": ("date", "dex", "sym"), "book5": ("date", "dex", "sym"),
                  "trades": ("date", "dex", "sym"),
                  "asset_ctx": ("date", "dex", "sym"), "bbo": ("date", "dex", "sym"),
                  "events": ("date", "dex", "sym"),
                  "candles": ("interval", "dex", "sym"), "funding": ("dex", "sym")}


def create_views(con: duckdb.DuckDBPyConnection, root: Path, depth: int = DEPTH) -> list:
    made = []
    for name, (rel, cols, scaled) in VIEWS.items():
        base = root / rel
        if not base.exists() or not any(base.rglob("*.parquet")):
            continue
        if name in ("book", "book5"):
            d = depth if name == "book" else min(depth, DEPTH5)
            cols, scaled = book_columns(d), book_scaled(d)
        glob = (base / "**" / "*.parquet").as_posix()
        parts = PARTITION_COLS[name]
        select = _select(cols, scaled) + list(parts)
        if "dex" in parts and "sym" in parts:
            select.append("dex || ':' || sym AS coin")
        if "date" in parts:
            select.append("CAST(date AS DATE) AS dt")
        con.execute("CREATE OR REPLACE VIEW %s AS SELECT %s FROM read_parquet('%s', "
                    "hive_partitioning=1, union_by_name=1)" % (name, ", ".join(select), glob))
        made.append(name)

    uni = root / "parquet/hl_hist/universe"
    if uni.exists() and any(uni.rglob("*.parquet")):
        con.execute("CREATE OR REPLACE VIEW universe AS SELECT *, CAST(date AS DATE) AS dt "
                    "FROM read_parquet('%s', hive_partitioning=1, union_by_name=1)"
                    % (uni / "**" / "*.parquet").as_posix())
        made.append("universe")

    # The shape almost every study starts from, so it is worth not rewriting each time.
    # Built on book5 when available: it is the 0.5s stream, and top-of-book is all it is
    # missing levels for. Falls back to the 20-level 5.4s stream.
    top_src = "book5" if "book5" in made else ("book" if "book" in made else None)
    if top_src:
        con.execute("""CREATE OR REPLACE VIEW book_top AS SELECT
              coin, dex, sym, dt, rx_ns, exch_ms, prev_ms,
              bid_px_0 AS bid, bid_sz_0 AS bid_sz, ask_px_0 AS ask, ask_sz_0 AS ask_sz,
              (bid_px_0 + ask_px_0) / 2 AS mid,
              ask_px_0 - bid_px_0 AS spread,
              CASE WHEN bid_sz_0 + ask_sz_0 > 0
                   THEN (bid_px_0 * ask_sz_0 + ask_px_0 * bid_sz_0) / (bid_sz_0 + ask_sz_0) END
                   AS microprice
            FROM %s""" % top_src)
        made.append("book_top")
    return made


def init(db_path: Path, root: Path, depth: int = DEPTH) -> list:
    db_path.parent.mkdir(parents=True, exist_ok=True)
    con = duckdb.connect(str(db_path))
    try:
        return create_views(con, root, depth)
    finally:
        con.close()


def dir_bytes(p: Path) -> int:
    return sum(f.stat().st_size for f in p.rglob("*") if f.is_file()) if p.exists() else 0


def report(root: Path) -> dict:
    """Bytes on disk per tier and per table, plus the observed per-day growth rate."""
    raw = raw_root(root) / "hl"
    days = sorted(d.name for d in raw.iterdir() if d.is_dir()) if raw.exists() else []
    per_day = [(d, dir_bytes(raw / d)) for d in days]
    tables = {}
    for name, (rel, _, _) in VIEWS.items():
        b = dir_bytes(root / rel)
        if b:
            n = sum(1 for _ in (root / rel).rglob("*.parquet"))
            tables[name] = (b, n)
    free = shutil.disk_usage(root if root.exists() else root.anchor).free
    return {"raw_days": per_day, "raw_bytes": dir_bytes(raw),
            "parquet_bytes": dir_bytes(root / "parquet"), "tables": tables, "free_bytes": free}


def apply_retention(root: Path, keep_days: int, dry_run: bool = False) -> list:
    """Delete raw hourly JSONL older than keep_days. Parquet is never touched.

    The raw files are the lossless archive, but once a day is converted they are the expensive
    half of the footprint and nothing queries them. Keep enough days to re-run a converter
    change against recent data.
    """
    raw = raw_root(root) / "hl"
    if not raw.exists():
        return []
    cutoff = datetime.now(timezone.utc).date() - timedelta(days=keep_days)
    removed = []
    for d in sorted(raw.iterdir()):
        if not d.is_dir():
            continue
        try:
            day = date.fromisoformat(d.name)
        except ValueError:
            continue
        if day >= cutoff:
            continue
        # Refuse to drop a day that was never converted. Note the inner any(): rglob returns a
        # generator, which is truthy even when it yields nothing.
        converted = any(any((root / "parquet/hl" / t / ("date=%s" % d.name)).rglob("*.parquet"))
                        for t in ("book", "book5"))
        if not converted:
            log.warning("%s has no parquet; keeping raw", d.name)
            continue
        removed.append((d.name, dir_bytes(d)))
        if not dry_run:
            shutil.rmtree(d)
    return removed


def _fmt(b: float) -> str:
    for unit in ("B", "KB", "MB", "GB", "TB"):
        if abs(b) < 1024 or unit == "TB":
            return "%.1f %s" % (b, unit)
        b /= 1024


def main(argv=None):
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    # DuckDB prints result tables with box-drawing characters, which the default Windows
    # console codepage (cp1252) cannot encode.
    try:
        sys.stdout.reconfigure(encoding="utf-8", errors="replace")
    except (AttributeError, OSError):
        pass
    p = argparse.ArgumentParser(description=__doc__,
                                formatter_class=argparse.RawDescriptionHelpFormatter)
    p.add_argument("--init", action="store_true", help="create/refresh the views")
    p.add_argument("--report", action="store_true", help="show disk usage and growth")
    p.add_argument("--sql", help="run a query against the catalog and print the result")
    p.add_argument("--retention", type=int, metavar="DAYS",
                   help="delete raw jsonl.gz older than DAYS (parquet is kept)")
    p.add_argument("--dry-run", action="store_true")
    p.add_argument("--depth", type=int, default=DEPTH, help="book levels the views expose")
    p.add_argument("--root", type=Path, help="default: $PARAGON_DATA_ROOT")
    p.add_argument("--db", type=Path, help="default: <root>/hl.duckdb")
    a = p.parse_args(argv)
    root = a.root or data_root()
    db = a.db or root / "hl.duckdb"
    if not any((a.init, a.report, a.sql, a.retention)):
        a.init = a.report = True

    if a.init:
        made = init(db, root, a.depth)
        log.info("catalog %s: %s", db, ", ".join(made) or "(no parquet found yet)")

    if a.retention:
        removed = apply_retention(root, a.retention, a.dry_run)
        freed = sum(b for _, b in removed)
        log.info("%s %d raw day(s), %s: %s", "would free" if a.dry_run else "freed",
                 len(removed), _fmt(freed), ", ".join(d for d, _ in removed) or "nothing")

    if a.report:
        r = report(root)
        print("\nroot: %s   free on volume: %s" % (root, _fmt(r["free_bytes"])))
        print("raw jsonl.gz : %s across %d day(s)" % (_fmt(r["raw_bytes"]), len(r["raw_days"])))
        print("parquet      : %s" % _fmt(r["parquet_bytes"]))
        if r["tables"]:
            print("\n  %-12s %12s %8s" % ("table", "bytes", "files"))
            for name, (b, n) in sorted(r["tables"].items(), key=lambda kv: -kv[1][0]):
                print("  %-12s %12s %8d" % (name, _fmt(b), n))
        if r["raw_days"]:
            print("\n  %-12s %12s" % ("raw day", "bytes"))
            for d, b in r["raw_days"][-10:]:
                print("  %-12s %12s" % (d, _fmt(b)))
            full = [b for _, b in r["raw_days"]]
            avg = sum(full) / len(full)
            print("\n  mean raw/day %s -> %s/yr; free space lasts ~%.0f days at that rate"
                  % (_fmt(avg), _fmt(avg * 365), r["free_bytes"] / max(avg, 1)))

    if a.sql:
        con = duckdb.connect(str(db))
        try:
            create_views(con, root, a.depth)
            print(con.sql(a.sql))
        finally:
            con.close()


if __name__ == "__main__":
    main()
