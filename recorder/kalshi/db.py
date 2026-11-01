"""The Kalshi catalog: DuckDB views over parquet/kalshi + parquet/kalshi_hist (kalshi.duckdb).

Built like the trade[XYZ] catalog (recorder.hl_db): no data copied, prices and sizes un-scaled
to DOUBLE, partition keys exposed as columns (`series`, `date` -> `dt`), plus `adapter` so a
whole family can be selected at once (WHERE adapter = 'weather_high').

  book, book_delta, trades, ticker, markets, events   live recordings (recorder.kalshi.to_parquet)
  settlements                                        every settled market (recorder.kalshi.backfill)
  candles                                            per-market candlesticks (backfill)
  book_top        best YES bid/ask, sizes, mid, spread  (YES ask = 1 - best NO bid)
  outcomes        one row per event: the real-world value it settled on, numeric where it is
                  one -- EOD commodity prices, observed high/low temps, gas prices, fed rates
  adapters        series -> adapter

    python -m recorder.kalshi.db --init --report
    python -m recorder.kalshi.db --sql "select * from outcomes where series='KXHIGHCHI' order by close_ts desc limit 10"
    python -m recorder.kalshi.db --retention 30
"""
import argparse
import logging
import shutil
import sys
from datetime import date, datetime, timedelta, timezone
from pathlib import Path

import duckdb

from recorder.hl_common import SCALE, data_root
from recorder.hl_db import _fmt, dir_bytes
from recorder.kalshi.adapters import ADAPTERS
from recorder.kalshi.common import kx_raw
from recorder.kalshi.to_parquet import (BOOK_DELTA_SCHEMA, MARKETS_SCHEMA, TICKER_SCHEMA,
                                        TRADES_SCHEMA, book_schema)
from recorder.hl_xyz_to_parquet import EVENTS_SCHEMA
from recorder.kalshi.backfill import CANDLE_SCHEMA, HIST_MARKETS_SCHEMA

log = logging.getLogger("kx_db")

BOOK_DEPTH = 5


def _scaled(schema, exclude=("rx_ns", "exch_ms", "seq", "end_ms", "dollar_volume",
                             "dollar_open_interest")) -> set:
    """Every int64 that is a 1e8-scaled price/size; timestamps and raw-dollar ints excepted."""
    return {f.name for f in schema if str(f.type) == "int64" and f.name not in exclude
            and not f.name.endswith("_ms")}


# view -> (parquet subdirectory under <root>/parquet, schema, partition columns)
VIEWS = {
    "book": ("kalshi/book", book_schema(BOOK_DEPTH), ("date", "series")),
    "book_delta": ("kalshi/book_delta", BOOK_DELTA_SCHEMA, ("date", "series")),
    "trades": ("kalshi/trades", TRADES_SCHEMA, ("date", "series")),
    "ticker": ("kalshi/ticker", TICKER_SCHEMA, ("date", "series")),
    "markets": ("kalshi/markets", MARKETS_SCHEMA, ("date", "series")),
    "events": ("kalshi/events", EVENTS_SCHEMA, ("date", "series")),
    "settlements": ("kalshi_hist/markets", HIST_MARKETS_SCHEMA, ("series",)),
    "candles": ("kalshi_hist/candles", CANDLE_SCHEMA, ("period", "series")),
}


def create_views(con, root: Path, depth: int = BOOK_DEPTH) -> list:
    made = []
    pairs = ", ".join("('%s', '%s')" % (s, a.name) for a in ADAPTERS.values() for s in a.series)
    con.execute("CREATE OR REPLACE VIEW adapters AS SELECT * FROM (VALUES %s) t(series, adapter)"
                % pairs)
    made.append("adapters")
    for name, (rel, schema, parts) in VIEWS.items():
        base = root / "parquet" / rel
        if not base.exists() or not any(base.rglob("*.parquet")):
            continue
        if name == "book":
            schema = book_schema(depth)
        scaled = _scaled(schema)
        cols = ["t.%s / %d.0 AS %s" % (f.name, SCALE, f.name) if f.name in scaled
                else "t.%s" % f.name for f in schema]
        cols += ["t.%s" % p for p in parts]
        if "date" in parts:
            cols.append("CAST(t.date AS DATE) AS dt")
        cols.append("a.adapter")
        con.execute(
            "CREATE OR REPLACE VIEW %s AS SELECT %s FROM read_parquet('%s', hive_partitioning=1, "
            "union_by_name=1) t LEFT JOIN adapters a USING (series)"
            % (name, ", ".join(cols), (base / "**" / "*.parquet").as_posix()))
        made.append(name)
    if "book" in made:
        con.execute("""CREATE OR REPLACE VIEW book_top AS SELECT
              ticker, series, adapter, dt, rx_ns, exch_ms, seq,
              yes_px_0 AS yes_bid, yes_sz_0 AS yes_bid_sz,
              1 - no_px_0 AS yes_ask, no_sz_0 AS yes_ask_sz,
              (yes_px_0 + 1 - no_px_0) / 2 AS mid,
              (1 - no_px_0) - yes_px_0 AS spread
            FROM book""")
        made.append("book_top")
    if "settlements" in made:
        # Every market of an event settles on the same reading, so one row per event. `value`
        # accepts a bare number with an optional $ or % ("3.75%"); text stays NULL.
        con.execute(r"""CREATE OR REPLACE VIEW outcomes AS SELECT
              series, adapter, event_ticker,
              make_timestamp(max(close_ms) * 1000) AS close_ts,
              any_value(expiration_value) AS value_text,
              TRY_CAST(regexp_extract(any_value(expiration_value),
                       '^\s*\$?\s*(-?[0-9]+(\.[0-9]+)?)\s*%?\s*$', 1) AS DOUBLE) AS value,
              count(*) AS n_markets, sum(volume) AS volume
            FROM settlements
            WHERE expiration_value IS NOT NULL
            GROUP BY series, adapter, event_ticker""")
        made.append("outcomes")
    return made


def init(db_path: Path, root: Path) -> list:
    db_path.parent.mkdir(parents=True, exist_ok=True)
    con = duckdb.connect(str(db_path))
    try:
        return create_views(con, root)
    finally:
        con.close()


def report(root: Path) -> dict:
    raw = kx_raw(root)
    days = sorted(d.name for d in raw.iterdir() if d.is_dir()) if raw.exists() else []
    tables = {}
    for name, (rel, _, _) in VIEWS.items():
        b = dir_bytes(root / "parquet" / rel)
        if b:
            tables[name] = (b, sum(1 for _ in (root / "parquet" / rel).rglob("*.parquet")))
    return {"raw_days": [(d, dir_bytes(raw / d)) for d in days], "raw_bytes": dir_bytes(raw),
            "parquet_bytes": dir_bytes(root / "parquet/kalshi") + dir_bytes(
                root / "parquet/kalshi_hist"),
            "tables": tables}


def apply_retention(root: Path, keep_days: int, dry_run: bool = False) -> list:
    """Delete raw Kalshi JSONL older than keep_days, never a day that has no parquet."""
    raw = kx_raw(root)
    if not raw.exists():
        return []
    cutoff = datetime.now(timezone.utc).date() - timedelta(days=keep_days)
    removed = []
    for d in sorted(raw.iterdir()):
        try:
            day = date.fromisoformat(d.name)
        except ValueError:
            continue
        if not d.is_dir() or day >= cutoff:
            continue
        if not any((root / "parquet/kalshi/events" / ("date=%s" % d.name)).rglob("*.parquet")):
            log.warning("%s has no parquet; keeping raw", d.name)
            continue
        removed.append((d.name, dir_bytes(d)))
        if not dry_run:
            shutil.rmtree(d)
    return removed


def main(argv=None):
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    try:
        sys.stdout.reconfigure(encoding="utf-8", errors="replace")
    except (AttributeError, OSError):
        pass
    p = argparse.ArgumentParser(description=__doc__,
                                formatter_class=argparse.RawDescriptionHelpFormatter)
    p.add_argument("--init", action="store_true", help="create/refresh the views")
    p.add_argument("--report", action="store_true", help="show disk usage")
    p.add_argument("--sql", help="run a query against the catalog and print the result")
    p.add_argument("--retention", type=int, metavar="DAYS",
                   help="delete raw jsonl.gz older than DAYS (parquet is kept)")
    p.add_argument("--dry-run", action="store_true")
    p.add_argument("--root", type=Path, help="default: $PARAGON_DATA_ROOT")
    p.add_argument("--db", type=Path, help="default: <root>/kalshi.duckdb")
    a = p.parse_args(argv)
    root = a.root or data_root()
    db = a.db or root / "kalshi.duckdb"
    if not any((a.init, a.report, a.sql, a.retention)):
        a.init = a.report = True
    if a.init:
        made = init(db, root)
        log.info("catalog %s: %s", db, ", ".join(made))
    if a.retention:
        removed = apply_retention(root, a.retention, a.dry_run)
        log.info("%s %d raw day(s), %s: %s", "would free" if a.dry_run else "freed",
                 len(removed), _fmt(sum(b for _, b in removed)),
                 ", ".join(d for d, _ in removed) or "nothing")
    if a.report:
        r = report(root)
        print("\nroot: %s" % root)
        print("raw jsonl.gz : %s across %d day(s)" % (_fmt(r["raw_bytes"]), len(r["raw_days"])))
        print("parquet      : %s" % _fmt(r["parquet_bytes"]))
        if r["tables"]:
            print("\n  %-12s %12s %8s" % ("table", "bytes", "files"))
            for name, (b, n) in sorted(r["tables"].items(), key=lambda kv: -kv[1][0]):
                print("  %-12s %12s %8d" % (name, _fmt(b), n))
        for d, b in r["raw_days"][-10:]:
            print("  raw %-10s %12s" % (d, _fmt(b)))
    if a.sql:
        con = duckdb.connect(str(db))
        try:
            create_views(con, root)
            print(con.sql(a.sql))
        finally:
            con.close()


if __name__ == "__main__":
    main()
