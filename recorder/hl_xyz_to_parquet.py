"""Convert raw Hyperliquid recordings (from hl_xyz_recorder) into partitioned parquet tables.

Layout: <out>/<table>/date=<YYYY-MM-DD>/dex=<dex>/sym=<SYMBOL>/part.parquet

`dex` and `sym` are hive partition keys rather than columns, so DuckDB and pyarrow both
reconstruct them on read and prune whole directories for single-symbol queries. (They are
also why the partition key is `sym=CL` and not `coin=xyz:CL`: ':' is illegal in a Windows path.)
Main-dex coins (no prefix, e.g. PAXG) land under `dex=main`.

Tables:
  book       one row per *changed* 20-level L2 snapshot (~5.4s cadence)
  book5      one row per *changed* 5-level L2 snapshot (~0.5s cadence)
  trades     one row per trade print, with both wallet addresses
  asset_ctx  one row per *changed* asset context (oracle, mark, funding, OI, volume)
  bbo        one row per top-of-book update (only if --bbo was recorded)
  events     connect / disconnect / start markers -- use these to find gaps

Two things keep the book table small, and both are lossless:
  * prices and sizes are stored as int64 scaled by 1e8, so parquet can DELTA_BINARY_PACKED
    them down the time axis. Between two 0.5s snapshots most of the 20 levels are unchanged,
    so the deltas are mostly zero.
  * byte-identical consecutive snapshots are dropped. `prev_ms` on each row carries the
    exchange timestamp of the last snapshot that was *confirmed identical*, so a reader still
    knows the book held from `exch_ms` through `prev_ms` of the following row.

(Restored 2026-10-25 from bytecode; see hl_xyz_recorder.py.)

    python -m recorder.hl_xyz_to_parquet                       # every unconverted day
    python -m recorder.hl_xyz_to_parquet --date 2026-10-04 --depth 5
"""
import argparse
import gzip
import json
import logging
import time
import zlib
from collections import defaultdict
from datetime import datetime, timedelta, timezone
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq

from recorder.hl_common import coin_parts, parquet_root, raw_root, to_scaled

log = logging.getLogger("hl_to_parquet")

DEPTH = 20   # levels/side on the 5.4s stream
DEPTH5 = 5   # levels/side on the 0.5s stream


def book_schema(depth: int) -> pa.Schema:
    f = [("rx_ns", pa.int64()), ("exch_ms", pa.int64()), ("prev_ms", pa.int64())]
    for side in ("bid", "ask"):
        for i in range(depth):
            f += [("%s_px_%d" % (side, i), pa.int64()),
                  ("%s_sz_%d" % (side, i), pa.int64()),
                  ("%s_n_%d" % (side, i), pa.int32())]
    return pa.schema(f)


TRADES_SCHEMA = pa.schema([
    ("rx_ns", pa.int64()), ("exch_ms", pa.int64()), ("tid", pa.int64()),
    ("side", pa.string()), ("px", pa.int64()), ("sz", pa.int64()),
    ("buyer", pa.string()), ("seller", pa.string()), ("hash", pa.string()),
])

# funding and premium are rates, not prices: they can carry more than 8 decimals, so they stay
# floats rather than being forced through the 1e8 scale.
CTX_SCHEMA = pa.schema([
    ("rx_ns", pa.int64()), ("funding", pa.float64()), ("open_interest", pa.int64()),
    ("prev_day_px", pa.int64()), ("day_ntl_vlm", pa.int64()), ("day_base_vlm", pa.int64()),
    ("premium", pa.float64()), ("oracle_px", pa.int64()), ("mark_px", pa.int64()),
    ("mid_px", pa.int64()), ("impact_bid", pa.int64()), ("impact_ask", pa.int64()),
])

BBO_SCHEMA = pa.schema([
    ("rx_ns", pa.int64()), ("exch_ms", pa.int64()),
    ("bid_px", pa.int64()), ("bid_sz", pa.int64()), ("bid_n", pa.int32()),
    ("ask_px", pa.int64()), ("ask_sz", pa.int64()), ("ask_n", pa.int32()),
])

EVENTS_SCHEMA = pa.schema([("rx_ns", pa.int64()), ("event", pa.string()), ("detail", pa.string())])

# Columns worth min/max statistics: the ones queries filter on. Skipping the rest trims footers.
_STATS_COLUMNS = ("rx_ns", "exch_ms", "prev_ms", "tid", "side", "ticker")


def encodings_for(schema: pa.Schema) -> tuple:
    """-> (dictionary columns, {column: encoding}).

    Integers get DELTA_BINARY_PACKED: timestamps step by a near-constant amount and book
    levels mostly repeat, so the deltas are tiny. Floats get BYTE_STREAM_SPLIT, which lets zstd
    see the shared exponent bytes. Strings (side, wallet addresses) are dictionary-encoded,
    and pyarrow forbids naming an encoding for a dictionary column, hence the split return.
    """
    dict_cols, enc = [], {}
    for f in schema:
        if pa.types.is_integer(f.type):
            enc[f.name] = "DELTA_BINARY_PACKED"
        elif pa.types.is_floating(f.type):
            enc[f.name] = "BYTE_STREAM_SPLIT"
        elif pa.types.is_boolean(f.type):
            enc[f.name] = "RLE"
        else:
            dict_cols.append(f.name)
    return dict_cols, enc


def _coin_dirs(coin: str) -> tuple:
    dex, sym = coin_parts(coin)
    return "dex=%s" % dex, "sym=%s" % sym


class PartWriter:
    """Lazily-opened ParquetWriter per (table, dex, sym), buffering rows columnwise.

    One writer lives for a whole day and appends a row group per filled buffer, so a day
    produces one file per symbol instead of 24. With 127 GB of RAM the buffers are free; the
    row groups they produce are what keeps the footer small relative to the data.
    """

    def __init__(self, out: Path, table: str, schema: pa.Schema, day: str,
                 batch_rows: int = 8192, compression: str = "zstd", compression_level: int = 6,
                 part_dirs=None):
        self.out, self.table, self.schema = Path(out), table, schema
        # key -> partition directory names under date=<day>. Defaults to the coin layout
        # (dex=, sym=); other venues pass their own (Kalshi: series=). Raises ValueError for a
        # key that cannot be made into a safe path.
        self.part_dirs = part_dirs or _coin_dirs
        self.day = day
        self.batch_rows = batch_rows
        self.compression, self.compression_level = compression, compression_level
        self.names = [f.name for f in schema]
        self._buf = defaultdict(lambda: {n: [] for n in self.names})
        self._writers = {}
        self.rows = defaultdict(int)
        self._unsafe = set()

    def _path(self, coin: str) -> Path:
        return self.out.joinpath(self.table, "date=%s" % self.day, *self.part_dirs(coin),
                                 "part.parquet")

    def append(self, coin: str, row: dict):
        if coin not in self._buf:
            if coin in self._unsafe:
                return
            try:
                self.part_dirs(coin)
            except ValueError as e:
                log.warning("%s: skipping %s", self.table, e)
                self._unsafe.add(coin)
                return
        b = self._buf[coin]
        for n in self.names:
            b[n].append(row.get(n))
        self.rows[coin] += 1
        if len(b[self.names[0]]) >= self.batch_rows:
            self._flush(coin)

    def _flush(self, coin: str):
        b = self._buf[coin]
        if not b[self.names[0]]:
            return
        batch = pa.record_batch([pa.array(b[n], type=self.schema.field(n).type)
                                 for n in self.names], schema=self.schema)
        w = self._writers.get(coin)
        if w is None:
            p = self._path(coin)
            p.parent.mkdir(parents=True, exist_ok=True)
            stats = [n for n in self.names if n in _STATS_COLUMNS]
            dict_cols, enc = encodings_for(self.schema)
            w = self._writers[coin] = pq.ParquetWriter(
                p, self.schema, compression=self.compression,
                compression_level=self.compression_level, version="2.6",
                write_statistics=stats or False,
                use_dictionary=dict_cols or False, column_encoding=enc)
        w.write_batch(batch)
        for n in self.names:
            b[n].clear()

    def close(self):
        for coin in list(self._buf):
            self._flush(coin)
        for w in self._writers.values():
            w.close()
        self._writers.clear()


def levels_to_row(rx_ns: int, d: dict, prev_ms: int, depth: int) -> dict:
    row = {"rx_ns": rx_ns, "exch_ms": d["time"], "prev_ms": prev_ms}
    for side, levels in zip(("bid", "ask"), d["levels"]):
        for i in range(depth):
            lv = levels[i] if i < len(levels) else None
            row["%s_px_%d" % (side, i)] = to_scaled(lv["px"]) if lv else None
            row["%s_sz_%d" % (side, i)] = to_scaled(lv["sz"]) if lv else None
            row["%s_n_%d" % (side, i)] = lv["n"] if lv else None
    return row


_CTX_SCALED = (("open_interest", "openInterest"), ("prev_day_px", "prevDayPx"),
               ("day_ntl_vlm", "dayNtlVlm"), ("day_base_vlm", "dayBaseVlm"),
               ("oracle_px", "oraclePx"), ("mark_px", "markPx"), ("mid_px", "midPx"))
_CTX_RATES = (("funding", "funding"), ("premium", "premium"))


def ctx_to_row(rx_ns: int, ctx: dict) -> dict:
    row = {"rx_ns": rx_ns}
    for out_name, key in _CTX_SCALED:
        row[out_name] = to_scaled(ctx.get(key))
    for out_name, key in _CTX_RATES:
        v = ctx.get(key)
        row[out_name] = None if v is None else float(v)
    impact = ctx.get("impactPxs") or [None, None]
    row["impact_bid"] = to_scaled(impact[0])
    row["impact_ask"] = to_scaled(impact[1] if len(impact) > 1 else None)
    return row


def read_hour(path: Path):
    """Yield every record in one hourly file, tolerating a torn tail.

    The file being converted may be the one the recorder had open when it was killed, so the
    final gzip member can be truncated: everything up to the last complete line is kept and
    the rest is dropped without failing the day. A line that is itself cut mid-JSON is skipped.
    Also reads multi-member files (one member per recorder restart within the hour).
    """
    try:
        with gzip.open(path, "rt") as fh:
            for line in fh:
                line = line.strip()
                if not line:
                    continue
                try:
                    yield json.loads(line)
                except json.JSONDecodeError:
                    continue
    except (EOFError, gzip.BadGzipFile, zlib.error) as e:
        log.debug("%s ends mid-stream (%s); stopping at the last intact record", path.name, e)


class DayConverter:
    """Converts one day's raw files, carrying dedup state across the hour boundary."""

    def __init__(self, out: Path, day: str, depth: int = DEPTH, depth5: int = DEPTH5,
                 **writer_kw):
        self.out, self.day = Path(out), day
        self.depths = {"book": depth, "book5": depth5}
        self.schemas = {"book": book_schema(depth), "book5": book_schema(depth5),
                        "trades": TRADES_SCHEMA, "asset_ctx": CTX_SCHEMA,
                        "bbo": BBO_SCHEMA, "events": EVENTS_SCHEMA}
        self.writers = {t: PartWriter(self.out, t, s, day, **writer_kw)
                        for t, s in self.schemas.items()}
        # Dedup keys per (table, coin) for books and per coin for ctx. A reconnect replays a
        # full snapshot, which must not be dropped as a duplicate, so these reset on
        # 'connected'. Trade ids are unique feed-wide, so a set catches the replayed last
        # trades every subscription starts with.
        self.prev_book = {}
        self.last_ms = {}
        self.prev_ctx = {}
        self.seen_tids = set()
        self.counts = defaultdict(int)
        self.dups = defaultdict(int)

    def convert_hour(self, path: Path):
        for rec in read_hour(path):
            self._record(rec, self.writers)

    def close(self):
        for w in self.writers.values():
            w.close()

    def _book_table(self, rec: dict, data: dict) -> str:
        """Which book table a snapshot belongs to.

        Recordings made since the role tag was added say so directly (`rl`). Older ones did
        not, and fall back to the level count -- correct except for a book genuinely thinner
        than 5 levels on the 20-level stream, which is why the tag was added.
        """
        role = rec.get("rl")
        if role in self.depths:
            return role
        levels = data.get("levels") or [[], []]
        return "book5" if max(len(levels[0]), len(levels[1])) <= DEPTH5 else "book"

    def _record(self, rec: dict, writers: dict):
        rx = rec["rx_ns"]
        if "event" in rec:
            detail = json.dumps({k: v for k, v in rec.items() if k not in ("rx_ns", "event")})
            writers["events"].append("_", {"rx_ns": rx, "event": rec["event"], "detail": detail})
            self.counts["events"] += 1
            if rec["event"] == "connected":
                # a fresh socket resends full snapshots; keep the first one after a gap
                self.prev_book.clear()
                self.prev_ctx.clear()
            return
        msg = rec.get("msg") or {}
        ch, data = msg.get("channel"), msg.get("data")
        if ch == "l2Book":
            table = self._book_table(rec, data)
            coin = data["coin"]
            slot = (table, coin)
            key = json.dumps(data["levels"], separators=(",", ":"))
            prev_ms = self.last_ms.get(slot, data["time"])
            self.last_ms[slot] = data["time"]
            if self.prev_book.get(slot) == key:
                self.dups[table] += 1
                return
            self.prev_book[slot] = key
            writers[table].append(coin, levels_to_row(rx, data, prev_ms, self.depths[table]))
            self.counts[table] += 1
        elif ch == "trades":
            for t in data:
                tid = t.get("tid")
                if tid is not None:
                    if tid in self.seen_tids:
                        self.dups["trades"] += 1
                        continue
                    self.seen_tids.add(tid)
                users = t.get("users") or [None, None]
                writers["trades"].append(t["coin"], {
                    "rx_ns": rx, "exch_ms": t["time"], "tid": t.get("tid"), "side": t.get("side"),
                    "px": to_scaled(t["px"]), "sz": to_scaled(t["sz"]),
                    "buyer": users[0], "seller": users[1] if len(users) > 1 else None,
                    "hash": t.get("hash")})
                self.counts["trades"] += 1
        elif ch == "activeAssetCtx":
            coin = data["coin"]
            row = ctx_to_row(rx, data["ctx"])
            key = tuple(v for k, v in row.items() if k != "rx_ns")
            if self.prev_ctx.get(coin) == key:
                self.dups["asset_ctx"] += 1
                return
            self.prev_ctx[coin] = key
            writers["asset_ctx"].append(coin, row)
            self.counts["asset_ctx"] += 1
        elif ch == "bbo":
            bid, ask = (data["bbo"] + [None, None])[:2]
            writers["bbo"].append(data["coin"], {
                "rx_ns": rx, "exch_ms": data["time"],
                "bid_px": to_scaled(bid["px"]) if bid else None,
                "bid_sz": to_scaled(bid["sz"]) if bid else None, "bid_n": bid["n"] if bid else None,
                "ask_px": to_scaled(ask["px"]) if ask else None,
                "ask_sz": to_scaled(ask["sz"]) if ask else None, "ask_n": ask["n"] if ask else None})
            self.counts["bbo"] += 1


def dir_bytes(p: Path) -> int:
    return sum(f.stat().st_size for f in p.rglob("*") if f.is_file()) if p.exists() else 0


def is_current(raw: Path, out: Path, day: str, tables=("book", "book5")) -> bool:
    """True if `day`'s parquet is at least as new as every raw hour it was built from.

    Lets the daily job re-run cheaply: finished days are skipped, while a day whose raw files
    grew after conversion (a partial day converted early, say) is rebuilt.
    """
    hours = list((raw / day).glob("*.jsonl.gz"))
    if not hours:
        return True
    parts = [p for t in tables for p in (out / t / ("date=%s" % day)).rglob("*.parquet")]
    if not parts:
        return False
    return min(p.stat().st_mtime for p in parts) >= max(h.stat().st_mtime for h in hours)


def convert_day(raw: Path, out: Path, day: str, depth: int = DEPTH, depth5: int = DEPTH5,
                **writer_kw) -> dict:
    hours = sorted((raw / day).glob("*.jsonl.gz"))
    if not hours:
        return {}
    conv = DayConverter(out, day, depth=depth, depth5=depth5, **writer_kw)
    t0 = time.monotonic()
    try:
        for h in hours:
            conv.convert_hour(h)
            log.info("  %s %s done", day, h.name)
    finally:
        conv.close()
    return {"rows": dict(conv.counts), "dups": dict(conv.dups), "hours": len(hours),
            "raw_bytes": sum(h.stat().st_size for h in hours),
            "pq_bytes": sum(dir_bytes(out / t / ("date=%s" % day)) for t in conv.schemas),
            "seconds": time.monotonic() - t0}


def main(argv=None):
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    p = argparse.ArgumentParser(description=__doc__,
                                formatter_class=argparse.RawDescriptionHelpFormatter)
    p.add_argument("--date", action="append",
                   help="YYYY-MM-DD (repeatable); default = every finished recorded day")
    p.add_argument("--depth", type=int, default=DEPTH,
                   help="levels per side to keep from the 5.4s stream (1-20)")
    p.add_argument("--depth5", type=int, default=DEPTH5,
                   help="levels per side to keep from the 0.5s stream (1-5)")
    p.add_argument("--batch-rows", type=int, default=8192, help="rows per parquet row group")
    p.add_argument("--compression-level", type=int, default=6, help="zstd level")
    p.add_argument("--force", action="store_true",
                   help="re-convert days whose parquet is already newer than their raw files")
    p.add_argument("--include-today", action="store_true",
                   help="also convert the UTC day still being recorded (rebuilt later anyway)")
    p.add_argument("--raw", type=Path, help="default: <PARAGON_RAW_ROOT>/hl")
    p.add_argument("--out", type=Path, help="default: <PARAGON_DATA_ROOT>/parquet/hl")
    a = p.parse_args(argv)
    raw = a.raw or raw_root() / "hl"
    out = a.out or parquet_root() / "hl"
    if a.date:
        days = a.date
    else:
        # The day still being written would be converted half-done and then skipped as
        # 'current' only until its next raw hour lands; leave it for tomorrow's run.
        today = datetime.now(timezone.utc).strftime("%Y-%m-%d")
        cutoff = (datetime.now(timezone.utc) - timedelta(minutes=10)).strftime("%Y-%m-%d")
        days = sorted(d.name for d in raw.iterdir() if d.is_dir()
                      and (a.include_today or (d.name < today and d.name <= cutoff)))
    for day in days:
        if not a.force and not a.date and is_current(raw, out, day):
            log.info("%s already current, skipping", day)
            continue
        s = convert_day(raw, out, day, depth=a.depth, depth5=a.depth5,
                        batch_rows=a.batch_rows, compression_level=a.compression_level)
        if not s:
            log.warning("%s: no raw files", day)
            continue
        log.info("%s %d hours in %.1fs | rows %s | dedup-skipped %s", day, s["hours"],
                 s["seconds"], s["rows"], s["dups"])
        log.info("%s raw.gz %.1f MB -> parquet %.1f MB (%.2fx)", day, s["raw_bytes"] / 1e6,
                 s["pq_bytes"] / 1e6, s["raw_bytes"] / max(s["pq_bytes"], 1))


if __name__ == "__main__":
    main()
