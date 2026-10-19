"""Convert raw Hyperliquid recordings (from hl_recorder) into per-day parquet tables.

Tables written to <out>/<table>/<YYYY-MM-DD>.parquet:
  book    one row per l2Book update: top-N levels of price / qty / order count
  bbo     one row per best bid/offer change
  trades  one row per trade print: price, qty, aggressor side, buyer and seller wallets (de-duplicated)
  ctx     one row per activeAssetCtx update: mark, oracle, mid, funding, OI, premium, impact prices
  meta    one row per market per REST snapshot: live/delisted, max leverage, size decimals + ctx fields
  events  started / connected / disconnected / markets_changed / stopped markers (use these to find gaps)

The recorder converts each finished UTC day by itself; run this by hand to re-convert, change
--depth, or convert today's partial day.

    python -m recorder.hl_to_parquet                              # every dex and day not yet converted
    python -m recorder.hl_to_parquet --dex xyz --date today --depth 10
"""
import argparse
import json
import os
import zlib
from collections import defaultdict
from datetime import datetime, timezone
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq

ROOT = Path(__file__).resolve().parents[1]
RAW = ROOT / "Data" / "raw" / "hyperliquid"
OUT = ROOT / "Data" / "parquet" / "hyperliquid"
TABLES = ("book", "bbo", "trades", "ctx", "meta", "events")
CHUNK = 1 << 16

F, I, S, B = pa.float64(), pa.int64(), pa.string(), pa.bool_()
CTX_FIELDS = {"markPx": "mark_px", "oraclePx": "oracle_px", "midPx": "mid_px", "funding": "funding",
              "openInterest": "open_interest", "premium": "premium", "dayNtlVlm": "day_ntl_vlm",
              "prevDayPx": "prev_day_px"}
CTX_COLS = [(name, F) for name in CTX_FIELDS.values()] + [("impact_bid_px", F), ("impact_ask_px", F)]


def schemas(depth):
    book = [("rx_ns", I), ("exch_ns", I), ("symbol", S)]
    for side in ("bid", "ask"):
        for i in range(depth):
            book += [(f"{side}_px_{i}", F), (f"{side}_qty_{i}", F), (f"{side}_n_{i}", I)]
    return {
        "book": pa.schema(book),
        "bbo": pa.schema([("rx_ns", I), ("exch_ns", I), ("symbol", S), ("bid_px", F), ("bid_qty", F),
                          ("bid_n", I), ("ask_px", F), ("ask_qty", F), ("ask_n", I)]),
        "trades": pa.schema([("rx_ns", I), ("exch_ns", I), ("symbol", S), ("px", F), ("qty", F), ("side", S),
                             ("tid", I), ("hash", S), ("buyer", S), ("seller", S)]),
        "ctx": pa.schema([("rx_ns", I), ("symbol", S)] + CTX_COLS),
        "meta": pa.schema([("rx_ns", I), ("symbol", S), ("live", B), ("max_leverage", I), ("sz_decimals", I),
                           ("only_isolated", B)] + CTX_COLS),
        "events": pa.schema([("rx_ns", I), ("event", S), ("detail", S)]),
    }


def to_float(x):
    try:
        return None if x is None else float(x)
    except (TypeError, ValueError):
        return None


def exch_ns(ms):
    return None if ms is None else int(ms) * 1_000_000


def ctx_values(c):
    row = {name: to_float(c.get(k)) for k, name in CTX_FIELDS.items()}
    impact = (c.get("impactPxs") or []) + [None, None]
    row["impact_bid_px"], row["impact_ask_px"] = to_float(impact[0]), to_float(impact[1])
    return row


def book_row(rx_ns, d, depth):
    row = {"rx_ns": rx_ns, "exch_ns": exch_ns(d.get("time")), "symbol": d["coin"]}
    levels = (d.get("levels") or []) + [[], []]
    for name, lv in (("bid", levels[0]), ("ask", levels[1])):
        for i in range(depth):
            l = lv[i] if i < len(lv) else {}
            row[f"{name}_px_{i}"], row[f"{name}_qty_{i}"] = to_float(l.get("px")), to_float(l.get("sz"))
            row[f"{name}_n_{i}"] = l.get("n")
    return row


def bbo_row(rx_ns, d):
    pair = list(d.get("bbo") or []) + [None, None]
    row = {"rx_ns": rx_ns, "exch_ns": exch_ns(d.get("time")), "symbol": d["coin"]}
    for name, l in (("bid", pair[0]), ("ask", pair[1])):
        l = l or {}
        row[f"{name}_px"], row[f"{name}_qty"], row[f"{name}_n"] = to_float(l.get("px")), to_float(l.get("sz")), l.get("n")
    return row


class Converter:
    """Turns raw records into table rows. Keeps only the state needed to de-duplicate trades."""

    def __init__(self, depth=5, only=None):
        self.depth, self.only = depth, set(only) if only else None
        self.tables = defaultdict(list)
        self.seen_trades = set()  # recent trades are re-sent on every (re)subscribe

    def _want(self, table):
        return self.only is None or table in self.only

    def feed(self, rec):
        rx, tables = rec["rx_ns"], self.tables
        if "event" in rec:
            if self._want("events"):
                tables["events"].append({"rx_ns": rx, "event": rec["event"], "detail": json.dumps(
                    {k: v for k, v in rec.items() if k not in ("rx_ns", "event")})})
            return
        msg = rec.get("msg") or {}
        ch, d = msg.get("channel"), msg.get("data")
        if ch == "l2Book" and self._want("book"):
            tables["book"].append(book_row(rx, d, self.depth))
        elif ch == "bbo" and self._want("bbo"):
            tables["bbo"].append(bbo_row(rx, d))
        elif ch == "trades" and self._want("trades"):
            for t in d:
                key = (t.get("coin"), t.get("tid"), t.get("hash"))
                if key in self.seen_trades:
                    continue
                self.seen_trades.add(key)
                users = (t.get("users") or []) + [None, None]
                tables["trades"].append({"rx_ns": rx, "exch_ns": exch_ns(t.get("time")), "symbol": t["coin"],
                                         "px": to_float(t.get("px")), "qty": to_float(t.get("sz")),
                                         "side": t.get("side"), "tid": t.get("tid"), "hash": t.get("hash"),
                                         "buyer": users[0], "seller": users[1]})
        elif ch == "activeAssetCtx" and self._want("ctx"):
            tables["ctx"].append({"rx_ns": rx, "symbol": d["coin"], **ctx_values(d.get("ctx") or {})})
        elif ch == "metaAndAssetCtxs" and self._want("meta"):
            meta, ctxs = d
            for u, c in zip(meta.get("universe", []), ctxs):
                tables["meta"].append({"rx_ns": rx, "symbol": u["name"], "live": not u.get("isDelisted", False),
                                       "max_leverage": u.get("maxLeverage"), "sz_decimals": u.get("szDecimals"),
                                       "only_isolated": bool(u.get("onlyIsolated", False)), **ctx_values(c or {})})
        elif ch == "error" and self._want("events"):
            tables["events"].append({"rx_ns": rx, "event": "exchange_error", "detail": json.dumps(d)})
        # pongs and subscription acks are dropped here; they remain in the raw files.

    def take(self):
        out, self.tables = self.tables, defaultdict(list)
        return out


def convert_records(records, depth=5, only=None):
    """records: iterable of decoded JSONL dicts. Returns {table: [row, ...]}, each sorted by rx_ns."""
    c = Converter(depth, only)
    for rec in records:
        c.feed(rec)
    tables = c.take()
    for rows in tables.values():
        rows.sort(key=lambda r: r["rx_ns"])
    return tables


def gzip_lines(path):
    """Lines of a gzip file made of several members (one per recorder start), streamed.

    Unlike gzip.open this survives a member cut short by a crash or power cut: it keeps everything
    up to the first bad byte and carries on with the next member instead of failing the whole hour.
    """
    data = path.read_bytes()  # compressed size only (an hour is tens of MB at most)
    pos = 0
    while True:
        start = data.find(b"\x1f\x8b\x08", pos)
        if start < 0:
            return
        d, i, step, tail = zlib.decompressobj(31), start, CHUNK, b""
        while i < len(data) and not d.eof:
            piece, before = data[i:i + step], d.copy()
            try:
                chunk = d.decompress(piece)
                i += len(piece)
            except zlib.error:
                if step == 1:
                    break  # found the first bad byte; everything before it is kept
                d, step = before, max(1, step // 64)  # retry this piece in smaller steps
                continue
            *lines, tail = (tail + chunk).split(b"\n")
            for line in lines:
                yield line.decode("utf-8", "replace")
        if tail:
            yield tail.decode("utf-8", "replace")
        pos = (min(i, len(data)) - len(d.unused_data)) if d.eof else start + 1


def read_day(raw_dir: Path, day: str):
    for f in sorted((raw_dir / day).glob("*.jsonl.gz")):
        for line in gzip_lines(f):
            line = line.strip()
            if not line:
                continue
            try:
                yield json.loads(line)
            except json.JSONDecodeError:
                continue  # a truncated last line from a crash


def write_tables(tables, out: Path, day: str, depth=5):
    written, sch = {}, schemas(depth)
    for name, rows in tables.items():
        if not rows:
            continue
        (out / name).mkdir(parents=True, exist_ok=True)
        path = out / name / f"{day}.parquet"
        tmp = path.with_suffix(".parquet.tmp")
        pq.write_table(pa.Table.from_pylist(rows, schema=sch[name]), tmp, compression="zstd")
        os.replace(tmp, path)  # readers never see a half-written file
        written[name] = (path, len(rows))
    return written


def convert_day(raw_dir: Path, out: Path, day: str, depth=5, batch_rows=20_000):
    """Stream one day of raw files into parquet, writing in row groups so memory stays flat
    however busy the day was."""
    out, sch, conv = Path(out), schemas(depth), Converter(depth)
    writers, counts = {}, defaultdict(int)

    def flush(force=False):
        for name, rows in list(conv.tables.items()):
            if rows and (force or len(rows) >= batch_rows):
                if name not in writers:
                    (out / name).mkdir(parents=True, exist_ok=True)
                    tmp = out / name / f"{day}.parquet.tmp"
                    writers[name] = (pq.ParquetWriter(tmp, sch[name], compression="zstd"), tmp)
                rows.sort(key=lambda r: r["rx_ns"])
                writers[name][0].write_table(pa.Table.from_pylist(rows, schema=sch[name]))
                counts[name] += len(rows)
                conv.tables[name] = []

    try:
        for i, rec in enumerate(read_day(Path(raw_dir), day)):
            conv.feed(rec)
            if i % 2_000 == 0:
                flush()
        flush(force=True)
    finally:
        for w, _ in writers.values():
            w.close()
    written = {}
    for name, (_, tmp) in writers.items():
        path = out / name / f"{day}.parquet"
        os.replace(tmp, path)  # readers never see a half-written file
        written[name] = (path, counts[name])
    return written


def main(argv=None):
    p = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    p.add_argument("--dex", action="append", help="repeatable; default = every recorded dex")
    p.add_argument("--date", action="append", help="YYYY-MM-DD or 'today' (repeatable); default = every "
                                                    "recorded day that has no parquet yet")
    p.add_argument("--depth", type=int, default=5, help="book levels per side to flatten (max 20)")
    p.add_argument("--raw", type=Path, help=f"default: {RAW.relative_to(ROOT)}")
    p.add_argument("--out", type=Path, help=f"default: {OUT.relative_to(ROOT)}")
    a = p.parse_args(argv)
    raw_root, out_root = a.raw or RAW, a.out or OUT
    if a.dex:
        dexes = a.dex
    else:
        dexes = sorted(d.name for d in raw_root.iterdir() if d.is_dir()) if raw_root.exists() else []
    today = datetime.now(timezone.utc).strftime("%Y-%m-%d")
    for dex in dexes:
        raw, out = raw_root / dex, out_root / dex
        if a.date:
            days = [today if d == "today" else d for d in a.date]
        else:
            days = [d.name for d in sorted(raw.iterdir()) if d.is_dir()
                    and not any((out / t / f"{d.name}.parquet").exists() for t in TABLES)]
        for day in days:
            for name, (path, n) in convert_day(raw, out, day, a.depth).items():
                shown = path.relative_to(ROOT) if path.is_relative_to(ROOT) else path
                print(f"{dex} {day} {name:7s} {n:>10,d} rows -> {shown}")


if __name__ == "__main__":
    main()
