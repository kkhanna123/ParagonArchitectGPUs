"""Convert raw Kalshi recordings (from recorder.kalshi.recorder) into partitioned parquet.

Layout: <out>/<table>/date=<YYYY-MM-DD>/series=<SERIES>/part.parquet  -- the trade[XYZ] layout
with `series` in place of dex/sym. A series' markets share one file per day (a 15-minute series
lists 96 markets a day; a file each would be mostly footer), with `ticker` as a column. Rows
are sorted by (ticker, rx_ns) before writing, so each market's levels sit next to each other
and delta-encode down the time axis.

Tables:
  book        one row per *changed* top-5 book per side, rebuilt from snapshot + deltas
  book_delta  the lossless book feed: every snapshot level (snap=true) and every delta
  trades      one row per trade
  ticker      one row per *changed* ticker update (best bid/ask + sizes, last, volume, OI)
  markets     contract metadata whenever it changed (strikes, times, status, result, value)
  events      connect / disconnect / start / seq_gap markers -- use these to find gaps

Kalshi books are two bid ladders: YES bids and NO bids. A NO bid at p is a YES offer at 1 - p,
so the YES ask is 1 - best NO bid (the catalog's book_top view does that arithmetic). Prices
and sizes are int64 scaled by 1e8, the same as the trade[XYZ] tables.

    python -m recorder.kalshi.to_parquet                  # every finished, unconverted day
    python -m recorder.kalshi.to_parquet --date 2026-10-31 --include-today
"""
import argparse
import json
import logging
import time
from collections import defaultdict
from datetime import datetime, timedelta, timezone
from pathlib import Path

import pyarrow as pa

from recorder.hl_common import SCALE, safe_part, to_scaled
from recorder.hl_xyz_to_parquet import EVENTS_SCHEMA, PartWriter, dir_bytes, is_current, read_hour
from recorder.kalshi.common import kx_parquet, kx_raw, ms, num, series_of

log = logging.getLogger("kx_to_parquet")

DEPTH = 5   # full depth is always rebuildable from book_delta


def book_schema(depth: int = DEPTH) -> pa.Schema:
    f = [("rx_ns", pa.int64()), ("exch_ms", pa.int64()), ("seq", pa.int64()),
         ("ticker", pa.string())]
    for side in ("yes", "no"):
        for i in range(depth):
            f += [("%s_px_%d" % (side, i), pa.int64()), ("%s_sz_%d" % (side, i), pa.int64())]
    return pa.schema(f)


BOOK_DELTA_SCHEMA = pa.schema([
    ("rx_ns", pa.int64()), ("exch_ms", pa.int64()), ("seq", pa.int64()), ("ticker", pa.string()),
    ("side", pa.string()), ("px", pa.int64()), ("delta", pa.int64()), ("snap", pa.bool_()),
])

TRADES_SCHEMA = pa.schema([
    ("rx_ns", pa.int64()), ("exch_ms", pa.int64()), ("ticker", pa.string()),
    ("trade_id", pa.string()), ("taker_side", pa.string()), ("yes_px", pa.int64()),
    ("no_px", pa.int64()), ("count", pa.int64()), ("block", pa.bool_()),
])

TICKER_SCHEMA = pa.schema([
    ("rx_ns", pa.int64()), ("exch_ms", pa.int64()), ("ticker", pa.string()),
    ("last_px", pa.int64()), ("yes_bid", pa.int64()), ("yes_ask", pa.int64()),
    ("yes_bid_sz", pa.int64()), ("yes_ask_sz", pa.int64()), ("volume", pa.int64()),
    ("open_interest", pa.int64()), ("dollar_volume", pa.int64()),
    ("dollar_open_interest", pa.int64()),
])

# Shared with the backfill's settled-market history, which adds volume and OI on the end.
MARKET_FIELDS = [
    ("ticker", pa.string()), ("event_ticker", pa.string()), ("status", pa.string()),
    ("result", pa.string()), ("title", pa.string()), ("yes_sub_title", pa.string()),
    ("strike_type", pa.string()), ("floor_strike", pa.float64()), ("cap_strike", pa.float64()),
    ("open_ms", pa.int64()), ("close_ms", pa.int64()), ("expected_expiration_ms", pa.int64()),
    ("expiration_ms", pa.int64()), ("settled_ms", pa.int64()),
    # The real-world reading the market settled on: a price, a temperature, a rate -- or, for
    # a few series, text ("Fed maintains rate"). Kept as text; the catalog adds a numeric view.
    ("expiration_value", pa.string()), ("settlement_value", pa.int64()),
    ("rules_primary", pa.string()),
]
MARKETS_SCHEMA = pa.schema([("rx_ns", pa.int64())] + MARKET_FIELDS)


def market_row(m: dict) -> dict:
    return {
        "ticker": m.get("ticker"), "event_ticker": m.get("event_ticker"),
        "status": m.get("status"), "result": m.get("result") or None,
        "title": m.get("title"), "yes_sub_title": m.get("yes_sub_title"),
        "strike_type": m.get("strike_type"), "floor_strike": num(m.get("floor_strike")),
        "cap_strike": num(m.get("cap_strike")), "open_ms": ms(m.get("open_time")),
        "close_ms": ms(m.get("close_time")),
        "expected_expiration_ms": ms(m.get("expected_expiration_time")),
        "expiration_ms": ms(m.get("expiration_time")), "settled_ms": ms(m.get("settlement_ts")),
        "expiration_value": m.get("expiration_value") or None,
        "settlement_value": to_scaled(m.get("settlement_value_dollars")),
        "rules_primary": m.get("rules_primary"),
    }


def _levels(msg: dict, side: str) -> list:
    """Snapshot levels for one side as [(px, size)] in 1e8 units, whichever schema is used.

    Current frames carry `<side>_dollars_fp` (dollar strings, fractional sizes); older ones
    `<side>_dollars` (dollar strings, integer sizes) or `<side>` (integer cents). The key name,
    not the magnitude, tells them apart: a bare 1 could be 1 cent or $1.
    """
    for key, cents in (("%s_dollars_fp" % side, False), ("%s_dollars" % side, False),
                       (side, True)):
        rows = msg.get(key)
        if rows is not None:
            if cents:
                return [(int(p) * SCALE // 100, int(q) * SCALE) for p, q in rows]
            return [(to_scaled(p), to_scaled(q)) for p, q in rows]
    return []


def _delta(msg: dict) -> tuple:
    if msg.get("price_dollars") is not None:
        px = to_scaled(msg["price_dollars"])
    else:
        px = int(msg["price"]) * SCALE // 100
    d = msg.get("delta_fp")
    return px, to_scaled(d) if d is not None else int(msg["delta"]) * SCALE


class SortedPartWriter(PartWriter):
    """PartWriter that holds the whole day and sorts by (ticker, rx_ns) before writing.

    A Kalshi day is small next to the trade[XYZ] book, so buffering it costs nothing, and the
    sort is worth ~2x on the book tables: interleaved markets defeat delta encoding.
    """

    def __init__(self, *a, row_group: int = 65536, **kw):
        kw["batch_rows"] = 1 << 62
        super().__init__(*a, part_dirs=lambda s: ("series=%s" % safe_part(s),), **kw)
        self.row_group = row_group

    def _flush(self, key: str):
        b = self._buf[key]
        n = len(b[self.names[0]])
        if not n:
            return
        if "ticker" in b:
            order = sorted(range(n), key=lambda i: (b["ticker"][i] or "", b["rx_ns"][i]))
            for name in self.names:
                col = b[name]
                b[name] = [col[i] for i in order]
        full = {name: b[name] for name in self.names}
        for start in range(0, n, self.row_group):
            for name in self.names:
                b[name] = full[name][start:start + self.row_group]
            super()._flush(key)


class DayConverter:
    def __init__(self, out: Path, day: str, depth: int = DEPTH, **writer_kw):
        self.out, self.day, self.depth = Path(out), day, depth
        self.schemas = {"book": book_schema(depth), "book_delta": BOOK_DELTA_SCHEMA,
                        "trades": TRADES_SCHEMA, "ticker": TICKER_SCHEMA,
                        "markets": MARKETS_SCHEMA, "events": EVENTS_SCHEMA}
        self.writers = {t: SortedPartWriter(self.out, t, s, day, **writer_kw)
                        for t, s in self.schemas.items()}
        self.books = {}        # ticker -> {"yes": {px: sz}, "no": {px: sz}}
        self.prev_top = {}     # ticker -> last written top-N
        self.prev_ticker = {}  # ticker -> last written ticker values
        self.prev_meta = {}    # ticker -> last written metadata
        self.seen_trades = set()
        self.counts = defaultdict(int)
        self.dups = defaultdict(int)

    def convert_hour(self, path: Path):
        for rec in read_hour(path):
            self._record(rec)

    def close(self):
        for w in self.writers.values():
            w.close()

    def _append(self, table: str, ticker: str, row: dict):
        self.writers[table].append(series_of(ticker), row)
        self.counts[table] += 1

    def _top(self, ticker: str, rx: int, exch_ms: int, seq):
        book = self.books[ticker]
        row = {"rx_ns": rx, "exch_ms": exch_ms, "seq": seq, "ticker": ticker}
        key = []
        for side in ("yes", "no"):
            levels = sorted(book[side].items(), reverse=True)[:self.depth]
            for i in range(self.depth):
                px, sz = levels[i] if i < len(levels) else (None, None)
                row["%s_px_%d" % (side, i)] = px
                row["%s_sz_%d" % (side, i)] = sz
            key.append(tuple(levels))
        key = tuple(key)
        if self.prev_top.get(ticker) == key:
            self.dups["book"] += 1
            return
        self.prev_top[ticker] = key
        self._append("book", ticker, row)

    def _record(self, rec: dict):
        rx = rec["rx_ns"]
        if "event" in rec:
            detail = json.dumps({k: v for k, v in rec.items() if k not in ("rx_ns", "event")})
            self.writers["events"].append("_", {"rx_ns": rx, "event": rec["event"],
                                                "detail": detail})
            self.counts["events"] += 1
            if rec["event"] == "connected":
                # a new socket resends every snapshot; books rebuild from those
                self.books.clear()
                self.prev_top.clear()
                self.prev_ticker.clear()
            return
        d = rec.get("msg") or {}
        typ = d.get("type")
        if rec.get("rl") == "rest":
            if typ == "markets":
                for m in d.get("markets") or []:
                    row = market_row(m)
                    t = row["ticker"]
                    if not t or self.prev_meta.get(t) == row:
                        self.dups["markets"] += int(bool(t))
                        continue
                    self.prev_meta[t] = row
                    self._append("markets", t, {"rx_ns": rx, **row})
            return
        msg = d.get("msg") or {}
        t = msg.get("market_ticker") if isinstance(msg, dict) else None
        if not t:
            return
        exch = msg.get("ts_ms") or d.get("sending_ts_ms")
        seq = d.get("seq")
        if typ == "orderbook_snapshot":
            book = self.books[t] = {"yes": {}, "no": {}}
            for side in ("yes", "no"):
                for px, sz in _levels(msg, side):
                    book[side][px] = sz
                    self._append("book_delta", t, {"rx_ns": rx, "exch_ms": exch, "seq": seq,
                                                   "ticker": t, "side": side, "px": px,
                                                   "delta": sz, "snap": True})
            self.prev_top.pop(t, None)
            self._top(t, rx, exch, seq)
        elif typ == "orderbook_delta":
            px, delta = _delta(msg)
            side = msg.get("side")
            self._append("book_delta", t, {"rx_ns": rx, "exch_ms": exch, "seq": seq, "ticker": t,
                                           "side": side, "px": px, "delta": delta,
                                           "snap": False})
            book = self.books.get(t)
            if book is None or side not in book:
                return  # a delta before its snapshot (only possible mid-file); skip the book
            sz = book[side].get(px, 0) + delta
            if sz > 0:
                book[side][px] = sz
            else:
                book[side].pop(px, None)
            self._top(t, rx, exch, seq)
        elif typ == "trade":
            tid = msg.get("trade_id")
            if tid in self.seen_trades:
                self.dups["trades"] += 1
                return
            if tid:
                self.seen_trades.add(tid)
            cnt = msg.get("count_fp")
            self._append("trades", t, {
                "rx_ns": rx, "exch_ms": exch, "ticker": t, "trade_id": tid,
                "taker_side": msg.get("taker_side"),
                "yes_px": to_scaled(msg.get("yes_price_dollars")),
                "no_px": to_scaled(msg.get("no_price_dollars")),
                "count": to_scaled(cnt) if cnt is not None else (
                    int(msg["count"]) * SCALE if msg.get("count") is not None else None),
                "block": msg.get("is_block_trade")})
        elif typ == "ticker":
            row = {"rx_ns": rx, "exch_ms": exch, "ticker": t,
                   "last_px": to_scaled(msg.get("price_dollars")),
                   "yes_bid": to_scaled(msg.get("yes_bid_dollars")),
                   "yes_ask": to_scaled(msg.get("yes_ask_dollars")),
                   "yes_bid_sz": to_scaled(msg.get("yes_bid_size_fp")),
                   "yes_ask_sz": to_scaled(msg.get("yes_ask_size_fp")),
                   "volume": to_scaled(msg.get("volume_fp")),
                   "open_interest": to_scaled(msg.get("open_interest_fp")),
                   "dollar_volume": msg.get("dollar_volume"),
                   "dollar_open_interest": msg.get("dollar_open_interest")}
            key = tuple(v for k, v in row.items() if k not in ("rx_ns", "exch_ms"))
            if self.prev_ticker.get(t) == key:
                self.dups["ticker"] += 1
                return
            self.prev_ticker[t] = key
            self._append("ticker", t, row)


def convert_day(raw: Path, out: Path, day: str, depth: int = DEPTH, **writer_kw) -> dict:
    hours = sorted((raw / day).glob("*.jsonl.gz"))
    if not hours:
        return {}
    conv = DayConverter(out, day, depth=depth, **writer_kw)
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
    p.add_argument("--date", action="append", help="YYYY-MM-DD (repeatable)")
    p.add_argument("--depth", type=int, default=DEPTH, help="book levels per side to keep")
    p.add_argument("--compression-level", type=int, default=6, help="zstd level")
    p.add_argument("--force", action="store_true", help="re-convert days already current")
    p.add_argument("--include-today", action="store_true",
                   help="also convert the UTC day still being recorded (rebuilt later anyway)")
    p.add_argument("--raw", type=Path, help="default: <PARAGON_RAW_ROOT>/kalshi")
    p.add_argument("--out", type=Path, help="default: <PARAGON_DATA_ROOT>/parquet/kalshi")
    a = p.parse_args(argv)
    raw = a.raw or kx_raw()
    out = a.out or kx_parquet()
    if not raw.exists():
        log.info("no raw recordings at %s yet", raw)
        return
    if a.date:
        days = a.date
    else:
        today = datetime.now(timezone.utc).strftime("%Y-%m-%d")
        cutoff = (datetime.now(timezone.utc) - timedelta(minutes=10)).strftime("%Y-%m-%d")
        days = sorted(d.name for d in raw.iterdir() if d.is_dir()
                      and (a.include_today or (d.name < today and d.name <= cutoff)))
    for day in days:
        if not a.force and not a.date and is_current(raw, out, day, tables=("events",)):
            log.info("%s already current, skipping", day)
            continue
        s = convert_day(raw, out, day, depth=a.depth, compression_level=a.compression_level)
        if not s:
            log.warning("%s: no raw files", day)
            continue
        log.info("%s %d hours in %.1fs | rows %s | dedup-skipped %s", day, s["hours"],
                 s["seconds"], s["rows"], s["dups"])
        log.info("%s raw.gz %.1f MB -> parquet %.1f MB (%.2fx)", day, s["raw_bytes"] / 1e6,
                 s["pq_bytes"] / 1e6, s["raw_bytes"] / max(s["pq_bytes"], 1))


if __name__ == "__main__":
    main()
