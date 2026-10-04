"""Convert raw AX recordings (from ax_recorder) into per-day parquet tables.

Tables written to <out>/<table>/<YYYY-MM-DD>.parquet:
  trades   one row per trade print (price, qty, aggressor side)
  tickers  one row per ticker update (last, mark, bid/ask, OI, bands, est. funding)
  book     one row per book update: top-N levels of price / qty / order count
  candles  one row per candle update
  events   connect / disconnect markers (use these to find gaps)

    python -m recorder.ax_to_parquet --env sandbox --date 2026-10-04
"""
import argparse
import gzip
import json
from collections import defaultdict
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq

ROOT = Path(__file__).resolve().parents[1]


def exch_ns(msg) -> int:
    return msg["ts"] * 1_000_000_000 + msg.get("tn", 0)


class Book:
    """Order book for one symbol. Levels map price string -> (qty, order quantities or None)."""

    def __init__(self):
        self.bids, self.asks = {}, {}

    def apply(self, msg):
        # st=true is a full snapshot; otherwise the message is a diff where qty 0 deletes a level.
        if msg.get("st", True):
            self.bids, self.asks = {}, {}
        for side, levels in ((self.bids, msg.get("b", [])), (self.asks, msg.get("a", []))):
            for lv in levels:
                if lv["q"] == 0:
                    side.pop(lv["p"], None)
                else:
                    side[lv["p"]] = (lv["q"], lv.get("o"))

    def top(self, n):
        bids = sorted(self.bids.items(), key=lambda kv: -float(kv[0]))[:n]
        asks = sorted(self.asks.items(), key=lambda kv: float(kv[0]))[:n]
        return bids, asks


def book_row(rx_ns, msg, book, depth):
    bids, asks = book.top(depth)
    row = {"rx_ns": rx_ns, "exch_ns": exch_ns(msg), "symbol": msg["s"], "level": int(msg["t"]),
           "snapshot": bool(msg.get("st", True))}
    for name, levels in (("bid", bids), ("ask", asks)):
        for i in range(depth):
            if i < len(levels):
                px, (qty, orders) = levels[i]
                row[f"{name}_px_{i}"] = float(px)
                row[f"{name}_qty_{i}"] = qty
                row[f"{name}_n_{i}"] = len(orders) if orders is not None else None
            else:
                row[f"{name}_px_{i}"] = row[f"{name}_qty_{i}"] = row[f"{name}_n_{i}"] = None
    return row


TICKER_FIELDS = {"p": "last_px", "q": "last_qty", "o": "open", "h": "high", "l": "low", "v": "volume",
                 "oi": "open_interest", "i": "state", "m": "mark_px", "bp": "bid_px", "ap": "ask_px",
                 "pl": "band_lower", "pu": "band_upper", "lsp": "last_settle_px", "lst": "last_settle_ts"}
FLOAT_FIELDS = {"last_px", "open", "high", "low", "mark_px", "bid_px", "ask_px", "band_lower", "band_upper",
                "last_settle_px"}


def to_float(x):
    return None if x is None else float(x)


def ticker_row(rx_ns, msg):
    row = {"rx_ns": rx_ns, "exch_ns": exch_ns(msg), "symbol": msg["s"]}
    for k, name in TICKER_FIELDS.items():
        v = msg.get(k)
        row[name] = to_float(v) if name in FLOAT_FIELDS else v
    ef = msg.get("ef") or {}
    row["est_funding_status"] = ef.get("status")
    row["est_funding_rate"] = to_float(ef.get("funding_rate"))
    row["est_benchmark_px"] = to_float(ef.get("benchmark_price"))
    return row


def convert_records(records, depth=10):
    """records: iterable of decoded JSONL dicts. Returns {table: [row, ...]}."""
    tables = defaultdict(list)
    books = defaultdict(Book)
    for rec in records:
        rx = rec["rx_ns"]
        if "event" in rec:
            tables["events"].append({"rx_ns": rx, "event": rec["event"],
                                     "detail": json.dumps({k: v for k, v in rec.items() if k not in ("rx_ns", "event")})})
            if rec["event"] == "connected":
                books.clear()  # books must be rebuilt from the next snapshot
            continue
        msg = rec["msg"]
        t = msg.get("t")
        if t == "t":
            tables["trades"].append({"rx_ns": rx, "exch_ns": exch_ns(msg), "symbol": msg["s"],
                                     "px": float(msg["p"]), "qty": msg["q"], "side": msg["d"]})
        elif t == "s":
            tables["tickers"].append(ticker_row(rx, msg))
        elif t in ("1", "2", "3"):
            book = books[msg["s"]]
            book.apply(msg)
            tables["book"].append(book_row(rx, msg, book, depth))
        elif t == "c":
            tables["candles"].append({"rx_ns": rx, "exch_ns": msg["ts"] * 1_000_000_000, "symbol": msg["symbol"],
                                      "width": msg["width"], "open": float(msg["open"]), "high": float(msg["high"]),
                                      "low": float(msg["low"]), "close": float(msg["close"]),
                                      "buy_volume": msg["buy_volume"], "sell_volume": msg["sell_volume"],
                                      "volume": msg["volume"]})
        # heartbeats ("h") and subscription acks are dropped here; they remain in the raw files.
    return tables


def read_day(raw_dir: Path, day: str):
    for f in sorted((raw_dir / day).glob("*.jsonl.gz")):
        with gzip.open(f, "rt") as fh:
            for line in fh:
                line = line.strip()
                if not line:
                    continue
                try:
                    yield json.loads(line)
                except json.JSONDecodeError:
                    continue  # a truncated last line from a crash


def write_tables(tables, out: Path, day: str):
    written = {}
    for name, rows in tables.items():
        if not rows:
            continue
        (out / name).mkdir(parents=True, exist_ok=True)
        path = out / name / f"{day}.parquet"
        pq.write_table(pa.Table.from_pylist(rows), path, compression="zstd")
        written[name] = (path, len(rows))
    return written


def main(argv=None):
    p = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    p.add_argument("--env", default="sandbox")
    p.add_argument("--date", action="append", help="YYYY-MM-DD (repeatable); default = every recorded day")
    p.add_argument("--depth", type=int, default=10, help="book levels per side to flatten")
    p.add_argument("--raw", type=Path)
    p.add_argument("--out", type=Path)
    a = p.parse_args(argv)
    raw = a.raw or ROOT / "Data" / "raw" / "ax" / a.env
    out = a.out or ROOT / "Data" / "parquet" / "ax" / a.env
    days = a.date or sorted(d.name for d in raw.iterdir() if d.is_dir())
    for day in days:
        for name, (path, n) in write_tables(convert_records(read_day(raw, day), a.depth), out, day).items():
            print(f"{day} {name:8s} {n:>9,d} rows -> {path.relative_to(ROOT) if path.is_relative_to(ROOT) else path}")


if __name__ == "__main__":
    main()
