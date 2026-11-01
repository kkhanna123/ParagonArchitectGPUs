"""Backfill Kalshi history over REST into parquet/kalshi_hist, merging into what is there.

  markets   every settled market of each adapter's series, with its result and the real-world
            value it settled on (`expiration_value`: the end-of-day commodity price, the
            observed high/low temperature, the AAA gas price, the fed funds rate...).
            -> kalshi_hist/markets/series=<S>/part.parquet          (one row per market)
  candles   per-market candlesticks (price OHLC/mean, closing bid/ask, volume, OI) at the
            adapter's `candle_period`, fetched once per market after it settles, and only for
            markets that traded.
            -> kalshi_hist/candles/period=<P>/series=<S>/part.parquet

Kalshi serves recent settled markets from /markets and moves older ones to /historical/markets
(the split is /historical/cutoff, ~2 months back). Nightly runs only need the recent endpoint;
--history also walks the archive, once.

    python -m recorder.kalshi.backfill                       # nightly: recent markets + candles
    python -m recorder.kalshi.backfill --history             # first run: also the archive
    python -m recorder.kalshi.backfill --adapters weather --no-candles
"""
import argparse
import logging
import time
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq

from recorder.hl_backfill import existing_max, merge_write, schema_matches
from recorder.kalshi.adapters import GROUPS, select
from recorder.kalshi.common import Rest, kx_hist, load_credentials, ms, safe_part, to_scaled
from recorder.kalshi.to_parquet import MARKET_FIELDS, market_row

log = logging.getLogger("kx_backfill")

HIST_MARKETS_SCHEMA = pa.schema(MARKET_FIELDS + [
    ("volume", pa.int64()), ("open_interest", pa.int64()), ("last_px", pa.int64()),
])

CANDLE_SCHEMA = pa.schema([
    ("ticker", pa.string()), ("end_ms", pa.int64()), ("open", pa.int64()), ("high", pa.int64()),
    ("low", pa.int64()), ("close", pa.int64()), ("mean", pa.int64()),
    ("yes_bid_close", pa.int64()), ("yes_ask_close", pa.int64()), ("volume", pa.int64()),
    ("open_interest", pa.int64()),
])

SETTLED = ("settled", "finalized", "determined")
MAX_CANDLES = 4000  # per request; longer markets are fetched in windows


def hist_row(m: dict) -> dict:
    return {**market_row(m), "volume": to_scaled(m.get("volume_fp")),
            "open_interest": to_scaled(m.get("open_interest_fp")),
            "last_px": to_scaled(m.get("last_price_dollars"))}


def candle_row(ticker: str, c: dict) -> dict:
    price, bid, ask = c.get("price") or {}, c.get("yes_bid") or {}, c.get("yes_ask") or {}
    return {"ticker": ticker, "end_ms": int(c["end_period_ts"]) * 1000,
            "open": to_scaled(price.get("open_dollars")),
            "high": to_scaled(price.get("high_dollars")),
            "low": to_scaled(price.get("low_dollars")),
            "close": to_scaled(price.get("close_dollars")),
            "mean": to_scaled(price.get("mean_dollars")),
            "yes_bid_close": to_scaled(bid.get("close_dollars")),
            "yes_ask_close": to_scaled(ask.get("close_dollars")),
            "volume": to_scaled(c.get("volume_fp")),
            "open_interest": to_scaled(c.get("open_interest_fp"))}


def fetch_markets(rest: Rest, series: str, since_s: int | None, history: bool) -> list:
    rows = list(rest.paginate("/markets", "markets", series_ticker=series, status="settled",
                              min_close_ts=since_s))
    if history:
        rows += list(rest.paginate("/historical/markets", "markets", series_ticker=series))
    return rows


def fetch_candles(rest: Rest, series: str, row: dict, period: int) -> list:
    start = (row["open_ms"] or row["close_ms"]) // 1000
    end = (row["close_ms"] or row["open_ms"]) // 1000 + period * 60
    out, step = [], MAX_CANDLES * period * 60
    for lo in range(start, end, step):
        d = rest.get("/series/%s/markets/%s/candlesticks" % (series, row["ticker"]),
                     start_ts=lo, end_ts=min(end, lo + step), period_interval=period)
        out += [candle_row(row["ticker"], c) for c in d.get("candlesticks") or []
                if c.get("end_period_ts")]
    return out


def backfill_series(rest: Rest, out: Path, series: str, period: int, history: bool,
                    candles: bool, cutoff_ms: int | None) -> dict:
    mpath = out / "markets" / ("series=%s" % safe_part(series)) / "part.parquet"
    mpath.parent.mkdir(parents=True, exist_ok=True)
    last = existing_max(mpath, "close_ms", HIST_MARKETS_SCHEMA)
    # Resume a few days before the newest stored close: settlement can lag the close by days.
    since = (last // 1000 - 5 * 86400) if last and not history else None
    rows = [hist_row(m) for m in fetch_markets(rest, series, since, history)]
    total, added = merge_write(rows, HIST_MARKETS_SCHEMA, mpath, "ticker") if rows else (0, 0)
    got = {"markets": total, "markets_new": added, "candles_new": 0}
    if not candles or not period or not mpath.exists():
        return got

    cpath = out / "candles" / ("period=%d" % period) / ("series=%s" % safe_part(series)) \
        / "part.parquet"
    cpath.parent.mkdir(parents=True, exist_ok=True)
    have = set()
    if cpath.exists() and schema_matches(cpath, CANDLE_SCHEMA):
        have = set(pq.read_table(cpath, columns=["ticker"])["ticker"].to_pylist())
    todo = [r for r in pq.read_table(mpath).to_pylist()
            if r["status"] in SETTLED and r["ticker"] not in have and (r["volume"] or 0) > 0
            and r["close_ms"] and (cutoff_ms is None or r["close_ms"] >= cutoff_ms)]
    batch = []
    for i, r in enumerate(todo, 1):
        try:
            batch += fetch_candles(rest, series, r, period)
        except Exception as e:  # noqa: BLE001 -- one bad market must not sink the series
            log.warning("%s candles failed: %s", r["ticker"], e)
        if len(batch) > 200_000 or i == len(todo):
            if batch:
                _, n = merge_write(batch, CANDLE_SCHEMA, cpath, ("ticker", "end_ms"))
                got["candles_new"] += n
            batch = []
            log.info("  %s candles: %d/%d markets", series, i, len(todo))
    return got


def main(argv=None):
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    p = argparse.ArgumentParser(description=__doc__,
                                formatter_class=argparse.RawDescriptionHelpFormatter)
    p.add_argument("--adapters", default="all",
                   help="comma-separated adapters and/or groups (%s)" % ", ".join(GROUPS))
    p.add_argument("--history", action="store_true",
                   help="also walk /historical/markets (the archive older than ~2 months)")
    p.add_argument("--no-candles", action="store_true")
    p.add_argument("--per-sec", type=float, default=10.0, help="REST request pacing")
    p.add_argument("--out", type=Path, help="default: <PARAGON_DATA_ROOT>/parquet/kalshi_hist")
    a = p.parse_args(argv)
    out = a.out or kx_hist()
    try:
        creds = load_credentials()
    except Exception as e:  # noqa: BLE001 -- market data is public; carry on unsigned
        log.warning("credentials unusable (%s); continuing unsigned", e)
        creds = None
    rest = Rest(creds, per_sec=a.per_sec)
    # Candlesticks for markets older than the cutoff live under /historical; skip those.
    try:
        cutoff_ms = ms(rest.get("/historical/cutoff").get("market_settled_ts"))
    except Exception:  # noqa: BLE001
        cutoff_ms = None
    t0 = time.monotonic()
    totals = {"markets_new": 0, "candles_new": 0}
    for ad in select([n.strip() for n in a.adapters.split(",") if n.strip()]):
        for s in ad.series:
            try:
                got = backfill_series(rest, out, s, ad.candle_period, a.history,
                                      not a.no_candles, cutoff_ms)
            except Exception as e:  # noqa: BLE001
                log.warning("%s failed: %s", s, e)
                continue
            for k in totals:
                totals[k] += got[k]
            log.info("[%s] %-14s markets %d (+%d)  candles +%d", ad.name, s, got["markets"],
                     got["markets_new"], got["candles_new"])
    log.info("done in %.0fs, %d requests: +%d markets, +%d candles -> %s",
             time.monotonic() - t0, rest.calls, totals["markets_new"], totals["candles_new"], out)


if __name__ == "__main__":
    main()
