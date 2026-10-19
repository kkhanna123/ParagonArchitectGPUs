"""Load Hyperliquid recordings into pandas.

    from recorder.hl_load import load, mid_series
    bbo  = load("bbo", "US500", start="2026-10-18")           # dex defaults to mkts (Markets.xyz)
    book = load("book", "xyz:CL", start="2026-10-18 14:00", end="2026-10-18 15:00", dex="xyz")
    px   = mid_series("US500", freq="1s")                     # regular grid, ready to join with Polymarket

Reads the daily parquet files; days not converted yet (e.g. today) are read straight from the raw
files, so the latest data is always included. Every frame gets a UTC `time` column: exchange time
where the message has one, else our receive time.
"""
from pathlib import Path

import pandas as pd

from recorder.hl_to_parquet import OUT, RAW, convert_records, read_day

TABLES = ("book", "bbo", "trades", "ctx", "meta", "events")


def _utc(t):
    ts = pd.Timestamp(t)
    return ts.tz_localize("UTC") if ts.tzinfo is None else ts.tz_convert("UTC")


def _days(start, end, raw, out):
    have = {d.name for d in raw.iterdir() if d.is_dir()} if raw.exists() else set()
    for t in TABLES:
        if (out / t).exists():
            have |= {f.stem for f in (out / t).glob("*.parquet")}
    lo = _utc(start).strftime("%Y-%m-%d") if start is not None else ""
    hi = _utc(end).strftime("%Y-%m-%d") if end is not None else "9999"
    return sorted(d for d in have if lo <= d <= hi)


def load(table, symbol=None, start=None, end=None, dex="mkts", raw=None, out=None, depth=5):
    """Rows of one table. symbol: "US500", "mkts:US500" or a list. start/end: UTC (end exclusive)."""
    if table not in TABLES:
        raise ValueError(f"unknown table {table!r}; choose from {TABLES}")
    raw, out = Path(raw or RAW / dex), Path(out or OUT / dex)
    frames = []
    for day in _days(start, end, raw, out):
        f = out / table / f"{day}.parquet"
        if f.exists():
            frames.append(pd.read_parquet(f))
        elif (raw / day).exists() and not any((out / t / f"{day}.parquet").exists() for t in TABLES):
            rows = convert_records(read_day(raw, day), depth, only={table}).get(table, [])
            if rows:
                frames.append(pd.DataFrame(rows))
    if not frames:
        return pd.DataFrame()
    df = pd.concat(frames, ignore_index=True)
    if symbol is not None and "symbol" in df:
        wanted = [symbol] if isinstance(symbol, str) else list(symbol)
        wanted = {s if ":" in s else f"{dex}:{s}" for s in wanted}
        df = df[df["symbol"].isin(wanted)]
    ns = df["exch_ns"].fillna(df["rx_ns"]) if "exch_ns" in df else df["rx_ns"]
    df.insert(0, "time", pd.to_datetime(ns.astype("int64"), unit="ns", utc=True))
    if start is not None:
        df = df[df["time"] >= _utc(start)]
    if end is not None:
        df = df[df["time"] < _utc(end)]
    return df.sort_values(["time", "rx_ns"], kind="stable").reset_index(drop=True)


def mid_series(symbol, freq="1s", start=None, end=None, dex="mkts", max_stale="5min", **kw):
    """Best bid / ask / mid on a regular UTC grid (last quote carried forward).

    Grid points more than `max_stale` after the last quote are NaN, so gaps in the recording show
    up as gaps, not as a stale price.
    """
    df = load("bbo", symbol, start, end, dex, **kw)
    if df.empty:
        return df
    if df["symbol"].nunique() > 1:
        raise ValueError(f"{symbol!r} matched several markets: {sorted(df['symbol'].unique())}")
    q = df.set_index("time")[["bid_px", "ask_px"]]
    q = q[~q.index.duplicated(keep="last")]
    q["mid"] = (q["bid_px"] + q["ask_px"]) / 2
    grid = pd.date_range(q.index[0].ceil(freq), q.index[-1].floor(freq), freq=freq, name="time")
    both = q.index.union(grid)
    out = q.reindex(both).ffill().reindex(grid)
    if max_stale:
        last_quote = pd.Series(q.index, index=q.index).reindex(both).ffill().reindex(grid)
        out[(grid - last_quote) > pd.Timedelta(max_stale)] = float("nan")
    return out
