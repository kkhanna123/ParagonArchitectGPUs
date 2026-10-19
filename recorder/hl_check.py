"""Health and data-quality checks for the Hyperliquid recorder.

    python -m recorder.hl_check --status            # is it running, is every market updating?
    python -m recorder.hl_check                     # quality report for the last 24h
    python -m recorder.hl_check --dex xyz --start 2026-10-18 --end 2026-10-19
"""
import argparse
import json
from datetime import datetime, timezone

import pandas as pd

from recorder.hl_load import load
from recorder.hl_to_parquet import RAW


def _dur(s):
    return f"{s:.0f}s" if s < 120 else f"{s / 60:.1f}min" if s < 7200 else f"{s / 3600:.1f}h"


def status_text(dex, raw=RAW):
    path = raw / dex / "status.json"
    if not path.exists():
        return f"[{dex}] no status file at {path}. Has the recorder run?"
    st = json.loads(path.read_text())
    age = (datetime.now(timezone.utc) - datetime.fromisoformat(st["updated"])).total_seconds()
    state = "RUNNING" if age < 60 else f"NOT RUNNING (last heartbeat {_dur(age)} ago)"
    lines = [f"[{dex}] {state}   pid {st['pid']}   since {st['started']}   reconnects {st['reconnects']}",
             f"  {'market':<18}{'last msg':>9}{'latency':>9}  messages"]
    for coin, m in st["markets"].items():
        a, lat = m["seconds_since_update"], m["latency_ms_median"]
        msgs = ", ".join(f"{k}={v:,}" for k, v in sorted(m["msgs"].items()))
        lines.append(f"  {coin:<18}{(_dur(a) + ' ago') if a is not None else '-':>9}"
                     f"{(str(lat) + 'ms') if lat is not None else '-':>9}  {msgs}")
    if age < 60:
        lines += [f"  WARNING: {w}" for w in st["warnings"]]
    return "\n".join(lines)


def report(dex, start, end=None, gap=30, **kw):
    out = [f"[{dex}] data quality {start} -> {end or 'now'} (UTC)"]
    ctx = load("ctx", start=start, end=end, dex=dex, **kw)
    if ctx.empty:
        return "\n".join(out + ["  no data in this window"])
    out.append("  coverage (ctx updates ~1/s, so gaps here are gaps in the recording):")
    out.append(f"  {'market':<18}{'first':>21}{'last':>21}{'rows':>10}{'gaps>' + str(gap) + 's':>9}  longest")
    for sym, g in ctx.groupby("symbol"):
        t = g["time"]
        gaps = t.diff().dt.total_seconds().dropna()
        out.append(f"  {sym:<18}{t.iloc[0]:%Y-%m-%d %H:%M:%S}  {t.iloc[-1]:%Y-%m-%d %H:%M:%S}{len(g):>10,}"
                   f"{(gaps > gap).sum():>9}  {_dur(gaps.max()) if len(gaps) else '-'}")
    book = load("book", start=start, end=end, dex=dex, **kw)
    if not book.empty:
        bad = (book["bid_px_0"] >= book["ask_px_0"]).sum()
        out.append(f"  book: {len(book):,} rows, {bad:,} crossed/locked {'(OK)' if bad == 0 else '(INVESTIGATE)'}")
    trades = load("trades", start=start, end=end, dex=dex, **kw)
    if not trades.empty:
        dup = trades.duplicated(["symbol", "tid", "hash"]).sum()
        wallets = trades["buyer"].notna().mean()
        out.append(f"  trades: {len(trades):,} rows, {dup:,} duplicates, wallet addresses on {wallets:.0%}")
    bbo = load("bbo", start=start, end=end, dex=dex, **kw)
    if not bbo.empty:
        lat = (bbo["rx_ns"] - bbo["exch_ns"]).dropna() / 1e6
        out.append(f"  latency exchange->us: median {lat.median():.0f} ms, p95 {lat.quantile(.95):.0f} ms"
                   + ("  <- check this computer's clock" if not -500 < lat.median() < 2000 else ""))
    ev = load("events", start=start, end=end, dex=dex, **kw)
    if not ev.empty:
        counts = ev["event"].value_counts()
        out.append("  events: " + ", ".join(f"{k}={v}" for k, v in sorted(counts.items())))
        for _, r in ev[ev["event"] == "disconnected"].tail(3).iterrows():
            reason = json.loads(r["detail"]).get("reason", "")
            out.append(f"    {r['time']:%Y-%m-%d %H:%M:%S} disconnected: {reason[:90]}")
    return "\n".join(out)


def main(argv=None):
    p = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    p.add_argument("--status", action="store_true", help="show the live status of running recorders")
    p.add_argument("--dex", action="append", help="repeatable; default = every recorded dex")
    p.add_argument("--start", help="UTC; default = 24h ago")
    p.add_argument("--end")
    p.add_argument("--gap", type=float, default=30, help="report gaps longer than this many seconds")
    a = p.parse_args(argv)
    dexes = a.dex or (sorted(d.name for d in RAW.iterdir() if d.is_dir()) if RAW.exists() else [])
    if not dexes:
        print(f"nothing recorded yet under {RAW}")
    start = a.start or (pd.Timestamp.now(tz="UTC") - pd.Timedelta("24h")).strftime("%Y-%m-%d %H:%M")
    for dex in dexes:
        print(status_text(dex) if a.status else report(dex, start, a.end, a.gap))
        print()


if __name__ == "__main__":
    main()
