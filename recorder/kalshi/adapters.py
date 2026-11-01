"""What the Kalshi collector records. One Adapter = one family of markets.

To collect something new, add an Adapter to ADAPTERS (and to a group if it should be in the
permanent set). Nothing else changes: the recorder discovers each series' markets over REST,
subscribes to their books / trades / tickers, and rolls them on and off as they list and close;
the converter partitions by series, so the new data lands in parquet/kalshi/<table>/series=<S>
and shows up in kalshi.duckdb on the next catalog refresh.

Find series tickers with:  python -m recorder.kalshi.adapters --search "natural gas"
"""
import argparse
import re
from dataclasses import dataclass

CHANNELS = ("orderbook_delta", "ticker", "trade")


@dataclass(frozen=True)
class Adapter:
    name: str
    series: tuple
    description: str = ""
    # How often to look for newly listed markets. Rolling 15-minute series need it every
    # minute; daily and slower series list a day or more ahead, so ten minutes is plenty.
    refresh_s: float = 600.0
    # Also subscribe to markets that have not opened yet but will within this many seconds,
    # so the opening snapshot and the first trades are not lost to the refresh interval.
    lead_s: float = 0.0
    # Keep a market subscribed this long after its close, for the final ticks and settlement.
    linger_s: float = 600.0
    # Skip markets closing further out than this (None = everything listed). Some series list
    # contracts years ahead that never trade.
    max_days_out: float | None = None
    channels: tuple = CHANNELS
    # Backfill candlestick interval in minutes (1, 60 or 1440); 0 disables candle backfill.
    candle_period: int = 1


ADAPTERS = {a.name: a for a in [
    Adapter("commodities_15m",
            ("KXWTI15M", "KXBRENT15M", "KXNATGAS15M", "KXGOLD15M", "KXSILVER15M",
             "KXPLATINUM15M", "KXPALLADIUM15M", "KXCOPPER15M", "KXCOFFEE15M"),
            "15-minute up/down markets on every commodity Kalshi lists one for",
            refresh_s=60.0, lead_s=180.0, linger_s=300.0),
    Adapter("commodities_eod",
            ("KXWTI", "KXBRENTD", "KXNATGASD", "KXGOLDD", "KXSILVERD", "KXCOPPERD", "KXCOFFEED"),
            "daily commodity price ladders; their settlement value is the end-of-day price"),
    Adapter("btc_15m", ("KXBTC15M",), "BTC 15-minute up/down",
            refresh_s=60.0, lead_s=180.0, linger_s=300.0),
    Adapter("weather_high",
            ("KXHIGHCHI", "KXHIGHAUS", "KXHIGHTSEA", "KXHIGHTMIN", "KXHIGHTSATX", "KXHIGHMIA"),
            "daily high temperature: Chicago, Austin, Seattle, Minneapolis, San Antonio, Miami"),
    Adapter("weather_low",
            ("KXLOWTCHI", "KXLOWTAUS", "KXLOWTSEA", "KXLOWTMIN", "KXLOWTSATX", "KXLOWTMIA"),
            "daily low temperature: Chicago, Austin, Seattle, Minneapolis, San Antonio, Miami"),
    Adapter("fed", ("KXFED", "KXFEDDECISION"),
            "fed funds target rate per FOMC meeting, and the meeting decision (hike/hold/cut)",
            candle_period=60),
    Adapter("gas_us", ("KXAAAGASD", "KXAAAGASW", "KXAAAGASM"),
            "US average gas price (AAA): daily, weekly and monthly"),
]}

GROUPS = {
    "all": tuple(ADAPTERS),
    "commodities": ("commodities_15m", "commodities_eod"),
    "weather": ("weather_high", "weather_low"),
    "fast": ("commodities_15m", "btc_15m"),
}


def select(names) -> list:
    """Adapters for a mix of adapter and group names, de-duplicated, in registry order."""
    want = set()
    for n in names:
        if n in GROUPS:
            want.update(GROUPS[n])
        elif n in ADAPTERS:
            want.add(n)
        else:
            raise KeyError("unknown adapter or group %r (have %s)"
                           % (n, ", ".join(sorted(ADAPTERS) + sorted(GROUPS))))
    return [a for a in ADAPTERS.values() if a.name in want]


def series_index(adapters=None) -> dict:
    """series ticker -> adapter name."""
    return {s: a.name for a in (adapters or ADAPTERS.values()) for s in a.series}


def main(argv=None):
    p = argparse.ArgumentParser(description=__doc__,
                                formatter_class=argparse.RawDescriptionHelpFormatter)
    p.add_argument("--search", help="regex over every Kalshi series' ticker, title, category")
    a = p.parse_args(argv)
    if not a.search:
        for ad in ADAPTERS.values():
            print("%-16s %s\n%16s %s" % (ad.name, ad.description, "", " ".join(ad.series)))
        return
    from recorder.kalshi.common import Rest
    rx = re.compile(a.search, re.I)
    rest = Rest()
    series = rest.get("/series", limit=10000).get("series") or []
    for s in sorted(series, key=lambda s: s["ticker"]):
        text = " ".join(str(s.get(k) or "") for k in ("ticker", "title", "category"))
        if rx.search(text):
            print("%-22s %-11s %-20s %s" % (s["ticker"], s.get("frequency"), s.get("category"),
                                            s.get("title")))


if __name__ == "__main__":
    main()
