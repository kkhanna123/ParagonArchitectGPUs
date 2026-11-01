"""Tests for the Kalshi collector: adapters, credentials, subscription manager, converter,
backfill rows and catalog. Message shapes are copied from wire traffic captured 2026-10-31."""
import asyncio
import json
from collections import Counter

import duckdb
import pyarrow as pa
import pyarrow.parquet as pq
import pytest

from recorder.hl_backfill import merge_write
from recorder.hl_xyz_recorder import HourlyWriter
from recorder.kalshi import adapters as kxa
from recorder.kalshi import db as kx_db
from recorder.kalshi.backfill import CANDLE_SCHEMA, candle_row, hist_row
from recorder.kalshi.common import load_credentials, market_meta, series_dirs, series_of
from recorder.kalshi.recorder import Feed, Universe
from recorder.kalshi.to_parquet import DayConverter, _levels, convert_day

T = "KXHIGHCHI-26NOV01-T71"
NS = 1793476350_000000000  # 2026-10-31 19:52 UTC

SNAP = {"type": "orderbook_snapshot", "sid": 1, "seq": 1, "msg": {
    "market_ticker": T, "market_id": "x", "clock": 1,
    "yes_dollars_fp": [["0.4000", "10.00"], ["0.4200", "5.00"]],
    "no_dollars_fp": [["0.5000", "7.00"], ["0.5500", "3.50"]]}, "sending_ts_ms": 1793476350827}


def delta(seq, px, d, side="yes", ts=1793476352039):
    return {"type": "orderbook_delta", "sid": 1, "seq": seq, "msg": {
        "market_ticker": T, "price_dollars": px, "delta_fp": d, "side": side,
        "ts_ms": ts}, "sending_ts_ms": ts + 9}


TRADE = {"type": "trade", "sid": 3, "seq": 1, "msg": {
    "trade_id": "0727dd99", "market_ticker": T, "yes_price_dollars": "0.4200",
    "no_price_dollars": "0.5800", "count_fp": "14.63", "taker_side": "yes",
    "is_block_trade": False, "ts": 1793476505, "ts_ms": 1793476505857}}
TICK = {"type": "ticker", "sid": 2, "msg": {
    "market_ticker": T, "price_dollars": "0.4200", "yes_bid_dollars": "0.4200",
    "yes_ask_dollars": "0.4500", "volume_fp": "100.00", "open_interest_fp": "50.00",
    "dollar_volume": 42, "dollar_open_interest": 21, "yes_bid_size_fp": "5.00",
    "yes_ask_size_fp": "3.50", "ts_ms": 1793476506008}}
MARKET = {"ticker": T, "event_ticker": "KXHIGHCHI-26NOV01", "status": "active",
          "title": "Highest temperature in Chicago?", "strike_type": "greater",
          "floor_strike": 71, "open_time": "2026-10-30T14:00:00Z",
          "close_time": "2026-11-02T07:00:00Z", "yes_bid_dollars": "0.42",
          "rules_primary": "If the highest temperature ..."}


# ---- adapters ----------------------------------------------------------------------------

def test_select_groups_and_names():
    names = [a.name for a in kxa.select(["weather", "btc_15m"])]
    assert names == ["btc_15m", "weather_high", "weather_low"]
    assert len(kxa.select(["all"])) == len(kxa.ADAPTERS)
    with pytest.raises(KeyError):
        kxa.select(["nope"])


def test_every_series_is_claimed_once_and_partition_safe():
    seen = [s for a in kxa.ADAPTERS.values() for s in a.series]
    assert len(seen) == len(set(seen))
    for s in seen:
        assert series_dirs(s + "-26NOV01-T1") == ("series=%s" % s,)


def test_requested_markets_are_covered():
    idx = kxa.series_index()
    for s in ("KXBTC15M", "KXWTI15M", "KXGOLD15M", "KXNATGAS15M", "KXWTI", "KXGOLDD",
              "KXFED", "KXAAAGASD"):
        assert s in idx
    for city in ("CHI", "AUS", "SEA", "MIN", "SATX", "MIA"):
        assert any(city in s and "HIGH" in s for s in idx)
        assert any(city in s and "LOW" in s for s in idx)


def test_series_of_and_unsafe_names():
    assert series_of(T) == "KXHIGHCHI"
    with pytest.raises(ValueError):
        series_dirs("../x-1")


# ---- credentials -------------------------------------------------------------------------

def test_load_credentials_finds_key_beside_env_file(tmp_path, monkeypatch):
    from cryptography.hazmat.primitives import serialization
    from cryptography.hazmat.primitives.asymmetric import rsa
    key = rsa.generate_private_key(public_exponent=65537, key_size=2048)
    (tmp_path / "k.pem").write_bytes(key.private_bytes(
        serialization.Encoding.PEM, serialization.PrivateFormat.PKCS8,
        serialization.NoEncryption()))
    env = tmp_path / ".env"
    env.write_text("KALSHI_API_KEY_ID=abc\nKALSHI_PRIVATE_KEY_PATH=/work/k.pem\n")
    for k in ("KALSHI_API_KEY_ID", "KALSHI_PRIVATE_KEY_PATH", "KALSHI_PRIVATE_KEY_PEM"):
        monkeypatch.delenv(k, raising=False)
    monkeypatch.setenv("KALSHI_ENV_FILE", str(env))
    c = load_credentials()
    h = c.headers("GET", "/trade-api/ws/v2")
    assert c.key_id == "abc" and h["KALSHI-ACCESS-KEY"] == "abc"
    assert len(h["KALSHI-ACCESS-SIGNATURE"]) > 100


def test_no_credentials_is_none(monkeypatch):
    for k in ("KALSHI_API_KEY_ID", "KALSHI_ENV_FILE"):
        monkeypatch.delenv(k, raising=False)
    assert load_credentials() is None


# ---- recorder ----------------------------------------------------------------------------

class FakeWS:
    def __init__(self):
        self.sent = []

    async def send(self, s):
        self.sent.append(json.loads(s))


def make_feed(tmp_path, tickers, close_ms=None):
    w = HourlyWriter(tmp_path / "raw")
    u = Universe([], rest=None, writer=w)
    for t in tickers:
        u.close_ms[t], u.channels[t], u._linger_ms[t] = close_ms, ("orderbook_delta",), 0
    f = Feed(u, w, creds=None)
    f.ws = FakeWS()
    return u, f


def test_feed_subscribes_then_adds_and_drops(tmp_path):
    u, f = make_feed(tmp_path, ["A-1", "B-1"])
    asyncio.run(f.sync())
    assert f.ws.sent[-1]["cmd"] == "subscribe"
    assert f.ws.sent[-1]["params"]["market_tickers"] == ["A-1", "B-1"]
    # nothing more goes out until the sid is known
    u.close_ms["C-1"], u.channels["C-1"], u._linger_ms["C-1"] = None, ("orderbook_delta",), 0
    asyncio.run(f.sync())
    assert len(f.ws.sent) == 1
    assert f.handle(json.dumps({"type": "subscribed", "id": 1,
                                "msg": {"channel": "orderbook_delta", "sid": 7}}), Counter())
    asyncio.run(f.sync())
    assert f.ws.sent[-1] == {"id": 2, "cmd": "update_subscription", "params": {
        "sids": [7], "market_tickers": ["C-1"], "action": "add_markets"}}
    del u.close_ms["A-1"]
    asyncio.run(f.sync())
    assert f.ws.sent[-1]["params"]["action"] == "delete_markets"
    assert f.ws.sent[-1]["params"]["market_tickers"] == ["A-1"]


def test_feed_seq_gap_forces_reconnect(tmp_path):
    _, f = make_feed(tmp_path, ["A-1"])
    f.handle(json.dumps(delta(1, "0.4", "1")), Counter())
    f.handle(json.dumps(delta(2, "0.4", "1")), Counter())
    with pytest.raises(ConnectionError):
        f.handle(json.dumps(delta(4, "0.4", "1")), Counter())


def test_universe_lingers_after_close(tmp_path):
    u, _ = make_feed(tmp_path, ["A-1"], close_ms=1000)
    u._linger_ms["A-1"] = 500
    assert u.wanted(now_ms=1400)["orderbook_delta"] == {"A-1"}
    assert u.wanted(now_ms=1600)["orderbook_delta"] == set()


def test_market_meta_drops_moving_fields():
    meta = market_meta(MARKET)
    assert "yes_bid_dollars" not in meta and meta["floor_strike"] == 71


# ---- converter ---------------------------------------------------------------------------

def test_levels_all_three_schemas():
    assert _levels({"yes_dollars_fp": [["0.4200", "5.50"]]}, "yes") == [(42_000_000, 550_000_000)]
    assert _levels({"yes_dollars": [["0.42", 5]]}, "yes") == [(42_000_000, 500_000_000)]
    assert _levels({"yes": [[42, 5]]}, "yes") == [(42_000_000, 500_000_000)]
    assert _levels({}, "no") == []


def write_day(tmp_path, records):
    w = HourlyWriter(tmp_path / "raw")
    w.write_event("connected", url="x")
    for i, (rl, m) in enumerate(records):
        w.write_raw(NS + i, 0, rl, json.dumps(m))
    w.close()
    return tmp_path / "raw"


def test_convert_day_books_trades_ticker_markets(tmp_path):
    recs = [("ws", SNAP), ("ws", delta(2, "0.4300", "2.00")),        # new best yes bid
            ("ws", delta(3, "0.4300", "-2.00")),                     # and gone again
            ("ws", delta(4, "0.3000", "1.00")),                      # a third yes level
            ("ws", TRADE), ("ws", TRADE),                            # replayed trade
            ("ws", TICK), ("ws", TICK),                              # unchanged ticker
            ("rest", {"type": "markets", "series": "KXHIGHCHI", "markets": [market_meta(MARKET)]}),
            ("rest", {"type": "markets", "series": "KXHIGHCHI", "markets": [market_meta(MARKET)]})]
    raw = write_day(tmp_path, recs)
    day = next(raw.iterdir()).name
    s = convert_day(raw, tmp_path / "pq", day)
    assert s["rows"]["book_delta"] == 4 + 3
    assert s["rows"]["trades"] == 1 and s["dups"]["trades"] == 1
    assert s["rows"]["ticker"] == 1 and s["rows"]["markets"] == 1
    book = pq.read_table(tmp_path / "pq/book" / ("date=%s" % day) / "series=KXHIGHCHI"
                         / "part.parquet").to_pylist()
    assert [r["yes_px_0"] for r in book] == [42_000_000, 43_000_000, 42_000_000, 42_000_000]
    assert book[0]["no_px_0"] == 55_000_000 and book[0]["no_sz_0"] == 350_000_000
    assert book[-1]["yes_px_2"] == 30_000_000


def test_catalog_views(tmp_path, monkeypatch):
    raw = write_day(tmp_path, [("ws", SNAP), ("ws", TRADE)])
    day = next(raw.iterdir()).name
    root = tmp_path / "root"
    convert_day(raw, root / "parquet/kalshi", day)
    hist = root / "parquet/kalshi_hist/markets/series=KXHIGHCHI"
    hist.mkdir(parents=True)
    from recorder.kalshi.backfill import HIST_MARKETS_SCHEMA
    rows = [hist_row({**MARKET, "ticker": T + str(i), "status": "finalized",
                      "expiration_value": "57.00", "volume_fp": "10"}) for i in range(3)]
    merge_write(rows, HIST_MARKETS_SCHEMA, hist / "part.parquet", "ticker")
    con = duckdb.connect()
    made = kx_db.create_views(con, root)
    assert {"book", "book_top", "trades", "settlements", "outcomes", "adapters"} <= set(made)
    bid, ask, adapter = con.execute("select yes_bid, yes_ask, adapter from book_top").fetchone()
    assert (bid, round(ask, 4), adapter) == (0.42, 0.45, "weather_high")
    assert con.execute("select count, yes_px from trades").fetchone() == (14.63, 0.42)
    assert con.execute("select value, n_markets from outcomes").fetchone() == (57.0, 3)


def test_connected_event_resets_books(tmp_path):
    conv = DayConverter(tmp_path / "pq", "2026-10-31")
    conv._record({"rx_ns": NS, "sh": 0, "rl": "ws", "msg": SNAP})
    assert T in conv.books
    conv._record({"rx_ns": NS + 1, "event": "connected"})
    assert conv.books == {}
    conv.close()


# ---- backfill rows -----------------------------------------------------------------------

def test_candle_row_and_composite_merge(tmp_path):
    c = {"end_period_ts": 1793393160, "open_interest_fp": "22573.80", "volume_fp": "37176.37",
         "price": {"close_dollars": "0.5300", "high_dollars": "0.6000", "low_dollars": "0.4800",
                   "mean_dollars": "0.5402", "open_dollars": "0.4900"},
         "yes_ask": {"close_dollars": "0.5400"}, "yes_bid": {"close_dollars": "0.5300"}}
    r = candle_row("X-1", c)
    assert r["end_ms"] == 1793393160000 and r["close"] == 53_000_000 and r["yes_ask_close"] == 54_000_000
    p = tmp_path / "c.parquet"
    merge_write([r, {**r, "ticker": "X-2"}], CANDLE_SCHEMA, p, ("ticker", "end_ms"))
    total, added = merge_write([{**r, "close": 1}], CANDLE_SCHEMA, p, ("ticker", "end_ms"))
    assert (total, added) == (2, 0)
    got = {x["ticker"]: x["close"] for x in pq.read_table(p).to_pylist()}
    assert got == {"X-1": 1, "X-2": 53_000_000}


def test_hist_row_keeps_text_outcomes():
    r = hist_row({"ticker": "KXFEDDECISION-26OCT-H26", "expiration_value": "Fed maintains rate",
                  "settlement_value_dollars": "0.0000", "volume_fp": "5.00"})
    assert r["expiration_value"] == "Fed maintains rate" and r["settlement_value"] == 0
    assert r["volume"] == 500_000_000
