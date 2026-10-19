import asyncio
import gzip
import json
import os
import random
import signal
import threading
import time
import zlib
from datetime import datetime, timezone
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

import pyarrow.parquet as pq
import pytest
import websockets

from recorder import hl_recorder
from recorder.ax_recorder import HourlyWriter
from recorder.hl_check import report, status_text
from recorder.hl_load import load, mid_series
from recorder.hl_to_parquet import convert_day, convert_records, read_day

# Shapes copied from real Hyperliquid messages (mkts dex, 2026-10-11).
BOOK = {"channel": "l2Book", "data": {"coin": "mkts:USTECH", "time": 1791760000123, "levels": [
    [{"px": "753.63", "sz": "38.595", "n": 16}, {"px": "753.51", "sz": "0.881", "n": 2}],
    [{"px": "753.74", "sz": "0.632", "n": 1}, {"px": "753.83", "sz": "0.239", "n": 1}]]}}
BBO = {"channel": "bbo", "data": {"coin": "mkts:US500", "time": 1791760000200,
                                  "bbo": [{"px": "779.87", "sz": "3.1", "n": 2}, {"px": "780.0", "sz": "0.6", "n": 1}]}}
TRADES = {"channel": "trades", "data": [{"coin": "mkts:US500", "side": "B", "px": "779.99", "sz": "0.25",
                                         "time": 1791760000300, "hash": "0xabc", "tid": 55,
                                         "users": ["0xbuyer", "0xseller"]}]}
CTX = {"channel": "activeAssetCtx", "data": {"coin": "mkts:US500", "ctx": {
    "markPx": "779.99", "oraclePx": "780.17", "midPx": "779.93", "funding": "0.0000057078", "openInterest": "2561.05",
    "premium": "-0.0002", "dayNtlVlm": "183814.2", "prevDayPx": "777.1", "impactPxs": ["779.8", "780.1"]}}}


def snapshot(dex, live, dead=()):
    uni = [{"name": f"{dex}:{c}", "szDecimals": 3, "maxLeverage": 25} for c in live]
    uni += [{"name": f"{dex}:{c}", "szDecimals": 2, "maxLeverage": 10, "isDelisted": True} for c in dead]
    return [{"universe": uni}, [CTX["data"]["ctx"]] * len(uni)]


# ---------------------------------------------------------------- conversion
def test_convert_records_tables():
    recs = [{"rx_ns": 1, "event": "connected", "conn": 0},
            {"rx_ns": 2, "msg": {"channel": "metaAndAssetCtxs", "dex": "mkts", "data": snapshot("mkts", ["US500"], ["GOLD"])}},
            {"rx_ns": 3, "msg": BOOK}, {"rx_ns": 4, "msg": BBO}, {"rx_ns": 5, "msg": TRADES},
            {"rx_ns": 6, "msg": TRADES},  # the same trade re-sent after a resubscribe
            {"rx_ns": 7, "msg": CTX}, {"rx_ns": 8, "msg": {"channel": "pong"}},
            {"rx_ns": 9, "msg": {"channel": "error", "data": "Invalid subscription"}}]
    t = convert_records(recs, depth=3)
    assert {k: len(v) for k, v in t.items()} == {"events": 2, "meta": 2, "book": 1, "bbo": 1, "trades": 1, "ctx": 1}
    b = t["book"][0]
    assert (b["bid_px_0"], b["bid_qty_0"], b["bid_n_0"]) == (753.63, 38.595, 16)
    assert b["ask_px_1"] == 753.83 and b["ask_px_2"] is None and b["exch_ns"] == 1791760000123_000000
    assert t["trades"][0] == {"rx_ns": 5, "exch_ns": 1791760000300_000000, "symbol": "mkts:US500", "px": 779.99,
                              "qty": 0.25, "side": "B", "tid": 55, "hash": "0xabc",
                              "buyer": "0xbuyer", "seller": "0xseller"}
    assert t["ctx"][0]["funding"] == 0.0000057078 and t["ctx"][0]["impact_ask_px"] == 780.1
    assert [m["live"] for m in t["meta"]] == [True, False]
    assert t["events"][1]["event"] == "exchange_error"


def test_reader_survives_a_crash_mid_file(tmp_path):
    ns = 1791760000_000000000  # 2026-10-11 23:06 UTC
    w = HourlyWriter(tmp_path, flush_every=0)
    for i in range(3):
        w.write_raw(ns + i, json.dumps(TRADES))
    w._fh.flush()
    f = w._path
    crashed = f.read_bytes()  # what is on disk if the power goes now: a gzip member with no end
    w.close()
    f.write_bytes(crashed)
    w = HourlyWriter(tmp_path)  # the recorder restarts in the same hour and appends a new member
    w.write_raw(ns + 10, json.dumps(BBO))
    w.close()
    with pytest.raises((EOFError, zlib.error, gzip.BadGzipFile)):
        list(gzip.open(f, "rt"))  # the standard reader fails on the whole hour
    recs = list(read_day(tmp_path, "2026-10-11"))
    assert [r["rx_ns"] - ns for r in recs] == [0, 1, 2, 10]


# ---------------------------------------------------------------- fake Hyperliquid
class FakeHL:
    """REST + websocket server that answers like api.hyperliquid.xyz for the parts we use."""

    def __init__(self):
        self.live, self.dead = ["US500", "USTECH"], ["GOLD"]
        self.drop_first_after = None
        self.rest_failures = 0
        self.connections = 0
        self.subs = []
        self.tid = 1000

    def rest(self, body):
        if body["type"] == "perpDexs":
            return [None, {"name": "xyz", "fullName": "XYZ"}, {"name": "km", "fullName": "Markets by Kinetiq"},
                    {"name": "mkts", "fullName": "Markets By Kinetiq"}]
        if self.rest_failures > 0:
            self.rest_failures -= 1
            raise RuntimeError("simulated outage")
        if body["dex"] == "km":
            return snapshot("km", [], ["OLD"])
        return snapshot(body["dex"], self.live, self.dead)

    def message(self, sub, resend=False):
        coin, now, px = sub["coin"], int(time.time() * 1000), 780 + random.random()
        if sub["type"] == "l2Book":
            lv = lambda s: [{"px": f"{px + s * 0.1 * i:.2f}", "sz": "1.5", "n": i} for i in range(1, 21)]
            return {"coin": coin, "time": now, "levels": [lv(-1), lv(1)]}
        if sub["type"] == "bbo":
            return {"coin": coin, "time": now, "bbo": [{"px": f"{px - .06:.2f}", "sz": "3", "n": 2},
                                                       {"px": f"{px + .06:.2f}", "sz": "1", "n": 1}]}
        if sub["type"] == "trades":
            if not resend:
                self.tid += 1
            return [{"coin": coin, "side": "A", "px": f"{px:.2f}", "sz": "0.1", "time": now, "hash": f"0x{coin}",
                     "tid": 1 if resend else self.tid, "users": ["0xb", "0xs"]}]
        return {"coin": coin, "ctx": CTX["data"]["ctx"]}

    async def ws(self, ws):
        self.connections += 1
        n, start, subs = self.connections, time.time(), []
        try:
            while True:
                try:
                    m = json.loads(await asyncio.wait_for(ws.recv(), 0.05))
                    if m["method"] == "subscribe":
                        s = m["subscription"]
                        subs.append(s)
                        self.subs.append(s)
                        await ws.send(json.dumps({"channel": "subscriptionResponse", "data": m}))
                        await ws.send(json.dumps({"channel": s["type"], "data": self.message(s, resend=True)}))
                    elif m["method"] == "ping":
                        await ws.send('{"channel":"pong"}')
                except asyncio.TimeoutError:
                    pass
                for s in subs:
                    await ws.send(json.dumps({"channel": s["type"], "data": self.message(s)}))
                if n == 1 and self.drop_first_after and time.time() - start > self.drop_first_after:
                    await ws.close(code=1011, reason="simulated drop")
                    return
        except websockets.ConnectionClosed:
            pass

    def start(self):
        fake = self

        class H(BaseHTTPRequestHandler):
            def log_message(self, *a):
                pass

            def do_POST(self):
                try:
                    body, code = json.dumps(fake.rest(json.loads(self.rfile.read(int(self.headers["Content-Length"]))))), 200
                except Exception as e:  # noqa: BLE001
                    body, code = str(e), 500
                self.send_response(code)
                self.end_headers()
                self.wfile.write(body.encode())

        self.http = ThreadingHTTPServer(("127.0.0.1", 0), H)
        threading.Thread(target=self.http.serve_forever, daemon=True).start()
        self.api_url = f"http://127.0.0.1:{self.http.server_address[1]}/info"
        ready = threading.Event()

        def serve():
            async def go():
                srv = await websockets.serve(self.ws, "127.0.0.1", 0)
                self.ws_url = f"ws://127.0.0.1:{srv.sockets[0].getsockname()[1]}"
                ready.set()
                await asyncio.Future()
            self.loop = asyncio.new_event_loop()
            try:
                self.loop.run_until_complete(go())
            except RuntimeError:
                pass

        threading.Thread(target=serve, daemon=True).start()
        ready.wait(5)
        return self


@pytest.fixture
def fake(monkeypatch):
    for k in ("NO_PROXY", "no_proxy"):
        monkeypatch.setenv(k, "127.0.0.1")
    monkeypatch.setattr(hl_recorder, "RETRY_DELAY", 0.05)
    monkeypatch.setattr(hl_recorder, "RESTART_BACKOFF", 0.2)
    f = FakeHL().start()
    yield f
    f.http.shutdown()
    f.loop.call_soon_threadsafe(f.loop.stop)


@pytest.fixture
def args(fake, tmp_path):
    def make(**kw):
        a = hl_recorder.parse_args(["--api-url", fake.api_url, "--ws-url", fake.ws_url,
                                    "--out", str(tmp_path / "raw"), "--parquet-out", str(tmp_path / "pq"),
                                    "--snapshot-every", "0.5", "--status-every", "0.3", "--duration", "3"])
        for k, v in kw.items():
            setattr(a, k, v)
        return a
    return make


def raw_records(tmp_path):
    return [r for d in sorted((tmp_path / "raw").iterdir()) if d.is_dir() for r in read_day(tmp_path / "raw", d.name)]


# ---------------------------------------------------------------- recorder
def test_auto_dex_skips_empty_km(fake):
    async def go():
        import aiohttp
        async with aiohttp.ClientSession() as s:
            return await hl_recorder.resolve_dex(s, fake.api_url, "auto")
    assert asyncio.run(go()) == "mkts"


def test_end_to_end(args, fake, tmp_path):
    fake.drop_first_after = 1.0
    hl_recorder.run(args(duration=4))
    recs = raw_records(tmp_path)
    events = [r["event"] for r in recs if "event" in r]
    assert events[0] == "markets_changed" and "started" in events and events[-1] == "stopped"
    assert events.count("connected") >= 2 and "disconnected" in events

    # raw keeps everything: all 20 book levels and the REST snapshots
    books = [r["msg"]["data"] for r in recs if r.get("msg", {}).get("channel") == "l2Book"]
    assert len(books[0]["levels"][0]) == 20
    assert any(r.get("msg", {}).get("channel") == "metaAndAssetCtxs" for r in recs)

    day = datetime.now(timezone.utc).strftime("%Y-%m-%d")
    written = convert_day(tmp_path / "raw", tmp_path / "pq", day, depth=5)
    book = pq.read_table(written["book"][0]).to_pandas()
    assert "bid_px_4" in book and "bid_px_5" not in book
    assert set(book["symbol"]) == {"mkts:US500", "mkts:USTECH"}  # delisted GOLD not subscribed
    trades = pq.read_table(written["trades"][0]).to_pandas()
    assert not trades.duplicated(["symbol", "tid", "hash"]).any() and (trades["tid"] == 1).sum() == 2
    assert set(trades["buyer"]) == {"0xb"}

    st = json.loads((tmp_path / "raw" / "status.json").read_text())
    assert st["dex"] == "mkts" and set(st["markets"]) == {"mkts:US500", "mkts:USTECH"}
    for p in (tmp_path / "raw").rglob("*"):  # Windows-safe names
        assert not set(p.name) & set(':*?"<>|'), p


def test_coins_and_channels(args, fake, tmp_path):
    hl_recorder.run(args(coins="US500,NOPE", channels="bbo,trades", duration=2))
    assert {s["coin"] for s in fake.subs} == {"mkts:US500"}
    assert {s["type"] for s in fake.subs} == {"bbo", "trades"}


def test_new_listing_is_picked_up(args, fake, tmp_path):
    threading.Timer(1.2, lambda: fake.live.append("SMALL2000")).start()
    hl_recorder.run(args(duration=4))
    changed = [r for r in raw_records(tmp_path) if r.get("event") == "markets_changed"]
    assert changed[-1]["added"] == ["mkts:SMALL2000"]
    assert any(s["coin"] == "mkts:SMALL2000" for s in fake.subs)


def test_restarts_after_outage(args, fake, tmp_path):
    fake.rest_failures = 6  # more than one start-up's worth of retries
    hl_recorder.run(args(duration=5))
    assert any(r.get("msg", {}).get("channel") == "bbo" for r in raw_records(tmp_path))


def test_ctrl_c_stops_cleanly(args, fake, tmp_path):
    threading.Timer(1.5, lambda: os.kill(os.getpid(), signal.SIGINT)).start()
    hl_recorder.run(args(duration=30))
    events = [r["event"] for r in raw_records(tmp_path) if "event" in r]
    assert events[-1] == "stopped"
    assert signal.getsignal(signal.SIGINT) is signal.default_int_handler


def test_one_recorder_per_folder(args, tmp_path):
    held = hl_recorder.FolderLock()
    assert held(tmp_path / "raw")
    try:
        with pytest.raises(SystemExit, match="already writing"):
            hl_recorder.run(args(duration=2))
    finally:
        held.release()


def test_finished_days_are_converted_automatically(args, fake, tmp_path):
    w = HourlyWriter(tmp_path / "raw")
    w.write_raw(1791760000_000000000, json.dumps(BBO))  # 2026-10-11
    w.close()
    hl_recorder.run(args(duration=2))
    assert pq.read_table(tmp_path / "pq" / "bbo" / "2026-10-11.parquet").num_rows == 1


# ---------------------------------------------------------------- loading and checks
def test_load_mid_series_and_check(args, fake, tmp_path):
    hl_recorder.run(args(duration=3))
    kw = {"raw": tmp_path / "raw", "out": tmp_path / "pq"}
    bbo = load("bbo", "US500", **kw)  # today: read straight from the raw files
    assert len(bbo) > 5 and set(bbo["symbol"]) == {"mkts:US500"} and bbo["time"].is_monotonic_increasing
    px = mid_series("US500", freq="100ms", **kw)
    assert px["mid"].notna().all() and (px["ask_px"] > px["bid_px"]).all()
    text = report("mkts", "2000-01-01", **kw)
    assert "mkts:US500" in text and "0 crossed/locked (OK)" in text and "0 duplicates" in text


def test_status_text(tmp_path):
    (tmp_path / "mkts").mkdir()
    st = {"updated": "2026-10-11T13:00:00+00:00", "started": "2026-10-11T12:00:00+00:00", "pid": 1, "dex": "mkts",
          "reconnects": 0, "connections": {}, "warnings": [],
          "markets": {"mkts:US500": {"seconds_since_update": 1.0, "msgs": {"bbo": 5}, "latency_ms_median": 80}}}
    (tmp_path / "mkts" / "status.json").write_text(json.dumps(st))
    text = status_text("mkts", raw=tmp_path)
    assert "NOT RUNNING" in text and "mkts:US500" in text and "80ms" in text


def test_mid_series_blanks_stale_stretches(tmp_path):
    w = HourlyWriter(tmp_path / "raw")
    t0 = 1791760000_000000000
    for dt_s, px in ((0, 100), (1.5, 101), (10, 102)):
        m = json.loads(json.dumps(BBO))
        m["data"]["time"] = (t0 + int(dt_s * 1e9)) // 1_000_000
        m["data"]["bbo"] = [{"px": str(px - .5), "sz": "1", "n": 1}, {"px": str(px + .5), "sz": "1", "n": 1}]
        w.write_raw(t0 + int(dt_s * 1e9), json.dumps(m))
    w.close()
    s = mid_series("US500", freq="1s", max_stale="3s", raw=tmp_path / "raw", out=tmp_path / "pq")
    assert list(s["mid"].iloc[:3]) == [100, 100, 101]   # carried forward until the next quote
    assert s["mid"].isna().sum() > 0 and s["mid"].iloc[-1] == 102
