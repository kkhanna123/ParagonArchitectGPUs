import asyncio
import gzip
import json

import pyarrow.parquet as pq
import websockets

from recorder.ax_recorder import HourlyWriter, build_subscriptions, record
from recorder.ax_to_parquet import Book, convert_records, read_day, write_tables

# Shapes copied from real sandbox messages (2026-10-04).
L3 = {"t": "3", "ts": 1791124368, "tn": 420359141, "s": "XAG-PERP", "st": True,
      "b": [{"p": "60.46", "q": 166, "o": [7, 4, 155]}, {"p": "60.45", "q": 992, "o": [992]}],
      "a": [{"p": "60.50", "q": 270, "o": [270]}, {"p": "60.51", "q": 166, "o": [166]}]}
TICKER = {"t": "s", "ts": 1791124368, "tn": 420359141, "s": "XAG-PERP", "p": "60.46", "q": 3, "o": "60.47",
          "l": "54.81", "h": "89.41", "v": 612141, "oi": 12037, "i": "OPEN", "m": "60.46", "bp": "60.46",
          "ap": "60.50", "pl": "54.71", "pu": "66.85", "lsp": "60.78", "lst": 1790953200}
TRADE = {"t": "t", "p": "60.46", "q": 7, "s": "XAG-PERP", "d": "S", "ts": 1791124370, "tn": 260953556}
CANDLE = {"t": "c", "symbol": "XAG-PERP", "ts": 1791124370, "open": "60.46", "high": "60.46", "low": "60.46",
          "close": "60.46", "buy_volume": 0, "sell_volume": 7, "volume": 7, "width": "1s"}


def test_book_snapshot_then_diff():
    b = Book()
    b.apply(L3)
    b.apply({"t": "3", "s": "XAG-PERP", "st": False,
             "b": [{"p": "60.46", "q": 0}, {"p": "60.47", "q": 5, "o": [5]}], "a": []})
    bids, asks = b.top(5)
    assert [p for p, _ in bids] == ["60.47", "60.45"]
    assert asks[0] == ("60.50", (270, [270]))
    b.apply({**L3, "b": [], "a": []})  # a new snapshot replaces everything
    assert b.top(5) == ([], [])


def test_convert_records_tables():
    recs = [{"rx_ns": 1, "event": "connected", "url": "x"}] + [
        {"rx_ns": i + 2, "msg": m} for i, m in enumerate([L3, TICKER, TRADE, CANDLE, {"t": "h", "ts": 1, "tn": 0}])]
    t = convert_records(recs, depth=3)
    assert {k: len(v) for k, v in t.items()} == {"events": 1, "book": 1, "tickers": 1, "trades": 1, "candles": 1}
    row = t["book"][0]
    assert (row["bid_px_0"], row["bid_qty_0"], row["bid_n_0"]) == (60.46, 166, 3)
    assert row["ask_px_1"] == 60.51 and row["ask_px_2"] is None
    assert row["exch_ns"] == 1791124368_420359141
    assert t["trades"][0] == {"rx_ns": 4, "exch_ns": 1791124370_260953556, "symbol": "XAG-PERP",
                              "px": 60.46, "qty": 7, "side": "S"}
    assert t["tickers"][0]["mark_px"] == 60.46 and t["tickers"][0]["open_interest"] == 12037


def test_writer_roundtrip_and_parquet(tmp_path):
    w = HourlyWriter(tmp_path / "raw")
    ns = 1791124368_000000000  # 2026-10-04 14:32 UTC
    w.write_raw(ns, json.dumps(TRADE))
    w.write_raw(ns + 3600 * 10**9, json.dumps(TRADE))  # next hour -> new file
    w.close()
    files = sorted((tmp_path / "raw").rglob("*.jsonl.gz"))
    assert [f.relative_to(tmp_path / "raw").as_posix() for f in files] == ["2026-10-04/14.jsonl.gz",
                                                                            "2026-10-04/15.jsonl.gz"]
    # a crash can leave a truncated last line; the reader must skip it
    with gzip.open(files[1], "at") as fh:
        fh.write('{"rx_ns":5,"msg":{"t":"t"')
    recs = list(read_day(tmp_path / "raw", "2026-10-04"))
    assert len(recs) == 2 and recs[0]["msg"] == TRADE
    written = write_tables(convert_records(recs), tmp_path / "pq", "2026-10-04")
    assert pq.read_table(written["trades"][0]).num_rows == 2


def test_build_subscriptions_unique_rids():
    subs = build_subscriptions(["A", "B"], "LEVEL_3", ["1s"])
    assert [s["rid"] for s in subs] == [1, 2, 3, 4]
    assert subs[1] == {"type": "subscribe_candles", "symbol": "A", "width": "1s", "rid": 2}


def test_record_reconnects_and_resubscribes(tmp_path):
    connections = []

    async def server(ws):
        sub = json.loads(await ws.recv())
        connections.append(sub)
        await ws.send(json.dumps({"rid": sub["rid"], "result": {"subscribed": sub["symbol"]}}))
        await ws.send(json.dumps(TRADE))
        await ws.close()  # drop the client to force a reconnect

    async def go():
        stop = asyncio.Event()
        writer = HourlyWriter(tmp_path / "raw", flush_every=0)
        async with websockets.serve(server, "127.0.0.1", 0) as srv:
            port = srv.sockets[0].getsockname()[1]

            async def token():
                return "t"

            async def stop_after_two():
                while len(connections) < 2:
                    await asyncio.sleep(0.05)
                await asyncio.sleep(0.2)
                stop.set()

            await asyncio.wait_for(asyncio.gather(
                record(f"ws://127.0.0.1:{port}", token, build_subscriptions(["XAG-PERP"], "LEVEL_3", []),
                       writer, stop, max_backoff=0.1),
                stop_after_two()), timeout=10)
        writer.close()

    asyncio.run(go())
    assert len(connections) >= 2 and all(c["symbol"] == "XAG-PERP" for c in connections)
    recs = [json.loads(l) for f in sorted((tmp_path / "raw").rglob("*.gz")) for l in gzip.open(f, "rt")]
    events = [r["event"] for r in recs if "event" in r]
    assert events[:3] == ["connected", "disconnected", "connected"]
    assert sum(1 for r in recs if r.get("msg", {}).get("t") == "t") >= 2
