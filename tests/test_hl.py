"""Tests for the Hyperliquid / trade[XYZ] collector, converter and local catalog.

Message shapes are copied from real wire traffic captured on 2026-10-04.
"""
import asyncio
import gzip
import json

import duckdb
import pyarrow as pa
import pyarrow.parquet as pq
import pytest
import websockets

from recorder import hl_db
from recorder.hl_backfill import (existing_max, merge_write, schema_matches, CANDLE_SCHEMA,
                                  FUNDING_SCHEMA)
from recorder.hl_common import (ROLE_BOOK, ROLE_BOOK5, ROLE_OTHER, assign_tiers,
                                build_subscriptions, shard, shard_by_role, to_scaled,
                                MAX_SUBSCRIPTIONS)
from recorder.hl_xyz_recorder import HourlyWriter, _channel_of, run_shard
from recorder.hl_xyz_to_parquet import convert_day, read_hour

BOOK = {"channel": "l2Book", "data": {"coin": "xyz:CL", "time": 1791128152148, "levels": [
    [{"px": "91.155", "sz": "19.685", "n": 2}, {"px": "91.154", "sz": "29.088", "n": 4}],
    [{"px": "91.162", "sz": "10.706", "n": 1}, {"px": "91.163", "sz": "7.272", "n": 2}]]}}
TRADES = {"channel": "trades", "data": [
    {"coin": "xyz:CL", "side": "B", "px": "91.154", "sz": "1.0", "time": 1791127949553,
     "hash": "0xd380f1", "tid": 1081293533429111,
     "users": ["0x72ce32e1a3b8d736905706a5e0866d20bbd12cfa",
               "0x4e60e3a4a32d63d245aa4e4ce304b4254b088f95"]}]}
CTX = {"channel": "activeAssetCtx", "data": {"coin": "xyz:CL", "ctx": {
    "funding": "0.00000625", "openInterest": "1466736.25", "prevDayPx": "91.062",
    "dayNtlVlm": "13879154.12", "premium": "-0.0000712993", "oraclePx": "91.165",
    "markPx": "91.16", "midPx": "91.158", "impactPxs": ["91.153", "91.164"],
    "dayBaseVlm": "152322.627"}}}
BBO = {"channel": "bbo", "data": {"coin": "io:SNDK", "time": 1791128153033, "bbo": [
    {"px": "1719.8", "sz": "6.2584", "n": 37}, {"px": "1719.9", "sz": "2.5397", "n": 37}]}}

NS = 1791128152_000000000  # 2026-10-04 15:35 UTC
DAY, HOUR = "2026-10-04", "15"


def mk_universe(**over):
    base = [
        {"coin": "xyz:CL", "dex": "xyz", "delisted": False, "day_ntl_vlm": 14e6},
        {"coin": "xyz:HO", "dex": "xyz", "delisted": False, "day_ntl_vlm": 400e3},
        {"coin": "xyz:SP500", "dex": "xyz", "delisted": False, "day_ntl_vlm": 36e6},
        {"coin": "xyz:NOISE", "dex": "xyz", "delisted": False, "day_ntl_vlm": 5e3},
        {"coin": "xyz:MIDCAP", "dex": "xyz", "delisted": False, "day_ntl_vlm": 2e6},
        {"coin": "xyz:H100", "dex": "xyz", "delisted": True, "day_ntl_vlm": 0.0},
    ]
    return [{**m, **over.get(m["coin"], {})} for m in base]


# ---------------------------------------------------------------- scaling & universe

@pytest.mark.parametrize("s,want", [
    ("91.155", 9115500000), ("1719.8", 171980000000), ("0", 0), ("1.0", 100000000),
    ("-0.0000712993", -7129), ("0.00000625", 625), ("152322.627", 15232262700000),
])
def test_to_scaled_is_exact(s, want):
    assert to_scaled(s) == want
    assert to_scaled(None) is None


def test_to_scaled_truncates_beyond_eight_decimals():
    # Hyperliquid never sends more than 8 decimals; make the behaviour explicit anyway.
    assert to_scaled("0.123456789") == 12345678


def test_assign_tiers_honours_always_then_volume():
    tiers = assign_tiers(mk_universe(), n_fast=4, min_vlm=100_000)
    # xyz:CL, xyz:HO and xyz:SP500 are in ALWAYS, so they take fast slots regardless of volume.
    assert tiers["xyz:HO"] == "fast"
    assert tiers["xyz:CL"] == tiers["xyz:SP500"] == "fast"
    # The 4th slot goes to the highest-volume remaining market.
    assert tiers["xyz:MIDCAP"] == "fast"
    # Below min_vlm and not in ALWAYS -> dropped entirely, and delisted is never recorded.
    assert "xyz:NOISE" not in tiers and "xyz:H100" not in tiers


def test_assign_tiers_demotes_overflow_to_slow():
    tiers = assign_tiers(mk_universe(), n_fast=3, min_vlm=100_000)
    assert sum(t == "fast" for t in tiers.values()) == 3
    assert tiers["xyz:MIDCAP"] == "slow"


def test_fast_tier_subscribes_both_book_modes():
    # 5 levels @ 0.5s and 20 levels @ 5.4s are separate streams; a fast market wants both.
    subs = build_subscriptions({"xyz:CL": "fast", "xyz:MIDCAP": "slow"})
    books = [(role, s) for role, s in subs if s["type"] == "l2Book"]
    cl = {role: s for role, s in books if s["coin"] == "xyz:CL"}
    assert cl[ROLE_BOOK5] == {"type": "l2Book", "coin": "xyz:CL", "fast": True}
    assert cl[ROLE_BOOK] == {"type": "l2Book", "coin": "xyz:CL"}
    # A slow market only gets the 20-level stream.
    mid = {role: s for role, s in books if s["coin"] == "xyz:MIDCAP"}
    assert set(mid) == {ROLE_BOOK}
    assert {s["type"] for _, s in subs} == {"l2Book", "trades", "activeAssetCtx"}
    assert len(subs) == 7


def test_build_subscriptions_bbo_is_fast_only_and_limit_is_enforced():
    subs = build_subscriptions({"xyz:CL": "fast", "xyz:MIDCAP": "slow"}, bbo=True)
    assert [s["coin"] for _, s in subs if s["type"] == "bbo"] == ["xyz:CL"]
    with pytest.raises(ValueError, match="exceeds"):
        build_subscriptions({"c%d" % i: "fast" for i in range(MAX_SUBSCRIPTIONS)})


def test_shard_by_role_never_mixes_roles_on_one_connection():
    # The role tag is per-connection, so a connection carrying two roles would make an
    # l2Book record unattributable to a stream.
    shards = shard_by_role(build_subscriptions({"xyz:CL": "fast", "xyz:GOLD": "fast",
                                                "xyz:MIDCAP": "slow"}), 2)
    for role, part in shards:
        assert part, "no empty connections"
        for sub in part:
            if role in (ROLE_BOOK5, ROLE_BOOK):
                assert sub["type"] == "l2Book"
                assert sub.get("fast", False) is (role == ROLE_BOOK5)
            else:
                assert sub["type"] != "l2Book"
    # 2 fast coins x (book5 + book + trades + ctx) + 1 slow x (book + trades + ctx)
    assert sum(len(p) for _, p in shards) == 11
    with pytest.raises(ValueError, match="connections exceeds"):
        shard_by_role(build_subscriptions({"c%d" % i: "fast" for i in range(20)}), 9)


def test_shard_round_robins_without_empty_shards():
    assert shard([1, 2, 3, 4, 5], 2) == [[1, 3, 5], [2, 4]]
    assert shard([1, 2], 5) == [[1], [2]]
    assert shard([1], 4) == [[1]]


def test_channel_of_reads_without_parsing():
    assert _channel_of(json.dumps(BOOK)) == "l2Book"
    assert _channel_of('{"channel":"pong"}') == "pong"
    assert _channel_of("{}") == "?"


# ---------------------------------------------------------------- raw writer

def test_writer_rotates_on_the_utc_hour_and_tolerates_truncation(tmp_path):
    w = HourlyWriter(tmp_path / "raw", flush_every=0)
    w.write_raw(NS, 0, ROLE_OTHER, json.dumps(TRADES))
    w.write_raw(NS + 3600 * 10**9, 1, ROLE_BOOK, json.dumps(BOOK))
    w.close()
    files = sorted(p.relative_to(tmp_path / "raw").as_posix()
                   for p in (tmp_path / "raw").rglob("*.jsonl.gz"))
    assert files == ["2026-10-04/15.jsonl.gz", "2026-10-04/16.jsonl.gz"]

    # A crash can leave a half-written final line; the reader must skip it, not raise.
    with gzip.open(tmp_path / "raw" / DAY / "15.jsonl.gz", "at") as fh:
        fh.write('{"rx_ns":5,"msg":{"channel"')
    recs = list(read_hour(tmp_path / "raw" / DAY / "15.jsonl.gz"))
    assert len(recs) == 1
    assert recs[0] == {"rx_ns": NS, "sh": 0, "rl": ROLE_OTHER, "msg": TRADES}


def test_write_event_is_stamped_with_the_current_hour(tmp_path):
    # Events carry their own wall-clock timestamp rather than a feed one, so they land in
    # whichever hourly file is current -- that is what makes them usable as gap markers.
    w = HourlyWriter(tmp_path / "raw", flush_every=0)
    w.write_event("disconnected", shard=2, reason="boom")
    w.close()
    [f] = list((tmp_path / "raw").rglob("*.jsonl.gz"))
    [rec] = list(read_hour(f))
    assert rec["event"] == "disconnected" and rec["shard"] == 2 and rec["reason"] == "boom"
    assert f.parent.name == w.path_for(rec["rx_ns"]).parent.name


def test_writer_reopen_mid_hour_appends_a_readable_gzip_member(tmp_path):
    for i in range(2):
        w = HourlyWriter(tmp_path / "raw", flush_every=0)
        w.write_raw(NS + i, i, ROLE_OTHER, json.dumps(BBO))
        w.close()
    assert len(list(read_hour(tmp_path / "raw" / DAY / "15.jsonl.gz"))) == 2


# ---------------------------------------------------------------- conversion

def records(*msgs, start=NS, role=ROLE_BOOK):
    return [{"rx_ns": start + i, "sh": 0, "rl": role, "msg": m} for i, m in enumerate(msgs)]


def write_raw(tmp_path, recs, hour=HOUR):
    p = tmp_path / "raw" / DAY / ("%s.jsonl.gz" % hour)
    p.parent.mkdir(parents=True, exist_ok=True)
    with gzip.open(p, "wt") as fh:
        for r in recs:
            fh.write(json.dumps(r) + "\n")
    return p


def convert(tmp_path, recs, depth=2, **kw):
    write_raw(tmp_path, recs)
    stats = convert_day(tmp_path / "raw", tmp_path / "parquet" / "hl", DAY, depth=depth, **kw)
    return stats


def read_table(tmp_path, table, dex="xyz", sym="CL"):
    p = (tmp_path / "parquet" / "hl" / table / ("date=%s" % DAY) / ("dex=%s" % dex) /
         ("sym=%s" % sym) / "part.parquet")
    return pq.read_table(p).to_pylist()


def test_book_rows_are_scaled_and_padded_to_depth(tmp_path):
    convert(tmp_path, records(BOOK), depth=3)
    rows = read_table(tmp_path, "book")
    assert len(rows) == 1
    r = rows[0]
    assert r["exch_ms"] == 1791128152148
    assert (r["bid_px_0"], r["bid_sz_0"], r["bid_n_0"]) == (9115500000, 1968500000, 2)
    assert (r["ask_px_1"], r["ask_n_1"]) == (9116300000, 2)
    # Only two levels per side came in; the third is null, not zero.
    assert r["bid_px_2"] is None and r["ask_sz_2"] is None and r["ask_n_2"] is None


def test_identical_books_are_deduped_and_prev_ms_marks_the_held_window(tmp_path):
    later = {"channel": "l2Book", "data": {**BOOK["data"], "time": BOOK["data"]["time"] + 500}}
    changed = {"channel": "l2Book", "data": {
        "coin": "xyz:CL", "time": BOOK["data"]["time"] + 1000,
        "levels": [[{"px": "91.150", "sz": "1.0", "n": 1}], BOOK["data"]["levels"][1]]}}
    stats = convert(tmp_path, records(BOOK, later, changed))
    assert stats["rows"]["book"] == 2 and stats["dups"]["book"] == 1
    rows = read_table(tmp_path, "book")
    # The dropped snapshot is still accounted for: the book held from the first row's
    # exch_ms through the second row's prev_ms.
    assert rows[0]["exch_ms"] == BOOK["data"]["time"]
    assert rows[1]["prev_ms"] == BOOK["data"]["time"] + 500
    assert rows[1]["exch_ms"] == BOOK["data"]["time"] + 1000


def test_reconnect_clears_dedup_state(tmp_path):
    recs = (records(BOOK) + [{"rx_ns": NS + 50, "event": "connected", "shard": 0}]
            + records(BOOK, start=NS + 100))
    stats = convert(tmp_path, recs)
    # The replayed snapshot after a reconnect must be kept: we cannot assume the book was
    # unchanged across a gap we did not observe.
    assert stats["rows"]["book"] == 2 and "book" not in stats["dups"]
    assert stats["rows"]["events"] == 1


def test_dedup_is_per_coin(tmp_path):
    other = {"channel": "l2Book", "data": {**BOOK["data"], "coin": "xyz:GOLD"}}
    stats = convert(tmp_path, records(BOOK, other))
    assert stats["rows"]["book"] == 2 and "book" not in stats["dups"]
    assert len(read_table(tmp_path, "book", sym="GOLD")) == 1


def test_dedup_survives_the_hour_boundary(tmp_path):
    write_raw(tmp_path, records(BOOK), hour="15")
    write_raw(tmp_path, records(BOOK, start=NS + 10**9), hour="16")
    stats = convert_day(tmp_path / "raw", tmp_path / "parquet" / "hl", DAY, depth=2)
    assert stats["rows"]["book"] == 1 and stats["dups"]["book"] == 1
    assert stats["hours"] == 2


def test_trades_keep_both_wallets(tmp_path):
    convert(tmp_path, records(TRADES))
    r = read_table(tmp_path, "trades")[0]
    assert r["px"] == 9115400000 and r["sz"] == 100000000 and r["side"] == "B"
    assert r["tid"] == 1081293533429111
    assert r["buyer"] == "0x72ce32e1a3b8d736905706a5e0866d20bbd12cfa"
    assert r["seller"] == "0x4e60e3a4a32d63d245aa4e4ce304b4254b088f95"


def test_asset_ctx_dedups_on_value_not_timestamp(tmp_path):
    moved = {"channel": "activeAssetCtx",
             "data": {"coin": "xyz:CL", "ctx": {**CTX["data"]["ctx"], "markPx": "91.17"}}}
    stats = convert(tmp_path, records(CTX, CTX, moved))
    assert stats["rows"]["asset_ctx"] == 2 and stats["dups"]["asset_ctx"] == 1
    r = read_table(tmp_path, "asset_ctx")[0]
    assert r["oracle_px"] == 9116500000
    assert r["funding"] == pytest.approx(0.00000625)
    # the feed sends 10-decimal rates, so the full precision must survive
    assert r["premium"] == pytest.approx(-0.0000712993, abs=1e-18)
    assert r["impact_bid"] == 9115300000 and r["impact_ask"] == 9116400000
    assert r["day_ntl_vlm"] == 1387915412000000


def test_bbo_and_non_xyz_dex_partitioning(tmp_path):
    convert(tmp_path, records(BBO))
    r = read_table(tmp_path, "bbo", dex="io", sym="SNDK")[0]
    assert r["bid_px"] == 171980000000 and r["ask_n"] == 37


def test_control_messages_are_ignored(tmp_path):
    noise = [{"channel": "pong"}, {"channel": "subscriptionResponse", "data": {}}]
    stats = convert(tmp_path, records(*noise))
    assert stats["rows"] == {}


def test_row_groups_and_statistics_are_trimmed(tmp_path):
    # 123 columns x per-column min/max would make the footer bigger than the data.
    convert(tmp_path, records(*[
        {"channel": "l2Book", "data": {**BOOK["data"], "time": BOOK["data"]["time"] + i,
                                       "levels": [[{"px": "91.%03d" % i, "sz": "1.0", "n": 1}],
                                                  BOOK["data"]["levels"][1]]}}
        for i in range(40)]), depth=2, batch_rows=16)
    p = (tmp_path / "parquet" / "hl" / "book" / ("date=%s" % DAY) / "dex=xyz" / "sym=CL" /
         "part.parquet")
    md = pq.ParquetFile(p).metadata
    assert md.num_rows == 40 and md.num_row_groups == 3
    by_name = {md.schema.column(i).name: md.row_group(0).column(i)
               for i in range(md.num_columns)}
    assert by_name["exch_ms"].statistics is not None
    assert by_name["bid_px_0"].statistics is None
    assert "DELTA_BINARY_PACKED" in by_name["bid_px_0"].encodings


def test_converter_reports_sizes(tmp_path):
    stats = convert(tmp_path, records(BOOK, TRADES, CTX))
    assert stats["raw_bytes"] > 0 and stats["pq_bytes"] > 0
    assert convert_day(tmp_path / "raw", tmp_path / "out", "2020-01-01") == {}


# ---------------------------------------------------------------- backfill merge

def test_merge_write_unions_and_lets_new_rows_win(tmp_path):
    p = tmp_path / "c.parquet"
    rows = [{"open_ms": 1, "close_ms": 2, "open": 1, "high": 1, "low": 1, "close": 1,
             "volume": 1, "n_trades": 1}]
    assert merge_write(rows, CANDLE_SCHEMA, p, "open_ms") == (1, 1)
    # A re-run overlaps the previous window: old bars persist, the shared bar is replaced
    # with its final value, and new bars are appended.
    newer = [{**rows[0], "close": 99}, {**rows[0], "open_ms": 2, "close": 7}]
    assert merge_write(newer, CANDLE_SCHEMA, p, "open_ms") == (2, 1)
    got = pq.read_table(p).to_pylist()
    assert [r["open_ms"] for r in got] == [1, 2]
    assert got[0]["close"] == 99


def test_merge_write_accumulates_beyond_the_api_window(tmp_path):
    p = tmp_path / "f.parquet"
    merge_write([{"time_ms": t, "funding_rate": float(t), "premium": 0.0} for t in range(10)],
                FUNDING_SCHEMA, p, "time_ms")
    # A later run only sees a window that starts after the first rows rolled off.
    total, added = merge_write([{"time_ms": t, "funding_rate": float(t), "premium": 0.0}
                                for t in range(5, 15)], FUNDING_SCHEMA, p, "time_ms")
    assert (total, added) == (15, 5)
    assert existing_max(p, "time_ms") == 14
    assert existing_max(tmp_path / "missing.parquet", "time_ms") is None


# ---------------------------------------------------------------- catalog

def test_catalog_unscales_prices_and_exposes_partitions(tmp_path):
    convert(tmp_path, records(BOOK, TRADES, CTX, BBO), depth=2)
    made = hl_db.init(tmp_path / "hl.duckdb", tmp_path, depth=2)
    assert {"book", "book_top", "trades", "asset_ctx", "bbo"} <= set(made)
    con = duckdb.connect(str(tmp_path / "hl.duckdb"))
    try:
        hl_db.create_views(con, tmp_path, depth=2)
        coin, dex, sym, bid, ask, spread = con.sql(
            "select coin, dex, sym, bid, ask, spread from book_top").fetchone()
        assert (coin, dex, sym) == ("xyz:CL", "xyz", "CL")
        assert bid == pytest.approx(91.155) and ask == pytest.approx(91.162)
        assert spread == pytest.approx(0.007)
        # microprice leans toward the side with less size resting on it
        assert con.sql("select microprice from book_top").fetchone()[0] == pytest.approx(
            (91.155 * 10.706 + 91.162 * 19.685) / (19.685 + 10.706))
        assert con.sql("select px, sz from trades").fetchone() == pytest.approx((91.154, 1.0))
        assert con.sql("select oracle_px, funding, premium, day_ntl_vlm from asset_ctx"
                       ).fetchone() == pytest.approx((91.165, 0.00000625, -0.0000712993,
                                                      13879154.12))
        assert con.sql("select dt from book").fetchone()[0].isoformat() == DAY
        assert con.sql("select coin, bid_px from bbo").fetchone() == ("io:SNDK",
                                                                      pytest.approx(1719.8))
    finally:
        con.close()


def test_catalog_skips_tables_with_no_files(tmp_path):
    assert hl_db.init(tmp_path / "hl.duckdb", tmp_path) == []


def _hist_part(tmp_path, *parts):
    p = tmp_path.joinpath("parquet", "hl_hist", *parts) / "part.parquet"
    p.parent.mkdir(parents=True, exist_ok=True)
    return p


def test_candles_and_funding_views_keep_millisecond_timestamps(tmp_path):
    # open_ms sits next to an `open` price; it must not be unscaled along with it.
    merge_write([{"open_ms": 1791128152000, "close_ms": 1791128212000, "open": to_scaled("91.1"),
                  "high": to_scaled("91.2"), "low": to_scaled("91.0"),
                  "close": to_scaled("91.15"), "volume": to_scaled("10.5"), "n_trades": 7}],
                CANDLE_SCHEMA,
                _hist_part(tmp_path, "candles", "interval=1m", "dex=xyz", "sym=CL"), "open_ms")
    merge_write([{"time_ms": 1791128152000, "funding_rate": 0.00000625,
                  "premium": -0.00007}], FUNDING_SCHEMA,
                _hist_part(tmp_path, "funding", "dex=xyz", "sym=CL"), "time_ms")
    con = duckdb.connect()
    try:
        hl_db.create_views(con, tmp_path)
        assert con.sql("select open_ms, open, n_trades, interval from candles").fetchone() == (
            1791128152000, pytest.approx(91.1), 7, "1m")
        assert con.sql("select time_ms, funding_rate from funding").fetchone() == (
            1791128152000, pytest.approx(0.00000625))
    finally:
        con.close()


# ---------------------------------------------------------------- retention

def test_retention_keeps_days_that_were_never_converted(tmp_path):
    write_raw(tmp_path, records(BOOK))
    old = tmp_path / "raw" / "hl" / "2020-01-01"
    old.mkdir(parents=True)
    (old / "00.jsonl.gz").write_bytes(b"x")
    assert hl_db.apply_retention(tmp_path, keep_days=1) == []
    assert old.exists(), "a day with no parquet must never be deleted"

    # Once the parquet exists, the raw day is redundant and can go.
    pqdir = tmp_path / "parquet" / "hl" / "book" / "date=2020-01-01" / "dex=xyz" / "sym=CL"
    pqdir.mkdir(parents=True)
    (pqdir / "part.parquet").write_bytes(b"x")
    assert hl_db.apply_retention(tmp_path, keep_days=1, dry_run=True) == [("2020-01-01", 1)]
    assert old.exists(), "dry run must not delete"
    assert hl_db.apply_retention(tmp_path, keep_days=1) == [("2020-01-01", 1)]
    assert not old.exists()


def test_report_counts_bytes_and_days(tmp_path):
    convert(tmp_path, records(BOOK, TRADES))
    (tmp_path / "raw" / "hl" / DAY).mkdir(parents=True, exist_ok=True)
    (tmp_path / "raw" / "hl" / DAY / "15.jsonl.gz").write_bytes(b"x" * 100)
    r = hl_db.report(tmp_path)
    assert r["raw_days"] == [(DAY, 100)]
    assert r["parquet_bytes"] > 0 and r["free_bytes"] > 0
    assert "book" in r["tables"] and r["tables"]["book"][1] == 1


# ---------------------------------------------------------------- recorder loop

def test_run_shard_reconnects_resubscribes_and_logs_gaps(tmp_path):
    seen = []

    async def server(ws):
        sub = json.loads(await ws.recv())
        seen.append(sub)
        await ws.send(json.dumps({"channel": "subscriptionResponse", "data": sub}))
        await ws.send(json.dumps(BOOK))
        await ws.close()  # drop the client to force a reconnect

    async def go():
        stop = asyncio.Event()
        writer = HourlyWriter(tmp_path / "raw", flush_every=0)
        async with websockets.serve(server, "127.0.0.1", 0) as srv:
            port = srv.sockets[0].getsockname()[1]

            async def until_two():
                while len(seen) < 2:
                    await asyncio.sleep(0.02)
                await asyncio.sleep(0.2)
                stop.set()

            await asyncio.wait_for(asyncio.gather(
                run_shard(0, [{"type": "l2Book", "coin": "xyz:CL", "fast": True}], writer, stop,
                          role=ROLE_BOOK5, url="ws://127.0.0.1:%d" % port, max_backoff=0.1,
                          sub_pause=0),
                until_two()), timeout=15)
        writer.close()

    asyncio.run(go())
    assert len(seen) >= 2
    assert all(s["subscription"]["fast"] is True for s in seen)
    recs = [json.loads(l) for f in sorted((tmp_path / "raw").rglob("*.gz"))
            for l in gzip.open(f, "rt")]
    events = [r["event"] for r in recs if "event" in r]
    assert events[:3] == ["connected", "disconnected", "connected"]
    books = [r for r in recs if r.get("msg", {}).get("channel") == "l2Book"]
    assert len(books) >= 2
    assert all(r["rl"] == ROLE_BOOK5 for r in books), "records carry the connection's role"


def test_run_shard_reconnects_when_the_feed_goes_silent(tmp_path):
    connects = []

    async def server(ws):
        connects.append(1)
        await ws.recv()
        await asyncio.sleep(5)  # never send anything: the stale timer must fire

    async def go():
        stop = asyncio.Event()
        writer = HourlyWriter(tmp_path / "raw", flush_every=0)
        async with websockets.serve(server, "127.0.0.1", 0) as srv:
            port = srv.sockets[0].getsockname()[1]

            async def until_two():
                while len(connects) < 2:
                    await asyncio.sleep(0.02)
                stop.set()

            await asyncio.wait_for(asyncio.gather(
                run_shard(0, [{"type": "l2Book", "coin": "xyz:CL"}], writer, stop,
                          url="ws://127.0.0.1:%d" % port, stale_after=0.3, max_backoff=0.1,
                          sub_pause=0),
                until_two()), timeout=15)
        writer.close()

    asyncio.run(go())
    recs = [json.loads(l) for f in sorted((tmp_path / "raw").rglob("*.gz"))
            for l in gzip.open(f, "rt")]
    reasons = [r.get("reason", "") for r in recs if r.get("event") == "disconnected"]
    assert any("TimeoutError" in r for r in reasons), reasons


# ---------------------------------------------------------------- dual book streams

BOOK20 = {"channel": "l2Book", "data": {"coin": "xyz:CL", "time": 1791128152148, "levels": [
    [{"px": "91.%03d" % (160 - i), "sz": "1.0", "n": 1} for i in range(20)],
    [{"px": "91.%03d" % (162 + i), "sz": "1.0", "n": 1} for i in range(20)]]}}
BOOK5 = {"channel": "l2Book", "data": {"coin": "xyz:CL", "time": 1791128152148, "levels": [
    [{"px": "91.%03d" % (160 - i), "sz": "1.0", "n": 1} for i in range(5)],
    [{"px": "91.%03d" % (162 + i), "sz": "1.0", "n": 1} for i in range(5)]]}}


def test_role_tag_routes_the_two_book_streams(tmp_path):
    recs = (records(BOOK20, role=ROLE_BOOK) +
            records(BOOK5, start=NS + 100, role=ROLE_BOOK5))
    stats = convert_day(write_raw(tmp_path, recs).parent.parent,
                        tmp_path / "parquet" / "hl", DAY)
    assert stats["rows"] == {"book": 1, "book5": 1}
    deep = read_table(tmp_path, "book")[0]
    assert deep["bid_px_19"] == to_scaled("91.141"), "20-level stream keeps all 20 levels"
    shallow = read_table(tmp_path, "book5")[0]
    assert shallow["bid_px_4"] == to_scaled("91.156")
    assert "bid_px_5" not in shallow, "book5 has no columns past level 4"


def test_role_tag_wins_over_the_level_count(tmp_path):
    # A 20-level subscription on a thin book sends <=5 levels. Without the tag that would be
    # misrouted into book5; with it, it stays in the 20-level table.
    stats = convert_day(write_raw(tmp_path, records(BOOK5, role=ROLE_BOOK)).parent.parent,
                        tmp_path / "parquet" / "hl", DAY)
    assert stats["rows"] == {"book": 1}
    assert read_table(tmp_path, "book")[0]["bid_px_19"] is None


def test_untagged_recordings_fall_back_to_the_level_count(tmp_path):
    recs = [{"rx_ns": NS, "sh": 0, "msg": BOOK20}, {"rx_ns": NS + 1, "sh": 0, "msg": BOOK5}]
    stats = convert_day(write_raw(tmp_path, recs).parent.parent,
                        tmp_path / "parquet" / "hl", DAY)
    assert stats["rows"] == {"book": 1, "book5": 1}


def test_the_two_streams_dedup_independently(tmp_path):
    # The same prices arriving on both streams are different rows, never a duplicate of
    # each other, and each stream's prev_ms advances on its own.
    recs = (records(BOOK5, role=ROLE_BOOK5) +
            records(BOOK5, start=NS + 10, role=ROLE_BOOK) +
            records(BOOK5, start=NS + 20, role=ROLE_BOOK5))
    stats = convert_day(write_raw(tmp_path, recs).parent.parent,
                        tmp_path / "parquet" / "hl", DAY)
    assert stats["rows"] == {"book5": 1, "book": 1}
    assert stats["dups"] == {"book5": 1}


def test_book_top_is_built_on_the_half_second_stream(tmp_path):
    convert_day(write_raw(tmp_path, records(BOOK20, role=ROLE_BOOK) +
                          records(BOOK5, start=NS + 5, role=ROLE_BOOK5)).parent.parent,
                tmp_path / "parquet" / "hl", DAY)
    con = duckdb.connect()
    try:
        made = hl_db.create_views(con, tmp_path)
        assert {"book", "book5", "book_top"} <= set(made)
        assert con.sql("select count(*) from book_top").fetchone()[0] == \
               con.sql("select count(*) from book5").fetchone()[0]
        assert con.sql("select bid, ask from book_top").fetchone() == pytest.approx(
            (91.160, 91.162))
    finally:
        con.close()


def test_trades_dedup_on_tid_across_reconnects(tmp_path):
    # Subscribing replays a backlog of recent trades, so a reconnect re-delivers rows we
    # already stored. They must not be double-counted.
    second = {"channel": "trades", "data": [
        TRADES["data"][0],                               # replayed by the backlog
        {**TRADES["data"][0], "tid": 999, "px": "91.2"}, # genuinely new
    ]}
    recs = (records(TRADES, role=ROLE_OTHER) +
            [{"rx_ns": NS + 50, "event": "connected", "shard": 0}] +
            records(second, start=NS + 100, role=ROLE_OTHER))
    stats = convert_day(write_raw(tmp_path, recs).parent.parent,
                        tmp_path / "parquet" / "hl", DAY)
    assert stats["rows"]["trades"] == 2 and stats["dups"]["trades"] == 1
    assert sorted(r["tid"] for r in read_table(tmp_path, "trades")) == [999, 1081293533429111]


def test_merge_write_discards_rows_from_a_stale_schema(tmp_path):
    # funding_rate was once a 1e8-scaled int. Merging those rows into the current double
    # schema coerced them and kept the scaling, so -0.001 read back as -100221.
    p = tmp_path / "f.parquet"
    old = pa.schema([("time_ms", pa.int64()), ("funding_rate", pa.int64()),
                     ("premium", pa.int64())])
    pq.write_table(pa.table({"time_ms": [1, 2], "funding_rate": [-100221, 625],
                             "premium": [-7129, 0]}, schema=old), p)
    assert not schema_matches(p, FUNDING_SCHEMA)
    # Nothing may resume past rows that are about to be discarded.
    assert existing_max(p, "time_ms", FUNDING_SCHEMA) is None
    assert existing_max(p, "time_ms") == 2, "without a schema it still reports the raw max"

    total, added = merge_write([{"time_ms": 2, "funding_rate": -0.00100221, "premium": -0.016}],
                               FUNDING_SCHEMA, p, "time_ms")
    assert (total, added) == (1, 1), "the two stale rows are gone, not merged"
    got = pq.read_table(p)
    assert got.schema.field("funding_rate").type == pa.float64()
    assert got.to_pylist() == [{"time_ms": 2, "funding_rate": -0.00100221, "premium": -0.016}]


def test_merge_write_keeps_rows_when_the_schema_matches(tmp_path):
    p = tmp_path / "f.parquet"
    merge_write([{"time_ms": 1, "funding_rate": 6.25e-06, "premium": 0.0}],
                FUNDING_SCHEMA, p, "time_ms")
    assert schema_matches(p, FUNDING_SCHEMA)
    assert merge_write([{"time_ms": 2, "funding_rate": 1.25e-05, "premium": 0.0}],
                       FUNDING_SCHEMA, p, "time_ms") == (2, 1)


# ---------------------------------------------------------------- single-instance lock

def test_lock_is_exclusive_and_names_the_holder(tmp_path):
    from recorder.hl_common import AlreadyRunning, acquire_lock
    lock = acquire_lock(tmp_path / "a.lock")
    try:
        with pytest.raises(AlreadyRunning, match="pid %d" % __import__("os").getpid()):
            acquire_lock(tmp_path / "a.lock")
    finally:
        lock.close()
    # Releasing it lets the next starter in, which is what makes a restart loop work.
    again = acquire_lock(tmp_path / "a.lock")
    again.close()


def test_lock_is_per_output_directory(tmp_path):
    from recorder.hl_common import acquire_lock
    a = acquire_lock(tmp_path / "a.lock")
    b = acquire_lock(tmp_path / "b.lock")  # a separate capture must still be allowed
    a.close(); b.close()


# ---------------------------------------------------------------- incremental conversion

def test_is_current_skips_only_days_already_converted(tmp_path):
    from recorder.hl_xyz_to_parquet import is_current
    raw, out = tmp_path / "raw", tmp_path / "parquet" / "hl"
    assert is_current(raw, out, DAY), "a day with no raw files needs no work"

    write_raw(tmp_path, records(BOOK20, role=ROLE_BOOK))
    assert not is_current(raw, out, DAY), "raw present, no parquet -> stale"

    convert_day(raw, out, DAY)
    assert is_current(raw, out, DAY), "freshly converted -> current"

    # A new hour arriving (the day still being recorded) must make it stale again.
    import os, time
    p = write_raw(tmp_path, records(BOOK20, role=ROLE_BOOK), hour="18")
    os.utime(p, (time.time() + 10, time.time() + 10))
    assert not is_current(raw, out, DAY)


def test_read_hour_tolerates_a_live_gzip_file(tmp_path):
    # The hour being recorded right now always ends mid-deflate-stream, because the writer
    # flushes every 5s. Reading it must yield the intact records, not raise.
    p = write_raw(tmp_path, records(BOOK20, TRADES, role=ROLE_BOOK))
    data = p.read_bytes()
    p.write_bytes(data[:-6])  # lop off the gzip trailer, as an unflushed tail looks
    recs = list(read_hour(p))
    assert len(recs) >= 1
    assert recs[0]["msg"]["channel"] == "l2Book"


def test_convert_day_survives_a_live_gzip_file(tmp_path):
    p = write_raw(tmp_path, records(BOOK20, role=ROLE_BOOK))
    p.write_bytes(p.read_bytes()[:-6])
    stats = convert_day(tmp_path / "raw", tmp_path / "parquet" / "hl", DAY)
    assert stats["rows"].get("book") == 1


def test_coin_parts_rejects_path_escapes():
    import pytest
    from recorder.hl_common import coin_parts
    assert coin_parts("xyz:CL") == ("xyz", "CL")
    assert coin_parts("PAXG") == ("main", "PAXG")
    for bad in ["..", "xyz:..", "../x:CL", "xyz:a/b", "xyz:a\b", "xyz:"]:
        with pytest.raises(ValueError):
            coin_parts(bad)
