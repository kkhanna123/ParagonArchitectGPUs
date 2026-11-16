"""recorder.hf_sync planning: which days/tables get pushed. No network."""
import os
import time

from recorder import hf_pull, hf_sync


def touch(p, data=b"x"):
    p.parent.mkdir(parents=True, exist_ok=True)
    p.write_bytes(data)


def tree(root):
    pq = root / "parquet"
    touch(pq / "hl/book5/date=2026-11-12/dex=xyz/sym=CL/part.parquet")
    touch(pq / "hl/book5/date=2026-11-13/dex=xyz/sym=CL/part.parquet")
    touch(pq / "kalshi/trades/date=2026-11-12/series=KXWTI15M/part.parquet")
    touch(pq / "kalshi/book/date=2026-11-12/series=KXWTI15M/part.parquet")
    touch(pq / "kalshi_hist/markets/series=KXWTI15M/part.parquet")
    (pq / "hl/events/date=2026-11-12").mkdir(parents=True)          # empty: skipped
    return pq


def test_plan_skips_today_and_empty(tmp_path):
    pq = tree(tmp_path)
    daily, hist = hf_sync.plan(pq, {}, today="2026-11-13")
    assert list(daily) == ["2026-11-12"]
    assert set(daily["2026-11-12"]) == {"hl/book5", "kalshi/trades", "kalshi/book"}
    assert set(hist) == {"kalshi_hist/markets"}


def test_plan_is_incremental_and_sees_changes(tmp_path):
    pq = tree(tmp_path)
    daily, hist = hf_sync.plan(pq, {}, today="2026-11-14")
    state = {"daily": daily, "hist": hist}
    assert hf_sync.plan(pq, state, today="2026-11-14") == ({}, {})
    f = pq / "kalshi/trades/date=2026-11-12/series=KXWTI15M/part.parquet"
    f.write_bytes(b"rewritten")
    os.utime(f, (time.time() + 5, time.time() + 5))
    daily2, hist2 = hf_sync.plan(pq, state, today="2026-11-14")
    assert daily2 == {"2026-11-12": {"kalshi/trades": hf_sync.fingerprint(f.parent.parent)}}
    assert hist2 == {}


def test_plan_exclude_and_only(tmp_path):
    pq = tree(tmp_path)
    daily, hist = hf_sync.plan(pq, {}, "2026-11-13", exclude={"kalshi/book"})
    assert "kalshi/book" not in daily["2026-11-12"]
    daily, hist = hf_sync.plan(pq, {}, "2026-11-13", only={"kalshi/trades"})
    assert daily == {"2026-11-12": {"kalshi/trades": [1, 1, daily["2026-11-12"]["kalshi/trades"][2]]}}
    assert hist == {}


def test_state_roundtrip(tmp_path):
    p = tmp_path / "s.json"
    assert hf_sync.load_state(p) == {}
    hf_sync.save_state(p, {"daily": {"2026-11-12": {"hl/book5": [1, 2, 3.0]}}})
    assert hf_sync.load_state(p)["daily"]["2026-11-12"]["hl/book5"] == [1, 2, 3.0]


def test_pull_patterns():
    assert hf_pull.patterns([], None, None, False) is None
    pats = hf_pull.patterns(["kalshi/trades"], "2026-11-01", "2026-11-02", True)
    assert "kalshi/trades/date=2026-11-01/**" in pats
    assert "kalshi/trades/date=2026-11-02/**" in pats
    assert "kalshi_hist/**" in pats
    pats = hf_pull.patterns(["hl/book5"], None, None, False)
    assert pats == ["README.md", "hl/book5/**"]
