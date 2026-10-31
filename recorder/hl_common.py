"""Shared pieces for the Hyperliquid / trade[XYZ] collectors: paths, REST, universe, tiering."""
import json
import logging
import os
import re
import time
import urllib.error
import urllib.request
from pathlib import Path

log = logging.getLogger("hl")

API = "https://api.hyperliquid.xyz"
WS_URL = "wss://api.hyperliquid.xyz/ws"
REPO = Path(__file__).resolve().parents[1]

# Hyperliquid prices and sizes are decimal strings with at most 8 decimal places, so a fixed
# 1e8 scale makes them exact int64s. Ints let parquet use delta encoding, which is the single
# biggest win in the book table (see Data/tradexyz/storage.md).
SCALE = 10**8

# WS limits per IP, from the docs (verified in the report): 10 connections, 1000 subscriptions,
# 2000 outbound messages/min. We stay well inside all three.
MAX_SUBSCRIPTIONS = 1000
MAX_CONNECTIONS = 10

# Coin names become hive partition directories (dex=<dex>/sym=<sym>), and they arrive from the
# exchange, so anything that could climb out of the output folder ("..", "/", "\\") is refused.
_SAFE_PART = re.compile(r"[A-Za-z0-9_@-][A-Za-z0-9_.@-]*")


def coin_parts(coin: str) -> tuple:
    """Split "dex:SYM" (main dex: "SYM") into path-safe (dex, sym); ValueError if unsafe."""
    dex, _, sym = coin.rpartition(":")
    dex = dex or "main"
    if not (_SAFE_PART.fullmatch(dex) and _SAFE_PART.fullmatch(sym)):
        raise ValueError("unsafe coin name for a partition path: %r" % coin)
    return dex, sym


def data_root() -> Path:
    """Where the finished data (parquet + the DuckDB catalog) lives. Set PARAGON_DATA_ROOT."""
    env = os.environ.get("PARAGON_DATA_ROOT")
    return Path(env) if env else Path("C:/ParagonData") if os.name == "nt" else REPO / "Data"


def parquet_root() -> Path:
    return data_root() / "parquet"


def raw_root(root: Path | None = None) -> Path:
    """Where the raw JSONL staging files live (PARAGON_RAW_ROOT, else <root or data root>/raw).

    Kept apart from the parquet so the data root can sit in a synced folder: the raw tier is
    GB/day of files appended every few seconds, which OneDrive would re-upload constantly and
    whose open handles it fights with. Parquet files are written once per day and are fine.
    """
    env = os.environ.get("PARAGON_RAW_ROOT")
    root = Path(env) if env else (root or data_root()) / "raw"
    if "onedrive" in str(root).lower():
        log.warning("raw root %s is inside OneDrive; it will be cloud-synced. "
                    "Set PARAGON_RAW_ROOT to a path outside OneDrive.", root)
    return root


def to_scaled(s) -> int | None:
    """'91.155' -> 9115500000. Exact: Decimal-free because the inputs have <= 8 decimals."""
    if s is None:
        return None
    neg = False
    s = str(s)
    if s[0] == "-":
        neg, s = True, s[1:]
    whole, _, frac = s.partition(".")
    v = int(whole or 0) * SCALE + int((frac + "00000000")[:8] or 0)
    return -v if neg else v


def info(body: dict, retries: int = 5, timeout: float = 30.0):
    """POST to /info with backoff. 429 means we tripped the 1200 weight/min budget."""
    data = json.dumps(body).encode()
    for attempt in range(retries):
        try:
            req = urllib.request.Request(f"{API}/info", data=data,
                                         headers={"Content-Type": "application/json"})
            with urllib.request.urlopen(req, timeout=timeout) as r:
                return json.load(r)
        except urllib.error.HTTPError as e:
            if e.code not in (429, 500, 502, 503, 504) or attempt == retries - 1:
                raise
            wait = 2.0 * 2**attempt
            log.warning("info %s -> HTTP %d; retrying in %.0fs", body.get("type"), e.code, wait)
            time.sleep(wait)
        except (urllib.error.URLError, TimeoutError) as e:
            if attempt == retries - 1:
                raise
            log.warning("info %s -> %s; retrying", body.get("type"), e)
            time.sleep(2.0 * 2**attempt)


# Markets we always record regardless of 24h volume, because the research plan needs them
# (Data/tradexyz/steps.md §1). Low-volume names like HO and PALLADIUM would otherwise be cut.
ALWAYS = {
    # commodities
    "xyz:CL", "xyz:BRENTOIL", "xyz:NATGAS", "xyz:HO", "xyz:GOLD", "xyz:SILVER",
    "xyz:PLATINUM", "xyz:PALLADIUM", "xyz:COPPER",
    # memory / semis / compute
    "xyz:DRAM", "xyz:MU", "xyz:SKHX", "xyz:SKHY", "xyz:SMSN", "xyz:SNDK", "xyz:KIOXIA",
    "xyz:CXMT", "xyz:WDC", "xyz:NVDA", "xyz:AMD", "xyz:TSM", "xyz:AVGO", "xyz:ASML",
    "xyz:ARM", "xyz:INTC", "xyz:CRWV", "xyz:NBIS", "xyz:IREN",
    # indices
    "xyz:SP500", "xyz:XYZ100",
    # cross-dex overlaps for same-asset spread studies
    "para:AVGO", "para:UNITREE", "para:CRWD", "para:IREN", "para:AAOI", "para:LRCX",
    "io:DRAM", "io:SNDK", "io:NBIS", "io:EWY",
}

DEXES = ("xyz", "para", "io")

# The permanent collection set, as asked for on 2026-10-04. Everything here runs on BOTH book
# streams (5 levels @ 0.5s and 20 levels @ 5.4s) regardless of volume.
#
# Two requests have no instrument on these venues and are therefore absent:
#   wheat    xyz:WHEAT and xyz:CORN are both delisted; there is no grain market on xyz/para/io.
#   uranium  xyz:URANIUM is delisted. xyz:URNM below is a uranium *miners ETF* standing in for
#            it, and it trades ~$0.03M/day, so treat it as a weak proxy rather than the thing.
# Both are open questions for the slow-poll research in Data/slowPoll/.
CORE = {
    "oil": ["xyz:CL", "xyz:BRENTOIL"],          # WTI and Brent; xyz:HO (diesel) is also live
    "natgas": ["xyz:NATGAS"],                   # xyz:TTF is delisted
    "uranium": ["xyz:URNM"],                    # proxy only -- see note above
    "palladium": ["xyz:PALLADIUM"],
    "sp500": ["xyz:SP500"],
    "cerebras": ["xyz:CBRS"],
    "nvidia": ["xyz:NVDA"],
    "amd": ["xyz:AMD"],
    "dram": ["xyz:DRAM", "io:DRAM"],            # both dexes, for the cross-dex spread
    "photonics": ["xyz:LITE", "xyz:AAOI", "xyz:LYTE",
                  "para:CIEN", "para:COHR", "para:GLW", "para:CRDO"],
    "sandisk": ["xyz:SNDK", "io:SNDK"],         # io:SNDK carries more volume than xyz:SNDK
    "micron": ["xyz:MU"],
    "gold": ["xyz:GOLD"],
}

# Every live commodity market on Hyperliquid, checked against perpDexs on 2026-10-25. Only xyz
# lists commodities among the HIP-3 dexes (flx/vntl/hyna/km/abcd/cash have no live markets,
# para/mkts/io have none), plus the main-dex PAXG perp, a tokenized-gold second venue for GOLD.
# Left out on purpose: equity ETFs that merely track commodities (xyz:XLE, para:GDX).
COMMODITIES = [
    "xyz:CL", "xyz:BRENTOIL", "xyz:HO", "xyz:NATGAS",                     # energy
    "xyz:GOLD", "xyz:SILVER", "xyz:PLATINUM", "xyz:PALLADIUM", "PAXG",    # precious metals
    "xyz:COPPER",                                                         # industrial metals
    "xyz:URNM",                                                           # uranium (miners ETF proxy)
]


def _dedup(coins):
    return list(dict.fromkeys(coins))


WATCHLISTS = {
    "core": [c for syms in CORE.values() for c in syms],
    # The permanent collection set: core + every commodity + the research list in ALWAYS.
    "all": _dedup([c for syms in CORE.values() for c in syms] + COMMODITIES + sorted(ALWAYS)),
}


def live_coins(dexes) -> set:
    """Names of the listed, non-delisted markets on `dexes` ('' = the main dex)."""
    out = set()
    for dex in dexes:
        meta = info({"type": "meta", "dex": dex} if dex else {"type": "meta"})
        out |= {u["name"] for u in meta["universe"] if not u.get("isDelisted")}
    return out


def fetch_universe(dexes=DEXES) -> list[dict]:
    """Live markets across the HIP-3 dexes, with 24h notional volume and open interest."""
    out = []
    for dex in dexes:
        meta, ctxs = info({"type": "metaAndAssetCtxs", "dex": dex})
        for u, c in zip(meta["universe"], ctxs):
            mark = float(c.get("markPx") or 0)
            out.append({
                "coin": u["name"], "dex": dex, "delisted": bool(u.get("isDelisted")),
                "max_leverage": u.get("maxLeverage"), "sz_decimals": u.get("szDecimals"),
                "day_ntl_vlm": float(c.get("dayNtlVlm") or 0),
                "open_interest_usd": float(c.get("openInterest") or 0) * mark,
                "mark_px": mark,
            })
    return out


def assign_tiers(universe, n_fast: int = 40, min_vlm: float = 100_000.0) -> dict[str, str]:
    """Split live markets into 'fast' (0.5s book) and 'slow' (~5s book).

    Depth at 0.5s costs ~100x more than at 5s, so only markets we actually study get it:
    everything in ALWAYS, then the highest-volume remainder up to `n_fast`. Markets under
    `min_vlm` and not in ALWAYS are dropped entirely -- their books barely move.
    """
    live = [m for m in universe if not m["delisted"]]
    tiers = {}
    fast = {m["coin"] for m in live if m["coin"] in ALWAYS}
    for m in sorted(live, key=lambda m: -m["day_ntl_vlm"]):
        if len(fast) >= n_fast:
            break
        fast.add(m["coin"])
    for m in live:
        if m["coin"] in fast:
            tiers[m["coin"]] = "fast"
        elif m["day_ntl_vlm"] >= min_vlm:
            tiers[m["coin"]] = "slow"
    return tiers


# The l2Book subscription trades depth against cadence, and the two modes are separate
# streams you can hold at the same time (verified 2026-10-04 on xyz:CL and xyz:SP500: both
# acks accepted, both streams delivered concurrently):
#
#   {"type":"l2Book","coin":C,"fast":true}   ->  5 levels/side every ~540 ms
#   {"type":"l2Book","coin":C}               -> 20 levels/side every ~5.4 s
#
# Neither alone is enough: we want the 0.5s cadence *and* the full book. So a fast-tier market
# is subscribed to both, and the two streams are stored as separate tables (book5, book).
# The wire message carries no flag saying which mode produced it -- only the level count, which
# is ambiguous for a genuinely thin book -- so each mode gets its own connection and the
# recorder tags every record with that connection's role.
ROLE_BOOK5 = "book5"   # 5 levels @ 0.5s
ROLE_BOOK = "book"     # 20 levels @ 5.4s
ROLE_OTHER = "other"   # trades, activeAssetCtx, bbo


def build_subscriptions(tiers: dict[str, str], channels=("trades", "activeAssetCtx"),
                        bbo: bool = False) -> list[tuple]:
    """-> [(role, subscription), ...]. Fast-tier coins get both book modes."""
    subs = []
    for coin, tier in sorted(tiers.items()):
        if tier == "fast":
            subs.append((ROLE_BOOK5, {"type": "l2Book", "coin": coin, "fast": True}))
        subs.append((ROLE_BOOK, {"type": "l2Book", "coin": coin}))
        for ch in channels:
            subs.append((ROLE_OTHER, {"type": ch, "coin": coin}))
        if bbo and tier == "fast":
            subs.append((ROLE_OTHER, {"type": "bbo", "coin": coin}))
    if len(subs) > MAX_SUBSCRIPTIONS:
        raise ValueError(f"{len(subs)} subscriptions exceeds the {MAX_SUBSCRIPTIONS}/IP limit")
    return subs


class AlreadyRunning(RuntimeError):
    pass


def acquire_lock(path: Path):
    """Take an exclusive lock keyed to an output directory, or raise AlreadyRunning.

    The permanent collector is started from the Startup folder, so logging in twice would
    otherwise run two of them: duplicate rows in the same raw tree, and 12 websockets against
    a 10-per-IP limit. Keyed on the output path so a second recorder writing somewhere else
    (a test capture, say) is still allowed.

    The lock is released when the process exits for any reason, including a kill, because the
    OS drops the file handle.

    Byte 0 is the lock itself and nothing writes to it; the holder's pid goes at offset 1. That
    split matters on Windows, where `msvcrt.locking` takes a byte range from the current file
    position: sharing one offset between the lock and the pid text made the *write* fail with
    PermissionError in the loser instead of the lock call failing cleanly.
    """
    path = Path(path)
    path.parent.mkdir(parents=True, exist_ok=True)
    if not path.exists():
        path.write_bytes(b"\0")
    fh = open(path, "r+b")
    try:
        fh.seek(0)
        if os.name == "nt":
            import msvcrt
            msvcrt.locking(fh.fileno(), msvcrt.LK_NBLCK, 1)
        else:
            import fcntl
            fcntl.flock(fh.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
    except OSError:
        try:
            fh.seek(1)
            holder = fh.read().decode("utf-8", "replace").strip() or "unknown pid"
        except OSError:
            holder = "unknown pid"
        fh.close()
        raise AlreadyRunning("another recorder already holds %s (%s)" % (path, holder))
    fh.seek(1)
    fh.truncate(1)
    fh.write(("pid %d started %s" % (os.getpid(), time.strftime("%Y-%m-%dT%H:%M:%S"))).encode())
    fh.flush()
    return fh  # keep a reference alive: closing it releases the lock


def shard(items: list, n: int) -> list[list]:
    """Round-robin into n non-empty shards, so one dropped connection loses 1/n of the feed."""
    n = max(1, min(n, len(items)))
    out = [[] for _ in range(n)]
    for i, it in enumerate(items):
        out[i % n].append(it)
    return out


def shard_by_role(subs: list[tuple], n: int) -> list[tuple]:
    """-> [(role, [subscription, ...]), ...], spreading each role over up to n connections.

    A connection carries exactly one role, which is what lets the recorder tag records with it.
    """
    groups = {}
    for role, sub in subs:
        groups.setdefault(role, []).append(sub)
    out = []
    for role in (ROLE_BOOK5, ROLE_BOOK, ROLE_OTHER):
        for part in shard(groups.get(role, []), n):
            out.append((role, part))
    if len(out) > MAX_CONNECTIONS:
        raise ValueError(f"{len(out)} connections exceeds the {MAX_CONNECTIONS}/IP limit")
    return out
