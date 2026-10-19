"""Record Hyperliquid websocket market data to hourly gzipped JSONL files (same format as ax_recorder).

Works for any HIP-3 dex: Markets.xyz (Kinetiq, `mkts`), trade[XYZ] (`xyz`), Paragon (`para`), ...

Every line is one of:
  {"rx_ns": <local receive time, ns>, "msg": <raw server message>}
  {"rx_ns": <ns>, "event": "started" | "connected" | "disconnected" | "markets_changed" | "stopped", ...}

Every --snapshot-every seconds the REST market list (metaAndAssetCtxs) is stored too, as
  {"rx_ns": <ns>, "msg": {"channel": "metaAndAssetCtxs", "dex": <dex>, "data": <raw response>}}
so the raw files hold everything. `hl_to_parquet.py` turns them into tables (and runs
automatically for each finished UTC day).

    python -m recorder.hl_recorder                                  # Markets.xyz, every live market
    python -m recorder.hl_recorder --dex xyz --coins CL,GOLD,SP500  # trade[XYZ]
    python -m recorder.hl_check --status                            # is it healthy?
"""
import argparse
import asyncio
import json
import logging
import os
import random
import signal
import statistics
import threading
import time
from collections import Counter, defaultdict, deque
from datetime import datetime, timedelta, timezone
from logging.handlers import RotatingFileHandler
from pathlib import Path

import aiohttp
import websockets

from recorder.ax_recorder import HourlyWriter

ROOT = Path(__file__).resolve().parents[1]
API_URL = "https://api.hyperliquid.xyz/info"
WS_URL = "wss://api.hyperliquid.xyz/ws"
CHANNELS = ("l2Book", "trades", "activeAssetCtx", "bbo")
KINETIQ_HINTS = ("kinetiq", "mkts", "km")   # how Markets.xyz's dexes are recognised in perpDexs
SUBS_PER_CONN = 100                         # Hyperliquid allows 1000 subscriptions / 10 sockets per IP
PING_EVERY = 30                             # Hyperliquid closes sockets that are silent for 60s
RESTART_BACKOFF = 5.0                       # first wait after a crash; doubles up to 5 min
RETRY_DELAY = 1.0                           # first wait after a failed REST call; doubles up to 30s
log = logging.getLogger("hl_recorder")


# ---------------------------------------------------------------- REST
async def info(session, api_url, body, retries=5):
    delay = RETRY_DELAY
    for attempt in range(1, retries + 1):
        try:
            async with session.post(api_url, json=body, timeout=aiohttp.ClientTimeout(total=15)) as r:
                r.raise_for_status()
                return await r.json(content_type=None)
        except Exception as e:  # noqa: BLE001  (network errors, 5xx, bad JSON)
            if attempt == retries:
                raise RuntimeError(f"REST {body.get('type')} failed after {retries} tries: {e}") from e
            log.warning("REST %s failed (%s); retrying in %.0fs", body.get("type"), e, delay)
            await asyncio.sleep(delay)
            delay = min(delay * 2, 30)


def full_name(dex, coin):
    return coin if ":" in coin else f"{dex}:{coin}"


def live_coins(meta_and_ctxs):
    meta = meta_and_ctxs[0]
    return sorted(u["name"] for u in meta.get("universe", []) if not u.get("isDelisted"))


async def resolve_dex(session, api_url, dex):
    """'auto' -> the Markets.xyz (Kinetiq) dex with the most live markets. Kinetiq has an old,
    empty `km` dex next to the live `mkts`, so matching on the name alone picks the wrong one."""
    if dex != "auto":
        return dex
    best, best_n = None, 0
    for d in [d for d in await info(session, api_url, {"type": "perpDexs"}) if d]:
        name, hay = d.get("name", ""), f"{d.get('name', '')} {d.get('fullName', '')}".lower()
        if not any(h == name.lower() or (len(h) > 2 and h in hay) for h in KINETIQ_HINTS):
            continue
        n = len(live_coins(await info(session, api_url, {"type": "metaAndAssetCtxs", "dex": name})))
        log.info("candidate dex %r (%s): %d live markets", name, d.get("fullName"), n)
        if n > best_n:
            best, best_n = name, n
    if not best:
        raise RuntimeError("no Markets.xyz dex with live markets found; pass --dex explicitly")
    return best


def build_subscriptions(coins, channels):
    return [{"method": "subscribe", "subscription": {"type": ch, "coin": c}} for c in coins for ch in channels]


# ---------------------------------------------------------------- health
class Stats:
    """Per-market freshness and feed latency, written to status.json for `hl_check --status`."""

    def __init__(self):
        self.last_rx = {}
        self.counts = defaultdict(Counter)
        self.latency_ms = defaultdict(lambda: deque(maxlen=300))
        self.connections = {}
        self.reconnects = 0

    def seen(self, rx_ns, raw):
        if raw.startswith('{"channel":"pong"') or raw.startswith('{"channel":"subscriptionResponse"'):
            return
        try:
            m = json.loads(raw)
        except ValueError:
            return
        data = m.get("data")
        first = data[0] if isinstance(data, list) and data else data
        if not isinstance(first, dict) or "coin" not in first:
            return
        coin = first["coin"]
        self.last_rx[coin] = rx_ns
        self.counts[coin][m.get("channel")] += 1
        if first.get("time"):
            self.latency_ms[coin].append(rx_ns / 1e6 - first["time"])

    def to_status(self, dex, coins, started_ns):
        now = time.time_ns()
        markets = {}
        for c in coins:
            last = self.last_rx.get(c)
            lat = self.latency_ms.get(c)
            markets[c] = {"seconds_since_update": round((now - last) / 1e9, 1) if last else None,
                          "msgs": dict(self.counts.get(c, {})),
                          "latency_ms_median": round(statistics.median(lat)) if lat else None}
        all_lat = [x for d in self.latency_ms.values() for x in d]
        warnings = [f"{c}: no data for over 2 minutes" for c, m in markets.items()
                    if m["seconds_since_update"] is None or m["seconds_since_update"] > 120]
        if all_lat and not -500 < statistics.median(all_lat) < 2000:
            warnings.append("median feed latency looks wrong: check this computer's clock is synced")
        return {"updated": datetime.now(timezone.utc).isoformat(timespec="seconds"),
                "started": datetime.fromtimestamp(started_ns / 1e9, timezone.utc).isoformat(timespec="seconds"),
                "pid": os.getpid(), "dex": dex, "markets": markets, "connections": self.connections,
                "reconnects": self.reconnects, "warnings": warnings}


# ---------------------------------------------------------------- websocket
async def record(ws_url, subscriptions, writer, stop, stats=None, conn=0, stale_after=60.0,
                 max_backoff=60.0, stats_every=300.0):
    """Connect, subscribe, and write every message until `stop` is set. Reconnects on any failure."""
    stats = stats or Stats()
    backoff = 1.0
    while not stop.is_set():
        reason = "stopped"
        try:
            async with websockets.connect(ws_url, ping_interval=None, max_size=None, open_timeout=15,
                                          close_timeout=2) as ws:
                for m in subscriptions:
                    await ws.send(json.dumps(m))
                writer.write_event("connected", conn=conn, url=ws_url, n_subscriptions=len(subscriptions))
                stats.connections[str(conn)] = "connected"
                log.info("socket %d connected, %d subscriptions", conn, len(subscriptions))
                backoff = 1.0
                counts, last_stats = Counter(), time.monotonic()
                last_msg = last_ping = time.monotonic()
                while not stop.is_set():
                    try:
                        raw = await asyncio.wait_for(ws.recv(), timeout=5)
                    except asyncio.TimeoutError:
                        raw = None
                    now = time.monotonic()
                    if raw is not None:
                        rx = time.time_ns()
                        writer.write_raw(rx, raw)
                        stats.seen(rx, raw)
                        last_msg = now
                        counts[raw[12:raw.find('"', 12)] if raw.startswith('{"channel":"') else "other"] += 1
                        if raw.startswith('{"channel":"error"'):
                            log.warning("server rejected request: %s", raw[:300])
                    if now - last_ping >= PING_EVERY:
                        await ws.send('{"method":"ping"}')
                        last_ping = now
                    if now - last_msg > stale_after:
                        raise asyncio.TimeoutError(f"no message for {stale_after:.0f}s")
                    if now - last_stats > stats_every:
                        log.info("socket %d msgs last %ds: %s", conn, stats_every, dict(counts))
                        counts, last_stats = Counter(), now
        except asyncio.CancelledError:
            writer.write_event("disconnected", conn=conn, reason="resubscribing")
            stats.connections.pop(str(conn), None)
            raise
        except Exception as e:  # network errors, server closes, stale feed: all mean reconnect
            reason = f"{type(e).__name__}: {e}"
            log.warning("socket %d lost (%s); reconnecting in %.0fs", conn, reason, backoff)
            stats.reconnects += 1
        writer.write_event("disconnected", conn=conn, reason=reason)
        stats.connections[str(conn)] = "reconnecting"
        if stop.is_set():
            break
        try:
            await asyncio.wait_for(stop.wait(), timeout=backoff * random.uniform(0.8, 1.2))
        except asyncio.TimeoutError:
            pass
        backoff = min(backoff * 2, max_backoff)


# ---------------------------------------------------------------- recorder
class Recorder:
    def __init__(self, args, stop):
        self.a, self.stop = args, stop
        self.stats = Stats()
        self.dex, self.coins, self.base, self.writer = None, [], None, None
        self.resubscribe = asyncio.Event()
        self.started_ns = time.time_ns()

    def choose(self, live):
        if not self.a.coins:
            return live
        want = {full_name(self.dex, c) for c in self.a.coins.split(",") if c}
        missing = sorted(want - set(live))
        if missing and missing != getattr(self, "_missing", None):
            log.warning("not live on %r, skipped: %s", self.dex, ", ".join(missing))
        self._missing = missing
        return [c for c in live if c in want]

    async def snapshot(self, session):
        data = await info(session, self.a.api_url, {"type": "metaAndAssetCtxs", "dex": self.dex})
        rx = time.time_ns()
        self.writer.write_raw(rx, json.dumps({"channel": "metaAndAssetCtxs", "dex": self.dex, "data": data},
                                             separators=(",", ":")))
        coins = self.choose(live_coins(data))
        if coins != self.coins:
            added, removed = sorted(set(coins) - set(self.coins)), sorted(set(self.coins) - set(coins))
            if self.coins:
                log.info("market list changed: added %s, removed %s", added, removed)
            self.writer.write_event("markets_changed", added=added, removed=removed)
            self.coins = coins
            self.resubscribe.set()

    async def snapshot_loop(self, session):
        while not await self.sleep(self.a.snapshot_every):
            try:
                await self.snapshot(session)
            except Exception as e:  # noqa: BLE001
                log.warning("market snapshot failed: %s", e)

    async def ws_loop(self):
        """(Re)subscribe whenever the market list changes. New sockets open before old ones close."""
        old = []
        try:
            while not self.stop.is_set():
                await self.wait_for(self.resubscribe)
                if self.stop.is_set():
                    break
                self.resubscribe.clear()
                subs = build_subscriptions(self.coins, self.a.channels.split(","))
                chunks = [subs[i:i + SUBS_PER_CONN] for i in range(0, len(subs), SUBS_PER_CONN)]
                new = [asyncio.create_task(record(self.a.ws_url, c, self.writer, self.stop, self.stats,
                                                  conn=i, stale_after=self.a.stale_after))
                       for i, c in enumerate(chunks)]
                for t in old:
                    t.cancel()
                await asyncio.gather(*old, return_exceptions=True)
                old = new
        finally:
            for t in old:
                t.cancel()
            await asyncio.gather(*old, return_exceptions=True)

    async def status_loop(self):
        path = self.base / "status.json"
        while True:
            try:
                tmp = path.with_suffix(".tmp")
                tmp.write_text(json.dumps(self.stats.to_status(self.dex, self.coins, self.started_ns), indent=1))
                os.replace(tmp, path)
            except OSError as e:
                log.warning("could not write status: %s", e)
            if await self.sleep(self.a.status_every):
                return

    async def convert_loop(self):
        """Convert every finished UTC day that has raw data but no parquet yet (incl. at startup)."""
        if self.a.no_convert:
            return
        from recorder import hl_to_parquet
        out = self.a.parquet_out or ROOT / "Data" / "parquet" / "hyperliquid" / self.dex
        while True:
            # wait 10 minutes past midnight so the last hour's file is closed
            cutoff = (datetime.now(timezone.utc) - timedelta(minutes=10)).strftime("%Y-%m-%d")
            for day in sorted(d.name for d in self.base.iterdir() if d.is_dir() and d.name < cutoff):
                if not any((out / t / f"{day}.parquet").exists() for t in hl_to_parquet.TABLES):
                    try:
                        written = await asyncio.to_thread(hl_to_parquet.convert_day, self.base, out, day,
                                                          self.a.depth)
                        log.info("converted %s to parquet: %s", day,
                                 ", ".join(f"{k}={n:,}" for k, (_, n) in written.items()))
                    except Exception as e:  # noqa: BLE001
                        log.error("parquet conversion of %s failed: %s", day, e)
            if await self.sleep(600):
                return

    async def sleep(self, seconds):
        """Sleep, returning True early if we are stopping."""
        try:
            await asyncio.wait_for(self.stop.wait(), timeout=seconds)
            return True
        except asyncio.TimeoutError:
            return False

    async def wait_for(self, event):
        waiters = [asyncio.create_task(event.wait()), asyncio.create_task(self.stop.wait())]
        await asyncio.wait(waiters, return_when=asyncio.FIRST_COMPLETED)
        for w in waiters:
            w.cancel()

    async def run(self, lock):
        async with aiohttp.ClientSession() as session:
            self.dex = await resolve_dex(session, self.a.api_url, self.a.dex)
            self.base = Path(self.a.out or ROOT / "Data" / "raw" / "hyperliquid" / self.dex)
            if not lock(self.base):
                raise SystemExit(f"another hl_recorder is already writing to {self.base}")
            self.writer = HourlyWriter(self.base)
            try:
                await self.snapshot(session)
                if not self.coins:
                    raise RuntimeError(f"no live markets to record on dex {self.dex!r}")
                log.info("recording %d markets on %r (%s) to %s", len(self.coins), self.dex,
                         " ".join(c.split(":", 1)[1] for c in self.coins), self.base)
                self.writer.write_event("started", dex=self.dex, coins=self.coins, channels=self.a.channels,
                                        pid=os.getpid())
                if self.a.duration:
                    asyncio.get_running_loop().call_later(self.a.duration, self.stop.set)
                tasks = [asyncio.create_task(c) for c in (self.snapshot_loop(session), self.ws_loop(),
                                                           self.status_loop(), self.convert_loop())]
                stopper = asyncio.create_task(self.stop.wait())
                done, _ = await asyncio.wait([stopper, *tasks], return_when=asyncio.FIRST_COMPLETED)
                crashed = None if self.stop.is_set() else next(t for t in done if t is not stopper)
                self.stop.set()
                await asyncio.gather(*tasks, return_exceptions=True)
                if crashed:
                    raise crashed.exception() or RuntimeError("background task exited")
            finally:
                self.writer.write_event("stopped")
                self.writer.close()


# ---------------------------------------------------------------- process
class FolderLock:
    """One recorder per output folder. Uses the OS file lock, so it frees itself if the process dies."""

    def __init__(self):
        self.fh = None

    def __call__(self, base):
        if self.fh:
            return True
        base.mkdir(parents=True, exist_ok=True)
        fh = open(base / ".lock", "a+")
        try:
            if os.name == "nt":
                import msvcrt
                fh.seek(0)
                msvcrt.locking(fh.fileno(), msvcrt.LK_NBLCK, 1)
            else:
                import fcntl
                fcntl.flock(fh.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
        except OSError:
            fh.close()
            return False
        self.fh = fh
        return True

    def release(self):
        if self.fh:
            self.fh.close()
            self.fh = None


def parse_args(argv=None):
    p = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    p.add_argument("--dex", default="auto", help="HIP-3 dex: mkts (Markets.xyz), xyz (trade[XYZ]), para, ...; "
                                                 "auto = Markets.xyz")
    p.add_argument("--coins", help="comma-separated, e.g. US500,USTECH; default = every live market")
    p.add_argument("--channels", default=",".join(CHANNELS), help=f"default: {','.join(CHANNELS)}")
    p.add_argument("--depth", type=int, default=5, help="book levels per side in the daily parquet "
                                                         "(raw files always keep all 20)")
    p.add_argument("--out", type=Path, help="default: Data/raw/hyperliquid/<dex>")
    p.add_argument("--parquet-out", type=Path, help="default: Data/parquet/hyperliquid/<dex>")
    p.add_argument("--no-convert", action="store_true", help="don't convert finished days to parquet")
    p.add_argument("--snapshot-every", type=float, default=60, help="seconds between REST market snapshots")
    p.add_argument("--status-every", type=float, default=10)
    p.add_argument("--stale-after", type=float, default=60.0, help="reconnect if a socket is silent this long")
    p.add_argument("--duration", type=float, help="stop after N seconds (default: run until Ctrl-C)")
    p.add_argument("--api-url", default=API_URL)
    p.add_argument("--ws-url", default=WS_URL)
    a = p.parse_args(argv)
    bad = set(a.channels.split(",")) - set(CHANNELS)
    if bad:
        p.error(f"unknown channels {sorted(bad)}")
    if not 1 <= a.depth <= 20:
        p.error("--depth must be 1-20")
    return a


def setup_logging(log_file=None):
    root = logging.getLogger()
    root.handlers.clear()
    root.setLevel(logging.INFO)
    fmt = logging.Formatter("%(asctime)s %(levelname)s %(message)s")
    handlers = [logging.StreamHandler()]
    if log_file:
        log_file.parent.mkdir(parents=True, exist_ok=True)
        handlers.append(RotatingFileHandler(log_file, maxBytes=10_000_000, backupCount=5, encoding="utf-8"))
    for h in handlers:
        h.setFormatter(fmt)
        root.addHandler(h)
    logging.getLogger("websockets").setLevel(logging.WARNING)


def run(args):
    """Run until Ctrl-C / SIGTERM / --duration, restarting after crashes or outages."""
    state = {"stopping": False, "loop": None, "stop": None}

    def request_stop(*_):
        if state["stopping"]:
            raise KeyboardInterrupt  # second Ctrl-C: skip the clean shutdown
        state["stopping"] = True
        log.info("stopping (Ctrl-C again to force)")
        if state["loop"]:
            state["loop"].call_soon_threadsafe(state["stop"].set)

    # signal.signal works on Windows too (loop.add_signal_handler does not)
    previous = {}
    if threading.current_thread() is threading.main_thread():
        for name in ("SIGINT", "SIGTERM", "SIGBREAK"):
            if hasattr(signal, name):
                previous[name] = signal.signal(getattr(signal, name), request_stop)

    async def once():
        state["loop"], state["stop"] = asyncio.get_running_loop(), asyncio.Event()
        if state["stopping"]:
            state["stop"].set()
        await Recorder(args, state["stop"]).run(lock)

    lock = FolderLock()
    deadline = time.monotonic() + args.duration if args.duration else None
    backoff = RESTART_BACKOFF
    try:
        while not state["stopping"]:
            if deadline:
                args.duration = deadline - time.monotonic()
                if args.duration <= 0:
                    break
            started = time.monotonic()
            try:
                asyncio.run(once())
                break
            except KeyboardInterrupt:
                break
            except Exception as e:  # noqa: BLE001
                if state["stopping"]:
                    break
                if time.monotonic() - started > 600:
                    backoff = RESTART_BACKOFF
                log.error("recorder crashed (%s: %s); restarting in %.0fs", type(e).__name__, e, backoff)
                end = time.monotonic() + backoff
                while time.monotonic() < end and not state["stopping"]:
                    time.sleep(0.2)
                backoff = min(backoff * 2, 300)
    finally:
        lock.release()
        for name, h in previous.items():
            signal.signal(getattr(signal, name), h)
    log.info("stopped")


def main(argv=None):
    args = parse_args(argv)
    setup_logging(ROOT / "Data" / "raw" / "hyperliquid" / f"recorder_{args.dex}.log")
    run(args)


if __name__ == "__main__":
    main()
