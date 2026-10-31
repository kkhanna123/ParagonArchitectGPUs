"""Record the Hyperliquid / trade[XYZ] websocket feed to hourly gzipped JSONL files.

Every line is one of:
  {"rx_ns": <local receive ns>, "sh": <shard>, "rl": <role>, "msg": <raw server message>}
  {"rx_ns": <ns>, "event": "connected" | "disconnected" | "start", ...}

Event records mark session boundaries so downstream code can find gaps. Raw messages are
stored untouched; `hl_xyz_to_parquet.py` turns them into tables.

Depth and cadence are a trade-off on this feed, and `fast`-tier markets get both sides of it:
`{"type":"l2Book","coin":C,"fast":true}` gives 5 levels/side every ~0.54s, while the plain
subscription gives 20 levels/side every ~5.4s. They are independent streams, so both are held
at once. Since the wire message says nothing about which mode produced it, each mode gets its
own connection and every record is tagged with that connection's role (`rl`).

See Data/tradexyz/storage.md for the measurements behind all of this.

(Restored 2026-10-25 from bytecode after the source was overwritten by an unrelated
`hl_recorder.py` in commit 1f983cf; renamed so the two no longer collide.)

    python -m recorder.hl_xyz_recorder --watchlist all       # the permanent collection set
    python -m recorder.hl_xyz_recorder --watchlist core      # the original 22-name core list
    python -m recorder.hl_xyz_recorder                      # auto universe, 0.5s books on tier-1
    python -m recorder.hl_xyz_recorder --n-fast 20          # a smaller 0.5s set
    python -m recorder.hl_xyz_recorder --coins xyz:CL,xyz:GOLD --all-fast
"""
import argparse
import asyncio
import gzip
import json
import logging
import random
import signal
import time
from collections import Counter
from datetime import datetime, timezone
from pathlib import Path

import websockets

from recorder.hl_common import (AlreadyRunning, ROLE_OTHER, WATCHLISTS, WS_URL, MAX_CONNECTIONS,
                                acquire_lock, assign_tiers, build_subscriptions, fetch_universe,
                                live_coins, raw_root, shard_by_role)

log = logging.getLogger("hl_recorder")


class HourlyWriter:
    """Appends records to <base>/<YYYY-MM-DD>/<HH>.jsonl.gz, rotating on the UTC hour.

    Safe to share across shards: every write completes without awaiting, so the
    single-threaded event loop never interleaves two of them.
    """

    def __init__(self, base: Path, flush_every: float = 5.0, compresslevel: int = 6):
        self.base = Path(base)
        self.flush_every = flush_every
        self.compresslevel = compresslevel
        self.bytes_written = 0
        self._fh = None
        self._path = None
        self._last_flush = 0.0

    def path_for(self, ns: int) -> Path:
        dt = datetime.fromtimestamp(ns / 1e9, tz=timezone.utc)
        return self.base / dt.strftime("%Y-%m-%d") / f"{dt:%H}.jsonl.gz"

    def _line(self, ns: int, line: str):
        path = self.path_for(ns)
        if path != self._path:
            self.close()
            path.parent.mkdir(parents=True, exist_ok=True)
            # append mode: a restart within the hour adds a second gzip member, which readers handle
            self._fh = gzip.open(path, "at", compresslevel=self.compresslevel)
            self._path = path
        self._fh.write(line + "\n")
        self.bytes_written += len(line) + 1
        now = time.monotonic()
        if now - self._last_flush > self.flush_every:
            self._fh.flush()
            self._last_flush = now

    def write_raw(self, ns: int, sh: int, role: str, raw: str):
        # The server message is already JSON; splicing it in avoids a parse + re-dump per
        # message, which is most of the CPU at 0.5s cadence across dozens of books.
        self._line(ns, '{"rx_ns":%d,"sh":%d,"rl":"%s","msg":%s}' % (ns, sh, role, raw))

    def write_event(self, event: str, **fields):
        ns = time.time_ns()
        self._line(ns, json.dumps({"rx_ns": ns, "event": event, **fields}, separators=(",", ":")))

    def close(self):
        if self._fh:
            self._fh.close()
        self._fh = None
        self._path = None


async def _pinger(ws, every: float = 45.0):
    """Hyperliquid drops sockets idle for 60s; a thin market's trades stream can be that quiet."""
    while True:
        await asyncio.sleep(every)
        await ws.send('{"method":"ping"}')


def _channel_of(raw: str) -> str:
    """Cheap channel sniff for the stats line, without parsing the JSON.

    Finds the value of the "channel" key; robust to whitespace after the colon. Returns '?'
    when there is no channel key.
    """
    i = raw.find('"channel"')
    if i < 0:
        return "?"
    start = raw.find('"', i + 9)
    if start < 0:
        return "?"
    end = raw.find('"', start + 1)
    return raw[start + 1:end] if end > start else "?"


async def run_shard(sh: int, subs: list, writer: HourlyWriter, stop: asyncio.Event,
                    role: str = ROLE_OTHER, url: str = WS_URL, stale_after: float = 30.0,
                    max_backoff: float = 60.0, stats_every: float = 300.0,
                    sub_pause: float = 0.02):
    """Hold one websocket for `subs`, reconnecting with jittered backoff until `stop` is set.

    `stale_after` catches the failure the library cannot see: a socket that stays open but
    stops delivering. Every role here carries at least one ~0.5-5s stream, so silence that
    long means the feed is dead, not quiet.
    """
    backoff = 1.0
    while not stop.is_set():
        reason = "stopped"
        ping = None
        try:
            async with websockets.connect(url, ping_interval=None, max_size=None) as ws:
                writer.write_event("connected", shard=sh, role=role, url=url,
                                   n_subscriptions=len(subs))
                log.info("shard %d (%s) connected, sending %d subscriptions", sh, role, len(subs))
                for s in subs:
                    await ws.send(json.dumps({"method": "subscribe", "subscription": s}))
                    # pace the burst: 2000 outbound messages/min is the per-IP cap
                    if sub_pause:
                        await asyncio.sleep(sub_pause)
                ping = asyncio.create_task(_pinger(ws))
                backoff = 1.0
                counts, last_stats = Counter(), time.monotonic()
                while not stop.is_set():
                    recv = asyncio.ensure_future(ws.recv())
                    stopper = asyncio.ensure_future(stop.wait())
                    done, _ = await asyncio.wait({recv, stopper}, timeout=stale_after,
                                                 return_when=asyncio.FIRST_COMPLETED)
                    stopper.cancel()
                    if recv not in done:
                        recv.cancel()
                        if stop.is_set():
                            break
                        raise asyncio.TimeoutError("no message for %gs" % stale_after)
                    raw = recv.result()
                    writer.write_raw(time.time_ns(), sh, role, raw)
                    if '"channel":"error"' in raw:
                        log.warning("shard %d server error: %s", sh, raw[:300])
                    counts[_channel_of(raw)] += 1
                    if time.monotonic() - last_stats > stats_every:
                        log.info("shard %d (%s) msgs/%ds: %s | %.1f MB written", sh, role,
                                 int(stats_every), dict(counts.most_common(6)),
                                 writer.bytes_written / 1e6)
                        counts, last_stats = Counter(), time.monotonic()
        except asyncio.CancelledError:
            raise
        except Exception as e:  # network errors, server closes, stale feed: all mean reconnect
            reason = f"{type(e).__name__}: {e}"
            log.warning("shard %d lost (%s); reconnecting in %.0fs", sh, reason, backoff)
        finally:
            if ping:
                ping.cancel()
        writer.write_event("disconnected", shard=sh, role=role, reason=reason)
        if stop.is_set():
            return
        try:
            await asyncio.wait_for(stop.wait(), timeout=backoff * random.uniform(0.8, 1.2))
        except asyncio.TimeoutError:
            pass
        backoff = min(backoff * 2, max_backoff)


def install_stop_handlers(stop: asyncio.Event):
    """SIGINT/SIGTERM set `stop`. Windows lacks add_signal_handler, hence the fallback."""
    loop = asyncio.get_running_loop()
    for sig in (signal.SIGINT, signal.SIGTERM):
        try:
            loop.add_signal_handler(sig, stop.set)
        except (NotImplementedError, AttributeError):
            signal.signal(sig, lambda *_: loop.call_soon_threadsafe(stop.set))


def parse_args(argv=None):
    p = argparse.ArgumentParser(description=__doc__,
                                formatter_class=argparse.RawDescriptionHelpFormatter)
    p.add_argument("--coins", help="comma-separated coins; default = auto from volume + the plan's list")
    p.add_argument("--watchlist", choices=sorted(WATCHLISTS),
                   help="a named set recorded entirely on both book streams (see hl_common.CORE)")
    p.add_argument("--n-fast", type=int, default=56, help="markets to record at 0.5s depth")
    p.add_argument("--min-vlm", type=float, default=100_000.0,
                   help="drop markets below this 24h notional unless they are in ALWAYS")
    p.add_argument("--all-fast", action="store_true", help="0.5s books for every --coins entry")
    p.add_argument("--bbo", action="store_true", help="also record the bbo stream on fast markets")
    p.add_argument("--connections", type=int, default=3,
                   help="shards per role; roles get separate connections (max %d total)"
                        % MAX_CONNECTIONS)
    p.add_argument("--out", type=Path, help="default: <PARAGON_RAW_ROOT>/hl")
    p.add_argument("--stale-after", type=float, default=30.0, help="reconnect if silent this long")
    p.add_argument("--duration", type=float, help="stop after N seconds (for test recordings)")
    p.add_argument("--no-lock", action="store_true",
                   help="skip the single-instance lock on the output directory")
    return p.parse_args(argv)


def resolve_tiers(args) -> dict:
    if args.watchlist:
        # A watchlist is the research set: every name gets both book streams, volume regardless.
        # Names no longer listed are dropped with a warning instead of drawing a server error
        # on every reconnect.
        want = WATCHLISTS[args.watchlist]
        try:
            live = live_coins({c.rpartition(":")[0] for c in want})
        except Exception as e:  # noqa: BLE001 -- REST down at boot: record what we were asked for
            log.warning("could not check which markets are live (%s); subscribing to all", e)
            live = set(want)
        dead = [c for c in want if c not in live]
        if dead:
            log.warning("not live, skipped: %s", ", ".join(dead))
        return {c: "fast" for c in want if c in live}
    if args.coins:
        coins = [c.strip() for c in args.coins.split(",") if c.strip()]
        if args.all_fast:
            return {c: "fast" for c in coins}
        universe = [m for m in fetch_universe() if m["coin"] in set(coins)]
    else:
        universe = fetch_universe()
    tiers = assign_tiers(universe, n_fast=args.n_fast, min_vlm=args.min_vlm)
    if args.all_fast:
        tiers = {c: "fast" for c in tiers}
    return tiers


async def _stop_after(seconds: float, stop: asyncio.Event):
    try:
        await asyncio.wait_for(stop.wait(), timeout=seconds)
    except asyncio.TimeoutError:
        log.info("duration reached, stopping")
        stop.set()


async def amain(args):
    writer_base = args.out or raw_root() / "hl"
    lock = None
    if not args.no_lock:
        try:
            lock = acquire_lock(writer_base.parent / (".%s.lock" % writer_base.name))
        except AlreadyRunning as e:
            raise SystemExit("refusing to start: %s" % e)

    tiers = resolve_tiers(args)
    if not tiers:
        raise SystemExit("no markets selected")
    subs = build_subscriptions(tiers, bbo=args.bbo)
    shards = shard_by_role(subs, args.connections)
    writer = HourlyWriter(writer_base)
    n_fast = sum(t == "fast" for t in tiers.values())
    log.info("recording %d markets (%d fast / %d slow), %d subscriptions over %d connections -> %s",
             len(tiers), n_fast, len(tiers) - n_fast, len(subs), len(shards), writer.base)
    for i, (role, part) in enumerate(shards):
        log.info("  shard %d: %-6s %d subscriptions", i, role, len(part))
    writer.write_event("start", tiers=tiers, n_subscriptions=len(subs),
                       shards=[[role, len(part)] for role, part in shards])

    stop = asyncio.Event()
    install_stop_handlers(stop)
    tasks = [asyncio.create_task(run_shard(i, part, writer, stop, role=role,
                                           stale_after=args.stale_after))
             for i, (role, part) in enumerate(shards)]
    if args.duration:
        tasks.append(asyncio.create_task(_stop_after(args.duration, stop)))
    try:
        await asyncio.gather(*tasks)
    finally:
        stop.set()
        for t in tasks:
            t.cancel()
        writer.close()
        if lock:
            lock.close()
        log.info("stopped; wrote %.1f MB of JSON", writer.bytes_written / 1e6)


def main(argv=None):
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    asyncio.run(amain(parse_args(argv)))


if __name__ == "__main__":
    main()
