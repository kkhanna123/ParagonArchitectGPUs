"""Record Kalshi market data for the families in adapters.py to hourly gzipped JSONL.

Same raw format as the trade[XYZ] recorder (recorder.hl_xyz_recorder), one line per record:
  {"rx_ns": <local receive ns>, "sh": 0, "rl": "ws",   "msg": <raw websocket message>}
  {"rx_ns": <ns>,               "sh": 0, "rl": "rest", "msg": {"type": "markets", ...}}
  {"rx_ns": <ns>, "event": "connected" | "disconnected" | "start" | "seq_gap", ...}

Websocket channels, per market: `orderbook_delta` (a full snapshot on subscribe, then one
message per price-level change), `ticker` (best bid/ask, last, volume, OI) and `trade`.
Kalshi will not subscribe a whole series, so each adapter's markets are discovered over REST
every `refresh_s` and added to / removed from the live subscriptions as they list and close
(`update_subscription`), which is what lets the 15-minute series roll without reconnecting.
Contract metadata (strikes, times, status, result, settlement value) is written as a `rest`
record whenever it changes, and once more after each market closes.

The websocket requires an API key (see common.load_credentials); REST market data does not.

    python -m recorder.kalshi.recorder                         # every adapter
    python -m recorder.kalshi.recorder --adapters fast,weather
    python -m recorder.kalshi.recorder --duration 60 --out C:/tmp/kx --no-lock
"""
import argparse
import asyncio
import json
import logging
import random
import time
from collections import Counter
from pathlib import Path

import websockets

from recorder.hl_common import AlreadyRunning, acquire_lock
from recorder.hl_xyz_recorder import HourlyWriter, install_stop_handlers
from recorder.kalshi.adapters import CHANNELS, GROUPS, select
from recorder.kalshi.common import (WS_PATH, WS_URL, Rest, kx_raw, load_credentials,
                                    market_meta, ms, series_of)

log = logging.getLogger("kx_recorder")

CHUNK = 250  # tickers per subscribe / update command


class Universe:
    """The markets each adapter currently wants recorded, kept fresh from REST."""

    def __init__(self, adapters, rest: Rest, writer: HourlyWriter):
        self.adapters, self.rest, self.writer = adapters, rest, writer
        self.close_ms = {}   # ticker -> close time (ms)
        self.channels = {}   # ticker -> channels its adapter records
        self.meta = {}       # ticker -> last metadata written
        self._linger_ms = {} # ticker -> its adapter's linger
        self.changed = asyncio.Event()

    def wanted(self, now_ms: int | None = None) -> dict:
        """channel -> set of tickers that should be subscribed right now."""
        now_ms = now_ms or int(time.time() * 1000)
        out = {ch: set() for ch in CHANNELS}
        for t, close in self.close_ms.items():
            if close is None or now_ms <= close + self._linger_ms[t]:
                for ch in self.channels[t]:
                    out[ch].add(t)
        return out

    def _list(self, adapter, series: str) -> list:
        now = time.time()
        found = list(self.rest.paginate("/markets", "markets", series_ticker=series,
                                        status="open"))
        if adapter.lead_s:
            soon = now + adapter.lead_s
            found += [m for m in self.rest.paginate("/markets", "markets", series_ticker=series,
                                                    status="unopened", max_pages=3)
                      if (ms(m.get("open_time")) or 0) / 1000 <= soon]
        if adapter.max_days_out is not None:
            horizon = (now + adapter.max_days_out * 86400) * 1000
            found = [m for m in found if (ms(m.get("close_time")) or 0) <= horizon]
        return found

    def _emit(self, series: str, adapter: str, markets: list, why: str):
        fresh = []
        for m in markets:
            meta = market_meta(m)
            if self.meta.get(m["ticker"]) != meta:
                self.meta[m["ticker"]] = meta
                fresh.append(meta)
        if fresh:
            self.writer.write_raw(time.time_ns(), 0, "rest", json.dumps(
                {"type": "markets", "why": why, "series": series, "adapter": adapter,
                 "markets": fresh}, separators=(",", ":")))

    def refresh(self, adapter) -> int:
        """List one adapter's markets; record metadata changes. Blocking (runs in a thread)."""
        n = 0
        for series in adapter.series:
            try:
                markets = self._list(adapter, series)
            except Exception as e:  # noqa: BLE001 -- keep the last known set and retry later
                log.warning("%s: listing %s failed: %s", adapter.name, series, e)
                continue
            for m in markets:
                t = m["ticker"]
                if t not in self.close_ms:
                    n += 1
                self.close_ms[t] = ms(m.get("close_time"))
                self.channels[t] = adapter.channels
                self._linger_ms[t] = int(adapter.linger_s * 1000)
            self._emit(series, adapter.name, markets, "listing")
        return n

    def finish(self) -> list:
        """Forget markets past close + linger, recording each one's final metadata."""
        now_ms = int(time.time() * 1000)
        done = [t for t, c in self.close_ms.items() if c is not None
                and now_ms > c + self._linger_ms[t]]
        for t in done:
            try:
                m = self.rest.get("/markets/%s" % t).get("market")
                if m:
                    self._emit(series_of(t), "", [m], "closed")
            except Exception as e:  # noqa: BLE001 -- the nightly backfill picks it up anyway
                log.debug("final metadata for %s failed: %s", t, e)
            for d in (self.close_ms, self.channels, self.meta, self._linger_ms):
                d.pop(t, None)
        return done


async def discover(u: Universe, adapter, stop: asyncio.Event):
    """Refresh one adapter on its own cadence until stopped."""
    while not stop.is_set():
        try:
            added = await asyncio.to_thread(u.refresh, adapter)
            gone = await asyncio.to_thread(u.finish)
            if added or gone:
                log.info("%s: +%d markets, -%d closed", adapter.name, added, len(gone))
                u.changed.set()
        except Exception:  # noqa: BLE001 -- never let discovery die
            log.exception("%s: discovery failed", adapter.name)
        try:
            await asyncio.wait_for(stop.wait(), timeout=adapter.refresh_s)
        except asyncio.TimeoutError:
            pass


class Feed:
    """One authenticated websocket, kept subscribed to Universe.wanted()."""

    def __init__(self, u: Universe, writer: HourlyWriter, creds, url: str = WS_URL):
        self.u, self.writer, self.creds, self.url = u, writer, creds, url
        self.ws = None
        self._reset()

    def _reset(self):
        self.sid = {}                               # channel -> sid
        self.subbed = {ch: set() for ch in CHANNELS}
        self.pending = {}                           # command id -> channel awaiting a sid
        self.last_seq = {}                          # sid -> last seq seen
        self._next_id = 0

    async def _send(self, cmd: str, params: dict, channel: str | None = None):
        self._next_id += 1
        if channel is not None:
            self.pending[self._next_id] = channel
        await self.ws.send(json.dumps({"id": self._next_id, "cmd": cmd, "params": params}))

    async def sync(self):
        """Bring the subscriptions in line with what the universe wants."""
        want = self.u.wanted()
        for ch in CHANNELS:
            if ch in self.pending.values():
                continue  # its sid is not known yet; resync once 'subscribed' arrives
            add = sorted(want[ch] - self.subbed[ch])
            drop = sorted(self.subbed[ch] - want[ch])
            sid = self.sid.get(ch)
            if drop and sid is not None:
                if not (self.subbed[ch] - set(drop)):
                    await self._send("unsubscribe", {"sids": [sid]})
                    self.sid.pop(ch)
                    self.last_seq.pop(sid, None)
                    sid = None
                else:
                    for i in range(0, len(drop), CHUNK):
                        await self._send("update_subscription", {
                            "sids": [sid], "market_tickers": drop[i:i + CHUNK],
                            "action": "delete_markets"})
                self.subbed[ch] -= set(drop)
            if not add:
                continue
            if sid is None:
                # The first chunk creates the sid; the rest wait for the next sync.
                first, add = add[:CHUNK], add[CHUNK:]
                await self._send("subscribe", {"channels": [ch], "market_tickers": first},
                                 channel=ch)
                self.subbed[ch] |= set(first)
                continue
            for i in range(0, len(add), CHUNK):
                await self._send("update_subscription", {
                    "sids": [sid], "market_tickers": add[i:i + CHUNK], "action": "add_markets"})
            self.subbed[ch] |= set(add)

    def handle(self, raw: str, counts: Counter) -> bool:
        """Book-keeping for one message. Returns True if the subscriptions need a resync."""
        try:
            d = json.loads(raw)
        except json.JSONDecodeError:
            return False
        typ = d.get("type")
        counts[typ] += 1
        if typ == "subscribed":
            ch = (d.get("msg") or {}).get("channel")
            sid = (d.get("msg") or {}).get("sid")
            self.pending.pop(d.get("id"), None)
            if ch:
                self.sid[ch] = sid
            return True
        if typ == "error":
            self.pending.pop(d.get("id"), None)
            log.warning("server error: %s", raw[:300])
            return False
        sid, seq = d.get("sid"), d.get("seq")
        if sid is not None and seq is not None:
            last = self.last_seq.get(sid)
            self.last_seq[sid] = seq
            if last is not None and seq != last + 1:
                # A lost book delta leaves every later level wrong until a fresh snapshot, and
                # resubscribing is the only way to get one: reconnect.
                self.writer.write_event("seq_gap", sid=sid, expected=last + 1, got=seq)
                raise ConnectionError("seq gap on sid %s: %s -> %s" % (sid, last, seq))
        return False

    async def run(self, stop: asyncio.Event, stale_after: float = 600.0,
                  max_backoff: float = 60.0, stats_every: float = 300.0):
        backoff = 1.0
        while not stop.is_set():
            reason = "stopped"
            try:
                async with websockets.connect(
                        self.url, additional_headers=self.creds.headers("GET", WS_PATH),
                        ping_interval=20, ping_timeout=30, max_size=None) as ws:
                    self.ws = ws
                    self._reset()
                    self.writer.write_event("connected", url=self.url)
                    await self.sync()
                    log.info("connected; subscribed %s",
                             {ch: len(s) for ch, s in self.subbed.items()})
                    backoff = 1.0
                    counts, last_stats = Counter(), time.monotonic()
                    while not stop.is_set():
                        recv = asyncio.ensure_future(ws.recv())
                        stopper = asyncio.ensure_future(stop.wait())
                        changed = asyncio.ensure_future(self.u.changed.wait())
                        done, _ = await asyncio.wait({recv, stopper, changed},
                                                     timeout=stale_after,
                                                     return_when=asyncio.FIRST_COMPLETED)
                        stopper.cancel()
                        changed.cancel()
                        if recv not in done:
                            recv.cancel()
                            if stop.is_set():
                                break
                            if self.u.changed.is_set():
                                self.u.changed.clear()
                                await self.sync()
                                continue
                            raise asyncio.TimeoutError("no message for %gs" % stale_after)
                        raw = recv.result()
                        if "\n" in raw:
                            # Kalshi ends every frame with a newline, which would split the
                            # record across lines. JSON escapes newlines inside strings, so a
                            # literal one is only ever whitespace and safe to drop.
                            raw = raw.replace("\n", "")
                        self.writer.write_raw(time.time_ns(), 0, "ws", raw)
                        if self.handle(raw, counts) or self.u.changed.is_set():
                            self.u.changed.clear()
                            await self.sync()
                        if time.monotonic() - last_stats > stats_every:
                            log.info("msgs/%ds: %s | subscribed %s | %.1f MB written",
                                     int(stats_every), dict(counts.most_common(6)),
                                     {ch: len(s) for ch, s in self.subbed.items()},
                                     self.writer.bytes_written / 1e6)
                            counts, last_stats = Counter(), time.monotonic()
            except asyncio.CancelledError:
                raise
            except Exception as e:  # network errors, server closes, seq gaps: all mean reconnect
                reason = "%s: %s" % (type(e).__name__, e)
                log.warning("websocket lost (%s); reconnecting in %.0fs", reason, backoff)
            finally:
                self.ws = None
            self.writer.write_event("disconnected", reason=reason)
            if stop.is_set():
                return
            try:
                await asyncio.wait_for(stop.wait(), timeout=backoff * random.uniform(0.8, 1.2))
            except asyncio.TimeoutError:
                pass
            backoff = min(backoff * 2, max_backoff)


def parse_args(argv=None):
    p = argparse.ArgumentParser(description=__doc__,
                                formatter_class=argparse.RawDescriptionHelpFormatter)
    p.add_argument("--adapters", default="all",
                   help="comma-separated adapters and/or groups (%s)" % ", ".join(GROUPS))
    p.add_argument("--out", type=Path, help="default: <PARAGON_RAW_ROOT>/kalshi")
    p.add_argument("--stale-after", type=float, default=600.0,
                   help="reconnect if the socket is silent this long (pings run regardless)")
    p.add_argument("--duration", type=float, help="stop after N seconds (for test recordings)")
    p.add_argument("--no-lock", action="store_true",
                   help="skip the single-instance lock on the output directory")
    return p.parse_args(argv)


async def amain(args):
    adapters = select([n.strip() for n in args.adapters.split(",") if n.strip()])
    creds = load_credentials()
    if creds is None:
        raise SystemExit("no Kalshi API key: set KALSHI_API_KEY_ID + KALSHI_PRIVATE_KEY_PATH, "
                         "or KALSHI_ENV_FILE pointing at a .env that does")
    base = args.out or kx_raw()
    lock = None
    if not args.no_lock:
        try:
            lock = acquire_lock(base.parent / (".%s.lock" % base.name))
        except AlreadyRunning as e:
            raise SystemExit("refusing to start: %s" % e)

    writer = HourlyWriter(base)
    rest = Rest(creds)
    u = Universe(adapters, rest, writer)
    writer.write_event("start", adapters={a.name: list(a.series) for a in adapters})
    # One synchronous pass first, so the socket opens with the whole set in one go.
    for a in adapters:
        n = await asyncio.to_thread(u.refresh, a)
        log.info("%-16s %3d markets across %s", a.name, n, ", ".join(a.series))
    log.info("recording %d markets -> %s", len(u.close_ms), base)

    stop = asyncio.Event()
    install_stop_handlers(stop)
    feed = Feed(u, writer, creds)
    tasks = [asyncio.create_task(feed.run(stop, stale_after=args.stale_after))]
    tasks += [asyncio.create_task(discover(u, a, stop)) for a in adapters]
    if args.duration:
        async def _stop_after():
            try:
                await asyncio.wait_for(stop.wait(), timeout=args.duration)
            except asyncio.TimeoutError:
                log.info("duration reached, stopping")
                stop.set()
        tasks.append(asyncio.create_task(_stop_after()))
    try:
        await asyncio.gather(*tasks)
    finally:
        stop.set()
        for t in tasks:
            t.cancel()
        writer.close()
        if lock:
            lock.close()
        log.info("stopped; wrote %.1f MB of JSON, %d REST calls", writer.bytes_written / 1e6,
                 rest.calls)


def main(argv=None):
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    asyncio.run(amain(parse_args(argv)))


if __name__ == "__main__":
    main()
