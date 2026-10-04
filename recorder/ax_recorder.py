"""Record Architect AX websocket market data to hourly gzipped JSONL files.

Every line is one of:
  {"rx_ns": <local receive time, ns>, "msg": <raw server message>}
  {"rx_ns": <ns>, "event": "connected" | "disconnected", ...}

Event records mark session boundaries so downstream code can detect gaps.
Raw messages are stored untouched; `ax_to_parquet.py` turns them into tables.

    python -m recorder.ax_recorder --env sandbox
    python -m recorder.ax_recorder --env prod --symbols NVDA-H100-2026-DEC,WTIOIL-PERP
"""
import argparse
import asyncio
import gzip
import json
import logging
import os
import random
import signal
import time
from collections import Counter
from datetime import datetime, timezone
from pathlib import Path

import aiohttp
import websockets
from dotenv import load_dotenv

HOSTS = {
    "sandbox": "gateway.sandbox.architect.exchange",
    "prod": "gateway.architect.exchange",
}
ROOT = Path(__file__).resolve().parents[1]
log = logging.getLogger("ax_recorder")


class HourlyWriter:
    """Appends records to <base>/<YYYY-MM-DD>/<HH>.jsonl.gz, rotating on the UTC hour."""

    def __init__(self, base: Path, flush_every: float = 5.0):
        self.base = Path(base)
        self.flush_every = flush_every
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
            # Appending adds a new gzip member, so restarts mid-hour stay readable.
            self._fh = gzip.open(path, "at")
            self._path = path
        self._fh.write(line + "\n")
        now = time.monotonic()
        if now - self._last_flush > self.flush_every:
            self._fh.flush()
            self._last_flush = now

    def write_raw(self, ns: int, raw: str):
        # raw is already valid JSON, so embed it without a parse/dump round trip.
        self._line(ns, f'{{"rx_ns":{ns},"msg":{raw}}}')

    def write_event(self, event: str, **fields):
        ns = time.time_ns()
        self._line(ns, json.dumps({"rx_ns": ns, "event": event, **fields}, separators=(",", ":")))

    def close(self):
        if self._fh:
            self._fh.close()
        self._fh = None
        self._path = None


async def get_token(host: str, key: str, secret: str, expiration_seconds: int = 3600) -> str:
    async with aiohttp.ClientSession() as s:
        async with s.post(
            f"https://{host}/api/authenticate",
            json={"api_key": key, "api_secret": secret, "expiration_seconds": expiration_seconds},
        ) as r:
            if r.status != 200:
                raise RuntimeError(f"auth failed on {host}: HTTP {r.status} {await r.text()}")
            return (await r.json())["token"]


async def list_symbols(host: str) -> list:
    async with aiohttp.ClientSession() as s:
        async with s.get(f"https://{host}/api/instruments") as r:
            r.raise_for_status()
            instruments = (await r.json())["instruments"]
    return sorted(i["symbol"] for i in instruments if not i.get("delisted_at"))


def build_subscriptions(symbols, level: str, candle_widths) -> list:
    msgs = []
    for sym in symbols:
        msgs.append({"type": "subscribe", "symbol": sym, "level": level})
        for w in candle_widths:
            msgs.append({"type": "subscribe_candles", "symbol": sym, "width": w})
    for rid, m in enumerate(msgs, start=1):
        m["rid"] = rid
    return msgs


async def record(ws_url, token_fn, subscriptions, writer, stop: asyncio.Event,
                 stale_after: float = 20.0, max_backoff: float = 60.0, stats_every: float = 60.0):
    """Connect, subscribe, and write every message until `stop` is set. Reconnects on any failure."""
    backoff = 1.0
    while not stop.is_set():
        reason = "stopped"
        try:
            token = await token_fn()
            async with websockets.connect(
                ws_url, additional_headers={"Authorization": f"Bearer {token}"}, max_size=None
            ) as ws:
                writer.write_event("connected", url=ws_url, n_subscriptions=len(subscriptions))
                log.info("connected to %s, sending %d subscriptions", ws_url, len(subscriptions))
                for m in subscriptions:
                    await ws.send(json.dumps(m))
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
                        raise asyncio.TimeoutError(f"no message for {stale_after}s")
                    raw = recv.result()
                    writer.write_raw(time.time_ns(), raw)
                    if raw.startswith('{"rid"') and '"error"' in raw:
                        log.warning("server rejected request: %s", raw[:300])
                    counts[raw[5:8] if raw.startswith('{"t":') else "ctl"] += 1
                    if time.monotonic() - last_stats > stats_every:
                        log.info("msgs last %ds: %s", stats_every, dict(counts))
                        counts, last_stats = Counter(), time.monotonic()
        except asyncio.CancelledError:
            raise
        except Exception as e:  # network errors, auth errors, stale feed: all mean reconnect
            reason = f"{type(e).__name__}: {e}"
            log.warning("connection lost (%s); reconnecting in %.0fs", reason, backoff)
        writer.write_event("disconnected", reason=reason)
        if stop.is_set():
            break
        try:
            await asyncio.wait_for(stop.wait(), timeout=backoff * random.uniform(0.8, 1.2))
        except asyncio.TimeoutError:
            pass
        backoff = min(backoff * 2, max_backoff)


def parse_args(argv=None):
    p = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    p.add_argument("--env", choices=HOSTS, default="sandbox")
    p.add_argument("--symbols", help="comma-separated; default = every listed instrument")
    p.add_argument("--level", default="LEVEL_3", choices=["LEVEL_1", "LEVEL_2", "LEVEL_3", "TRADES"])
    p.add_argument("--candles", default="", help="comma-separated candle widths to also record, e.g. 1s,1m")
    p.add_argument("--out", type=Path, help="default: Data/raw/ax/<env>")
    p.add_argument("--stale-after", type=float, default=20.0, help="reconnect if silent this long (heartbeats are ~5s)")
    return p.parse_args(argv)


async def amain(args):
    load_dotenv(ROOT / ".env")
    key, secret = os.environ["ARCHITECT_API_KEY"], os.environ["ARCHITECT_API_SECRET"]
    host = HOSTS[args.env]
    symbols = args.symbols.split(",") if args.symbols else await list_symbols(host)
    widths = [w for w in args.candles.split(",") if w]
    subs = build_subscriptions(symbols, args.level, widths)
    writer = HourlyWriter(args.out or ROOT / "Data" / "raw" / "ax" / args.env)
    log.info("recording %d symbols at %s to %s", len(symbols), args.level, writer.base)

    stop = asyncio.Event()
    loop = asyncio.get_running_loop()
    for sig in (signal.SIGINT, signal.SIGTERM):
        loop.add_signal_handler(sig, stop.set)
    try:
        await record(f"wss://{host}/md/ws", lambda: get_token(host, key, secret), subs, writer, stop,
                     stale_after=args.stale_after)
    finally:
        writer.close()
        log.info("stopped")


def main(argv=None):
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    asyncio.run(amain(parse_args(argv)))


if __name__ == "__main__":
    main()
