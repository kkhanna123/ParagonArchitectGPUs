"""Shared pieces for the Kalshi collector: endpoints, request signing, REST, paths."""
import base64
import json
import logging
import os
import threading
import time
import urllib.error
import urllib.parse
import urllib.request
from dataclasses import dataclass
from pathlib import Path

from recorder.hl_common import parquet_root, raw_root, safe_part, to_scaled  # noqa: F401

log = logging.getLogger("kalshi")

HOST = "https://api.elections.kalshi.com"
API_PREFIX = "/trade-api/v2"
API = HOST + API_PREFIX
WS_PATH = "/trade-api/ws/v2"
WS_URL = "wss://api.elections.kalshi.com" + WS_PATH


def kx_raw(root: Path | None = None) -> Path:
    return raw_root(root) / "kalshi"


def kx_parquet() -> Path:
    return parquet_root() / "kalshi"


def kx_hist() -> Path:
    return parquet_root() / "kalshi_hist"


def series_of(ticker: str) -> str:
    """'KXHIGHCHI-26NOV01-T71' -> 'KXHIGHCHI'. Market and event tickers lead with their series."""
    return ticker.split("-", 1)[0]


def series_dirs(ticker: str) -> tuple:
    """Partition directories for a market ticker (PartWriter's `part_dirs`)."""
    return ("series=%s" % safe_part(series_of(ticker)),)


def ms(iso: str | None) -> int | None:
    """'2026-10-30T21:00:23.560701Z' -> epoch ms. None passes through."""
    if not iso:
        return None
    from datetime import datetime
    return int(datetime.fromisoformat(iso.replace("Z", "+00:00")).timestamp() * 1000)


def num(v) -> float | None:
    """A strike or settlement value as a float, or None when it is absent or not numeric."""
    if v is None or v == "":
        return None
    try:
        return float(v)
    except (TypeError, ValueError):
        return None


# ---- credentials -------------------------------------------------------------------------

@dataclass
class Credentials:
    key_id: str
    private_key: object  # cryptography RSAPrivateKey

    def headers(self, method: str, path: str) -> dict:
        """Kalshi's auth: RSA-PSS/SHA256 over "<ts_ms><METHOD><path>", path without query."""
        from cryptography.hazmat.primitives import hashes
        from cryptography.hazmat.primitives.asymmetric import padding
        ts = str(int(time.time() * 1000))
        sig = self.private_key.sign(
            (ts + method + path).encode(),
            padding.PSS(mgf=padding.MGF1(hashes.SHA256()), salt_length=padding.PSS.DIGEST_LENGTH),
            hashes.SHA256())
        return {"KALSHI-ACCESS-KEY": self.key_id, "KALSHI-ACCESS-TIMESTAMP": ts,
                "KALSHI-ACCESS-SIGNATURE": base64.b64encode(sig).decode()}


def _read_env_file(path: Path) -> dict:
    out = {}
    for line in path.read_text(encoding="utf-8").splitlines():
        line = line.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        k, _, v = line.partition("=")
        out[k.strip()] = v.strip().strip('"').strip("'")
    return out


def load_credentials() -> Credentials | None:
    """API key from KALSHI_API_KEY_ID + KALSHI_PRIVATE_KEY_PATH (or _PEM), else from the .env
    file named by KALSHI_ENV_FILE. None if neither is configured.

    A key path that does not exist is retried as a file of the same name beside the .env: the
    existing .env was written for a Docker mount (/work/...), and the key sits next to it.
    """
    env = dict(os.environ)
    env_file = env.get("KALSHI_ENV_FILE")
    if env_file and Path(env_file).exists() and not env.get("KALSHI_API_KEY_ID"):
        env = {**_read_env_file(Path(env_file)), **{k: v for k, v in env.items()
                                                    if k.startswith("KALSHI_") and v}}
    key_id = env.get("KALSHI_API_KEY_ID")
    if not key_id:
        return None
    pem = env.get("KALSHI_PRIVATE_KEY_PEM")
    if pem:
        data = pem.encode()
    else:
        kp = Path(env.get("KALSHI_PRIVATE_KEY_PATH", ""))
        if not kp.is_file() and env_file:
            kp = Path(env_file).parent / kp.name
        if not kp.is_file():
            raise FileNotFoundError("Kalshi private key not found (KALSHI_PRIVATE_KEY_PATH=%s)"
                                    % env.get("KALSHI_PRIVATE_KEY_PATH"))
        data = kp.read_bytes()
    from cryptography.hazmat.primitives import serialization
    return Credentials(key_id, serialization.load_pem_private_key(data, password=None))


# ---- REST --------------------------------------------------------------------------------

class Rest:
    """Small GET client: paced under the read rate limit, retrying 429/5xx with backoff.

    Market data is public, so credentials are optional here; when given, requests are signed
    (which also lifts them into the account's rate-limit tier). Thread-safe, so the recorder can
    call it from worker threads.
    """

    def __init__(self, creds: Credentials | None = None, per_sec: float = 8.0,
                 timeout: float = 30.0, retries: int = 6):
        self.creds, self.timeout, self.retries = creds, timeout, retries
        self.min_gap = 1.0 / per_sec if per_sec else 0.0
        self._lock = threading.Lock()
        self._next = 0.0
        self.calls = 0

    def _pace(self):
        with self._lock:
            now = time.monotonic()
            wait = self._next - now
            self._next = max(now, self._next) + self.min_gap
        if wait > 0:
            time.sleep(wait)

    def get(self, path: str, **params) -> dict:
        params = {k: v for k, v in params.items() if v is not None}
        url = API + path + ("?" + urllib.parse.urlencode(params) if params else "")
        for attempt in range(self.retries):
            self._pace()
            headers = {"Accept": "application/json"}
            if self.creds:
                headers.update(self.creds.headers("GET", API_PREFIX + path))
            try:
                self.calls += 1
                with urllib.request.urlopen(urllib.request.Request(url, headers=headers),
                                            timeout=self.timeout) as r:
                    return json.load(r)
            except urllib.error.HTTPError as e:
                if e.code not in (429, 500, 502, 503, 504) or attempt == self.retries - 1:
                    raise
                log.debug("GET %s -> %d, retrying", path, e.code)
            except (urllib.error.URLError, TimeoutError, ConnectionError):
                if attempt == self.retries - 1:
                    raise
            time.sleep(min(30.0, 0.5 * 2 ** attempt))
        raise RuntimeError("unreachable")

    def paginate(self, path: str, key: str, limit: int = 1000, max_pages: int = 1000, **params):
        """Yield every item of a cursor-paginated list endpoint."""
        cursor = None
        for _ in range(max_pages):
            d = self.get(path, limit=limit, cursor=cursor, **params)
            yield from d.get(key) or []
            cursor = d.get("cursor")
            if not cursor or not d.get(key):
                return


# Market fields that describe the contract and its resolution. Prices, sizes and volume move
# every second and are recorded from the websocket instead, so leaving them out of the metadata
# snapshot means a snapshot only changes when the contract itself does.
MARKET_META = ("ticker", "event_ticker", "market_type", "title", "yes_sub_title", "no_sub_title",
               "strike_type", "floor_strike", "cap_strike", "open_time", "close_time",
               "expected_expiration_time", "expiration_time", "status", "result",
               "expiration_value", "settlement_value_dollars", "settlement_ts", "rules_primary")


def market_meta(m: dict) -> dict:
    return {k: m.get(k) for k in MARKET_META if m.get(k) is not None}
