"""Run the daily upkeep for the trade[XYZ] collector, on a schedule, without Task Scheduler.

Task Scheduler registration is denied on this machine (both `schtasks` and
`Register-ScheduledTask` return Access denied, even for an unelevated per-user logon task), so
the daily job is a small always-on process started from the Startup folder instead. It sleeps
until the next run time and shells out to the collector's own modules, so a failure in one step
is logged and the next step still runs.

Steps, in this order:
  1. convert    raw JSONL -> parquet. Must precede retention.
  2. backfill   the `all` watchlist. The 1m candle window is only ~3.5 days deep and backfill
                runs merge into existing files, so running this daily is what accumulates
                continuous 1-minute history past what the API will ever serve.
  3. catalog    refresh the DuckDB views so new partitions are visible.
  4. retention  delete raw JSONL older than --keep-days. Parquet is never touched.

    python -m recorder.service.daily_runner --once          # run the steps now and exit
    python -m recorder.service.daily_runner                 # loop, running daily at --at
"""
import argparse
import logging
import subprocess
import sys
import time
from datetime import datetime, timedelta
from pathlib import Path

REPO = Path(__file__).resolve().parents[2]
if str(REPO) not in sys.path:
    sys.path.insert(0, str(REPO))

from recorder.hl_common import AlreadyRunning, WATCHLISTS, acquire_lock, data_root, raw_root  # noqa: E402

log = logging.getLogger("daily_runner")


def steps(keep_days: int) -> list:
    coins = ",".join(WATCHLISTS["all"])
    return [
        ("convert", [sys.executable, "-m", "recorder.hl_xyz_to_parquet"]),
        ("backfill", [sys.executable, "-m", "recorder.hl_backfill", "--coins", coins]),
        ("catalog", [sys.executable, "-m", "recorder.hl_db", "--init", "--report"]),
        ("retention", [sys.executable, "-m", "recorder.hl_db", "--retention", str(keep_days)]),
    ]


def run_once(keep_days: int, timeout: float = 6 * 3600) -> dict:
    results = {}
    for name, cmd in steps(keep_days):
        log.info("---- %s ----", name)
        t0 = time.monotonic()
        try:
            p = subprocess.run(cmd, cwd=REPO, capture_output=True, text=True, timeout=timeout)
            for line in (p.stdout or "").splitlines() + (p.stderr or "").splitlines():
                log.info("  %s", line)
            results[name] = p.returncode
            log.info("%s finished rc=%d in %.0fs", name, p.returncode, time.monotonic() - t0)
        except subprocess.TimeoutExpired:
            results[name] = "timeout"
            log.error("%s timed out after %.0fs", name, timeout)
        except Exception as e:  # a broken step must not kill the scheduler
            results[name] = "error: %s" % e
            log.exception("%s raised", name)
    return results


def next_run(at: str, now: datetime | None = None) -> datetime:
    """The next occurrence of local time HH:MM, strictly in the future."""
    now = now or datetime.now()
    hh, mm = (int(x) for x in at.split(":"))
    target = now.replace(hour=hh, minute=mm, second=0, microsecond=0)
    if target <= now:
        target += timedelta(days=1)
    return target


def sleep_until(when: datetime, chunk: float = 300.0):
    """Sleep in chunks so a suspend/resume or clock change is noticed within `chunk` seconds
    rather than overshooting the run by however long the machine was asleep."""
    while True:
        remaining = (when - datetime.now()).total_seconds()
        if remaining <= 0:
            return
        time.sleep(min(remaining, chunk))


def main(argv=None):
    p = argparse.ArgumentParser(description=__doc__,
                                formatter_class=argparse.RawDescriptionHelpFormatter)
    p.add_argument("--once", action="store_true", help="run the steps immediately and exit")
    p.add_argument("--at", default="00:40", help="local HH:MM to run daily (default 00:40)")
    p.add_argument("--keep-days", type=int, default=30, help="raw JSONL retention")
    p.add_argument("--log-dir", type=Path, help="default: <PARAGON_DATA_ROOT>/logs")
    a = p.parse_args(argv)

    log_dir = a.log_dir or data_root() / "logs"
    log_dir.mkdir(parents=True, exist_ok=True)
    handlers = [logging.StreamHandler(),
                logging.FileHandler(log_dir / ("maintenance-%s.log" % datetime.now().strftime("%Y-%m-%d")),
                                    encoding="utf-8")]
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s",
                        handlers=handlers)

    if a.once:
        log.info("running maintenance once")
        log.info("results: %s", run_once(a.keep_days))
        return

    try:
        lock = acquire_lock(raw_root() / ".daily_runner.lock")
    except AlreadyRunning as e:
        raise SystemExit("refusing to start: %s" % e)
    log.info("daily runner up; next run %s", next_run(a.at))
    try:
        while True:
            sleep_until(next_run(a.at))
            # Re-open the log each day so files stay per-day rather than growing forever.
            for h in list(logging.getLogger().handlers):
                if isinstance(h, logging.FileHandler):
                    logging.getLogger().removeHandler(h)
                    h.close()
            fh = logging.FileHandler(
                log_dir / ("maintenance-%s.log" % datetime.now().strftime("%Y-%m-%d")),
                encoding="utf-8")
            fh.setFormatter(logging.Formatter("%(asctime)s %(levelname)s %(message)s"))
            logging.getLogger().addHandler(fh)
            log.info("==== maintenance start ====")
            log.info("results: %s", run_once(a.keep_days))
            log.info("==== maintenance done; next run %s ====", next_run(a.at))
    finally:
        lock.close()


if __name__ == "__main__":
    main()
