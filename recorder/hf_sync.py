"""Mirror the finished parquet to a private Hugging Face dataset so the rest of the team can pull it.

Layout on the Hub mirrors <PARAGON_DATA_ROOT>/parquet exactly, at the repo root (the same layout
as the one-off upload of 2026-11-09 in kkhanna123/paragon-market-data):

  hl/<table>/date=<D>/dex=<X>/sym=<S>/part.parquet        live trade[XYZ] recordings
  kalshi/<table>/date=<D>/series=<S>/part.parquet          live Kalshi recordings
  hl_hist/..., kalshi_hist/...                     REST backfills (rewritten daily)

Daily tables are pushed one commit per UTC day, oldest first, and only for days that are over
(date < today UTC), so a half-written day is never published. A day whose files change later
(a late conversion) is pushed again. The backfill trees are pushed whenever they change. What
has been pushed is remembered in <PARAGON_DATA_ROOT>/hf_sync_state.json, keyed by a size+mtime
fingerprint, so a run with nothing new makes no network calls beyond the auth check.

The DuckDB catalogs are not uploaded: their views hold absolute paths to this machine.
`python -m recorder.hf_pull` downloads and rebuilds them on the other side.

    python -m recorder.hf_sync --dry-run                  # what would be pushed
    python -m recorder.hf_sync                            # push (ParagonGlobal/paragon-market-data)
    python -m recorder.hf_sync --repo org/name --exclude kalshi/book

Auth: run `hf auth login` once (a token with write access). The repo is created private if it
does not exist, and the sync refuses to push to a repo that is public.

Exit codes: 0 ok / nothing to do / another sync already running, 1 some pushes failed,
3 not logged in or no write access, 4 repo is public.
"""
import argparse
import hashlib
import json
import logging
import os
import sys
from datetime import datetime, timezone
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from recorder.hl_common import AlreadyRunning, acquire_lock, data_root, raw_root  # noqa: E402

log = logging.getLogger("hf_sync")

DEFAULT_REPO = "ParagonGlobal/paragon-market-data"   # org dataset: every member can pull
DAILY_GROUPS = ("hl", "kalshi")          # parquet/<group>/<table>/date=<D>/...
HIST_GROUPS = ("hl_hist", "kalshi_hist")  # parquet/<group>/<table>/... (no date partition)
STATE_FILE = "hf_sync_state.json"
CARD = Path(__file__).with_name("hf_dataset_card.md")


def fingerprint(path: Path) -> list:
    """[n files, total bytes, newest mtime] of every parquet file under `path`."""
    n = size = 0
    newest = 0.0
    for f in path.rglob("*.parquet"):
        st = f.stat()
        n, size, newest = n + 1, size + st.st_size, max(newest, st.st_mtime)
    return [n, size, round(newest, 3)]


def plan(parquet: Path, state: dict, today: str, exclude=(), only=None) -> tuple:
    """(daily, hist): what needs pushing.

    daily: {date: {"group/table": fingerprint}} for finished days that are new or changed.
    hist:  {"group/table": fingerprint} for backfill trees that are new or changed.
    """
    def wanted(key):
        return key not in exclude and (not only or key in only)

    daily, hist = {}, {}
    for g in DAILY_GROUPS:
        for tdir in sorted(p for p in (parquet / g).glob("*") if p.is_dir()):
            key = "%s/%s" % (g, tdir.name)
            if not wanted(key):
                continue
            for ddir in sorted(tdir.glob("date=*")):
                d = ddir.name.split("=", 1)[1]
                if d >= today:
                    continue
                fp = fingerprint(ddir)
                if fp[0] and state.get("daily", {}).get(d, {}).get(key) != fp:
                    daily.setdefault(d, {})[key] = fp
    for g in HIST_GROUPS:
        for tdir in sorted(p for p in (parquet / g).glob("*") if p.is_dir()):
            key = "%s/%s" % (g, tdir.name)
            if not wanted(key):
                continue
            fp = fingerprint(tdir)
            if fp[0] and state.get("hist", {}).get(key) != fp:
                hist[key] = fp
    return dict(sorted(daily.items())), hist


def load_state(path: Path) -> dict:
    try:
        return json.loads(path.read_text())
    except (OSError, ValueError):
        return {}


def save_state(path: Path, state: dict):
    tmp = path.with_suffix(".tmp")
    tmp.write_text(json.dumps(state, indent=1, sort_keys=True))
    os.replace(tmp, path)


def gb(n: int) -> str:
    return "%.2f GB" % (n / 1e9)


def resolve_repo(api, repo: str | None) -> str:
    if repo:
        return repo
    api.whoami()  # raises when not logged in
    return os.environ.get("PARAGON_HF_REPO") or DEFAULT_REPO


def ensure_private_repo(api, repo: str) -> bool:
    """Create the dataset private if missing. False if it exists and is public."""
    from huggingface_hub.utils import RepositoryNotFoundError
    try:
        info = api.dataset_info(repo)
    except RepositoryNotFoundError:
        api.create_repo(repo, repo_type="dataset", private=True)
        log.info("created private dataset %s", repo)
        return True
    return bool(info.private)


def push_card(api, repo: str, state: dict, dry: bool):
    text = CARD.read_text(encoding="utf-8").replace("{REPO}", repo)
    if state.get("card") == hashlib.sha1(text.encode()).hexdigest() and not dry:
        return
    if dry:
        log.info("would push README.md (dataset card)")
        return
    api.upload_file(path_or_fileobj=text.encode(), path_in_repo="README.md", repo_id=repo,
                    repo_type="dataset", commit_message="Update dataset card")
    state["card"] = hashlib.sha1(text.encode()).hexdigest()


def main(argv=None) -> int:
    p = argparse.ArgumentParser(description=__doc__,
                                formatter_class=argparse.RawDescriptionHelpFormatter)
    p.add_argument("--repo", help="dataset id (default: $PARAGON_HF_REPO, else %s)" % DEFAULT_REPO)
    p.add_argument("--exclude", default=os.environ.get("PARAGON_HF_EXCLUDE", ""),
                   help="comma list of group/table to skip, e.g. kalshi/book "
                        "(default: $PARAGON_HF_EXCLUDE)")
    p.add_argument("--only", default="", help="comma list of group/table to push (default: all)")
    p.add_argument("--max-days", type=int, default=0,
                   help="push at most this many days this run (0 = all); useful for the first upload")
    p.add_argument("--dry-run", action="store_true")
    a = p.parse_args(argv)
    if not logging.getLogger().handlers:
        logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")

    root = data_root()
    parquet = root / "parquet"
    state_path = root / STATE_FILE
    state = load_state(state_path)
    today = datetime.now(timezone.utc).strftime("%Y-%m-%d")
    exclude = {s.strip() for s in a.exclude.split(",") if s.strip()}
    only = {s.strip() for s in a.only.split(",") if s.strip()}
    daily, hist = plan(parquet, state, today, exclude, only)
    if a.max_days:
        daily = dict(list(daily.items())[:a.max_days])
    total = sum(fp[1] for d in daily.values() for fp in d.values()) + sum(fp[1] for fp in hist.values())
    log.info("data root %s: %d day(s) and %d backfill table(s) to push, %s%s",
             root, len(daily), len(hist), gb(total),
             " (excluding %s)" % ", ".join(sorted(exclude)) if exclude else "")

    from huggingface_hub import HfApi
    api = HfApi()
    try:
        repo = resolve_repo(api, a.repo)
    except Exception as e:
        log.error("not logged in to Hugging Face (%s). Run: .venv\\Scripts\\hf auth login", e)
        return 3

    if a.dry_run:
        for d, tables in daily.items():
            log.info("would push %s: %s (%s)", d, ", ".join(tables),
                     gb(sum(fp[1] for fp in tables.values())))
        for k, fp in hist.items():
            log.info("would push %s (%s)", k, gb(fp[1]))
        push_card(api, repo, state, dry=True)
        log.info("target: %s (dataset)", repo)
        return 0

    try:
        lock = acquire_lock(raw_root() / ".hf_sync.lock")  # noqa: F841 -- held until exit
    except AlreadyRunning as e:
        log.warning("another hf_sync is running (%s); skipping this run", e)
        return 0

    try:
        if not ensure_private_repo(api, repo):
            log.error("refusing to push: dataset %s is PUBLIC. Make it private on the Hub first.", repo)
            return 4
    except Exception as e:
        log.error("cannot reach/create %s: %s", repo, e)
        return 3

    failed = 0
    try:
        push_card(api, repo, state, dry=False)
    except Exception as e:
        log.warning("dataset card not pushed: %s", e)

    for d, tables in daily.items():
        patterns = ["%s/date=%s/**" % (k, d) for k in tables]
        try:
            api.upload_folder(repo_id=repo, repo_type="dataset", folder_path=str(parquet),
                              path_in_repo="", allow_patterns=patterns,
                              commit_message="Data %s (%s)" % (d, ", ".join(tables)))
        except Exception as e:
            failed += 1
            log.error("push %s failed: %s", d, e)
            continue
        state.setdefault("daily", {}).setdefault(d, {}).update(tables)
        save_state(state_path, state)
        log.info("pushed %s: %d table(s), %s", d, len(tables), gb(sum(fp[1] for fp in tables.values())))

    if hist:
        try:
            api.upload_folder(repo_id=repo, repo_type="dataset", folder_path=str(parquet),
                              path_in_repo="", allow_patterns=["%s/**" % k for k in hist],
                              commit_message="Backfill tables (%s)" % ", ".join(hist))
            state.setdefault("hist", {}).update(hist)
            save_state(state_path, state)
            log.info("pushed backfill tables: %s", ", ".join(hist))
        except Exception as e:
            failed += 1
            log.error("push of backfill tables failed: %s", e)

    save_state(state_path, state)
    log.info("done: %s, %d failure(s)", repo, failed)
    return 1 if failed else 0


if __name__ == "__main__":
    sys.exit(main())
