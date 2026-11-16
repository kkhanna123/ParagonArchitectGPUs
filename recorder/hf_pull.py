"""Download the Paragon market data from Hugging Face and rebuild the local DuckDB catalogs.

For teammates: you need read access to the private dataset (ask to be added) and `hf auth login`.

    python -m recorder.hf_pull --dest D:/ParagonData                                    # everything
    python -m recorder.hf_pull --dest D:/ParagonData --tables kalshi/trades,kalshi/ticker,hl/asset_ctx \
                               --start 2026-11-01 --end 2026-11-12
    python -m recorder.hf_pull --dest D:/ParagonData --hist                              # backfills only

Files land in <dest>/parquet/... in the same layout the collectors write, then
`recorder.hl_db --init` and `recorder.kalshi.db --init` build <dest>/hl.duckdb and
<dest>/kalshi.duckdb. Point the analysis code at it with PARAGON_DATA_ROOT=<dest>.
Re-running only downloads files that changed.
"""
import argparse
import os
import subprocess
import sys
from datetime import date, timedelta
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[1]
DEFAULT_REPO = os.environ.get("PARAGON_HF_REPO", "ParagonGlobal/paragon-market-data")


def day_range(start: str, end: str) -> list:
    d0, d1 = date.fromisoformat(start), date.fromisoformat(end)
    return [(d0 + timedelta(i)).isoformat() for i in range((d1 - d0).days + 1)]


def patterns(tables, start, end, hist) -> list | None:
    """allow_patterns for snapshot_download; None = everything."""
    if not tables and not (start or end) and not hist:
        return None
    pats = ["README.md"]
    days = day_range(start or "2026-01-01", end or date.today().isoformat()) if (start or end) else None
    if tables or days:
        for t in (tables or ["hl/*", "kalshi/*"]):
            if days:
                pats += ["%s/date=%s/**" % (t, d) for d in days]
            else:
                pats.append("%s/**" % t)
    if hist or not tables:
        pats += ["hl_hist/**", "kalshi_hist/**"]
    return pats


def main(argv=None):
    p = argparse.ArgumentParser(description=__doc__,
                                formatter_class=argparse.RawDescriptionHelpFormatter)
    p.add_argument("--repo", default=DEFAULT_REPO, help="dataset id (default %(default)s)")
    p.add_argument("--dest", type=Path, required=True, help="local data root to create/update")
    p.add_argument("--tables", default="", help="comma list of group/table, e.g. kalshi/trades,hl/book5")
    p.add_argument("--start", help="first UTC day (YYYY-MM-DD)")
    p.add_argument("--end", help="last UTC day")
    p.add_argument("--hist", action="store_true", help="also pull the backfill tables")
    p.add_argument("--no-catalog", action="store_true", help="skip rebuilding the DuckDB catalogs")
    a = p.parse_args(argv)

    from huggingface_hub import snapshot_download
    tables = [t.strip() for t in a.tables.split(",") if t.strip()]
    pats = patterns(tables, a.start, a.end, a.hist)
    a.dest.mkdir(parents=True, exist_ok=True)
    print("downloading %s -> %s%s" % (a.repo, a.dest, "" if pats is None else " (%d patterns)" % len(pats)))
    snapshot_download(repo_id=a.repo, repo_type="dataset", local_dir=str(a.dest / "parquet"),
                      allow_patterns=pats, max_workers=8)

    if a.no_catalog:
        return
    env = dict(os.environ, PARAGON_DATA_ROOT=str(a.dest.resolve()))
    for mod in ("recorder.hl_db", "recorder.kalshi.db"):
        print("building catalog:", mod)
        subprocess.run([sys.executable, "-m", mod, "--init"], cwd=REPO_ROOT, env=env, check=False)
    print("done. Use it with:  set PARAGON_DATA_ROOT=%s" % a.dest.resolve())


if __name__ == "__main__":
    main()
