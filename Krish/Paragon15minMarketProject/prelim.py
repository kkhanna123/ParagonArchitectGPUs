"""Prelim: can the trade[XYZ] Hyperliquid price feed beat Kalshi's 15-minute commodity markets?

Kalshi's KX*15M series ask "will <commodity> be up in the next 15 minutes?". Each one settles YES
iff the Pyth 1-minute close at window close >= the strike, and the strike is the previous window's
settlement value. So at any moment inside a window the fair YES price is a digital option:

    fair = Phi( (ln S_t + b - ln K) / sqrt(sigma^2 * tau + s_b^2) )

  S_t    live HL price (activeAssetCtx oracle or mid, 1s grid, no look-ahead)
  b      HL -> Pyth log basis, the median of the last N settled windows
  sigma  per-second realised vol of the HL price over the trailing 30 min (10s returns)
  s_b    RMS of recent basis-forecast errors (settlement-source noise near expiry)

This script:
  1. aligns HL with Kalshi settlements: which HL price / timestamp offset reproduces the Pyth
     settlement best (chosen on the train days only)
  2. checks calibration: model fair vs the Kalshi mid, both scored against outcomes
  3. checks lead-lag: does the Kalshi mid move toward fair over the next seconds?
  4. backtests taker entries (one per window, held to settlement, Kalshi taker fees, latency),
     choosing the threshold on train days and reporting the test days

    .venv/Scripts/python.exe Krish/Paragon15minMarketProject/prelim.py              # from the repo root
    .venv/Scripts/python.exe Krish/Paragon15minMarketProject/prelim.py --start 2026-11-01 --end 2026-11-12

Outputs go to Krish/Paragon15minMarketProject/out/ (CSVs, PNGs, summary.json).
"""
import argparse
import json
import os
import sys
from scipy.special import ndtr
from pathlib import Path

import duckdb
import numpy as np
import pandas as pd

HERE = Path(__file__).resolve().parent
REPO = HERE.parents[1]
OUT = HERE / "out"

# Kalshi series -> trade[XYZ] symbol. KXCOFFEE15M has no xyz market.
SERIES_SYM = {
    "KXWTI15M": "CL", "KXBRENT15M": "BRENTOIL", "KXNATGAS15M": "NATGAS",
    "KXGOLD15M": "GOLD", "KXSILVER15M": "SILVER", "KXPLATINUM15M": "PLATINUM",
    "KXPALLADIUM15M": "PALLADIUM", "KXCOPPER15M": "COPPER",
}
DEX = "xyz"

BASIS_N = 8            # windows in the basis median
BASIS_ERR_N = 20       # windows in the basis-error RMS
VOL_WIN_S = 1800       # trailing vol window
STEP_S = 5             # decision grid inside a window
MIN_TAU_S = 10         # stop trading this close to expiry
FIRST_DECISION_S = 30  # wait for the previous window to settle (~24s after close)
MAX_SPREAD = 0.10      # ignore books wider than this
FEE_COEF = 0.07        # Kalshi taker fee per contract = coef * P * (1 - P)
THRESHOLDS = [0.00, 0.01, 0.02, 0.03, 0.05, 0.08]
LATENCIES = [0.0, 1.0, 3.0]
OFFSETS = [-60, -30, 0, 30, 60]   # seconds vs close_ms tried for the settlement candle


def data_root() -> Path:
    env = os.environ.get("PARAGON_DATA_ROOT")
    for p in ([Path(env)] if env else []) + [REPO.parent / "HyperliquidData", Path("C:/ParagonData")]:
        if (p / "kalshi.duckdb").exists() and (p / "hl.duckdb").exists():
            return p
    sys.exit("no hl.duckdb/kalshi.duckdb found; set PARAGON_DATA_ROOT")


def ncdf(x):
    return ndtr(x)


def fee(p):
    return FEE_COEF * p * (1.0 - p)


# ---- loading ---------------------------------------------------------------------------------

def load_hl(con, start, end) -> dict:
    """sym -> DataFrame on a 1s grid (index = epoch second) with log oracle / log mid.

    Bucket s holds the last update received in [s, s+1). A decision at integer second t reads
    bucket t-1, so nothing from the future leaks in. Forward-filled at most 120s.
    """
    syms = ", ".join("'%s'" % s for s in SERIES_SYM.values())
    df = con.sql(f"""
        SELECT sym, (rx_ns // 1000000000)::BIGINT AS sec,
               last(oracle_px ORDER BY rx_ns) AS oracle, last(mid_px ORDER BY rx_ns) AS mid
        FROM asset_ctx
        WHERE dex = '{DEX}' AND sym IN ({syms})
          AND dt BETWEEN DATE '{start}' - 1 AND DATE '{end}' + 1
        GROUP BY ALL""").df()
    out = {}
    for sym, g in df.groupby("sym"):
        g = g.set_index("sec").sort_index()
        idx = np.arange(g.index[0], g.index[-1] + 1)
        g = g[["oracle", "mid"]].reindex(idx).ffill(limit=120)
        g = np.log(g.where(g > 0))
        out[sym] = g
    return out


def add_vol(hl: dict, col: str):
    """Per-second variance from overlapping 10s returns over the trailing VOL_WIN_S."""
    for g in hl.values():
        r10 = g[col].diff(10)
        g["var_s"] = (r10 ** 2 / 10.0).rolling(VOL_WIN_S, min_periods=VOL_WIN_S // 3).mean()


def load_settlements(con, start, end) -> pd.DataFrame:
    ser = ", ".join("'%s'" % s for s in SERIES_SYM)
    st = con.sql(f"""
        SELECT series, ticker, event_ticker, floor_strike AS strike, open_ms, close_ms,
               TRY_CAST(expiration_value AS DOUBLE) AS settle_px, settlement_value AS outcome,
               volume
        FROM settlements
        WHERE series IN ({ser}) AND strike_type = 'greater_or_equal'
          AND status IN ('settled', 'finalized') AND settlement_value IN (0, 1)
          AND close_ms BETWEEN epoch_ms(DATE '{start}') - 86400000
                           AND epoch_ms(DATE '{end}' + 1)
        ORDER BY series, close_ms""").df()
    st["sym"] = st.series.map(SERIES_SYM)
    st["day"] = pd.to_datetime(st.close_ms, unit="ms").dt.strftime("%Y-%m-%d")
    return st.dropna(subset=["strike", "settle_px"])


def load_quotes(con, start, end) -> dict:
    ser = ", ".join("'%s'" % s for s in SERIES_SYM)
    q = con.sql(f"""
        SELECT ticker, rx_ns, yes_bid, yes_ask, yes_bid_sz, yes_ask_sz
        FROM ticker
        WHERE series IN ({ser}) AND dt BETWEEN DATE '{start}' AND DATE '{end}' + 1
        ORDER BY ticker, rx_ns""").df()
    return {t: (g.rx_ns.to_numpy() // 1_000_000,  # ms
                g[["yes_bid", "yes_ask", "yes_bid_sz", "yes_ask_sz"]].to_numpy())
            for t, g in q.groupby("ticker", sort=False)}


def hl_at(g: pd.DataFrame, col: str, sec: np.ndarray) -> np.ndarray:
    """Value of column `col` known at integer second(s) `sec` (bucket sec-1)."""
    idx = np.asarray(sec, dtype=np.int64) - 1 - g.index[0]
    ok = (idx >= 0) & (idx < len(g))
    out = np.full(len(idx), np.nan)
    out[ok] = g[col].to_numpy()[idx[ok]]
    return out


# ---- 1. settlement alignment -------------------------------------------------------------------

def basis_table(st: pd.DataFrame, hl: dict, col: str, off: int) -> pd.DataFrame:
    """Per window: realised basis b = ln(settle / HL at close+off), its forecast from prior
    windows, and the forecast error."""
    rows = []
    for series, g in st.groupby("series"):
        h = hl.get(SERIES_SYM[series])
        if h is None:
            continue
        g = g.sort_values("close_ms").copy()
        sec = g.close_ms.to_numpy() // 1000 + off
        g["b"] = np.log(g.settle_px.to_numpy()) - hl_at(h, col, sec + 1)  # bucket = sec
        # forecast for window j uses windows i with close_i <= open_j (already settled)
        b_hat, b_err_rms = [], []
        closes, bs = g.close_ms.to_numpy(), g.b.to_numpy()
        errs = np.full(len(g), np.nan)
        for j, op in enumerate(g.open_ms.to_numpy()):
            k = np.searchsorted(closes, op, side="right")
            prior = bs[max(0, k - BASIS_N * 3):k]
            prior = prior[~np.isnan(prior)][-BASIS_N:]
            bh = np.median(prior) if len(prior) >= 3 else np.nan
            b_hat.append(bh)
            e_prior = errs[max(0, k - BASIS_ERR_N * 3):k]
            e_prior = e_prior[~np.isnan(e_prior)][-BASIS_ERR_N:]
            b_err_rms.append(np.sqrt(np.mean(e_prior ** 2)) if len(e_prior) >= 5 else np.nan)
            errs[j] = bs[j] - bh
        g["b_hat"], g["s_b"], g["b_err"] = b_hat, b_err_rms, errs
        rows.append(g)
    return pd.concat(rows, ignore_index=True)


def choose_alignment(st, hl, train_days):
    res = []
    for col in ("oracle", "mid"):
        for off in OFFSETS:
            bt = basis_table(st, hl, col, off)
            tr = bt[bt.day.isin(train_days)]
            e = tr.b_err.dropna()
            # direction check: does HL+basis at close reproduce the outcome?
            sym_px = np.log(tr.settle_px) - tr.b + tr.b_hat  # = HL(close) + b_hat
            pred = (sym_px >= np.log(tr.strike)).astype(float)
            ok = ~np.isnan(sym_px)
            res.append(dict(col=col, off=off, n=int(len(e)),
                            mad_bp=float(np.median(np.abs(e)) * 1e4),
                            rms_bp=float(np.sqrt(np.mean(e ** 2)) * 1e4),
                            dir_hit=float((pred[ok] == tr.outcome[ok]).mean())))
    res = pd.DataFrame(res).sort_values("mad_bp")
    return res


# ---- 2-4. decision grid ------------------------------------------------------------------------

def decision_grid(bt, hl, quotes, col, latency_s) -> pd.DataFrame:
    """One row per (window, decision time): model fair, Kalshi quote seen, quote at fill."""
    rows = []
    for r in bt.itertuples():
        h = hl.get(r.sym)
        q = quotes.get(r.ticker)
        if h is None or q is None or np.isnan(r.b_hat) or np.isnan(r.s_b):
            continue
        qms, qv = q
        t = np.arange(r.open_ms // 1000 + FIRST_DECISION_S, r.close_ms // 1000 - MIN_TAU_S + 1,
                      STEP_S)
        lp = hl_at(h, col, t)
        var_s = hl_at(h, "var_s", t)
        tau = r.close_ms / 1000 - t
        sd = np.sqrt(var_s * tau + r.s_b ** 2)
        z = (lp + r.b_hat - np.log(r.strike)) / sd
        fair = ncdf(z)
        i_seen = np.searchsorted(qms, t * 1000, side="right") - 1
        i_fill = np.searchsorted(qms, t * 1000 + latency_s * 1000, side="right") - 1
        ok = (i_seen >= 0) & ~np.isnan(fair) & (var_s > 0)
        if not ok.any():
            continue
        t, fair, tau, i_seen, i_fill = t[ok], fair[ok], tau[ok], i_seen[ok], i_fill[ok]
        seen, fill = qv[i_seen], qv[i_fill]
        rows.append(pd.DataFrame(dict(
            series=r.series, ticker=r.ticker, day=r.day, t=t, tau=tau, fair=fair,
            bid=seen[:, 0], ask=seen[:, 1], bid_sz=seen[:, 2], ask_sz=seen[:, 3],
            bid_f=fill[:, 0], ask_f=fill[:, 1], outcome=r.outcome)))
    g = pd.concat(rows, ignore_index=True)
    valid = (g.bid > 0) & (g.ask < 1) & (g.ask > g.bid) & (g.ask - g.bid <= MAX_SPREAD)
    g = g[valid].copy()
    g["mid"] = (g.bid + g.ask) / 2
    return g


def trades(grid, thr) -> pd.DataFrame:
    """First signal per window. Limit at the seen price; fills only if the fill-time quote is
    still at least as good (then at the fill-time price)."""
    e_yes = grid.fair - grid.ask - fee(grid.ask)
    e_no = grid.bid - grid.fair - fee(1 - grid.bid)
    g = grid.assign(e_yes=e_yes, e_no=e_no)
    sig = g[(g.e_yes > thr) | (g.e_no > thr)]
    first = sig.groupby("ticker", sort=False).head(1).copy()
    yes = first.e_yes >= first.e_no
    first["side"] = np.where(yes, "yes", "no")
    filled_yes = yes & (first.ask_f <= first.ask) & (first.ask_f < 1)
    filled_no = ~yes & (first.bid_f >= first.bid) & (first.bid_f > 0)
    first = first[filled_yes | filled_no].copy()
    yes = first.side == "yes"
    px = np.where(yes, first.ask_f, 1 - first.bid_f)            # cost of the contract bought
    win = np.where(yes, first.outcome, 1 - first.outcome)
    first["price"] = px
    first["edge"] = np.where(yes, first.e_yes, first.e_no)
    first["pnl"] = win - px - fee(px)
    return first


def summarize(tr: pd.DataFrame, by=None) -> pd.DataFrame:
    def f(x):
        n = len(x)
        sd = x.pnl.std(ddof=1) if n > 1 else np.nan
        daily = x.groupby("day").pnl.sum()
        return pd.Series(dict(
            n=n, hit=x.pnl.gt(0).mean(), avg_px=x.price.mean(), avg_edge=x.edge.mean(),
            pnl_per=x.pnl.mean(), t_stat=x.pnl.mean() / (sd / np.sqrt(n)) if n > 1 else np.nan,
            total=x.pnl.sum(), days_pos=f"{(daily > 0).sum()}/{len(daily)}"))
    if by is None:
        return f(tr).to_frame().T
    return tr.groupby(by).apply(f, include_groups=False)


def calibration(grid) -> pd.DataFrame:
    g = grid.copy()
    g["tau_b"] = pd.cut(g.tau, [0, 60, 180, 420, 900], labels=["<1m", "1-3m", "3-7m", "7-15m"])
    out = g.groupby("tau_b", observed=True).apply(lambda x: pd.Series(dict(
        n=len(x),
        brier_model=((x.fair - x.outcome) ** 2).mean(),
        brier_kalshi=((x.mid - x.outcome) ** 2).mean(),
        brier_blend=(((x.fair + x.mid) / 2 - x.outcome) ** 2).mean(),
        mean_abs_gap=(x.fair - x.mid).abs().mean())), include_groups=False)
    return out


def convergence(grid, horizons=(5, 15, 30, 60)) -> pd.DataFrame:
    """Mean change in Kalshi mid over the next h seconds, bucketed by (fair - mid) now."""
    g = grid.sort_values(["ticker", "t"]).copy()
    g["gap"] = g.fair - g.mid
    g["gap_b"] = pd.cut(g.gap, [-1, -0.10, -0.05, -0.02, 0.02, 0.05, 0.10, 1])
    out = {}
    for h in horizons:
        k = h // STEP_S
        fut = g.groupby("ticker").mid.shift(-k)
        out[f"dmid_{h}s"] = (fut - g.mid).groupby(g.gap_b, observed=True).mean()
    res = pd.DataFrame(out)
    res.insert(0, "n", g.groupby("gap_b", observed=True).size())
    res.insert(1, "mean_gap", g.groupby("gap_b", observed=True).gap.mean())
    return res


# ---- main --------------------------------------------------------------------------------------

def main(argv=None):
    sys.stdout.reconfigure(encoding="utf-8")
    p = argparse.ArgumentParser(description=__doc__,
                                formatter_class=argparse.RawDescriptionHelpFormatter)
    p.add_argument("--start", default="2026-11-01")
    p.add_argument("--end", default="2026-11-12")
    p.add_argument("--split", default="2026-11-07", help="first test day")
    a = p.parse_args(argv)
    OUT.mkdir(exist_ok=True)
    pd.set_option("display.width", 200, "display.max_columns", 30,
                  "display.float_format", "{:.4f}".format)

    root = data_root()
    hcon = duckdb.connect(str(root / "hl.duckdb"), read_only=True)
    kcon = duckdb.connect(str(root / "kalshi.duckdb"), read_only=True)
    print("data root:", root)

    hl = load_hl(hcon, a.start, a.end)
    st = load_settlements(kcon, a.start, a.end)
    quotes = load_quotes(kcon, a.start, a.end)
    print(f"HL syms: {sorted(hl)} | settled windows: {len(st)} | tickers with quotes: {len(quotes)}")

    days = sorted(d for d in st.day.unique() if a.start <= d <= a.end)
    train = [d for d in days if d < a.split]
    test = [d for d in days if d >= a.split]
    print("train days:", train, "\ntest days:", test)

    # 1. alignment
    al = choose_alignment(st, hl, train)
    print("\n== 1. settlement alignment (train) ==\n", al.to_string(index=False))
    al.to_csv(OUT / "alignment.csv", index=False)
    best = al.iloc[0]
    col, off = best.col, int(best.off)
    add_vol(hl, col)
    bt = basis_table(st, hl, col, off)
    bt = bt[bt.day.isin(days)]
    per_series = bt.groupby("series").apply(lambda x: pd.Series(dict(
        n=len(x), basis_bp=x.b.median() * 1e4, err_mad_bp=x.b_err.abs().median() * 1e4,
        err_rms_bp=np.sqrt((x.b_err ** 2).mean()) * 1e4,
        move_mad_bp=(np.log(x.settle_px / x.strike)).abs().median() * 1e4)),
        include_groups=False)
    print(f"\nusing HL {col} at close{off:+d}s\n", per_series.to_string())
    per_series.to_csv(OUT / "basis_by_series.csv")

    # 2-4
    summary = dict(alignment=dict(col=col, off=off), train=train, test=test, runs={})
    grids = {}
    for lat in LATENCIES:
        grid = decision_grid(bt, hl, quotes, col, lat)
        grids[lat] = grid
        for thr in THRESHOLDS:
            for name, dd in (("train", train), ("test", test)):
                tr = trades(grid[grid.day.isin(dd)], thr)
                s = summarize(tr).iloc[0].to_dict() if len(tr) else dict(n=0)
                summary["runs"][f"lat{lat}_thr{thr}_{name}"] = {
                    k: (float(v) if isinstance(v, (int, float, np.floating, np.integer)) else v)
                    for k, v in s.items()}

    g0 = grids[LATENCIES[0]]
    print(f"\ndecision rows: {len(g0):,} over {g0.ticker.nunique():,} windows")
    cal = calibration(g0)
    print("\n== 2. calibration: Brier vs outcome (lower is better) ==\n", cal.to_string())
    cal.to_csv(OUT / "calibration.csv")
    conv = convergence(g0)
    print("\n== 3. lead-lag: Kalshi mid change after a fair-vs-mid gap ==\n", conv.to_string())
    conv.to_csv(OUT / "convergence.csv")

    print("\n== 4. backtest grid (pnl per contract, $) ==")
    rows = []
    for k, v in summary["runs"].items():
        lat, thr, name = k.split("_")
        rows.append(dict(latency=lat[3:], thr=thr[3:], split=name, **v))
    grid_df = pd.DataFrame(rows)
    piv = grid_df.pivot_table(index=["latency", "thr"], columns="split",
                              values=["n", "pnl_per", "t_stat"], aggfunc="first")
    print(piv.to_string())
    grid_df.to_csv(OUT / "backtest_grid.csv", index=False)

    # threshold chosen on train at 1s latency, reported on test
    lat = 1.0
    tr_scores = {thr: summary["runs"][f"lat{lat}_thr{thr}_train"] for thr in THRESHOLDS}
    best_thr = max(THRESHOLDS, key=lambda t: (tr_scores[t].get("total", -1e9)))
    tt = trades(grids[lat][grids[lat].day.isin(test)], best_thr)
    print(f"\nchosen on train (lat={lat}s): thr={best_thr}")
    by_series = summarize(tt, "series")
    by_side = summarize(tt, "side")
    tt["tau_b"] = pd.cut(tt.tau, [0, 60, 180, 420, 900], labels=["<1m", "1-3m", "3-7m", "7-15m"])
    by_tau = summarize(tt, "tau_b")
    print("test, by series\n", by_series.to_string())
    print("test, by side\n", by_side.to_string())
    print("test, by time-to-close at entry\n", by_tau.to_string())
    print("test, all\n", summarize(tt).to_string())
    tt.to_csv(OUT / "test_trades.csv", index=False)
    by_series.to_csv(OUT / "test_by_series.csv")
    summary["chosen"] = dict(latency=lat, thr=best_thr, test=summarize(tt).iloc[0].astype(str).to_dict())
    (OUT / "summary.json").write_text(json.dumps(summary, indent=1, default=str))

    plots(bt, cal, conv, tt, grids[lat], best_thr, train, test)
    print("\nwrote", OUT)


def plots(bt, cal, conv, tt, grid, thr, train, test):
    import matplotlib
    matplotlib.use("Agg")
    import matplotlib.pyplot as plt
    fig, ax = plt.subplots(2, 2, figsize=(13, 9))
    for s, g in bt.groupby("series"):
        ax[0, 0].plot(pd.to_datetime(g.close_ms, unit="ms"), g.b * 1e4, lw=0.6, label=s)
    ax[0, 0].set_title("HL -> Pyth settlement basis (bp)")
    ax[0, 0].legend(fontsize=6)
    ax[0, 0].set_ylim(np.nanpercentile(bt.b * 1e4, 1) - 5, np.nanpercentile(bt.b * 1e4, 99) + 5)
    x = np.arange(len(cal))
    ax[0, 1].bar(x - 0.2, cal.brier_model, 0.4, label="model (HL)")
    ax[0, 1].bar(x + 0.2, cal.brier_kalshi, 0.4, label="Kalshi mid")
    ax[0, 1].set_xticks(x, cal.index.astype(str))
    ax[0, 1].set_title("Brier score by time to close (lower = better)")
    ax[0, 1].legend()
    for c in [c for c in conv.columns if c.startswith("dmid")]:
        ax[1, 0].plot(conv.mean_gap, conv[c], marker="o", label=c)
    ax[1, 0].axhline(0, c="k", lw=0.5)
    ax[1, 0].plot(conv.mean_gap, conv.mean_gap, "k--", lw=0.5, label="full convergence")
    ax[1, 0].set_xlabel("fair - Kalshi mid at t")
    ax[1, 0].set_title("Kalshi mid change after t")
    ax[1, 0].legend(fontsize=7)
    for name, dd in (("train", train), ("test", test)):
        t = trades(grid[grid.day.isin(dd)], thr).sort_values("t")
        ax[1, 1].plot(pd.to_datetime(t.t, unit="s"), t.pnl.cumsum(), label=f"{name} (n={len(t)})")
    ax[1, 1].set_title(f"cumulative $ per 1-contract entry (thr={thr}, 1s latency, after fees)")
    ax[1, 1].legend()
    fig.autofmt_xdate()
    fig.tight_layout()
    fig.savefig(OUT / "prelim.png", dpi=120)


if __name__ == "__main__":
    main()
