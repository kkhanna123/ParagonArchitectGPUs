"""Follow-ups to prelim.py: who leads (HL or Kalshi), and is the model's loss a tuning artefact?

    .venv/Scripts/python.exe Krish/Paragon15minMarketProject/diag.py
"""
import sys

import duckdb
import numpy as np
import pandas as pd

import prelim as P


def main():
    sys.stdout.reconfigure(encoding="utf-8")
    pd.set_option("display.width", 200, "display.float_format", "{:.4f}".format)
    root = P.data_root()
    hcon = duckdb.connect(str(root / "hl.duckdb"), read_only=True)
    kcon = duckdb.connect(str(root / "kalshi.duckdb"), read_only=True)
    start, end = "2026-11-01", "2026-11-12"
    hl = P.load_hl(hcon, start, end)
    st = P.load_settlements(kcon, start, end)
    quotes = P.load_quotes(kcon, start, end)
    days = sorted(d for d in st.day.unique() if start <= d <= end)

    tau_bins = [0, 60, 180, 420, 900]
    res = {}
    for col in ("oracle", "mid"):
        P.add_vol(hl, col)
        bt = P.basis_table(st, hl, col, 0)
        bt = bt[bt.day.isin(days)]
        for sb_mult in (1.0, 0.0):
            b2 = bt.assign(s_b=bt.s_b * sb_mult)
            g = P.decision_grid(b2, hl, quotes, col, 0.0)
            tb = pd.cut(g.tau, tau_bins)
            res[f"{col}, s_b x{sb_mult:g}"] = ((g.fair - g.outcome) ** 2).groupby(tb, observed=True).mean()
            if col == "oracle" and sb_mult == 1.0:
                res["Kalshi mid"] = ((g.mid - g.outcome) ** 2).groupby(tb, observed=True).mean()
                base = g
    print("== Brier by time to close (lower is better) ==")
    print(pd.DataFrame(res).T.to_string())

    # who leads: corr of Kalshi mid changes with HL-fair changes k steps earlier (k>0: HL leads)
    g = base.sort_values(["ticker", "t"])
    d_mid = g.groupby("ticker").mid.diff()
    d_fair = g.groupby("ticker").fair.diff()
    rows = []
    for k in range(-4, 5):
        sh = d_fair.groupby(g.ticker).shift(k)
        ok = d_mid.notna() & sh.notna()
        rows.append(dict(lag_s=k * P.STEP_S, corr=np.corrcoef(d_mid[ok], sh[ok])[0, 1]))
    print("\n== corr(dKalshi mid at t, dHL fair at t - lag), 5s changes; lag>0 means HL leads ==")
    print(pd.DataFrame(rows).to_string(index=False))

    # per-series last-minute Brier
    last = base[base.tau < 60]
    print("\n== last minute Brier per series ==")
    print(last.groupby("series").apply(lambda x: pd.Series(dict(
        n=len(x), model=((x.fair - x.outcome) ** 2).mean(),
        kalshi=((x.mid - x.outcome) ** 2).mean())), include_groups=False).to_string())


if __name__ == "__main__":
    main()
