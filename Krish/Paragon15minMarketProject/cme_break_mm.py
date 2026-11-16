"""Make markets in Kalshi 15m commodity windows off the HL fair, during the CME daily break vs normal hours.

Question: when CME is shut (17:00-18:00 ET, Mon-Thu) do Kalshi books widen, does the HL perp
fair become *the* price, and can a maker quoting fair +/- h earn spread net of adverse selection?
How fast must the quote follow the fair?

Fill model (no queue data): replay every Kalshi trade. A taker buying YES at p fills our ask a
if a <= p ("touch") or a < p ("strict", we are strictly ahead of the book); a taker selling YES
at p fills our bid b if b >= p (or b > p). Fill size = min(trade count, Q). Our quotes never
cross the pre-trade Kalshi book. Every fill is held to settlement.

Latency L: the quote at trade time t is built from the HL price known at t - L. L < 0 means the
quote sees the price |L| s before our HL recorder did, i.e. a source faster than the HL feed
(Pyth Hermes, CME/ICE direct). Same clock for both feeds (one machine).

    .venv/Scripts/python.exe Krish/Paragon15minMarketProject/cme_break_mm.py
"""
import json
import sys

import duckdb
import numpy as np
import pandas as pd

import prelim as P

LATENCIES = [-10, -5, -3, -2, -1, 0, 1, 2, 5]
HALF_SPREADS = [0.01, 0.02, 0.03, 0.05]
Q = 100                 # max contracts per fill
STOP_TAU_S = 10         # pull quotes this close to expiry
MAKER_FEE = 0.0         # Kalshi maker fee coef (0.0175 on maker-fee markets); sensitivity below
LIP_PER_WINDOW = 20.0   # $ pool per window while the series LIP ran (ended 2026-10-28)
LIP_TARGET = 300        # contracts, the program's target size


def regime_of(open_ms):
    et = pd.DatetimeIndex(pd.to_datetime(np.asarray(open_ms), unit="ms", utc=True)).tz_convert("America/New_York")
    hm = et.hour * 60 + et.minute
    wk = et.dayofweek < 4                                  # Mon-Thu have a daily break
    return np.select([wk & (hm >= 17 * 60) & (hm < 18 * 60),
                      (hm >= 18 * 60) & (hm < 18 * 60 + 30)],
                     ["cme_break", "reopen_30m"], "normal")


def load_trades(con, start, end):
    ser = ", ".join("'%s'" % s for s in P.SERIES_SYM)
    return con.sql(f"""
        SELECT ticker, rx_ns, taker_side, yes_px, count
        FROM trades WHERE series IN ({ser}) AND dt BETWEEN DATE '{start}' AND DATE '{end}' + 1
        ORDER BY ticker, rx_ns""").df()


def fills_for(bt, hl, quotes, trades_by_ticker, L, h, strict):
    out = []
    for r in bt.itertuples():
        tr = trades_by_ticker.get(r.ticker)
        q = quotes.get(r.ticker)
        g = hl.get(r.sym)
        if tr is None or q is None or g is None or np.isnan(r.b_hat) or np.isnan(r.s_b):
            continue
        t = tr.rx_ns.to_numpy() / 1e9
        keep = (t >= r.open_ms / 1000) & (t <= r.close_ms / 1000 - STOP_TAU_S)
        if not keep.any():
            continue
        tr, t = tr[keep], t[keep]
        sec = np.floor(t - L).astype(np.int64) + 1          # hl_at reads bucket sec-1
        lp, var_s = P.hl_at(g, "oracle", sec), P.hl_at(g, "var_s", sec)
        tau = r.close_ms / 1000 - t
        fair = P.ncdf((lp + r.b_hat - np.log(r.strike)) / np.sqrt(var_s * tau + r.s_b ** 2))
        qms, qv = q
        i = np.searchsorted(qms, tr.rx_ns.to_numpy() // 1_000_000 - 1, side="right") - 1
        mb = np.where(i >= 0, qv[np.maximum(i, 0), 0], np.nan)
        ma = np.where(i >= 0, qv[np.maximum(i, 0), 1], np.nan)
        bid = np.floor((fair - h) * 100 + 1e-9) / 100
        ask = np.ceil((fair + h) * 100 - 1e-9) / 100
        bid = np.minimum(bid, ma - 0.01)                    # never cross the book
        ask = np.maximum(ask, mb + 0.01)
        p, side, cnt = tr.yes_px.to_numpy(), tr.taker_side.to_numpy(), tr["count"].to_numpy()
        if strict:
            hit_ask = (side == "yes") & (ask < p)
            hit_bid = (side == "no") & (bid > p)
        else:
            hit_ask = (side == "yes") & (ask <= p)
            hit_bid = (side == "no") & (bid >= p)
        ok = ~np.isnan(fair) & (var_s > 0) & (bid > 0) & (ask < 1)
        hit_ask &= ok
        hit_bid &= ok
        if not (hit_ask.any() or hit_bid.any()):
            continue
        qty = np.minimum(cnt, Q)
        px = np.where(hit_ask, ask, bid)
        sel = hit_ask | hit_bid
        sgn = np.where(hit_ask, -1.0, 1.0)                  # +1 = we bought YES
        pnl = sgn * (r.outcome - px) - MAKER_FEE * px * (1 - px)
        out.append(pd.DataFrame(dict(
            ticker=r.ticker, series=r.series, regime=r.regime, day=r.day, tau=tau[sel],
            qty=qty[sel], px=px[sel], fair=fair[sel], sgn=sgn[sel], pnl=pnl[sel],
            mkt_spread=(ma - mb)[sel])))
    return pd.concat(out, ignore_index=True) if out else pd.DataFrame()


def summarize(f, n_days_by_regime):
    def g(x):
        w = x.qty
        pnl_c = (x.pnl * w).sum() / w.sum()
        # t-stat on per-window $ PnL (fills inside a window are correlated)
        win = (x.pnl * w).groupby(x.ticker).sum()
        return pd.Series(dict(contracts=w.sum(), windows=x.ticker.nunique(),
                              pnl_per_contract=pnl_c, usd_total=(x.pnl * w).sum(),
                              usd_per_day=(x.pnl * w).sum() / n_days_by_regime[x.name],
                              t_windows=win.mean() / (win.std(ddof=1) / np.sqrt(len(win)))
                              if len(win) > 2 else np.nan))
    return f.groupby("regime").apply(g, include_groups=False)


def main():
    sys.stdout.reconfigure(encoding="utf-8")
    pd.set_option("display.width", 220, "display.max_columns", 30,
                  "display.float_format", "{:.4f}".format)
    root = P.data_root()
    hcon = duckdb.connect(str(root / "hl.duckdb"), read_only=True)
    kcon = duckdb.connect(str(root / "kalshi.duckdb"), read_only=True)
    start, end = "2026-11-01", "2026-11-12"
    hl = P.load_hl(hcon, start, end)
    P.add_vol(hl, "oracle")
    st = P.load_settlements(kcon, start, end)
    quotes = P.load_quotes(kcon, start, end)
    trades = load_trades(kcon, start, end)
    tbt = {t: g for t, g in trades.groupby("ticker", sort=False)}
    bt = P.basis_table(st, hl, "oracle", 0)
    bt = bt[(bt.day >= start) & (bt.day <= end)].copy()
    bt["regime"] = regime_of(bt.open_ms)
    days = bt.groupby("regime").day.nunique().to_dict()
    out = {}

    # --- A. what the break looks like ------------------------------------------------------
    inv = bt.pivot_table(index="series", columns="regime", values="ticker", aggfunc="count").fillna(0)
    print("== windows per regime ==\n", inv.astype(int).to_string())
    grid = P.decision_grid(bt, hl, quotes, "oracle", 0.0)
    grid = grid.merge(bt[["ticker", "regime"]], on="ticker")
    # spreads incl. books wider than prelim's 10c filter: re-read raw quote widths on the grid
    raw = []
    for r in bt.itertuples():
        q = quotes.get(r.ticker)
        if q is None:
            continue
        ts = np.arange(r.open_ms + 30_000, r.close_ms - 10_000, 5_000)
        i = np.searchsorted(q[0], ts, side="right") - 1
        v = q[1][i[i >= 0]]
        raw.append(pd.DataFrame(dict(regime=r.regime, series=r.series, spread=v[:, 1] - v[:, 0],
                                     top_sz=np.minimum(v[:, 2], v[:, 3]))))
    raw = pd.concat(raw)
    vol = trades.merge(bt[["ticker", "regime"]], on="ticker").groupby("regime")["count"].sum()
    nwin = bt.groupby("regime").size()
    look = pd.DataFrame(dict(
        windows=nwin,
        spread_med=raw.groupby("regime").spread.median(),
        spread_mean=raw.groupby("regime").spread.mean(),
        pct_wider_5c=raw.groupby("regime").spread.apply(lambda s: (s > 0.05).mean()),
        top_size_med=raw.groupby("regime").top_sz.median(),
        contracts_per_window=vol / nwin,
        basis_err_mad_bp=bt.groupby("regime").b_err.apply(lambda s: s.abs().median() * 1e4),
        brier_model=grid.groupby("regime").apply(lambda x: ((x.fair - x.outcome) ** 2).mean(),
                                                 include_groups=False),
        brier_kalshi=grid.groupby("regime").apply(lambda x: ((x.mid - x.outcome) ** 2).mean(),
                                                  include_groups=False)))
    print("\n== market state by regime ==\n", look.to_string())
    by_series = raw.groupby(["regime", "series"]).spread.median().unstack(0)
    print("\nmedian spread by series\n", by_series.to_string())

    # lead-lag per regime: corr(dKalshi mid_t, dHL fair_{t-lag})
    g = grid.sort_values(["ticker", "t"])
    dm, df_ = g.groupby("ticker").mid.diff(), g.groupby("ticker").fair.diff()
    ll = {}
    for reg in g.regime.unique():
        m = g.regime == reg
        row = {}
        for k in range(-2, 3):
            sh = df_.groupby(g.ticker).shift(k)
            ok = m & dm.notna() & sh.notna()
            row[f"{k * P.STEP_S:+d}s"] = np.corrcoef(dm[ok], sh[ok])[0, 1] if ok.sum() > 50 else np.nan
        ll[reg] = row
    ll = pd.DataFrame(ll).T
    print("\n== lead-lag: corr(dKalshi mid, dHL fair lagged); +5s column = HL leads ==\n", ll.to_string())

    # --- B. maker simulation -----------------------------------------------------------------
    rows = []
    for strict in (False, True):
        for h in HALF_SPREADS:
            for L in LATENCIES:
                f = fills_for(bt, hl, quotes, tbt, L, h, strict)
                if f.empty:
                    continue
                s = summarize(f, days)
                for reg, v in s.iterrows():
                    rows.append(dict(fill="strict" if strict else "touch", h=h, L=L, regime=reg, **v))
    sim = pd.DataFrame(rows)
    sim.to_csv(P.OUT / "mm_sim.csv", index=False)
    for fill in ("touch", "strict"):
        piv = sim[sim.fill == fill].pivot_table(index=["regime", "h"], columns="L",
                                                values="pnl_per_contract")
        print(f"\n== maker pnl $/contract, fill={fill}  (columns: latency vs HL feed, s) ==\n",
              piv.to_string())
    piv_d = sim[sim.fill == "touch"].pivot_table(index=["regime", "h"], columns="L",
                                                 values="usd_per_day")
    print("\n== maker $/day (Q=100, touch) ==\n", piv_d.to_string())
    piv_c = sim[(sim.fill == "touch")].pivot_table(index=["regime", "h"], columns="L",
                                                   values="contracts")
    print("\n== contracts filled (touch) ==\n", piv_c.round(0).to_string())

    # break-even latency per regime/h: largest L with pnl > 0
    be = {}
    for (fill, reg, h), x in sim.groupby(["fill", "regime", "h"]):
        pos = x[x.pnl_per_contract > 0].L
        be[(fill, reg, h)] = pos.max() if len(pos) else None
    be = pd.Series(be).unstack(0)
    print("\n== largest latency (s, vs HL feed) still profitable; NaN = never ==\n", be.to_string())

    # --- C. liquidity rewards (historical program) ---------------------------------------------
    lip = raw.groupby("regime").top_sz.median()
    share = LIP_TARGET / (LIP_TARGET + lip)
    lip_tab = pd.DataFrame(dict(top_size_med=lip, est_share_if_300_at_inside=share,
                                usd_per_window=LIP_PER_WINDOW * share,
                                windows_per_day=nwin / pd.Series(days),
                                usd_per_day=LIP_PER_WINDOW * share * nwin / pd.Series(days)))
    print("\n== LIP upside if the $20/window program were live (it ended 2026-10-28) ==\n",
          lip_tab.to_string())

    out.update(market_state=look.round(5).reset_index().to_dict("records"),
               lead_lag=ll.round(4).reset_index().to_dict("records"),
               breakeven_latency=be.reset_index().astype(str).to_dict("records"),
               lip=lip_tab.round(3).reset_index().to_dict("records"))
    (P.OUT / "mm_summary.json").write_text(json.dumps(out, indent=1, default=str))
    look.to_csv(P.OUT / "mm_market_state.csv")


if __name__ == "__main__":
    main()
