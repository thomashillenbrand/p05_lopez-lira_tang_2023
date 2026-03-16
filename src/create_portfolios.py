from __future__ import annotations

from pathlib import Path

import pandas as pd

from settings import config

DATA_DIR = Path(config("DATA_DIR"))

SCORES_PATH = (
    DATA_DIR / "daily_headline_polarity.parquet"
)  # ticker, date, n_headlines, score_sum, score ∈ {-1,0,1}
CRSP_PATH = (
    DATA_DIR / "CRSP_stock_daily.parquet"
)  # needs: ticker,dlycaldt,dlyopen,dlyclose
OUT_PATH = DATA_DIR / "portfolio_daily_returns.parquet"


def norm_ticker(s: pd.Series) -> pd.Series:
    return s.astype(str).str.upper().str.strip()


def is_nyse_exchange(s: pd.Series) -> pd.Series:
    x = s.astype(str).str.upper().str.strip()
    return x.isin({"N", "NYSE", "1"})


def value_weighted_return(df: pd.DataFrame, ret_col: str, weight_col: str) -> float:
    x = df[[ret_col, weight_col]].dropna()
    if x.empty:
        return float("nan")
    w = x[weight_col]
    if w.sum() <= 0:
        return float("nan")
    return (x[ret_col] * w).sum() / w.sum()


def main():
    # --- Load firm-day signal ---
    sig = pd.read_parquet(SCORES_PATH)
    need_sig = {"ticker", "date", "score"}
    if not need_sig.issubset(sig.columns):
        raise KeyError(
            f"{SCORES_PATH.name} must include {need_sig}, got {set(sig.columns)}"
        )

    sig = sig.copy()
    sig["ticker"] = norm_ticker(sig["ticker"])
    sig["date"] = pd.to_datetime(sig["date"], errors="coerce").dt.date
    sig = sig.dropna(subset=["ticker", "date", "score"])

    # Daily counts (include neutrals in totals; neutrals are non-trading)
    counts = (
        sig.groupby("date")["score"]
        .value_counts()
        .unstack(fill_value=0)
        .rename(columns={-1: "n_neg", 0: "n_neu", 1: "n_pos"})
        .reset_index()
    )
    for c in ["n_neg", "n_neu", "n_pos"]:
        if c not in counts.columns:
            counts[c] = 0
    counts["n_total"] = counts["n_neg"] + counts["n_neu"] + counts["n_pos"]

    # Actionable signals only for returns
    sig_tr = sig[sig["score"].isin([-1, 1])].copy()

    # --- Load CRSP open/close and compute returns ---
    crsp = pd.read_parquet(CRSP_PATH)
    need_crsp = {"ticker", "date", "dlyopen", "dlyclose"}
    if not need_crsp.issubset(crsp.columns):
        raise KeyError(
            f"{CRSP_PATH.name} must include {need_crsp}, got {set(crsp.columns)}"
        )

    crsp = crsp.copy()
    crsp["ticker"] = norm_ticker(crsp["ticker"])
    crsp["date"] = pd.to_datetime(crsp["date"], errors="coerce").dt.date

    # Compute returns and lagged characteristics at PERMNO level
    crsp = crsp.sort_values(["permno", "date"]).reset_index(drop=True)
    crsp["prev_close"] = crsp.groupby("permno")["dlyclose"].shift(1)
    crsp["lag_cap"] = crsp.groupby("permno")["dlycap"].shift(1)

    crsp["ret_oc"] = crsp["dlyclose"] / crsp["dlyopen"] - 1
    crsp["ret_ir"] = crsp["dlyopen"] / crsp["prev_close"] - 1
    crsp["ret_cc"] = crsp["dlyclose"] / crsp["prev_close"] - 1

    # NYSE 20th percentile breakpoint using prior-day market cap
    nyse = crsp[is_nyse_exchange(crsp["primaryexch"])].copy()
    nyse_bp = (
        nyse.dropna(subset=["lag_cap"])
        .groupby("date")["lag_cap"]
        .quantile(0.20)
        .rename("nyse_cap_p20")
        .reset_index()
    )
    crsp = crsp.merge(nyse_bp, on="date", how="left")

    # Figure 5 restricted sample:
    # prior-day close > $5 and prior-day cap > NYSE 20th percentile
    crsp["eligible_restricted"] = (crsp["prev_close"] > 5) & (
        crsp["lag_cap"] > crsp["nyse_cap_p20"]
    )

    crsp = crsp.dropna(subset=["ret_oc", "ret_ir"])

    # ---------------------------
    # PERMNO mapping (minimal)
    # ---------------------------
    crsp = crsp.dropna(
        subset=["permno", "ticker", "date", "dlyopen", "dlyclose"]
    ).copy()

    # Build a (ticker, date) -> permno map from CRSP itself
    # If multiple permnos share a ticker-date, drop those ambiguous cases
    permno_map = crsp.sort_values(["ticker", "date", "permno"])[
        ["ticker", "date", "permno"]
    ].drop_duplicates(subset=["ticker", "date"], keep="first")

    # Attach permno to actionable signals
    sig_tr = sig_tr.merge(permno_map, on=["ticker", "date"], how="inner")

    # Merge signals to returns on permno+date
    df = sig_tr.merge(
        crsp[
            [
                "permno",
                "date",
                "ret_oc",
                "ret_ir",
                "lag_cap",
                "eligible_restricted",
                "prev_close",
                "nyse_cap_p20",
            ]
        ],
        on=["permno", "date"],
        how="inner",
    )

    # Separate Figure 5 restrictions
    df["eligible_not_small"] = df["lag_cap"] > df["nyse_cap_p20"]
    df["eligible_price_gt_5"] = df["prev_close"] > 5

    # Equal-weight legs for DRIFT (open->close)
    long_leg = (
        df[df["score"] == 1]
        .groupby("date", as_index=False)
        .agg(
            ret_long=("ret_oc", "mean"),
            n_long=("ret_oc", "size"),
        )
    )
    short_leg = (
        df[df["score"] == -1]
        .groupby("date", as_index=False)
        .agg(
            ret_short_raw=("ret_oc", "mean"),
            n_short=("ret_oc", "size"),
        )
    )
    short_leg["ret_short"] = -short_leg["ret_short_raw"]
    short_leg = short_leg.drop(columns=["ret_short_raw"])

    def build_restricted_ls(
        series_df: pd.DataFrame, eligibility_col: str, out_col: str
    ) -> pd.DataFrame:
        """
        Minimal Figure 5 fix only:
        build restricted series as true long-short portfolios, with no
        long-only / short-only fallback.

        This does NOT change Table 1 logic below.
        """
        x = series_df[series_df[eligibility_col].fillna(False)].copy()

        long_leg_x = (
            x[x["score"] == 1]
            .groupby("date", as_index=False)
            .agg(
                ret_long_x=("ret_oc", "mean"),
                n_long_x=("ret_oc", "size"),
            )
        )

        short_leg_x = (
            x[x["score"] == -1]
            .groupby("date", as_index=False)
            .agg(
                ret_short_x_raw=("ret_oc", "mean"),
                n_short_x=("ret_oc", "size"),
            )
        )
        short_leg_x["ret_short_x"] = -short_leg_x["ret_short_x_raw"]
        short_leg_x = short_leg_x.drop(columns=["ret_short_x_raw"])

        out_x = (
            long_leg_x.merge(short_leg_x, on="date", how="outer")
            .sort_values("date")
            .reset_index(drop=True)
        )

        for c in ["ret_long_x", "ret_short_x"]:
            if c not in out_x.columns:
                out_x[c] = 0.0
            out_x[c] = out_x[c].fillna(0.0)

        for c in ["n_long_x", "n_short_x"]:
            if c not in out_x.columns:
                out_x[c] = 0
            out_x[c] = out_x[c].fillna(0).astype(int)

        out_x[out_col] = 0.0

        # Minimal fix: require both sides, no one-sided fallback
        both_x = (out_x["n_long_x"] >= 1) & (out_x["n_short_x"] >= 1)
        out_x.loc[both_x, out_col] = (
            out_x.loc[both_x, "ret_long_x"] + out_x.loc[both_x, "ret_short_x"]
        )

        return out_x[["date", out_col]]

    # =====================================
    # RESTRICTED SAMPLE FOR FIGURE 5
    # =====================================
    rest = build_restricted_ls(df, "eligible_restricted", "ret_ls_restricted")
    not_small = build_restricted_ls(df, "eligible_not_small", "ret_ls_not_small")
    price_gt_5 = build_restricted_ls(df, "eligible_price_gt_5", "ret_ls_price_gt_5")

    mkt = (
        crsp.dropna(subset=["date", "ret_oc", "lag_cap"])
        .loc[lambda x: x["lag_cap"] > 0]
        .groupby("date")
        .apply(lambda x: value_weighted_return(x, "ret_oc", "lag_cap"))
        .rename("ret_mkt_vw")
        .reset_index()
    )

    # Equal-weight legs for INITIAL REACTION (prev close->open)
    ir_long_leg = (
        df[df["score"] == 1]
        .groupby("date", as_index=False)
        .agg(
            ret_ir_long=("ret_ir", "mean"),
        )
    )
    ir_short_leg = (
        df[df["score"] == -1]
        .groupby("date", as_index=False)
        .agg(
            ret_ir_short_raw=("ret_ir", "mean"),
        )
    )
    ir_short_leg["ret_ir_short"] = -ir_short_leg["ret_ir_short_raw"]
    ir_short_leg = ir_short_leg.drop(columns=["ret_ir_short_raw"])

    out = (
        counts.merge(long_leg, on="date", how="left")
        .merge(short_leg, on="date", how="left")
        .merge(ir_long_leg, on="date", how="left")
        .merge(ir_short_leg, on="date", how="left")
        .merge(rest[["date", "ret_ls_restricted"]], on="date", how="left")
        .merge(not_small[["date", "ret_ls_not_small"]], on="date", how="left")
        .merge(price_gt_5[["date", "ret_ls_price_gt_5"]], on="date", how="left")
        .merge(mkt[["date", "ret_mkt_vw"]], on="date", how="left")
        .sort_values("date")
        .reset_index(drop=True)
    )

    # Fill missing realized legs with 0 returns, 0 counts
    out["ret_long"] = out["ret_long"].fillna(0.0)
    out["ret_short"] = out["ret_short"].fillna(0.0)
    out["ret_ir_long"] = out["ret_ir_long"].fillna(0.0)
    out["ret_ir_short"] = out["ret_ir_short"].fillna(0.0)

    # Figure 5 series only
    out["ret_ls_restricted"] = out["ret_ls_restricted"].fillna(0.0)
    out["ret_ls_not_small"] = out["ret_ls_not_small"].fillna(0.0)
    out["ret_ls_price_gt_5"] = out["ret_ls_price_gt_5"].fillna(0.0)
    out["ret_mkt_vw"] = out["ret_mkt_vw"].fillna(0.0)

    out["n_long"] = out["n_long"].fillna(0).astype(int)
    out["n_short"] = out["n_short"].fillna(0).astype(int)

    # --------------------------------------------------
    # TABLE 1 LOGIC BELOW: LEFT EXACTLY UNCHANGED
    # --------------------------------------------------
    # Trade flags based on SIGNAL counts (paper logic)
    out["trade_long"] = out["n_pos"] >= 1
    out["trade_short"] = out["n_neg"] >= 1

    both_legs = (out["n_pos"] >= 2) & (out["n_neg"] >= 2)
    long_only_ls = (out["n_pos"] >= 1) & (out["n_neg"] < 2)
    short_only_ls = (out["n_neg"] >= 1) & (out["n_pos"] < 2)
    out["trade_ls"] = both_legs | long_only_ls | short_only_ls

    # LS DRIFT return: both legs if eligible; else single leg
    out["ret_ls"] = 0.0
    out.loc[both_legs, "ret_ls"] = (
        out.loc[both_legs, "ret_long"] + out.loc[both_legs, "ret_short"]
    )
    out.loc[long_only_ls, "ret_ls"] = out.loc[long_only_ls, "ret_long"]
    out.loc[short_only_ls, "ret_ls"] = out.loc[short_only_ls, "ret_short"]

    # LS INITIAL REACTION return: same leg rule
    out["ret_ir_ls"] = 0.0
    out.loc[both_legs, "ret_ir_ls"] = (
        out.loc[both_legs, "ret_ir_long"] + out.loc[both_legs, "ret_ir_short"]
    )
    out.loc[long_only_ls, "ret_ir_ls"] = out.loc[long_only_ls, "ret_ir_long"]
    out.loc[short_only_ls, "ret_ir_ls"] = out.loc[short_only_ls, "ret_ir_short"]

    out.to_parquet(OUT_PATH, index=False)
    print(f"Wrote {OUT_PATH} (rows={len(out):,})")
    print(out.head())


if __name__ == "__main__":
    main()
