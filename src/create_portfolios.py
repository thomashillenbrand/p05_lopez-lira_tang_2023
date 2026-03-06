from __future__ import annotations

from pathlib import Path

import pandas as pd
from settings import config

DATA_DIR = Path(config("DATA_DIR"))

SCORES_PATH = DATA_DIR / "daily_headline_polarity.parquet"   # ticker, date, n_headlines, score_sum, score ∈ {-1,0,1}
CRSP_PATH = DATA_DIR / "CRSP_clean_daily.parquet"            # needs: ticker,dlycaldt,dlyopen,dlyclose
OUT_PATH = DATA_DIR / "portfolio_daily_returns.parquet"


def norm_ticker(s: pd.Series) -> pd.Series:
    return s.astype(str).str.upper().str.strip()


def main():
    # --- Load firm-day signal ---
    sig = pd.read_parquet(SCORES_PATH)
    need_sig = {"ticker", "date", "score"}
    if not need_sig.issubset(sig.columns):
        raise KeyError(f"{SCORES_PATH.name} must include {need_sig}, got {set(sig.columns)}")

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

    # --- Load CRSP open/close and compute open->close (drift/tradable) return ---
    crsp = pd.read_parquet(CRSP_PATH)
    need_crsp = {"ticker", "date", "dlyopen", "dlyclose"}
    if not need_crsp.issubset(crsp.columns):
        raise KeyError(f"{CRSP_PATH.name} must include {need_crsp}, got {set(crsp.columns)}")

    crsp = crsp.copy()
    crsp["ticker"] = norm_ticker(crsp["ticker"])
    crsp["date"] = pd.to_datetime(crsp["date"], errors="coerce").dt.date
    # Drift return (open->close)
    crsp["ret_oc"] = (crsp["dlyclose"] / crsp["dlyopen"]) - 1

    # Initial reaction return (prev close -> open), computed within ticker
    crsp = crsp.sort_values(["permno", "date"])
    crsp["prev_close"] = crsp.groupby("permno")["dlyclose"].shift(1)
    crsp["ret_ir"] = (crsp["dlyopen"] / crsp["prev_close"]) - 1

    crsp = crsp.dropna(subset=["ret_oc", "ret_ir"])

    # ---------------------------
    # PERMNO mapping (minimal)
    # ---------------------------
    # Keep only what we need for mapping and returns
    crsp = crsp.dropna(subset=["permno", "ticker", "date", "dlyopen", "dlyclose"]).copy()

    # Build a (ticker, date) -> permno map from CRSP itself
    # If multiple permnos share a ticker-date (rare), keep the first.
    permno_map = crsp[["ticker", "date", "permno"]].drop_duplicates(subset=["ticker", "date"])

    # Attach permno to actionable signals
    sig_tr = sig_tr.merge(permno_map, on=["ticker", "date"], how="inner")

    # ---------------------------
    # Compute returns (permno-based)
    # ---------------------------
    crsp = crsp.sort_values(["permno", "date"])

    # Drift: open -> close
    crsp["ret_oc"] = (crsp["dlyclose"] / crsp["dlyopen"]) - 1

    # Initial Reaction: prev close -> open (permno lag, NOT ticker lag)
    crsp["prev_close"] = crsp.groupby("permno")["dlyclose"].shift(1)
    crsp["ret_ir"] = (crsp["dlyopen"] / crsp["prev_close"]) - 1

    crsp = crsp.dropna(subset=["ret_oc", "ret_ir"])

    # Merge signals to returns on permno+date
    df = sig_tr.merge(crsp[["permno", "date", "ret_oc", "ret_ir"]], on=["permno", "date"], how="inner")
    
    # Equal-weight legs for DRIFT (open->close)
    long_leg = df[df["score"] == 1].groupby("date", as_index=False).agg(
        ret_long=("ret_oc", "mean"),
        n_long=("ret_oc", "size"),
    )
    short_leg = df[df["score"] == -1].groupby("date", as_index=False).agg(
        ret_short_raw=("ret_oc", "mean"),
        n_short=("ret_oc", "size"),
    )
    short_leg["ret_short"] = -short_leg["ret_short_raw"]
    short_leg = short_leg.drop(columns=["ret_short_raw"])

    # Equal-weight legs for INITIAL REACTION (prev close->open)
    ir_long_leg = df[df["score"] == 1].groupby("date", as_index=False).agg(
        ret_ir_long=("ret_ir", "mean"),
    )
    ir_short_leg = df[df["score"] == -1].groupby("date", as_index=False).agg(
        ret_ir_short_raw=("ret_ir", "mean"),
    )
    ir_short_leg["ret_ir_short"] = -ir_short_leg["ret_ir_short_raw"]
    ir_short_leg = ir_short_leg.drop(columns=["ret_ir_short_raw"])

    out = (
        counts.merge(long_leg, on="date", how="left")
            .merge(short_leg, on="date", how="left")
            .merge(ir_long_leg, on="date", how="left")
            .merge(ir_short_leg, on="date", how="left")
            .sort_values("date")
            .reset_index(drop=True)
    )

    # Fill missing realized legs with 0 returns, 0 counts
    out["ret_long"] = out["ret_long"].fillna(0.0)
    out["ret_short"] = out["ret_short"].fillna(0.0)
    out["ret_ir_long"] = out["ret_ir_long"].fillna(0.0)
    out["ret_ir_short"] = out["ret_ir_short"].fillna(0.0)

    out["n_long"] = out["n_long"].fillna(0).astype(int)
    out["n_short"] = out["n_short"].fillna(0).astype(int)

    # Trade flags based on SIGNAL counts (paper logic)
    out["trade_long"] = out["n_pos"] >= 1
    out["trade_short"] = out["n_neg"] >= 1

    both_legs = (out["n_pos"] >= 2) & (out["n_neg"] >= 2)
    long_only_ls = (out["n_pos"] >= 1) & (out["n_neg"] < 2)
    short_only_ls = (out["n_neg"] >= 1) & (out["n_pos"] < 2)
    out["trade_ls"] = both_legs | long_only_ls | short_only_ls

    # LS DRIFT return: both legs if eligible; else single leg
    out["ret_ls"] = 0.0
    out.loc[both_legs, "ret_ls"] = out.loc[both_legs, "ret_long"] + out.loc[both_legs, "ret_short"]
    out.loc[long_only_ls, "ret_ls"] = out.loc[long_only_ls, "ret_long"]
    out.loc[short_only_ls, "ret_ls"] = out.loc[short_only_ls, "ret_short"]

    # LS INITIAL REACTION return: same leg rule
    out["ret_ir_ls"] = 0.0
    out.loc[both_legs, "ret_ir_ls"] = out.loc[both_legs, "ret_ir_long"] + out.loc[both_legs, "ret_ir_short"]
    out.loc[long_only_ls, "ret_ir_ls"] = out.loc[long_only_ls, "ret_ir_long"]
    out.loc[short_only_ls, "ret_ir_ls"] = out.loc[short_only_ls, "ret_ir_short"]


    out.to_parquet(OUT_PATH, index=False)
    print(f"Wrote {OUT_PATH} (rows={len(out):,})")
    print(out.head())


if __name__ == "__main__":
    main()