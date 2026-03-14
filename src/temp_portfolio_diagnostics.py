from __future__ import annotations

from pathlib import Path

import matplotlib.pyplot as plt
import pandas as pd
from settings import config

DATA_DIR = Path(config("DATA_DIR"))

SCORES_PATH = DATA_DIR / "daily_headline_polarity.parquet"
CRSP_PATH = DATA_DIR / "CRSP_stock_daily.parquet"

OUT_COUNTS_CSV = DATA_DIR / "portfolio_size_diagnostics_daily.csv"
OUT_SUMMARY_CSV = DATA_DIR / "portfolio_size_diagnostics_summary.csv"
OUT_PLOT_ALL = DATA_DIR / "portfolio_size_diagnostics_all.png"
OUT_PLOT_GRID = DATA_DIR / "portfolio_size_diagnostics_grid.png"


def norm_ticker(s: pd.Series) -> pd.Series:
    return s.astype(str).str.upper().str.strip()


def is_nyse_exchange(s: pd.Series) -> pd.Series:
    x = s.astype(str).str.upper().str.strip()
    return x.isin({"N", "NYSE", "1"})


def load_base_df() -> pd.DataFrame:
    # ---------------------------
    # Load signals
    # ---------------------------
    sig = pd.read_parquet(SCORES_PATH)
    need_sig = {"ticker", "date", "score"}
    if not need_sig.issubset(sig.columns):
        raise KeyError(f"{SCORES_PATH.name} must include {need_sig}, got {set(sig.columns)}")

    sig = sig.copy()
    sig["ticker"] = norm_ticker(sig["ticker"])
    sig["date"] = pd.to_datetime(sig["date"], errors="coerce").dt.date
    sig = sig.dropna(subset=["ticker", "date", "score"])

    # Actionable only
    sig_tr = sig[sig["score"].isin([-1, 1])].copy()

    # ---------------------------
    # Load CRSP
    # ---------------------------
    crsp = pd.read_parquet(CRSP_PATH)
    need_crsp = {
        "permno",
        "ticker",
        "date",
        "dlyopen",
        "dlyclose",
        "dlycap",
        "primaryexch",
    }
    if not need_crsp.issubset(crsp.columns):
        raise KeyError(f"{CRSP_PATH.name} must include {need_crsp}, got {set(crsp.columns)}")

    crsp = crsp.copy()
    crsp["ticker"] = norm_ticker(crsp["ticker"])
    crsp["date"] = pd.to_datetime(crsp["date"], errors="coerce").dt.date

    crsp = crsp.sort_values(["permno", "date"]).reset_index(drop=True)
    crsp["prev_close"] = crsp.groupby("permno")["dlyclose"].shift(1)
    crsp["lag_cap"] = crsp.groupby("permno")["dlycap"].shift(1)

    # NYSE size breakpoint
    nyse = crsp[is_nyse_exchange(crsp["primaryexch"])].copy()
    nyse_bp = (
        nyse.dropna(subset=["lag_cap"])
        .groupby("date")["lag_cap"]
        .quantile(0.20)
        .rename("nyse_cap_p20")
        .reset_index()
    )
    crsp = crsp.merge(nyse_bp, on="date", how="left")

    crsp["eligible_restricted"] = (
        (crsp["prev_close"] > 5)
        & (crsp["lag_cap"] > crsp["nyse_cap_p20"])
    )

    # Keep rows where lagged characteristics exist
    crsp = crsp.dropna(subset=["permno", "ticker", "date", "prev_close", "lag_cap"]).copy()

    # ---------------------------
    # Mapping: match your current create_portfolios.py
    # ---------------------------
    # If your current create_portfolios.py uses a different mapping rule,
    # copy that exact block here.
    permno_map = (
        crsp.sort_values(["ticker", "date", "permno"])[["ticker", "date", "permno"]]
        .drop_duplicates(subset=["ticker", "date"], keep="first")
    )

    sig_tr = sig_tr.merge(permno_map, on=["ticker", "date"], how="inner")

    df = sig_tr.merge(
        crsp[
            [
                "permno",
                "date",
                "prev_close",
                "lag_cap",
                "nyse_cap_p20",
                "eligible_restricted",
            ]
        ],
        on=["permno", "date"],
        how="inner",
    )

    df["eligible_not_small"] = df["lag_cap"] > df["nyse_cap_p20"]
    df["eligible_price_gt_5"] = df["prev_close"] > 5

    return df


def counts_for_subset(df: pd.DataFrame, name: str) -> pd.DataFrame:
    out = (
        df.groupby("date")["score"]
        .value_counts()
        .unstack(fill_value=0)
        .rename(columns={1: "n_long", -1: "n_short"})
        .reset_index()
    )

    if "n_long" not in out.columns:
        out["n_long"] = 0
    if "n_short" not in out.columns:
        out["n_short"] = 0

    out["n_long"] = out["n_long"].astype(int)
    out["n_short"] = out["n_short"].astype(int)
    out["n_total"] = out["n_long"] + out["n_short"]
    out["portfolio"] = name

    return out[["portfolio", "date", "n_long", "n_short", "n_total"]]


def build_daily_counts(df: pd.DataFrame) -> pd.DataFrame:
    pieces = []

    # Table 1-style base universe
    pieces.append(counts_for_subset(df, "table1_unrestricted"))

    # Same data, shown in portfolio labels
    pieces.append(counts_for_subset(df[df["score"] == 1], "table1_long_only"))
    pieces.append(counts_for_subset(df[df["score"] == -1], "table1_short_only"))
    pieces.append(counts_for_subset(df, "table1_long_short"))

    # Figure 5 restricted variants
    pieces.append(
        counts_for_subset(
            df[df["eligible_restricted"].fillna(False)],
            "figure5_restricted",
        )
    )
    pieces.append(
        counts_for_subset(
            df[df["eligible_not_small"].fillna(False)],
            "figure5_not_small",
        )
    )
    pieces.append(
        counts_for_subset(
            df[df["eligible_price_gt_5"].fillna(False)],
            "figure5_price_gt_5",
        )
    )

    out = pd.concat(pieces, ignore_index=True).sort_values(["portfolio", "date"]).reset_index(drop=True)
    return out


def build_summary(daily_counts: pd.DataFrame) -> pd.DataFrame:
    summary = (
        daily_counts.groupby("portfolio")
        .agg(
            start_date=("date", "min"),
            end_date=("date", "max"),
            trading_days=("date", "nunique"),
            avg_n_long=("n_long", "mean"),
            median_n_long=("n_long", "median"),
            min_n_long=("n_long", "min"),
            max_n_long=("n_long", "max"),
            avg_n_short=("n_short", "mean"),
            median_n_short=("n_short", "median"),
            min_n_short=("n_short", "min"),
            max_n_short=("n_short", "max"),
            avg_n_total=("n_total", "mean"),
            median_n_total=("n_total", "median"),
            min_n_total=("n_total", "min"),
            max_n_total=("n_total", "max"),
        )
        .reset_index()
    )

    return summary


def make_overlay_plot(daily_counts: pd.DataFrame, out_path: Path) -> None:
    fig, ax = plt.subplots(figsize=(14, 8))

    order = [
        "table1_long_short",
        "figure5_restricted",
        "figure5_not_small",
        "figure5_price_gt_5",
    ]

    for portfolio in order:
        sub = daily_counts[daily_counts["portfolio"] == portfolio].sort_values("date")
        if sub.empty:
            continue
        ax.plot(sub["date"], sub["n_long"], label=f"{portfolio} long")
        ax.plot(sub["date"], sub["n_short"], linestyle="--", label=f"{portfolio} short")

    ax.set_title("Portfolio Size Diagnostics: Long and Short Counts")
    ax.set_xlabel("Date")
    ax.set_ylabel("Number of Stocks")
    ax.legend(loc="upper right", fontsize=9, ncol=2)
    ax.grid(True, alpha=0.3)

    fig.tight_layout()
    fig.savefig(out_path, dpi=300, bbox_inches="tight")
    plt.close(fig)


def make_grid_plot(daily_counts: pd.DataFrame, out_path: Path) -> None:
    portfolios = [
        "table1_long_short",
        "table1_long_only",
        "table1_short_only",
        "figure5_restricted",
        "figure5_not_small",
        "figure5_price_gt_5",
    ]

    fig, axes = plt.subplots(3, 2, figsize=(14, 12), sharex=True)
    axes = axes.flatten()

    for ax, portfolio in zip(axes, portfolios):
        sub = daily_counts[daily_counts["portfolio"] == portfolio].sort_values("date")
        if sub.empty:
            ax.set_visible(False)
            continue

        ax.plot(sub["date"], sub["n_long"], label="long")
        ax.plot(sub["date"], sub["n_short"], label="short")
        ax.set_title(portfolio)
        ax.set_ylabel("Stocks")
        ax.grid(True, alpha=0.3)
        ax.legend()

    for ax in axes[-2:]:
        ax.set_xlabel("Date")

    fig.suptitle("Portfolio Size Diagnostics by Portfolio", fontsize=16)
    fig.tight_layout(rect=[0, 0, 1, 0.98])
    fig.savefig(out_path, dpi=300, bbox_inches="tight")
    plt.close(fig)


def main() -> None:
    df = load_base_df()
    daily_counts = build_daily_counts(df)
    summary = build_summary(daily_counts)

    daily_counts.to_csv(OUT_COUNTS_CSV, index=False)
    summary.to_csv(OUT_SUMMARY_CSV, index=False)

    make_overlay_plot(daily_counts, OUT_PLOT_ALL)
    make_grid_plot(daily_counts, OUT_PLOT_GRID)

    print("\n==== Portfolio Size Diagnostics Summary ====\n")
    print(summary.to_string(index=False))
    print(f"\nWrote {OUT_COUNTS_CSV}")
    print(f"Wrote {OUT_SUMMARY_CSV}")
    print(f"Wrote {OUT_PLOT_ALL}")
    print(f"Wrote {OUT_PLOT_GRID}")


if __name__ == "__main__":
    main()
