from __future__ import annotations

from pathlib import Path

import numpy as np
import pandas as pd

from settings import config

DATA_DIR = Path(config("DATA_DIR"))
OUTPUT_DIR = Path(config("OUTPUT_DIR"))

PORT_PATH = DATA_DIR / "portfolio_daily_returns.parquet"
OUT_PAPER = OUTPUT_DIR / "table1_overnight_paper_sample.csv"
OUT_FULL = OUTPUT_DIR / "table1_overnight_full_sample.csv"
PAPER_START = pd.Timestamp("2021-10-01")
PAPER_END = pd.Timestamp("2024-05-31")


def annualized_sharpe(x: pd.Series, periods_per_year: int = 252) -> float:
    x = pd.to_numeric(x, errors="coerce").dropna()
    if len(x) < 2:
        return np.nan
    sd = x.std(ddof=1)
    if pd.isna(sd) or sd == 0:
        return np.nan
    return np.sqrt(periods_per_year) * x.mean() / sd


def hit_rate(x: pd.Series) -> float:
    x = pd.to_numeric(x, errors="coerce").dropna()
    if len(x) == 0:
        return np.nan
    return (x > 0).mean() * 100.0


def mean_pct(x: pd.Series) -> float:
    x = pd.to_numeric(x, errors="coerce").dropna()
    if len(x) == 0:
        return np.nan
    return x.mean() * 100.0


def summarize_portfolio(
    df: pd.DataFrame, label: str, ir_col: str, drift_col: str, trade_col: str
) -> dict:
    traded = df[df[trade_col].fillna(False)].copy()

    return {
        "Portfolio": label,
        "Initial Reaction Hit Rate (%)": hit_rate(traded[ir_col]),
        "Initial Reaction Mean Return (%)": mean_pct(traded[ir_col]),
        "Drift Hit Rate (%)": hit_rate(traded[drift_col]),
        "Drift Mean Return (%)": mean_pct(traded[drift_col]),
        "Drift Sharpe Ratio": annualized_sharpe(traded[drift_col]),
        "Trading Days": int(traded.shape[0]),
        "Firm-Day Observations": np.nan,  # filled only on summary row
    }


def build_table(df: pd.DataFrame) -> pd.DataFrame:
    rows = [
        summarize_portfolio(
            df, "Long-Short Portfolio", "ret_ir_ls", "ret_ls", "trade_ls"
        ),
        summarize_portfolio(
            df, "Long-Only Portfolio", "ret_ir_long", "ret_long", "trade_long"
        ),
        summarize_portfolio(
            df, "Short-Only Portfolio", "ret_ir_short", "ret_short", "trade_short"
        ),
    ]

    out = pd.DataFrame(rows)

    summary_row = pd.DataFrame(
        [
            {
                "Portfolio": "Sample Summary",
                "Initial Reaction Hit Rate (%)": np.nan,
                "Initial Reaction Mean Return (%)": np.nan,
                "Drift Hit Rate (%)": np.nan,
                "Drift Mean Return (%)": np.nan,
                "Drift Sharpe Ratio": np.nan,
                "Trading Days": int(df["date"].nunique()),
                "Firm-Day Observations": int(df["n_total"].fillna(0).sum()),
            }
        ]
    )

    out = pd.concat([out, summary_row], ignore_index=True)

    value_cols = [
        "Initial Reaction Hit Rate (%)",
        "Initial Reaction Mean Return (%)",
        "Drift Hit Rate (%)",
        "Drift Mean Return (%)",
        "Drift Sharpe Ratio",
    ]
    out[value_cols] = out[value_cols].round(3)

    return out


def filter_window(
    df: pd.DataFrame, start: pd.Timestamp | None, end: pd.Timestamp | None
) -> pd.DataFrame:
    out = df.copy()
    if start is not None:
        out = out[out["date"] >= start]
    if end is not None:
        out = out[out["date"] <= end]
    return out


def main():
    df = pd.read_parquet(PORT_PATH).copy()
    need = {
        "date",
        "n_total",
        "ret_ir_ls",
        "ret_ls",
        "trade_ls",
        "ret_ir_long",
        "ret_long",
        "trade_long",
        "ret_ir_short",
        "ret_short",
        "trade_short",
    }
    if not need.issubset(df.columns):
        raise KeyError(f"{PORT_PATH.name} must include {need}, got {set(df.columns)}")

    df["date"] = pd.to_datetime(df["date"], errors="coerce")
    df = df.dropna(subset=["date"]).sort_values("date").reset_index(drop=True)

    # Paper sample
    df_paper = filter_window(df, PAPER_START, PAPER_END)
    table_paper = build_table(df_paper)
    table_paper.to_csv(OUT_PAPER, index=False)

    # Full sample
    table_full = build_table(df)
    table_full.to_csv(OUT_FULL, index=False)

    print(f"Wrote {OUT_PAPER} (rows={len(table_paper):,})")
    print(f"Wrote {OUT_FULL} (rows={len(table_full):,})")
    print("\nPaper sample date range:")
    print(df_paper["date"].min(), "to", df_paper["date"].max())
    print("\nFull sample date range:")
    print(df["date"].min(), "to", df["date"].max())


if __name__ == "__main__":
    main()
