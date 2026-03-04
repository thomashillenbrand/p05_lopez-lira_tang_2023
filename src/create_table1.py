from __future__ import annotations

from pathlib import Path

import numpy as np
import pandas as pd
from settings import config

DATA_DIR = Path(config("DATA_DIR"))

PORT_PATH = DATA_DIR / "portfolio_daily_returns.parquet"
SCORES_PATH = DATA_DIR / "daily_headline_polarity.parquet"
OUT_CSV = DATA_DIR / "table1_overnight_drift.csv"


ANN_FACTOR = np.sqrt(252)


def summarize(ret: pd.Series) -> dict:
    """Return hit rate (%), mean return (%), annualized Sharpe, and N days."""
    ret = pd.to_numeric(ret, errors="coerce").dropna()
    n = int(ret.shape[0])
    if n == 0:
        return {"hit": np.nan, "mean": np.nan, "sharpe": np.nan, "n": 0}

    hit = 100.0 * (ret > 0).mean()
    mean = 100.0 * ret.mean()

    sd = ret.std(ddof=1)
    sharpe = (ANN_FACTOR * ret.mean() / sd) if (sd is not None and sd > 0) else np.nan

    return {"hit": hit, "mean": mean, "sharpe": sharpe, "n": n}


def main():
    port = pd.read_parquet(PORT_PATH).sort_values("date")

    required = {
        "date",
        "ret_ls", "ret_long", "ret_short",
        "trade_ls", "trade_long", "trade_short",
        "n_total",
    }
    missing = required - set(port.columns)
    if missing:
        raise KeyError(f"{PORT_PATH.name} missing columns: {missing}")

    # Firm-day observations (paper counts firm-day combinations incl neutrals)
    scores = pd.read_parquet(SCORES_PATH)
    firm_day_obs = int(scores.shape[0])

    specs = [
        ("Long-Short Portfolio", "trade_ls", "ret_ls"),
        ("Long-Only Portfolio", "trade_long", "ret_long"),
        ("Short-Only Portfolio", "trade_short", "ret_short"),
    ]

    rows = []
    for name, trade_col, ret_col in specs:
        s = summarize(port.loc[port[trade_col], ret_col])
        rows.append(
            {
                "Portfolio": name,
                "Hit Rate (%)": s["hit"],
                "Mean Return (%)": s["mean"],
                "Sharpe Ratio": s["sharpe"],
                "Trading Days": s["n"],
            }
        )

    table = pd.DataFrame(rows)

    # Summary rows similar to the paper (for overnight portion)
    summary = pd.DataFrame(
        [
            {"Portfolio": "Firm-Day Observations", "Hit Rate (%)": firm_day_obs},
            {"Portfolio": "Trading Days", "Hit Rate (%)": int(port["trade_ls"].sum())},
        ]
    )

    out = pd.concat([table, summary], ignore_index=True)

    # nicer formatting (optional)
    for c in ["Hit Rate (%)", "Mean Return (%)", "Sharpe Ratio"]:
        if c in out.columns:
            out[c] = out[c].astype(float)

    out.to_csv(OUT_CSV, index=False)
    print(out)
    print(f"\nWrote: {OUT_CSV}")


if __name__ == "__main__":
    main()