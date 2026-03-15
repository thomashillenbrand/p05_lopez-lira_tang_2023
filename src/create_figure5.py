from __future__ import annotations

from pathlib import Path

import matplotlib.dates as mdates
import matplotlib.pyplot as plt
import matplotlib.ticker as mticker
import pandas as pd
from settings import config

DATA_DIR = Path(config("DATA_DIR"))
OUTPUT_DIR = Path(config("OUTPUT_DIR"))

PORT_PATH = DATA_DIR / "portfolio_daily_returns.parquet"

OUT_PAPER_PNG = OUTPUT_DIR / "figure5_paper_sample.png"
OUT_FULL_PNG = OUTPUT_DIR / "figure5_full_sample.png"

OUT_PAPER_CSV = OUTPUT_DIR / "figure5_paper_sample_series.csv"
OUT_FULL_CSV = OUTPUT_DIR / "figure5_full_sample_series.csv"

# Match the paper window
PAPER_START = pd.Timestamp("2021-10-01")
PAPER_END = pd.Timestamp("2024-05-31")


def prepare_series(df: pd.DataFrame) -> pd.DataFrame:
    out = df.copy()
    out["date"] = pd.to_datetime(out["date"], errors="coerce")
    out = out.dropna(subset=["date"]).sort_values("date").reset_index(drop=True)

    needed = [
        "ret_ls",
        "ret_ls_not_small",
        "ret_ls_price_gt_5",
        "ret_mkt_vw",
    ]
    missing = [c for c in needed if c not in out.columns]
    if missing:
        raise KeyError(
            "Missing required columns in portfolio_daily_returns.parquet: "
            + ", ".join(missing)
        )

    for c in needed:
        out[c] = pd.to_numeric(out[c], errors="coerce").fillna(0.0)

    # Cumulative wealth of $1 invested
    out["wealth_ls"] = (1.0 + out["ret_ls"]).cumprod()
    out["wealth_not_small"] = (1.0 + out["ret_ls_not_small"]).cumprod()
    out["wealth_price_gt_5"] = (1.0 + out["ret_ls_price_gt_5"]).cumprod()
    out["wealth_mkt_vw"] = (1.0 + out["ret_mkt_vw"]).cumprod()

    return out


def make_plot(df: pd.DataFrame, out_png: Path) -> None:
    fig, ax = plt.subplots(figsize=(8.4, 5.8), facecolor="white")
    ax.set_facecolor("white")

    # Match paper ordering/style as closely as possible
    ax.plot(df["date"], df["wealth_ls"], label="Long–Short", linewidth=1.2)
    ax.plot(
        df["date"],
        df["wealth_not_small"],
        label="Not Small",
        linewidth=1.0,
        linestyle="--",
    )
    ax.plot(
        df["date"],
        df["wealth_price_gt_5"],
        label="Price > 5",
        linewidth=1.0,
        linestyle=":",
    )
    ax.plot(
        df["date"],
        df["wealth_mkt_vw"],
        label="Market Value–Weighted",
        linewidth=1.0,
        linestyle="-.",
    )

    # Paper is effectively shown on log scale
    ax.set_yscale("log")

    ax.set_xlabel("Date", fontsize=14)
    ax.set_ylabel("Portfolio Value in $", fontsize=18)

    # Year ticks only
    ax.xaxis.set_major_locator(mdates.YearLocator())
    ax.xaxis.set_major_formatter(mdates.DateFormatter("%Y"))

    # Show log ticks as plain numbers
    yticks = [1, 2, 3, 4, 5, 6, 7, 8, 9]
    ax.set_yticks(yticks)
    ax.yaxis.set_major_formatter(mticker.FormatStrFormatter("%g"))
    ax.yaxis.set_minor_formatter(mticker.NullFormatter())

    ax.set_ylim(0.6, 10)

    ax.legend(
        loc="upper left",
        frameon=False,
        fontsize=12,
        handlelength=0.6,
        handletextpad=0.3,
        borderaxespad=1.5,
    )

    ax.grid(False)
    ax.tick_params(axis="both", labelsize=11)

    fig.tight_layout()
    fig.savefig(out_png, dpi=300, bbox_inches="tight")
    plt.close(fig)


def export_series(df: pd.DataFrame, out_csv: Path) -> None:
    df[
        [
            "date",
            "wealth_ls",
            "wealth_not_small",
            "wealth_price_gt_5",
            "wealth_mkt_vw",
        ]
    ].to_csv(out_csv, index=False)


def main() -> None:
    df = pd.read_parquet(PORT_PATH)

    # Full sample
    full = prepare_series(df)
    export_series(full, OUT_FULL_CSV)
    make_plot(full, OUT_FULL_PNG)

    # Paper sample
    paper = df.copy()
    paper["date"] = pd.to_datetime(paper["date"], errors="coerce")
    paper = paper[
        (paper["date"] >= PAPER_START) & (paper["date"] <= PAPER_END)
    ].copy()

    paper = prepare_series(paper)
    export_series(paper, OUT_PAPER_CSV)
    make_plot(paper, OUT_PAPER_PNG)

    print(f"Wrote {OUT_PAPER_PNG}")
    print(f"Wrote {OUT_FULL_PNG}")
    print(f"Wrote {OUT_PAPER_CSV}")
    print(f"Wrote {OUT_FULL_CSV}")


if __name__ == "__main__":
    main()
