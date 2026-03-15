from __future__ import annotations

from pathlib import Path

import pandas as pd

from settings import config

OUTPUT_DIR = Path(config("OUTPUT_DIR"))

LABEL_CSV = OUTPUT_DIR / "openai_output_label_proportions.csv"
TABLE1_PAPER_CSV = OUTPUT_DIR / "table1_overnight_paper_sample.csv"
TABLE1_FULL_CSV = OUTPUT_DIR / "table1_overnight_full_sample.csv"

LABEL_TEX = OUTPUT_DIR / "label_ratio_table.tex"
TABLE1_PAPER_TEX = OUTPUT_DIR / "replication_table1_paper_sample.tex"
TABLE1_FULL_TEX = OUTPUT_DIR / "replication_table1_full_sample.tex"

SUMMARY_CRSP_CSV = OUTPUT_DIR / "summary_stats_crsp_universe.csv"
SUMMARY_NEWS_YEAR_CSV = OUTPUT_DIR / "summary_stats_news_by_year.csv"
SUMMARY_NEWS_TIMING_CSV = OUTPUT_DIR / "summary_stats_news_by_timing.csv"

SUMMARY_CRSP_TEX = OUTPUT_DIR / "summary_stats_crsp_universe.tex"
SUMMARY_NEWS_YEAR_TEX = OUTPUT_DIR / "summary_stats_news_by_year.tex"
SUMMARY_NEWS_TIMING_TEX = OUTPUT_DIR / "summary_stats_news_by_timing.tex"


def fmt3(x) -> str:
    try:
        return f"{float(x):.3f}"
    except Exception:
        return str(x)


def escape_latex_text(value) -> str:
    text = str(value)
    replacements = {
        "\\": r"\\textbackslash{}",
        "&": r"\\&",
        "%": r"\\%",
        "$": r"\\$",
        "#": r"\\#",
        "_": r"\\_",
        "{": r"\\{",
        "}": r"\\}",
        "~": r"\\textasciitilde{}",
        "^": r"\\textasciicircum{}",
    }
    return "".join(replacements.get(char, char) for char in text)


def make_label_ratio_table() -> None:
    df = pd.read_csv(LABEL_CSV)

    df = df[
        [
            "label",
            "proportion_2021_2024_sample_period",
            "proportion_full_sample",
        ]
    ].copy()

    df.columns = [
        "Label",
        "Sample Period (2021--2024)",
        "Full Sample",
    ]

    latex = df.to_latex(
        index=False,
        escape=True,
        float_format=lambda x: f"{x:.3f}",
        column_format="lcc",
    )

    LABEL_TEX.parent.mkdir(parents=True, exist_ok=True)
    LABEL_TEX.write_text(latex, encoding="utf-8")
    print(f"Wrote {LABEL_TEX}")


def build_compact_table1(df: pd.DataFrame) -> str:
    rows = []
    summary_row = None

    for _, r in df.iterrows():
        portfolio = str(r["Portfolio"]).strip()

        if portfolio.lower() == "sample summary":
            summary_row = r
            continue

        rows.append(
            rf"\multicolumn{{4}}{{l}}{{\textbf{{{escape_latex_text(portfolio)}}}}} \\"
        )
        rows.append(
            "Hit Rate (\\%)"
            + " & "
            + fmt3(r["Initial Reaction Hit Rate (%)"])
            + " & "
            + fmt3(r["Drift Hit Rate (%)"])
            + " & "
            + fmt3(r["Drift Sharpe Ratio"])
            + r" \\"
        )
        rows.append(
            "Mean Return (\\%)"
            + " & "
            + fmt3(r["Initial Reaction Mean Return (%)"])
            + " & "
            + fmt3(r["Drift Mean Return (%)"])
            + " & "
            + r" \\"
        )
        rows.append(r"\addlinespace")

    if summary_row is not None:
        obs = summary_row.get("Firm-Day Observations", "")
        days = summary_row.get("Trading Days", "")
    else:
        ls = df.iloc[0]
        obs = ls.get("Firm-Day Observations", "")
        days = ls.get("Trading Days", "")

    obs = escape_latex_text(obs)
    days = escape_latex_text(days)

    body = "\n".join(rows)

    table = (
        rf"""
\resizebox{{0.58\textwidth}}{{!}}{{%
\begin{{tabular}}{{lccc}}
\toprule
& \multicolumn{{3}}{{c}}{{Overnight News}} \\
\cmidrule(lr){{2-4}}
Metric & Initial Reaction & Drift & Sharpe Ratio \\
\midrule
{body}
\midrule
Firm-Day Observations & {obs} & {obs} &  \\
Trading Days & {days} & {days} &  \\
\bottomrule
\end{{tabular}}%
}}
""".strip()
        + "\n"
    )

    return table


def make_replication_table1(input_csv: Path, output_tex: Path) -> None:
    df = pd.read_csv(input_csv)
    latex = build_compact_table1(df)

    output_tex.parent.mkdir(parents=True, exist_ok=True)
    output_tex.write_text(latex, encoding="utf-8")
    print(f"Wrote {output_tex}")


def make_summary_tables():

    # ---------- CRSP universe ----------
    df = pd.read_csv(SUMMARY_CRSP_CSV)

    latex = df.to_latex(
        index=False,
        float_format=lambda x: f"{x:.2f}",
        column_format="lccccc",
        escape=True,
    )

    SUMMARY_CRSP_TEX.write_text(latex)
    print(f"Wrote {SUMMARY_CRSP_TEX}")

    # ---------- News distribution by year ----------
    df = pd.read_csv(SUMMARY_NEWS_YEAR_CSV)

    latex = df.to_latex(
        index=False,
        float_format=lambda x: f"{x:.1f}",
        column_format="lccccc",
        escape=True,
    )

    SUMMARY_NEWS_YEAR_TEX.write_text(latex)
    print(f"Wrote {SUMMARY_NEWS_YEAR_TEX}")

    # ---------- News distribution by timing ----------
    df = pd.read_csv(SUMMARY_NEWS_TIMING_CSV)

    latex = df.to_latex(
        index=False,
        float_format=lambda x: f"{x:.1f}",
        column_format="lccccc",
        escape=True,
    )

    SUMMARY_NEWS_TIMING_TEX.write_text(latex)
    print(f"Wrote {SUMMARY_NEWS_TIMING_TEX}")


def main() -> None:
    make_label_ratio_table()
    make_replication_table1(TABLE1_PAPER_CSV, TABLE1_PAPER_TEX)
    make_replication_table1(TABLE1_FULL_CSV, TABLE1_FULL_TEX)
    make_summary_tables()


if __name__ == "__main__":
    main()
