from __future__ import annotations

from pathlib import Path

import numpy as np
import pandas as pd

from settings import config

DATA_DIR = Path(config("DATA_DIR"))
OUTPUT_DIR = Path(config("OUTPUT_DIR"))

CRSP_PATH = DATA_DIR / "CRSP_stock_daily.parquet"
RAVENPACK_PATH = DATA_DIR / "RAVENPACK.parquet"

OUT_CRSP_CSV = OUTPUT_DIR / "summary_stats_crsp_universe.csv"
OUT_NEWS_YEAR_CSV = OUTPUT_DIR / "summary_stats_news_by_year.csv"
OUT_NEWS_TIMING_CSV = OUTPUT_DIR / "summary_stats_news_by_timing.csv"


def norm_ticker(s: pd.Series) -> pd.Series:
    return s.astype(str).str.upper().str.strip()


def percentile(x: pd.Series, q: float) -> float:
    x = pd.to_numeric(x, errors="coerce").dropna()
    if x.empty:
        return np.nan
    return float(x.quantile(q))


def summarize_series(x: pd.Series) -> dict[str, float]:
    x = pd.to_numeric(x, errors="coerce").dropna()
    return {
        "Mean": float(x.mean()) if not x.empty else np.nan,
        "SD": float(x.std()) if not x.empty else np.nan,
        "P25": percentile(x, 0.25),
        "Median": float(x.median()) if not x.empty else np.nan,
        "P75": percentile(x, 0.75),
    }


def classify_timing(ts_utc: pd.Series) -> pd.Series:
    """
    Approximate paper-style timing split using UTC timestamps.

    Assumption:
    - U.S. regular trading hours are roughly 9:30am to 4:00pm ET.
    - In UTC this is approximately:
        * 14:30 to 21:00 during EST
        * 13:30 to 20:00 during EDT

    To avoid calendar/DST complexity in a lightweight summary script,
    classify by converting to America/New_York and using local market hours.
    """
    ts = pd.to_datetime(ts_utc, errors="coerce", utc=True)
    ts_ny = ts.dt.tz_convert("America/New_York")

    minutes = ts_ny.dt.hour * 60 + ts_ny.dt.minute
    market_open = 9 * 60 + 30
    market_close = 16 * 60

    timing = np.where(
        (minutes >= market_open) & (minutes < market_close),
        "Intraday",
        "Overnight",
    )
    return pd.Series(timing, index=ts_utc.index)


def create_crsp_summary() -> pd.DataFrame:
    crsp = pd.read_parquet(CRSP_PATH).copy()

    required = {"ticker", "date", "dlycap", "dlyopen", "dlyclose"}
    missing = required.difference(crsp.columns)
    if missing:
        raise KeyError(f"{CRSP_PATH.name} missing required columns: {sorted(missing)}")

    crsp["ticker"] = norm_ticker(crsp["ticker"])
    crsp["date"] = pd.to_datetime(crsp["date"], errors="coerce")
    crsp["dlycap"] = pd.to_numeric(crsp["dlycap"], errors="coerce")
    crsp["dlyopen"] = pd.to_numeric(crsp["dlyopen"], errors="coerce")
    crsp["dlyclose"] = pd.to_numeric(crsp["dlyclose"], errors="coerce")

    crsp = crsp.dropna(
        subset=["ticker", "date", "dlycap", "dlyopen", "dlyclose"]
    ).copy()
    crsp = crsp[(crsp["dlyopen"] > 0) & (crsp["dlyclose"] > 0)].copy()

    # Market cap in millions
    crsp["market_cap_m"] = crsp["dlycap"] / 1_000_000.0

    # Daily close-to-close-ish return proxy unavailable from columns,
    # so use open-to-close daily return from available fields.
    crsp["daily_return_pct"] = ((crsp["dlyclose"] / crsp["dlyopen"]) - 1.0) * 100.0

    rows = []

    mc = summarize_series(crsp["market_cap_m"])
    rows.append({"Metric": "Market Cap ($M)", **mc})

    dr = summarize_series(crsp["daily_return_pct"])
    rows.append({"Metric": "Daily Return (%)", **dr})

    out = pd.DataFrame(rows)
    return out


def create_news_by_year() -> pd.DataFrame:
    rp = pd.read_parquet(RAVENPACK_PATH).copy()

    required = {"rp_entity_id", "rpa_date_utc", "headline"}
    missing = required.difference(rp.columns)
    if missing:
        raise KeyError(
            f"{RAVENPACK_PATH.name} missing required columns: {sorted(missing)}"
        )

    rp["rpa_date_utc"] = pd.to_datetime(rp["rpa_date_utc"], errors="coerce")
    rp["headline"] = rp["headline"].astype(str)
    rp = rp.dropna(subset=["rp_entity_id", "rpa_date_utc", "headline"]).copy()

    rp["year"] = rp["rpa_date_utc"].dt.year
    rp["date_only"] = rp["rpa_date_utc"].dt.date

    out = (
        rp.groupby("year")
        .agg(
            Headlines=("headline", "size"),
            Firms=("rp_entity_id", "nunique"),
            Days=("date_only", "nunique"),
        )
        .reset_index()
        .rename(columns={"year": "Year"})
    )

    out["Per Day"] = out["Headlines"] / out["Days"]
    out["Per Firm"] = out["Headlines"] / out["Firms"]

    for col in ["Per Day", "Per Firm"]:
        out[col] = out[col].round(1)

    return out[["Year", "Headlines", "Firms", "Days", "Per Day", "Per Firm"]]


def create_news_by_timing() -> pd.DataFrame:
    rp = pd.read_parquet(RAVENPACK_PATH).copy()

    required = {"rp_entity_id", "timestamp_utc", "headline"}
    missing = required.difference(rp.columns)
    if missing:
        raise KeyError(
            f"{RAVENPACK_PATH.name} missing required columns: {sorted(missing)}"
        )

    rp["timestamp_utc"] = pd.to_datetime(rp["timestamp_utc"], errors="coerce", utc=True)
    rp["headline"] = rp["headline"].astype(str)
    rp = rp.dropna(subset=["rp_entity_id", "timestamp_utc", "headline"]).copy()

    rp["Timing"] = classify_timing(rp["timestamp_utc"])
    rp["date_only"] = rp["timestamp_utc"].dt.date

    out = (
        rp.groupby("Timing")
        .agg(
            Headlines=("headline", "size"),
            Firms=("rp_entity_id", "nunique"),
            Days=("date_only", "nunique"),
        )
        .reset_index()
    )

    total_headlines = out["Headlines"].sum()
    out["% Total"] = (out["Headlines"] / total_headlines * 100.0).round(1)
    out["Per Day"] = (out["Headlines"] / out["Days"]).round(1)

    # Order like the paper
    order = pd.Categorical(
        out["Timing"], categories=["Overnight", "Intraday"], ordered=True
    )
    out = out.assign(_order=order).sort_values("_order").drop(columns="_order")

    return out[["Timing", "Headlines", "% Total", "Firms", "Per Day"]]


def main() -> None:
    crsp_summary = create_crsp_summary()
    news_year = create_news_by_year()
    news_timing = create_news_by_timing()

    crsp_summary.to_csv(OUT_CRSP_CSV, index=False)
    news_year.to_csv(OUT_NEWS_YEAR_CSV, index=False)
    news_timing.to_csv(OUT_NEWS_TIMING_CSV, index=False)

    print(f"Wrote {OUT_CRSP_CSV}")
    print(crsp_summary.to_string(index=False))
    print()

    print(f"Wrote {OUT_NEWS_YEAR_CSV}")
    print(news_year.to_string(index=False))
    print()

    print(f"Wrote {OUT_NEWS_TIMING_CSV}")
    print(news_timing.to_string(index=False))


if __name__ == "__main__":
    main()
