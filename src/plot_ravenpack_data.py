from pathlib import Path

import pandas as pd
import plotly.express as px

from settings import config

DATA_DIR = Path(config("DATA_DIR"))
OUTPUT_DIR = Path(config("OUTPUT_DIR"))

MARKET_OPEN_HOUR = 9
MARKET_CLOSE_HOUR = 16
ROLLING_DAYS = 7  # can make zero or none to not use rolling avg


df = pd.read_parquet(DATA_DIR / "RAVENPACK.parquet")

ts = pd.to_datetime(df["timestamp_utc"], errors="coerce")
if ts.dt.tz is None:
    ts = ts.dt.tz_localize("UTC")
ts_et = ts.dt.tz_convert("America/New_York")

df["date_et"] = ts_et.dt.date
hour_et = ts_et.dt.hour + ts_et.dt.minute / 60.0 + ts_et.dt.second / 3600.0
df["is_overnight"] = (hour_et < MARKET_OPEN_HOUR) | (hour_et >= MARKET_CLOSE_HOUR)

daily = (
    df.groupby("date_et")["is_overnight"]
      .agg(n_total="size", n_overnight="sum")
      .sort_index()
)
daily["p_overnight"] = daily["n_overnight"] / daily["n_total"]
daily["p_intraday"] = 1.0 - daily["p_overnight"]

plot_df = daily[["p_overnight", "p_intraday"]].copy()
if ROLLING_DAYS and ROLLING_DAYS > 1:
    plot_df = plot_df.rolling(ROLLING_DAYS, min_periods=ROLLING_DAYS).mean()

plot_long = (
    plot_df.reset_index()
          .melt(id_vars="date_et", var_name="series", value_name="proportion")
)

series_name = {
    "p_overnight": "Overnight (<9am or ≥4pm ET)",
    "p_intraday": "Intraday (9am–4pm ET)",
}
plot_long["series"] = plot_long["series"].map(series_name)

avg_overnight = plot_df["p_overnight"].mean()
avg_intraday = plot_df["p_intraday"].mean()

fig = px.line(
    plot_long,
    x="date_et",
    y="proportion",
    color="series",
    title="Proportion of Overnight vs Intraday News",
    labels={"date_et": "Date (US/Eastern)", "proportion": "Proportion", "series": ""},
)

# Average reference lines + left-anchored annotations
fig.add_hline(y=avg_overnight, line_dash="dash")
fig.add_hline(y=avg_intraday, line_dash="dash")

fig.add_annotation(
    x="2022-01-01", y=0.70, xref="x", yref="y",
    text=f"Avg Overnight = {avg_overnight:.2f}",
    showarrow=False, xanchor="left"
)
fig.add_annotation(
    x="2022-01-01", y=0.30, xref="x", yref="y",
    text=f"Avg Intraday = {avg_intraday:.2f}",
    showarrow=False, xanchor="left"
)

out = OUTPUT_DIR / "ravenpack_overnight_intraday_proportion.html"
fig.write_html(out, include_plotlyjs="cdn")
print(f"Saved: {out}")