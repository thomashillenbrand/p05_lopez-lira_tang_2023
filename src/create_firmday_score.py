from __future__ import annotations

"""
create_firmday_score.py

Build firm-day scores from OpenAI batch outputs and align to Lopez-Lira & Tang
overnight timing (9:00 / 16:00 ET). Includes verbose, step-by-step coverage checks.

Reads:
  OUTPUT_DIR/openai_headline_batch_output*.jsonl
  DATA_DIR/id_to_row_mapping*.json
  DATA_DIR/openai_headline_requests*.jsonl
  DATA_DIR/RAVENPACK_cleaned.parquet
  DATA_DIR/CRSP_clean_daily.parquet

Writes:
  DATA_DIR/daily_headline_polarity.parquet

Output columns:
  ticker, date (trade_date), n_headlines, score_sum, score
"""

import json
import re
from datetime import time as dtime
from pathlib import Path
from typing import Iterable

import pandas as pd
from settings import config


DATA_DIR = Path(config("DATA_DIR"))
OUTPUT_DIR = Path(config("OUTPUT_DIR"))

RAVENPACK_PATH = DATA_DIR / "RAVENPACK_cleaned.parquet"
CRSP_PATH = DATA_DIR / "CRSP_clean_daily.parquet"
OUT_PATH = DATA_DIR / "daily_headline_polarity.parquet"


YESNOUNK = {"YES", "NO", "UNKNOWN"}


def iter_jsonl(p: Path) -> Iterable[dict]:
    with p.open("r", encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if line:
                yield json.loads(line)


def norm_ticker(s: pd.Series) -> pd.Series:
    return s.astype(str).str.upper().str.strip()


def norm_text(s: pd.Series) -> pd.Series:
    return s.astype(str).str.strip()


def extract_label(content: str) -> str | None:
    if not content:
        return None
    first = content.strip().splitlines()[0].strip().upper()
    tok = re.split(r"\W+", first)[0].strip()
    if tok in YESNOUNK:
        return tok
    for k in ("YES", "NO", "UNKNOWN"):
        if k in first:
            return k
    return None


def label_to_score(label: str | None) -> int:
    if label == "YES":
        return 1
    if label == "NO":
        return -1
    return 0


def headline_from_user_prompt(user_content: str) -> str | None:
    m = re.search(r"\bHeadline:\s*(.*)\s*$", user_content or "", flags=re.DOTALL)
    return m.group(1).strip() if m else None


def load_outputs() -> pd.DataFrame:
    paths = sorted(OUTPUT_DIR.glob("openai_headline_batch_output*.jsonl"))
    if not paths:
        raise FileNotFoundError(f"No outputs found in {OUTPUT_DIR} matching openai_headline_batch_output*.jsonl")

    rows = []
    empty_choices = empty_content = unparseable = 0

    for p in paths:
        for obj in iter_jsonl(p):
            cid = obj.get("custom_id")
            body = (obj.get("response") or {}).get("body") or {}
            choices = body.get("choices") or []
            if not cid or not choices:
                empty_choices += 1
                continue
            content = ((choices[0].get("message") or {}).get("content")) or ""
            if not content:
                empty_content += 1
                continue
            label = extract_label(content)
            if label is None:
                unparseable += 1
                continue
            rows.append({"custom_id": cid, "label": label, "score": label_to_score(label)})

    df = pd.DataFrame(rows).drop_duplicates("custom_id")
    if df.empty:
        raise ValueError("Parsed 0 scored outputs from batch output JSONL files.")

    print(f"[CHECK] outputs parsed unique custom_id: {len(df):,}")
    if empty_choices or empty_content or unparseable:
        print(f"[CHECK] outputs skipped: empty_choices={empty_choices:,}, empty_content={empty_content:,}, unparseable={unparseable:,}")
    return df


def load_mapping() -> pd.DataFrame:
    paths = sorted(DATA_DIR.glob("id_to_row_mapping*.json"))
    if not paths:
        raise FileNotFoundError(f"No mapping files found in {DATA_DIR} matching id_to_row_mapping*.json")

    frames = []
    for p in paths:
        m = pd.read_json(p, orient="index")
        m.index.name = "custom_id"
        frames.append(m.reset_index())

    df = pd.concat(frames, ignore_index=True).drop_duplicates("custom_id")
    need = {"custom_id", "ticker", "entity_name", "date"}
    if not need.issubset(df.columns):
        raise KeyError(f"Mapping missing columns. Need {need}, got {set(df.columns)}")

    df["ticker"] = norm_ticker(df["ticker"])
    df["entity_name"] = norm_text(df["entity_name"])
    df["date"] = pd.to_datetime(df["date"], errors="coerce").dt.date
    df = df.dropna(subset=["custom_id", "ticker", "entity_name", "date"])

    print(f"[CHECK] mapping rows unique custom_id: {len(df):,}")
    return df[["custom_id", "ticker", "entity_name", "date"]]


def load_requests() -> pd.DataFrame:
    paths = sorted(DATA_DIR.glob("openai_headline_requests*.jsonl"))
    if not paths:
        raise FileNotFoundError(f"No request files found in {DATA_DIR} matching openai_headline_requests*.jsonl")

    rows = []
    bad = 0
    for p in paths:
        for obj in iter_jsonl(p):
            cid = obj.get("custom_id")
            body = obj.get("body") or {}
            messages = body.get("messages") or []
            user_content = None
            for m in messages:
                if m.get("role") == "user":
                    user_content = m.get("content")
                    break
            hl = headline_from_user_prompt(user_content or "")
            if cid and hl:
                rows.append({"custom_id": cid, "headline": hl})
            else:
                bad += 1

    df = pd.DataFrame(rows).drop_duplicates("custom_id")
    if df.empty:
        raise ValueError("Parsed 0 headlines from openai_headline_requests*.jsonl.")
    df["headline"] = norm_text(df["headline"])

    print(f"[CHECK] requests parsed unique custom_id: {len(df):,} (bad rows skipped={bad:,})")
    return df


def load_trading_days() -> pd.DatetimeIndex:
    crsp = pd.read_parquet(CRSP_PATH)
    if "date" not in crsp.columns:
        raise KeyError(f"CRSP file missing 'date' column: {CRSP_PATH}")

    d = pd.to_datetime(crsp["date"], errors="coerce").dropna().dt.normalize()
    idx = pd.DatetimeIndex(sorted(d.unique()))
    if len(idx) == 0:
        raise ValueError("CRSP trading calendar is empty after parsing.")

    print(f"[CHECK] trading days in CRSP calendar: {len(idx):,} (min={idx.min().date()}, max={idx.max().date()})")
    return idx


def next_td(trading_days: pd.DatetimeIndex, day: pd.Timestamp, strict: bool) -> pd.Timestamp:
    day = pd.Timestamp(day).normalize()
    pos = trading_days.searchsorted(day, side=("right" if strict else "left"))
    return trading_days[pos] if pos < len(trading_days) else pd.NaT


def compute_trade_date(trading_days: pd.DatetimeIndex, timestamp_et: pd.Series) -> pd.Series:
    ts = pd.to_datetime(timestamp_et, errors="coerce")
    if ts.dt.tz is None:
        raise ValueError("timestamp_et must be timezone-aware (ET).")

    cal_day = ts.dt.tz_convert("America/New_York").dt.normalize().dt.tz_localize(None)
    t = ts.dt.tz_convert("America/New_York").dt.time

    after_close = t >= dtime(16, 0)
    pre_9 = t < dtime(9, 0)

    is_td = pd.Series(cal_day.isin(trading_days), index=cal_day.index)
    trade = cal_day.copy()

    if after_close.any():
        trade.loc[after_close] = [next_td(trading_days, d, strict=True) for d in cal_day.loc[after_close]]

    mask = pre_9 & (~is_td)
    if mask.any():
        trade.loc[mask] = [next_td(trading_days, d, strict=False) for d in cal_day.loc[mask]]

    return trade


def recover_timestamps(scored: pd.DataFrame) -> pd.DataFrame:
    rp = pd.read_parquet(RAVENPACK_PATH)

    need_cols = {"map_ticker", "entity_name", "headline", "timestamp_utc"}
    if not need_cols.issubset(rp.columns):
        raise KeyError(f"RAVENPACK_cleaned missing required columns {need_cols}. Got {set(rp.columns)}")

    rp = rp[list(need_cols)].copy().rename(columns={"map_ticker": "ticker"})
    rp["ticker"] = norm_ticker(rp["ticker"])
    rp["entity_name"] = norm_text(rp["entity_name"])
    rp["headline"] = norm_text(rp["headline"])

    ts = pd.to_datetime(rp["timestamp_utc"], errors="coerce")
    if ts.dt.tz is None:
        ts = ts.dt.tz_localize("UTC")
    rp["timestamp_et"] = ts.dt.tz_convert("America/New_York")
    rp["date"] = rp["timestamp_et"].dt.date

    key = ["ticker", "entity_name", "headline", "date"]

    # Coverage check: how many rows have potential matches by ticker-date alone?
    rp_td = rp[["ticker", "date"]].drop_duplicates()
    scored_td = scored[["ticker", "date"]].drop_duplicates()
    td_overlap = scored_td.merge(rp_td, on=["ticker", "date"], how="inner")
    print(f"[CHECK] ticker-date overlap between scored rows and RavenPack: {len(td_overlap):,} unique ticker-date pairs")

    out = scored.merge(rp[key + ["timestamp_et"]], on=key, how="left")

    # If duplicates, keep earliest timestamp_et per custom_id
    out = out.sort_values(["custom_id", "timestamp_et"]).groupby("custom_id", as_index=False).first()

    miss = out["timestamp_et"].isna().sum()
    print(f"[CHECK] timestamp recovery success: {(len(out) - miss):,}/{len(out):,} ({100.0*(len(out)-miss)/len(out):.2f}%)")
    if miss:
        # show a few unmatched examples to debug headline mismatches
        sample = out.loc[out["timestamp_et"].isna(), ["custom_id", "ticker", "entity_name", "date", "headline"]].head(5)
        print("[CHECK] sample unmatched rows (first 5):")
        print(sample.to_string(index=False))

    return out


def main():
    outputs = load_outputs()    # custom_id,label,score
    mapping = load_mapping()    # custom_id,ticker,entity_name,date (calendar)
    requests = load_requests()  # custom_id,headline

    # Merge coverage checks
    scored = outputs.merge(mapping, on="custom_id", how="left", indicator=True)
    print("[CHECK] after merge outputs→mapping:")
    print(scored["_merge"].value_counts(dropna=False).to_string())
    scored = scored.drop(columns=["_merge"])

    scored = scored.merge(requests, on="custom_id", how="left", indicator=True)
    print("[CHECK] after merge (prev)→requests:")
    print(scored["_merge"].value_counts(dropna=False).to_string())
    scored = scored.drop(columns=["_merge"])

    # Required fields for timestamp recovery
    before_drop = len(scored)
    scored = scored.dropna(subset=["ticker", "entity_name", "date", "headline"])
    print(f"[CHECK] rows after dropping missing ticker/entity/date/headline: {len(scored):,} (dropped {before_drop-len(scored):,})")

    # Timestamp recovery (key choke point)
    scored = recover_timestamps(scored)

    # Drop rows without timestamp
    before_ts = len(scored)
    scored = scored.dropna(subset=["timestamp_et"])
    print(f"[CHECK] rows after dropping missing timestamp_et: {len(scored):,} (dropped {before_ts-len(scored):,})")

    trading_days = load_trading_days()

    scored["trade_date"] = compute_trade_date(trading_days, scored["timestamp_et"])
    before_td = len(scored)
    scored = scored.dropna(subset=["trade_date"])
    print(f"[CHECK] rows after dropping missing trade_date: {len(scored):,} (dropped {before_td-len(scored):,})")

    scored["date"] = pd.to_datetime(scored["trade_date"]).dt.date

    # Aggregate to firm-day (ticker, trade_date)
    daily = (
        scored.groupby(["ticker", "date"], as_index=False)
        .agg(n_headlines=("score", "size"), score_sum=("score", "sum"))
        .sort_values(["ticker", "date"])
        .reset_index(drop=True)
    )
    daily["score"] = daily["score_sum"].apply(lambda x: 1 if x > 0 else (-1 if x < 0 else 0))

    print(f"[CHECK] final firm-day rows: {len(daily):,}")
    if len(daily) > 0:
        print(f"[CHECK] firm-day date range: {daily['date'].min()} → {daily['date'].max()}")
        # how many unique dates?
        print(f"[CHECK] unique trading dates in firm-day: {daily['date'].nunique():,}")

    daily.to_parquet(OUT_PATH, index=False)
    print(f"Wrote: {OUT_PATH} (rows={len(daily):,})")


if __name__ == "__main__":
    main()