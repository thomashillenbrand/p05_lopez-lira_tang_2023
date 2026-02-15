from pathlib import Path
import re

import pandas as pd
from rapidfuzz.distance import OSA

from settings import config

DATA_DIR = Path(config("DATA_DIR"))

CRSP_TICKERS_FILE = DATA_DIR / "CRSP_unique_tickers.parquet"
RAVENPACK_FILE = DATA_DIR / "RAVENPACK.parquet"
OUTPUT_FILE = DATA_DIR / "RAVENPACK_cleaned.parquet"

#clean tickers

def _norm_ticker_series(s: pd.Series) -> pd.Series:
    s = s.astype(str).str.upper().str.strip()
    s = s.str.replace(r"\s+", "", regex=True)
    s = s.replace({"": pd.NA, "NAN": pd.NA, "NONE": pd.NA, "NULL": pd.NA})
    return s

#clean headlines for OSA dedupe
def _norm_headline(s: str) -> str:
    s = (s or "").lower().strip()
    s = re.sub(r"\s+", " ", s)
    return s

# OSA dedupe function for firm-day headlines
# this is based on the filtering procedure in the paper where they remove headlines with OSA similarity > 0.60 to a higher-relevance headline for the same firm-day

def osa_dedupe_firm_day(g: pd.DataFrame, threshold: float = 0.60) -> pd.DataFrame:
    """
    Firm-day headline dedupe using Optimal String Alignment similarity (0..1).
    Keeps the first headline, drops subsequent ones with similarity > threshold.
    """
    # Prefer highest relevance if present; otherwise sort by timestamp
    if "event_relevance" in g.columns:
        g = g.sort_values(["event_relevance", "timestamp_utc"], ascending=[False, True])
    elif "relevance" in g.columns:
        g = g.sort_values(["relevance", "timestamp_utc"], ascending=[False, True])
    else:
        g = g.sort_values(["timestamp_utc"], ascending=[True])

    kept_rows = []
    kept_texts = []

    for _, row in g.iterrows():
        h = _norm_headline(row.get("headline", ""))
        if not h:
            continue

        if not any(OSA.normalized_similarity(h, kh) > threshold for kh in kept_texts):
            kept_rows.append(row)
            kept_texts.append(h)

    if not kept_rows:
        return g.iloc[0:0].copy()

    return pd.DataFrame(kept_rows)


def main():
    if not CRSP_TICKERS_FILE.exists():
        raise FileNotFoundError(f"Missing CRSP tickers file: {CRSP_TICKERS_FILE}")
    if not RAVENPACK_FILE.exists():
        raise FileNotFoundError(f"Missing RavenPack file: {RAVENPACK_FILE}")

    print("Loading CRSP tickers...")
    crsp = pd.read_parquet(CRSP_TICKERS_FILE)
    if "ticker" not in crsp.columns:
        raise KeyError(
            f"CRSP tickers parquet must contain column 'ticker'. "
            f"Available columns: {list(crsp.columns)}"
        )

    crsp["ticker"] = _norm_ticker_series(crsp["ticker"])
    crsp = crsp.dropna(subset=["ticker"])
    crsp_ticker_set = set(crsp["ticker"].unique())
    n_crsp_tickers = len(crsp_ticker_set)

    print("Loading RavenPack...")
    rp = pd.read_parquet(RAVENPACK_FILE)
    if "map_ticker" not in rp.columns:
        raise KeyError(
            f"'map_ticker' not found in RavenPack file. Available columns: {list(rp.columns)}"
        )

    rp["map_ticker"] = _norm_ticker_series(rp["map_ticker"])

    # Counts before changing anything
    n_rows_original = len(rp)
    n_unique_tickers_original = rp["map_ticker"].dropna().nunique()

    # Step 1: Filter to CRSP tickers
    rp_filt = rp[rp["map_ticker"].isin(crsp_ticker_set)].copy()

    n_rows_after_crsp = len(rp_filt)
    n_unique_tickers_after_crsp = rp_filt["map_ticker"].dropna().nunique()

    print("\n=== Step 1: CRSP Ticker Filter ===")
    print(f"RavenPack rows (original): {n_rows_original:,}")
    print(f"RavenPack rows (after CRSP ticker filter): {n_rows_after_crsp:,}")
    print(f"CRSP unique tickers: {n_crsp_tickers:,}")
    print(f"RavenPack unique tickers (original): {n_unique_tickers_original:,}")
    print(f"RavenPack unique tickers (after CRSP filter): {n_unique_tickers_after_crsp:,}")

    # Step 2: Firm-day OSA dedupe
    required_cols = {"rp_entity_id", "rpa_date_utc", "timestamp_utc", "headline"}
    missing = required_cols - set(rp_filt.columns)
    if missing:
        raise KeyError(
            f"Cannot OSA-dedupe because RavenPack is missing columns: {sorted(missing)}"
        )

    print("\nApplying firm-day OSA headline dedupe (threshold > 0.60)...")

    # Optional speed-up: only apply to firm-days with >1 headline
    sizes = rp_filt.groupby(["rp_entity_id", "rpa_date_utc"]).size()
    multi_keys = sizes[sizes > 1].index

    rp_single = rp_filt.merge(
        pd.DataFrame(index=multi_keys).reset_index(),
        on=["rp_entity_id", "rpa_date_utc"],
        how="left",
        indicator=True,
    )
    rp_multi = rp_single[rp_single["_merge"] == "both"].drop(columns=["_merge"])
    rp_single = rp_single[rp_single["_merge"] == "left_only"].drop(columns=["_merge"])

    # Dedupe only multi-headline firm-days
    rp_multi_deduped = (
        rp_multi.groupby(["rp_entity_id", "rpa_date_utc"], group_keys=False)
               .apply(osa_dedupe_firm_day)
               .reset_index(drop=True)
    )

    rp_final = pd.concat([rp_single, rp_multi_deduped], ignore_index=True)

    n_rows_after_osa = len(rp_final)
    n_unique_tickers_after_osa = rp_final["map_ticker"].dropna().nunique()

    print("\n=== Step 2: After OSA Dedupe ===")
    print(f"Rows before OSA (after CRSP filter): {n_rows_after_crsp:,}")
    print(f"Rows after OSA dedupe: {n_rows_after_osa:,}")
    print(f"Unique tickers after OSA dedupe: {n_unique_tickers_after_osa:,}")

    # Save the final output to the clean file
    rp_final.to_parquet(OUTPUT_FILE, index=False)
    print(f"\nSaved final cleaned RavenPack parquet to: {OUTPUT_FILE}")

#in the future we can overwrite the original RavenPack parquet with the cleaned version, but for now I had it saved it to a new file to preserve the original raw data and have an easier time comparing the two versions as we iterate on the cleaning steps

if __name__ == "__main__":
    main()
