# src/clean_crsp.py
from pathlib import Path
import pandas as pd

from settings import config

DATA_DIR = Path(config("DATA_DIR"))

RAVENPACK_CLEAN_FILE = DATA_DIR / "RAVENPACK_cleaned.parquet"
CRSP_RAW_FILE = DATA_DIR / "CRSP_stock_daily.parquet"    
OUTPUT_FILE = DATA_DIR / "CRSP_clean_daily.parquet"


def _norm_ticker_series(s: pd.Series) -> pd.Series:
    s = s.astype(str).str.upper().str.strip()
    s = s.str.replace(r"\s+", "", regex=True)
    # optional: remove common punctuation differences
    s = s.str.replace(".", "", regex=False)
    s = s.str.replace("/", "", regex=False)
    s = s.replace({"": pd.NA, "NAN": pd.NA, "NONE": pd.NA, "NULL": pd.NA})
    return s


def main():
    if not RAVENPACK_CLEAN_FILE.exists():
        raise FileNotFoundError(f"Missing RavenPack cleaned file: {RAVENPACK_CLEAN_FILE}")
    if not CRSP_RAW_FILE.exists():
        raise FileNotFoundError(f"Missing CRSP file: {CRSP_RAW_FILE}")

    print("Loading RavenPack cleaned...")
    rp = pd.read_parquet(RAVENPACK_CLEAN_FILE, columns=["map_ticker"])
    rp["map_ticker"] = _norm_ticker_series(rp["map_ticker"])
    rp = rp.dropna(subset=["map_ticker"])
    rp_tickers = set(rp["map_ticker"].unique())
    print(f"RavenPack unique tickers: {len(rp_tickers):,}")

    print("Loading CRSP daily...")
    crsp = pd.read_parquet(CRSP_RAW_FILE)

    # If CRSP has ticker column named differently, change here:
    if "ticker" not in crsp.columns:
        raise KeyError(f"CRSP file missing 'ticker'. Available: {list(crsp.columns)}")

    crsp["ticker_norm"] = _norm_ticker_series(crsp["ticker"])
    n_before = len(crsp)
    u_before = crsp["ticker_norm"].nunique(dropna=True)

    crsp_filt = crsp.loc[crsp["ticker_norm"].isin(rp_tickers)].copy()

    n_after = len(crsp_filt)
    u_after = crsp_filt["ticker_norm"].nunique(dropna=True)

    print("\n=== CRSP Filter to RavenPack Universe ===")
    print(f"CRSP rows before: {n_before:,}")
    print(f"CRSP rows after:  {n_after:,}")
    print(f"CRSP unique tickers before: {u_before:,}")
    print(f"CRSP unique tickers after:  {u_after:,}")

    # Keep both raw + norm ticker (norm helps later merges/debugging)
    crsp_filt.to_parquet(OUTPUT_FILE, index=False)
    print(f"\nSaved: {OUTPUT_FILE}")


if __name__ == "__main__":
    main()