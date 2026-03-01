import json
import time
from pathlib import Path
from zoneinfo import ZoneInfo

import pandas as pd
from openai import OpenAI
from settings import config

DATA_DIR = Path(config("DATA_DIR"))
OUTPUT_DIR = Path(config("OUTPUT_DIR"))
OPENAI_MODEL = config("OPENAI_MODEL")

INPUT_CANDIDATE = DATA_DIR / "RAVENPACK_cleaned.parquet"
REQUESTS_JSONL = DATA_DIR / "openai_headline_requests.jsonl"
ID_ROW_JSON = DATA_DIR / "id_to_row_mapping.json"

SYSTEM_PROMPT = (
    "Forget all your previous instructions. Pretend you are a financial expert. "
    "You are a financial expert with stock recommendation experience. "
    "Answer \"YES\" if good news, \"NO\" if bad news, or \"UNKNOWN\" if uncertain in the first line. "
    "Then elaborate with one short and concise sentence on the next line."
)


def get_input_path() -> Path:
    """Helper method to get the input path for RavenPack cleaned parquet, with error handling."""
    if INPUT_CANDIDATE.exists():
        return INPUT_CANDIDATE
    tried = str(INPUT_CANDIDATE)
    raise FileNotFoundError(
        f"Could not find cleaned RavenPack parquet. Tried: {tried}"
    )


def pick_column(df: pd.DataFrame, options: list[str], required: bool = True) -> str | None:
    """Helper method to pick the first matching column from a list of options, with error handling.

    Args:
        df (pd.DataFrame): The DataFrame to check for columns.
        options (list[str]): List of column name options to look for.
        required (bool): Whether at least one column must be found. If True and no columns are found, raises KeyError.

    Returns:
        str | None: The name of the first matching column found, or None if no columns are found and required is False.
    """
    for col in options:
        if col in df.columns:
            return col
    if required:
        raise KeyError(f"Missing required columns. Need one of: {options}")
    return None


def make_requests_jsonl(df: pd.DataFrame, model: str) -> dict[str, dict[str, str]]:
    """Helper method to create the JSONL file of requests for OpenAI batch, and build the id to row mapping.

    Args:
        df (pd.DataFrame): DataFrame containing the RavenPack headlines data.
        model (str): The OpenAI model to specify in the request body.

    Returns:
        dict[str, dict[str, str]]: A mapping of custom_id to original row data (ticker, date, entity_name) for later reference.
    """

    timestamp_col = pick_column(df, ["timestamp_utc"])
    ticker_col = pick_column(df, ["map_ticker"])
    entity_name_col = pick_column(df, ["entity_name"])
    headline_col = pick_column(df, ["headline"])
    timestamp_et_col = 'timestamp_et'
    date_col = 'date'
    
    # Maybe we do this part of ravenpack data cleaning?
    ts = pd.to_datetime(df[timestamp_col], errors='coerce')
    if ts.dt.tz is None:
        ts = ts.dt.tz_localize('UTC')

    timestamp_et = ts.dt.tz_convert(ZoneInfo('America/New_York'))
    date_series = timestamp_et.dt.date

    headlines_df = df[[ticker_col, entity_name_col, headline_col]].copy()
    headlines_df[date_col] = date_series
    headlines_df[timestamp_et_col] = timestamp_et
    headlines_df = headlines_df.rename(
        columns={
            date_col: "date",
            ticker_col: "ticker",
            entity_name_col: "entity_name",
            headline_col: "headline",
        }
    )

    # basic cleaning, but should be done prior.. we can remove this later
    headlines_df["ticker"] = headlines_df["ticker"].astype(str).str.upper().str.strip()
    headlines_df["entity_name"] = headlines_df["entity_name"].astype(str).str.strip()
    headlines_df["headline"] = headlines_df["headline"].astype(str).str.strip()
    headlines_df = headlines_df.dropna(subset=["date"])
    headlines_df = headlines_df[headlines_df["headline"] != ""]
    headlines_df = headlines_df.reset_index(drop=True)

    id_to_row: dict[str, dict[str, str]] = {}
    with REQUESTS_JSONL.open("w", encoding="utf-8") as f:
        for idx, row in headlines_df.iterrows():
            custom_id = f"rp-{idx}"
            id_to_row[custom_id] = {
                "ticker": str(row["ticker"]),
                "date": str(row["date"]),
                "entity_name": str(row["entity_name"]),
            }

            request_obj = {
                "custom_id": custom_id,
                "method": "POST",
                "url": "/v1/chat/completions",
                "body": {
                    "model": model,
                    "temperature": 0,
                    "messages": [
                        {
                            "role": "system",
                            "content": SYSTEM_PROMPT,
                        },
                        {
                            "role": "user",
                            "content": (
                                f"Is this headline good or bad for the stock price of {row['entity_name']} in the short term?\n"
                                f"Headline: {row['headline']}"
                            ),
                        },
                    ],
                },
            }
            f.write(json.dumps(request_obj) + "\n")

    print(f"Wrote requests jsonl: {REQUESTS_JSONL}")
    print(f"Number of headlines queued: {len(id_to_row):,}")
    with ID_ROW_JSON.open("w", encoding="utf-8") as f:
        json.dump(id_to_row, f, indent=2)
    print(f"Wrote id to row mapping json: {ID_ROW_JSON}")
    return id_to_row


def main():
    """Main method to drive the process of creating, submitting, and downloading the OpenAI batch job and its results."""

    if not OPENAI_MODEL:
        raise EnvironmentError("OPENAI_MODEL is not set in the environment.")
    
    input_path = get_input_path()
    print(f"Using input parquet: {input_path}")
    print(f"Using model: {OPENAI_MODEL}")

    full_df = pd.read_parquet(input_path)
    df = full_df.iloc[500:501]  # for testing, remove this later
    # TEH TODO: create jsonl per 50,000 rows
    id_to_row = make_requests_jsonl(df, model=OPENAI_MODEL)
    
    return id_to_row


if __name__ == "__main__":
    main()
