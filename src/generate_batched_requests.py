import json
from pathlib import Path

import pandas as pd

from settings import config

DATA_DIR = Path(config("DATA_DIR"))
OUTPUT_DIR = Path(config("OUTPUT_DIR"))
OPENAI_MODEL = config("OPENAI_MODEL")
OPENAI_BATCH_SIZE = config("OPENAI_BATCH_SIZE", cast=int)
MAX_BATCH_SIZE = 50000  # OpenAI batch size limit is 50,000 rows in a JSONL

INPUT_CANDIDATE = DATA_DIR / "RAVENPACK_cleaned.parquet"
REQUESTS_JSONL_BASE = DATA_DIR / "openai_headline_requests.jsonl"
ID_ROW_JSON_BASE = DATA_DIR / "id_to_row_mapping.json"

SYSTEM_PROMPT = (
    "Forget all your previous instructions. Pretend you are a financial expert. "
    "You are a financial expert with stock recommendation experience. "
    'Answer "YES" if good news, "NO" if bad news, or "UNKNOWN" if uncertain in the first line. '
    "Then elaborate with one short and concise sentence on the next line."
)


def get_input_path() -> Path:
    """Helper method to get the input path for RavenPack cleaned parquet, with error handling."""
    if INPUT_CANDIDATE.exists():
        return INPUT_CANDIDATE
    tried = str(INPUT_CANDIDATE)
    raise FileNotFoundError(f"Could not find cleaned RavenPack parquet. Tried: {tried}")


def pick_column(
    df: pd.DataFrame, options: list[str], required: bool = True
) -> str | None:
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


def make_requests_jsonl(
    df: pd.DataFrame,
    model: str,
    requests_output_dir: Path,
    id_row_json_output_dir: Path,
    starting_row_idx: int = 0,
) -> dict[str, dict[str, str]]:
    """Helper method to create the JSONL file of requests for OpenAI batch, and build the id to row mapping.

    Args:
        df (pd.DataFrame): DataFrame containing the RavenPack headlines data.
        model (str): The OpenAI model to specify in the request body.

    Returns:
        dict[str, dict[str, str]]: A mapping of custom_id to original row data (ticker, date, entity_name) for later reference.
    """

    ticker_col = pick_column(df, ["map_ticker"])
    entity_name_col = pick_column(df, ["entity_name"])
    headline_col = pick_column(df, ["headline"])
    headline_date_col = pick_column(df, ["headline_date"])
    date_col = pick_column(df, ["headline_date"])

    headlines_df = df[
        [ticker_col, entity_name_col, headline_col, headline_date_col]
    ].copy()
    headlines_df = headlines_df.rename(
        columns={
            date_col: "date",
            ticker_col: "ticker",
            entity_name_col: "entity_name",
            headline_col: "headline",
        }
    )
    headlines_df = headlines_df.reset_index(drop=True)

    id_to_row: dict[str, dict[str, str]] = {}
    with requests_output_dir.open("w", encoding="utf-8") as f:
        for idx, row in headlines_df.iterrows():
            custom_id = f"rp-{starting_row_idx + idx}"
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

    print(f"Wrote requests jsonl: {requests_output_dir}")
    print(f"Number of headlines batched: {len(id_to_row):,}")
    with id_row_json_output_dir.open("w", encoding="utf-8") as f:
        json.dump(id_to_row, f, indent=2)
    print(f"Wrote id to row mapping json: {id_row_json_output_dir}")


def main():
    """Main method to drive the process of creating, submitting, and downloading the OpenAI batch job and its results."""

    if not OPENAI_MODEL:
        raise EnvironmentError("OPENAI_MODEL is not set in the environment.")

    if OPENAI_BATCH_SIZE > MAX_BATCH_SIZE:
        raise EnvironmentError(
            f"OPENAI_BATCH_SIZE of {OPENAI_BATCH_SIZE} exceeds the maximum allowed batch size of {MAX_BATCH_SIZE}."
        )

    input_path = get_input_path()
    print(f"Using input parquet: {input_path}")
    print(f"Using model: {OPENAI_MODEL}")

    full_df = pd.read_parquet(input_path)
    size = full_df.shape[0]
    print(f"Read input parquet with {size:,} rows.")

    if size <= OPENAI_BATCH_SIZE:
        print(
            f"Input size {size:,} is less than or equal to batch size {OPENAI_BATCH_SIZE:,}. No batching required."
        )
        chunk_size = size
        make_requests_jsonl(
            full_df,
            model=OPENAI_MODEL,
            requests_output_dir=REQUESTS_JSONL_BASE.with_suffix(".1.jsonl"),
            id_row_json_output_dir=ID_ROW_JSON_BASE.with_suffix(".1.json"),
        )
    else:
        chunk_size = OPENAI_BATCH_SIZE
        print(f"Using batch chunk size of {chunk_size:,} for OpenAI submission.")

        to_process = size
        prev_chunk_end = 0
        json_idx = 1

        while to_process > 0:
            print(f"Marking chunk of size {chunk_size:,}...")
            chunk = (prev_chunk_end, prev_chunk_end + min(chunk_size, size + 1))
            chunk_df = full_df.iloc[chunk[0] : chunk[1]]

            make_requests_jsonl(
                chunk_df,
                model=OPENAI_MODEL,
                requests_output_dir=REQUESTS_JSONL_BASE.with_suffix(
                    f".{json_idx}.jsonl"
                ),
                id_row_json_output_dir=ID_ROW_JSON_BASE.with_suffix(
                    f".{json_idx}.json"
                ),
                starting_row_idx=chunk[0],
            )

            prev_chunk_end += min(chunk_size, size + 1)
            to_process -= chunk_size
            json_idx += 1
            print(
                f"Processed chunk {json_idx - 1}, {max(to_process, 0):,} rows remaining"
            )


if __name__ == "__main__":
    main()
