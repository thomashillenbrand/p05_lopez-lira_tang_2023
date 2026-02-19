import json
import time
from pathlib import Path
from zoneinfo import ZoneInfo

import pandas as pd
from openai import OpenAI
from settings import config

DATA_DIR = Path(config("DATA_DIR"))
OUTPUT_DIR = Path(config("OUTPUT_DIR"))
OPENAI_API_KEY = config("OPENAI_API_KEY")
OPENAI_MODEL = config("OPENAI_MODEL")

INPUT_CANDIDATE = DATA_DIR / "RAVENPACK_cleaned.parquet"
REQUESTS_JSONL = DATA_DIR / "openai_headline_requests.jsonl"
SCORES_PARQUET = DATA_DIR / "daily_headline_polarity.parquet"

BATCH_OUTPUT_JSONL = OUTPUT_DIR / "openai_headline_batch_output.jsonl"
BATCH_ERROR_JSONL = OUTPUT_DIR / "openai_headline_batch_errors.jsonl"
METADATA_JSON = OUTPUT_DIR / "openai_headline_batch_metadata.json"
ID_ROW_JSON = OUTPUT_DIR / "id_to_row_mapping.json"

SYSTEM_PROMPT = (
    "Forget all your previous instructions. Pretend you are a financial expert. "
    "You are a financial expert with stock recommendation experience. "
    "Answer \"YES\" if good news, \"NO\" if bad news, or \"UNKNOWN\" if uncertain in the first line. "
    "Then elaborate with one short and concise sentence on the next line."
)


def get_input_path() -> Path:
    if INPUT_CANDIDATE.exists():
        return INPUT_CANDIDATE
    tried = str(INPUT_CANDIDATE)
    raise FileNotFoundError(
        f"Could not find cleaned RavenPack parquet. Tried: {tried}"
    )


def pick_column(df: pd.DataFrame, options: list[str], required: bool = True) -> str | None:
    for col in options:
        if col in df.columns:
            return col
    if required:
        raise KeyError(f"Missing required columns. Need one of: {options}")
    return None


def make_requests_jsonl(df: pd.DataFrame, model: str) -> dict[str, dict[str, str]]:
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

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


def upload_batch_file(client: OpenAI) -> str:
    with REQUESTS_JSONL.open("rb") as fp:
        uploaded = client.files.create(
            file=fp,
            purpose="batch",
        )
    file_id = uploaded.id
    print(f"Uploaded batch file: {file_id}")
    return file_id


def create_batch_job(client: OpenAI, input_file_id: str) -> str:
    batch = client.batches.create(
        input_file_id=input_file_id,
        endpoint="/v1/chat/completions",
        completion_window="24h",
        metadata={"job_name": "ravenpack_headline_scoring"},
    )
    batch_id = batch.id
    print(f"Created batch job: {batch_id}")
    return batch_id


def poll_for_batch_job(client: OpenAI, batch_id: str, poll_seconds: int = 15):
    terminal_states = {"completed", "failed", "expired", "cancelled"}
    while True:
        data = client.batches.retrieve(batch_id)
        status = data.status
        print(f"Batch status: {status}")
        if status in terminal_states:
            return data
        time.sleep(poll_seconds)


def download_file_content(client: OpenAI, file_id: str, out_path: Path) -> None:
    file_content = client.files.content(file_id)
    if hasattr(file_content, "read"):
        content_bytes = file_content.read()
    elif hasattr(file_content, "content"):
        content_bytes = file_content.content
    else:
        content_bytes = bytes(file_content)

    if isinstance(content_bytes, str):
        content_bytes = content_bytes.encode("utf-8")

    out_path.parent.mkdir(parents=True, exist_ok=True)
    out_path.write_bytes(content_bytes)
    print(f"Saved file content to: {out_path}")


def main():
    if not OPENAI_API_KEY:
        raise EnvironmentError("OPENAI_API_KEY is not set in the environment.")

    if not OPENAI_MODEL:
        raise EnvironmentError("OPENAI_MODEL is not set in the environment.")

    openai_client = OpenAI(api_key=OPENAI_API_KEY)
    
    input_path = get_input_path()
    print(f"Using input parquet: {input_path}")
    print(f"Using model: {OPENAI_MODEL}")

    full_df = pd.read_parquet(input_path)
    df = full_df.iloc[500:510]  # for testing, remove this later
    # TEH TODO: create jsonl per 50,000 rows
    id_to_row = make_requests_jsonl(df, model=OPENAI_MODEL)

    input_file_id = upload_batch_file(openai_client)
    batch_id = create_batch_job(openai_client, input_file_id)
    batch_data = poll_for_batch_job(openai_client, batch_id)

    METADATA_JSON.parent.mkdir(parents=True, exist_ok=True)
    METADATA_JSON.write_text(
        json.dumps(batch_data.model_dump(), indent=2, default=str),
        encoding="utf-8",
    )
    print(f"Saved batch metadata to: {METADATA_JSON}")

    status = batch_data.status
    if status != "completed":
        raise RuntimeError(f"Batch job did not complete successfully. Status: {status}")

    output_file_id = batch_data.output_file_id
    error_file_id = batch_data.error_file_id
    if not output_file_id:
        print("Batch completed but output_file_id is missing.")
    else:
        download_file_content(openai_client, output_file_id, BATCH_OUTPUT_JSONL)
        
    if error_file_id:
        download_file_content(openai_client, error_file_id, BATCH_ERROR_JSONL)
    
    return id_to_row


if __name__ == "__main__":
    main()
