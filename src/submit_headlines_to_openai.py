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

REQUESTS_JSONL = DATA_DIR / "openai_headline_requests.jsonl"
BATCH_OUTPUT_JSONL = OUTPUT_DIR / "openai_headline_batch_output.jsonl"
BATCH_ERROR_JSONL = OUTPUT_DIR / "openai_headline_batch_errors.jsonl"
METADATA_JSON = OUTPUT_DIR / "openai_headline_batch_metadata.json"


def upload_batch_file(client: OpenAI) -> str:
    """Helper method to upload the JSONL file of requests to OpenAI and return the file ID.

    Args:
        client (OpenAI): An instance of the OpenAI client.

    Returns:
        str: The file ID of the uploaded batch file.
    """
    with REQUESTS_JSONL.open("rb") as fp:
        uploaded = client.files.create(
            file=fp,
            purpose="batch",
        )
    file_id = uploaded.id
    print(f"Uploaded batch file: {file_id}")
    return file_id


def create_batch_job(client: OpenAI, input_file_id: str) -> str:
    """Helper method to create an OpenAI batch job with the given input file ID.

    Args:
        client (OpenAI): An instance of the OpenAI client.
        input_file_id (str): The file ID of the uploaded batch file containing the requests.

    Returns:
        str: The batch job ID of the created batch job.
    """
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
    """Helper method to poll for the status of the OpenAI batch job until it reaches a terminal state,
    then return the batch data.
    
    Args:
        client (OpenAI): An instance of the OpenAI client.
        batch_id (str): The batch job ID to poll for.
        poll_seconds (int): The number of seconds to wait between polling attempts.

    Returns:
        Batch: The retrieved batch data when it reaches a terminal state.
    """
    terminal_states = {"completed", "failed", "expired", "cancelled"}
    while True:
        data = client.batches.retrieve(batch_id)
        status = data.status
        print(f"Batch status: {status}")
        if status in terminal_states:
            return data
        time.sleep(poll_seconds)


def download_file_content(client: OpenAI, file_id: str, out_path: Path) -> None:
    """Helper method to download the content of a file from OpenAI given its file ID, and save it to the specified path.

    Args:
        client (OpenAI): An instance of the OpenAI client.
        file_id (str): The file ID of the file to download.
        out_path (Path): The local path where the downloaded file content should be saved.
    """
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
    """Main method to drive the process of creating, submitting, and downloading the OpenAI batch job and its results."""
    if not OPENAI_API_KEY:
        raise EnvironmentError("OPENAI_API_KEY is not set in the environment.")

    openai_client = OpenAI(api_key=OPENAI_API_KEY)
    
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


if __name__ == "__main__":
    main()
