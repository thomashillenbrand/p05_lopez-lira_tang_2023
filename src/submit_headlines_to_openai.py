import json
import re
import time
from pathlib import Path

from openai import OpenAI

from settings import config

DATA_DIR = Path(config("DATA_DIR"))
OPENAI_API_KEY = config("OPENAI_API_KEY")

REQUESTS_GLOB = "openai_headline_requests.*.jsonl"


def get_request_files(data_dir: Path = DATA_DIR) -> list[tuple[int, Path]]:
    """Find request files in the form openai_headline_requests.X.jsonl and sort by X."""
    pattern = re.compile(r"^openai_headline_requests\.(\d+)\.jsonl$")
    indexed_files: list[tuple[int, Path]] = []

    for file_path in data_dir.glob(REQUESTS_GLOB):
        match = pattern.match(file_path.name)
        if match:
            indexed_files.append((int(match.group(1)), file_path))

    indexed_files.sort(key=lambda item: item[0])
    if not indexed_files:
        raise FileNotFoundError(
            f"No request files found matching pattern: {data_dir / REQUESTS_GLOB}"
        )

    return indexed_files


def upload_batch_file(client: OpenAI, request_jsonl: Path) -> str:
    """Helper method to upload the JSONL file of requests to OpenAI and return the file ID.

    Args:
        client (OpenAI): An instance of the OpenAI client.

    Returns:
        str: The file ID of the uploaded batch file.
    """
    with request_jsonl.open("rb") as fp:
        uploaded = client.files.create(
            file=fp,
            purpose="batch",
        )
    file_id = uploaded.id
    print(f"Uploaded batch file {request_jsonl.name}: {file_id}")
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


def poll_for_batch_jobs(
    client: OpenAI,
    batch_jobs: list[tuple[int, str]],
    poll_seconds: int = 15,
) -> dict[int, object]:
    """Poll all batch jobs until each reaches a terminal state, returning data by index."""
    terminal_states = {"completed", "failed", "expired", "cancelled"}
    remaining = {idx: batch_id for idx, batch_id in batch_jobs}
    results: dict[int, object] = {}

    while remaining:
        completed_this_round: list[int] = []
        for idx, batch_id in remaining.items():
            data = client.batches.retrieve(batch_id)
            status = data.status
            print(f"Batch {idx} status: {status}")
            if status in terminal_states:
                results[idx] = data
                completed_this_round.append(idx)

        for idx in completed_this_round:
            remaining.pop(idx)

        if remaining:
            time.sleep(poll_seconds)

    return results


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

    result_paths = list(DATA_DIR.glob("openai_headline_batch_output.*.jsonl"))
    if result_paths:
        print(
            f"Batch output files already exist in {DATA_DIR}: {[p.name for p in result_paths]}"
        )
        print(
            "Skipping batch job submission to avoid duplicates. If you want to resubmit, please remove existing batch output files first."
        )
        return

    if not OPENAI_API_KEY:
        raise EnvironmentError("OPENAI_API_KEY is not set in the environment.")

    openai_client = OpenAI(api_key=OPENAI_API_KEY)
    request_files = get_request_files(DATA_DIR)
    print(f"Found {len(request_files)} request file(s).")

    batch_jobs: list[tuple[int, str]] = []
    for idx, request_path in request_files:
        input_file_id = upload_batch_file(openai_client, request_path)
        batch_id = create_batch_job(openai_client, input_file_id)
        batch_jobs.append((idx, batch_id))

    all_batch_data = poll_for_batch_jobs(openai_client, batch_jobs)

    failed_indices: list[int] = []
    for idx, _ in request_files:
        batch_data = all_batch_data[idx]
        metadata_path = DATA_DIR / f"openai_headline_batch_metadata.{idx}.json"
        metadata_path.write_text(
            json.dumps(batch_data.model_dump(), indent=2, default=str),
            encoding="utf-8",
        )
        print(f"Saved batch metadata to: {metadata_path}")

        status = batch_data.status
        if status != "completed":
            failed_indices.append(idx)
            continue

        output_file_id = batch_data.output_file_id
        error_file_id = batch_data.error_file_id

        if output_file_id:
            output_path = DATA_DIR / f"openai_headline_batch_output.{idx}.jsonl"
            download_file_content(openai_client, output_file_id, output_path)
        else:
            print(f"Batch {idx} completed but output_file_id is missing.")

        if error_file_id:
            error_path = DATA_DIR / f"openai_headline_batch_errors.{idx}.jsonl"
            download_file_content(openai_client, error_file_id, error_path)

    if failed_indices:
        raise RuntimeError(
            "Some batch jobs did not complete successfully for indices: "
            f"{failed_indices}"
        )


if __name__ == "__main__":
    main()
