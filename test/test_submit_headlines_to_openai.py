import json
import sys
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import Mock

import pytest

sys.path.append(str(Path(__file__).resolve().parents[1] / "src"))

import submit_headlines_to_openai as sho


def test_get_request_files_returns_sorted_indexed_files(tmp_path):
    (tmp_path / "openai_headline_requests.2.jsonl").write_text("{}\n", encoding="utf-8")
    (tmp_path / "openai_headline_requests.1.jsonl").write_text("{}\n", encoding="utf-8")
    (tmp_path / "openai_headline_requests.bad.jsonl").write_text(
        "{}\n", encoding="utf-8"
    )

    result = sho.get_request_files(tmp_path)

    assert [idx for idx, _ in result] == [1, 2]
    assert [p.name for _, p in result] == [
        "openai_headline_requests.1.jsonl",
        "openai_headline_requests.2.jsonl",
    ]


def test_get_request_files_raises_when_none_found(tmp_path):
    with pytest.raises(FileNotFoundError, match="No request files found"):
        sho.get_request_files(tmp_path)


def test_upload_batch_file_returns_uploaded_id(tmp_path):
    request_jsonl = tmp_path / "openai_headline_requests.1.jsonl"
    request_jsonl.write_text('{"x": 1}\n', encoding="utf-8")

    client = Mock()
    client.files.create.return_value = SimpleNamespace(id="file-123")

    result = sho.upload_batch_file(client, request_jsonl)

    assert result == "file-123"
    client.files.create.assert_called_once()
    call_kwargs = client.files.create.call_args.kwargs
    assert call_kwargs["purpose"] == "batch"


def test_create_batch_job_returns_batch_id_and_uses_expected_args():
    client = Mock()
    client.batches.create.return_value = SimpleNamespace(id="batch-abc")

    result = sho.create_batch_job(client, input_file_id="file-123")

    assert result == "batch-abc"
    client.batches.create.assert_called_once_with(
        input_file_id="file-123",
        endpoint="/v1/chat/completions",
        completion_window="24h",
        metadata={"job_name": "ravenpack_headline_scoring"},
    )


def test_poll_for_batch_jobs_retries_until_terminal(monkeypatch):
    client = Mock()

    states = {
        "batch-1": ["in_progress", "completed"],
        "batch-2": ["failed"],
    }

    def retrieve(batch_id):
        status = states[batch_id].pop(0)
        return SimpleNamespace(status=status, id=batch_id)

    client.batches.retrieve.side_effect = retrieve

    sleep_calls = []
    monkeypatch.setattr(sho.time, "sleep", lambda s: sleep_calls.append(s))

    result = sho.poll_for_batch_jobs(
        client,
        batch_jobs=[(1, "batch-1"), (2, "batch-2")],
        poll_seconds=3,
    )

    assert set(result.keys()) == {1, 2}
    assert result[1].status == "completed"
    assert result[2].status == "failed"
    assert sleep_calls == [3]


def test_download_file_content_handles_read_interface(tmp_path):
    client = Mock()
    client.files.content.return_value = SimpleNamespace(read=lambda: b"line1\nline2\n")

    out_path = tmp_path / "download" / "out.jsonl"
    sho.download_file_content(client, "file-1", out_path)

    assert out_path.read_bytes() == b"line1\nline2\n"


def test_download_file_content_handles_content_attribute_with_text(tmp_path):
    client = Mock()
    client.files.content.return_value = SimpleNamespace(content="hello")

    out_path = tmp_path / "download" / "out.txt"
    sho.download_file_content(client, "file-2", out_path)

    assert out_path.read_bytes() == b"hello"


def test_main_returns_early_when_output_already_exists(tmp_path, monkeypatch):
    (tmp_path / "openai_headline_batch_output.1.jsonl").write_text(
        "existing\n", encoding="utf-8"
    )

    openai_ctor = Mock(side_effect=AssertionError("OpenAI should not be constructed"))
    monkeypatch.setattr(sho, "OpenAI", openai_ctor)

    sho.main()


def test_main_raises_when_api_key_missing(monkeypatch, tmp_path):
    monkeypatch.setattr(sho, "OPENAI_API_KEY", None)
    monkeypatch.setattr(sho, "DATA_DIR", tmp_path / "data")

    with pytest.raises(EnvironmentError, match="OPENAI_API_KEY is not set"):
        sho.main()


def test_main_happy_path_writes_metadata_and_downloads_files(monkeypatch, tmp_path):
    data_dir = tmp_path / "data"
    data_dir.mkdir()

    (data_dir / "openai_headline_requests.1.jsonl").write_text("{}\n", encoding="utf-8")

    class FakeBatchData:
        def __init__(self):
            self.status = "completed"
            self.output_file_id = "out-file-1"
            self.error_file_id = "err-file-1"

        def model_dump(self):
            return {
                "status": self.status,
                "output_file_id": self.output_file_id,
                "error_file_id": self.error_file_id,
            }

    fake_client = Mock()

    monkeypatch.setattr(sho, "DATA_DIR", data_dir)
    monkeypatch.setattr(sho, "OPENAI_API_KEY", "test-key")
    monkeypatch.setattr(sho, "OpenAI", lambda api_key: fake_client)

    monkeypatch.setattr(sho, "upload_batch_file", lambda client, p: "uploaded-file-1")
    monkeypatch.setattr(
        sho, "create_batch_job", lambda client, input_file_id: "batch-1"
    )
    monkeypatch.setattr(
        sho, "poll_for_batch_jobs", lambda client, jobs: {1: FakeBatchData()}
    )

    downloaded = []

    def fake_download(client, file_id, out_path):
        downloaded.append((file_id, out_path.name))
        out_path.write_text("downloaded\n", encoding="utf-8")

    monkeypatch.setattr(sho, "download_file_content", fake_download)

    sho.main()

    metadata_path = data_dir / "openai_headline_batch_metadata.1.json"
    assert metadata_path.exists()

    metadata = json.loads(metadata_path.read_text(encoding="utf-8"))
    assert metadata["status"] == "completed"

    assert downloaded == [
        ("out-file-1", "openai_headline_batch_output.1.jsonl"),
        ("err-file-1", "openai_headline_batch_errors.1.jsonl"),
    ]


def test_main_raises_when_any_batch_not_completed(monkeypatch, tmp_path):
    data_dir = tmp_path / "data"
    data_dir.mkdir()

    (data_dir / "openai_headline_requests.1.jsonl").write_text("{}\n", encoding="utf-8")

    class FailedBatchData:
        status = "failed"
        output_file_id = None
        error_file_id = None

        def model_dump(self):
            return {"status": self.status}

    fake_client = Mock()

    monkeypatch.setattr(sho, "DATA_DIR", data_dir)
    monkeypatch.setattr(sho, "OPENAI_API_KEY", "test-key")
    monkeypatch.setattr(sho, "OpenAI", lambda api_key: fake_client)

    monkeypatch.setattr(sho, "upload_batch_file", lambda client, p: "uploaded-file-1")
    monkeypatch.setattr(
        sho, "create_batch_job", lambda client, input_file_id: "batch-1"
    )
    monkeypatch.setattr(
        sho, "poll_for_batch_jobs", lambda client, jobs: {1: FailedBatchData()}
    )

    with pytest.raises(RuntimeError, match="did not complete successfully"):
        sho.main()
