import json
from unittest.mock import Mock

import pandas as pd
import pytest

import generate_batched_requests as gbr


def test_get_input_path_returns_candidate_when_exists(tmp_path, monkeypatch):
    candidate = tmp_path / "RAVENPACK_cleaned.parquet"
    candidate.touch()
    monkeypatch.setattr(gbr, "INPUT_CANDIDATE", candidate)

    result = gbr.get_input_path()

    assert result == candidate


def test_get_input_path_raises_when_missing(tmp_path, monkeypatch):
    candidate = tmp_path / "RAVENPACK_cleaned.parquet"
    monkeypatch.setattr(gbr, "INPUT_CANDIDATE", candidate)

    with pytest.raises(FileNotFoundError, match="Could not find cleaned RavenPack parquet"):
        gbr.get_input_path()


def test_pick_column_returns_first_match():
    df = pd.DataFrame({"col_b": [1], "col_c": [2]})

    result = gbr.pick_column(df, ["col_a", "col_b", "col_c"])

    assert result == "col_b"


def test_pick_column_returns_none_when_not_required():
    df = pd.DataFrame({"col_b": [1]})

    result = gbr.pick_column(df, ["col_a"], required=False)

    assert result is None


def test_pick_column_raises_when_required_missing():
    df = pd.DataFrame({"col_b": [1]})

    with pytest.raises(KeyError, match="Missing required columns"):
        gbr.pick_column(df, ["col_a"], required=True)


def test_make_requests_jsonl_writes_jsonl_and_mapping(tmp_path):
    df = pd.DataFrame(
        {
            "map_ticker": ["AAPL", "MSFT"],
            "entity_name": ["Apple", "Microsoft"],
            "headline": ["Apple beats earnings", "Microsoft launches product"],
            "headline_date": ["2025-01-02", "2025-01-03"],
        }
    )

    requests_path = tmp_path / "openai_headline_requests.1.jsonl"
    mapping_path = tmp_path / "id_to_row_mapping.1.json"

    gbr.make_requests_jsonl(
        df,
        model="gpt-test-model",
        requests_output_dir=requests_path,
        id_row_json_output_dir=mapping_path,
        starting_row_idx=10,
    )

    lines = requests_path.read_text(encoding="utf-8").splitlines()
    assert len(lines) == 2

    first_request = json.loads(lines[0])
    second_request = json.loads(lines[1])

    assert first_request["custom_id"] == "rp-10"
    assert second_request["custom_id"] == "rp-11"
    assert first_request["body"]["model"] == "gpt-test-model"
    assert first_request["body"]["messages"][0]["content"] == gbr.SYSTEM_PROMPT
    assert "Apple" in first_request["body"]["messages"][1]["content"]

    id_mapping = json.loads(mapping_path.read_text(encoding="utf-8"))
    assert id_mapping["rp-10"]["ticker"] == "AAPL"
    assert id_mapping["rp-10"]["date"] == "2025-01-02"
    assert id_mapping["rp-11"]["entity_name"] == "Microsoft"


def test_main_single_batch_calls_make_requests_once(monkeypatch):
    full_df = pd.DataFrame(
        {
            "map_ticker": ["AAPL", "MSFT"],
            "entity_name": ["Apple", "Microsoft"],
            "headline": ["h1", "h2"],
            "headline_date": ["2025-01-02", "2025-01-03"],
        }
    )

    make_requests_mock = Mock()

    monkeypatch.setattr(gbr, "OPENAI_MODEL", "gpt-test-model")
    monkeypatch.setattr(gbr, "OPENAI_BATCH_SIZE", 10)
    monkeypatch.setattr(gbr, "get_input_path", lambda: gbr.Path("dummy.parquet"))
    monkeypatch.setattr(gbr.pd, "read_parquet", lambda _: full_df)
    monkeypatch.setattr(gbr, "make_requests_jsonl", make_requests_mock)

    gbr.main()

    make_requests_mock.assert_called_once()
    _, kwargs = make_requests_mock.call_args
    assert kwargs["model"] == "gpt-test-model"
    assert kwargs["requests_output_dir"].name.endswith(".1.jsonl")
    assert kwargs["id_row_json_output_dir"].name.endswith(".1.json")


def test_main_multiple_batches_calls_make_requests_per_chunk(monkeypatch):
    full_df = pd.DataFrame(
        {
            "map_ticker": ["AAPL", "MSFT", "GOOG", "AMZN", "META"],
            "entity_name": ["Apple", "Microsoft", "Google", "Amazon", "Meta"],
            "headline": ["h1", "h2", "h3", "h4", "h5"],
            "headline_date": [
                "2025-01-02",
                "2025-01-03",
                "2025-01-04",
                "2025-01-05",
                "2025-01-06",
            ],
        }
    )

    make_requests_mock = Mock()

    monkeypatch.setattr(gbr, "OPENAI_MODEL", "gpt-test-model")
    monkeypatch.setattr(gbr, "OPENAI_BATCH_SIZE", 2)
    monkeypatch.setattr(gbr, "get_input_path", lambda: gbr.Path("dummy.parquet"))
    monkeypatch.setattr(gbr.pd, "read_parquet", lambda _: full_df)
    monkeypatch.setattr(gbr, "make_requests_jsonl", make_requests_mock)

    gbr.main()

    assert make_requests_mock.call_count == 3

    first_call = make_requests_mock.call_args_list[0]
    second_call = make_requests_mock.call_args_list[1]
    third_call = make_requests_mock.call_args_list[2]

    assert len(first_call.args[0]) == 2
    assert len(second_call.args[0]) == 2
    assert len(third_call.args[0]) == 1

    assert first_call.kwargs["starting_row_idx"] == 0
    assert second_call.kwargs["starting_row_idx"] == 2
    assert third_call.kwargs["starting_row_idx"] == 4


def test_main_raises_when_model_missing(monkeypatch):
    monkeypatch.setattr(gbr, "OPENAI_MODEL", "")

    with pytest.raises(EnvironmentError, match="OPENAI_MODEL is not set"):
        gbr.main()


def test_main_raises_when_batch_size_exceeds_limit(monkeypatch):
    monkeypatch.setattr(gbr, "OPENAI_MODEL", "gpt-test-model")
    monkeypatch.setattr(gbr, "OPENAI_BATCH_SIZE", gbr.MAX_BATCH_SIZE + 1)

    with pytest.raises(EnvironmentError, match="exceeds the maximum allowed batch size"):
        gbr.main()
