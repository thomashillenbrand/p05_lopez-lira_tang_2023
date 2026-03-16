import json
import sys
from pathlib import Path

import pandas as pd
import pytest

sys.path.append(str(Path(__file__).resolve().parents[1] / "src"))

import create_openai_responses_table as cort


def test_extract_label_parses_direct_and_fallback_tokens():
    assert cort.extract_label("YES\npositive") == "YES"
    assert cort.extract_label("no: negative signal") == "NO"
    # Fallback checks YES/NO/UNKNOWN in that order, so UNKNOWN strings can map to NO.
    assert cort.extract_label("Model says UNKNOWN outcome") == "NO"
    assert cort.extract_label("") is None
    assert cort.extract_label("MAYBE") is None


def test_iter_jsonl_reads_non_empty_lines(tmp_path):
    p = tmp_path / "rows.jsonl"
    p.write_text('{"a": 1}\n\n{"b": 2}\n', encoding="utf-8")

    result = list(cort.iter_jsonl(p))

    assert result == [{"a": 1}, {"b": 2}]


def test_load_mapping_supports_dict_and_list_and_deduplicates(tmp_path, monkeypatch):
    data_dir = tmp_path / "data"
    data_dir.mkdir()

    mapping_dict = {
        "rp-1": {"date": "2024-01-02", "ticker": "AAPL"},
        "rp-2": {"date": "bad-date", "ticker": "MSFT"},
    }
    mapping_list = [
        {"custom_id": "rp-3", "date": "2024-01-03", "ticker": "GOOG"},
        {"id": "rp-1", "date": "2024-01-04", "ticker": "AAPL_DUP"},
    ]

    (data_dir / "id_to_row_mapping.1.json").write_text(
        json.dumps(mapping_dict), encoding="utf-8"
    )
    (data_dir / "id_to_row_mapping.2.json").write_text(
        json.dumps(mapping_list), encoding="utf-8"
    )

    monkeypatch.setattr(cort, "DATA_DIR", data_dir)

    result = cort.load_mapping().sort_values("custom_id").reset_index(drop=True)

    # rp-2 drops due to invalid date; rp-1 dedup keeps first occurrence from file 1.
    assert list(result["custom_id"]) == ["rp-1", "rp-3"]
    assert str(result.loc[0, "date"].date()) == "2024-01-02"
    assert str(result.loc[1, "date"].date()) == "2024-01-03"


def test_load_mapping_raises_when_no_files(tmp_path, monkeypatch):
    monkeypatch.setattr(cort, "DATA_DIR", tmp_path)

    with pytest.raises(FileNotFoundError, match="No mapping files found"):
        cort.load_mapping()


def test_load_mapping_raises_when_date_field_missing(tmp_path, monkeypatch):
    data_dir = tmp_path / "data"
    data_dir.mkdir()
    (data_dir / "id_to_row_mapping.1.json").write_text(
        json.dumps({"rp-1": {"ticker": "AAPL"}}), encoding="utf-8"
    )
    monkeypatch.setattr(cort, "DATA_DIR", data_dir)

    with pytest.raises(KeyError, match="must contain a 'date' field"):
        cort.load_mapping()


def test_load_outputs_parses_labels_and_deduplicates(tmp_path, monkeypatch):
    data_dir = tmp_path / "data"
    data_dir.mkdir()

    rows = [
        {
            "custom_id": "rp-1",
            "response": {"body": {"choices": [{"message": {"content": "YES\\npositive"}}]}},
        },
        {
            "custom_id": "rp-2",
            "response": {"body": {"choices": [{"message": {"content": "NO: negative"}}]}},
        },
        {
            "custom_id": "rp-1",
            "response": {"body": {"choices": [{"message": {"content": "UNKNOWN"}}]}},
        },
        {
            "custom_id": "rp-3",
            "response": {"body": {"choices": [{"message": {"content": "MAYBE"}}]}},
        },
        {"response": {"body": {"choices": [{"message": {"content": "YES"}}]}}},
    ]

    (data_dir / "openai_headline_batch_output.1.jsonl").write_text(
        "\n".join(json.dumps(r) for r in rows) + "\n", encoding="utf-8"
    )

    monkeypatch.setattr(cort, "DATA_DIR", data_dir)

    result = cort.load_outputs().sort_values("custom_id").reset_index(drop=True)

    assert list(result["custom_id"]) == ["rp-1", "rp-2"]
    assert list(result["label"]) == ["YES", "NO"]


def test_load_outputs_raises_when_no_files(tmp_path, monkeypatch):
    monkeypatch.setattr(cort, "DATA_DIR", tmp_path)

    with pytest.raises(FileNotFoundError, match="No output files found"):
        cort.load_outputs()


def test_make_prop_table_builds_expected_counts_and_proportions():
    df = pd.DataFrame(
        {
            "label": ["YES", "NO", "UNKNOWN", "YES"],
            "date": pd.to_datetime(["2022-01-01", "2023-01-01", "2020-01-01", "2025-01-01"]),
        }
    )

    out = cort.make_prop_table(df)

    yes = out.loc[out["label"] == "YES"].iloc[0]
    no = out.loc[out["label"] == "NO"].iloc[0]
    unknown = out.loc[out["label"] == "UNKNOWN"].iloc[0]

    assert yes["count_full_sample"] == 2
    assert no["count_full_sample"] == 1
    assert unknown["count_full_sample"] == 1

    # Sample window includes 2022 and 2023 only in this fixture.
    assert yes["count_2021_2024_sample_period"] == 1
    assert no["count_2021_2024_sample_period"] == 1
    assert unknown["count_2021_2024_sample_period"] == 0

    assert yes["proportion_full_sample"] == pytest.approx(0.5)
    assert no["proportion_full_sample"] == pytest.approx(0.25)
    assert unknown["proportion_full_sample"] == pytest.approx(0.25)


def test_main_merges_builds_table_and_writes_csv(monkeypatch, tmp_path):
    mapping = pd.DataFrame(
        {
            "custom_id": ["rp-1", "rp-2", "rp-3"],
            "date": pd.to_datetime(["2022-01-01", "2023-01-01", "2025-01-01"]),
        }
    )
    outputs = pd.DataFrame(
        {
            "custom_id": ["rp-1", "rp-2", "rp-x"],
            "label": ["YES", "NO", "UNKNOWN"],
        }
    )

    monkeypatch.setattr(cort, "load_mapping", lambda: mapping)
    monkeypatch.setattr(cort, "load_outputs", lambda: outputs)

    out_csv = tmp_path / "openai_output_label_proportions.csv"
    monkeypatch.setattr(cort, "OUT_CSV", out_csv)

    captured = {}

    def fake_to_csv(self, path, index=False):
        captured["df"] = self.copy()
        captured["path"] = path
        captured["index"] = index

    monkeypatch.setattr(pd.DataFrame, "to_csv", fake_to_csv)

    cort.main()

    assert captured["path"] == out_csv
    assert captured["index"] is False

    result = captured["df"]
    assert set(result["label"]) == {"YES", "NO", "UNKNOWN"}

    # Only rp-1 and rp-2 survive the inner merge.
    yes = result.loc[result["label"] == "YES"].iloc[0]
    no = result.loc[result["label"] == "NO"].iloc[0]
    unknown = result.loc[result["label"] == "UNKNOWN"].iloc[0]

    assert yes["count_full_sample"] == 1
    assert no["count_full_sample"] == 1
    assert unknown["count_full_sample"] == 0