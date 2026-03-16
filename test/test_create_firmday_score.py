import json
import sys
from pathlib import Path

import pandas as pd
import pytest

sys.path.append(str(Path(__file__).resolve().parents[1] / "src"))

import create_firmday_score as cfs


def test_extract_label_handles_direct_tokens():
    assert cfs.extract_label("YES\nThis is positive") == "YES"
    assert cfs.extract_label("NO\nThis is negative") == "NO"
    assert cfs.extract_label("UNKNOWN\nNot enough context") == "UNKNOWN"


def test_extract_label_handles_punctuation_and_fallback_contains():
    assert cfs.extract_label("YES: likely good") == "YES"
    assert cfs.extract_label("Verdict = NO") == "NO"
    # Current fallback checks YES/NO before UNKNOWN, so UNKNOWN strings can map to NO.
    assert cfs.extract_label("Model says UNKNOWN outcome") == "NO"


def test_extract_label_returns_none_for_empty_or_unparseable_text():
    assert cfs.extract_label("") is None
    assert cfs.extract_label("MAYBE") is None


def test_label_to_score_maps_expected_values():
    assert cfs.label_to_score("YES") == 1
    assert cfs.label_to_score("NO") == -1
    assert cfs.label_to_score("UNKNOWN") == 0
    assert cfs.label_to_score(None) == 0


def test_load_outputs_parses_and_deduplicates(tmp_path, monkeypatch):
    output_file = tmp_path / "openai_headline_batch_output.1.jsonl"
    rows = [
        {
            "custom_id": "rp-1",
            "response": {
                "body": {"choices": [{"message": {"content": "YES\npositive"}}]}
            },
        },
        {
            "custom_id": "rp-2",
            "response": {
                "body": {"choices": [{"message": {"content": "NO\nnegative"}}]}
            },
        },
        {
            "custom_id": "rp-1",
            "response": {
                "body": {"choices": [{"message": {"content": "YES\nduplicate"}}]}
            },
        },
    ]
    output_file.write_text(
        "\n".join(json.dumps(r) for r in rows) + "\n", encoding="utf-8"
    )

    monkeypatch.setattr(cfs, "DATA_DIR", tmp_path)

    result = cfs.load_outputs().sort_values("custom_id").reset_index(drop=True)

    assert len(result) == 2
    assert result.loc[0, "custom_id"] == "rp-1"
    assert result.loc[0, "label"] == "YES"
    assert result.loc[0, "score"] == 1
    assert result.loc[1, "custom_id"] == "rp-2"
    assert result.loc[1, "score"] == -1


def test_load_outputs_raises_when_no_files(tmp_path, monkeypatch):
    monkeypatch.setattr(cfs, "DATA_DIR", tmp_path)

    with pytest.raises(FileNotFoundError, match="No outputs found"):
        cfs.load_outputs()


def test_load_outputs_raises_when_parsed_df_is_empty(tmp_path, monkeypatch):
    output_file = tmp_path / "openai_headline_batch_output.1.jsonl"
    rows = [
        {
            "custom_id": "rp-1",
            "response": {
                "body": {"choices": [{"message": {"content": "MAYBE\nunclear"}}]}
            },
        },
        {
            "custom_id": "rp-2",
            "response": {
                "body": {"choices": [{"message": {"content": "NEUTRAL\nno signal"}}]}
            },
        },
    ]
    output_file.write_text(
        "\n".join(json.dumps(r) for r in rows) + "\n", encoding="utf-8"
    )

    monkeypatch.setattr(cfs, "DATA_DIR", tmp_path)

    with pytest.raises(ValueError, match="Parsed 0 scored outputs"):
        cfs.load_outputs()


def test_load_mapping_reads_and_normalizes(tmp_path, monkeypatch):
    mapping_1 = {
        "rp-1": {"ticker": " aapl ", "entity_name": " Apple ", "date": "2025-01-03"},
    }
    mapping_2 = {
        "rp-2": {"ticker": "msft", "entity_name": "Microsoft", "date": "2025-01-04"},
        "rp-1": {"ticker": "AAPL", "entity_name": "Apple", "date": "2025-01-03"},
    }

    (tmp_path / "id_to_row_mapping.1.json").write_text(
        json.dumps(mapping_1), encoding="utf-8"
    )
    (tmp_path / "id_to_row_mapping.2.json").write_text(
        json.dumps(mapping_2), encoding="utf-8"
    )

    monkeypatch.setattr(cfs, "DATA_DIR", tmp_path)

    result = cfs.load_mapping().sort_values("custom_id").reset_index(drop=True)

    assert len(result) == 2
    assert result.loc[0, "custom_id"] == "rp-1"
    assert result.loc[0, "ticker"] == "AAPL"
    assert result.loc[0, "entity_name"] == "Apple"
    assert str(result.loc[0, "date"]) == "2025-01-03"
    assert result.loc[1, "custom_id"] == "rp-2"
    assert result.loc[1, "ticker"] == "MSFT"
    assert result.loc[1, "entity_name"] == "Microsoft"
    assert str(result.loc[1, "date"]) == "2025-01-04"


def test_load_trading_days_normalizes_and_sorts(monkeypatch):
    crsp_df = pd.DataFrame(
        {
            "date": [
                "2025-01-03 14:00:00",
                "2025-01-02 09:30:00",
                "2025-01-03 16:00:00",
            ]
        }
    )

    monkeypatch.setattr(cfs.pd, "read_parquet", lambda _: crsp_df)

    result = cfs.load_trading_days()

    expected = pd.DatetimeIndex(pd.to_datetime(["2025-01-02", "2025-01-03"]))
    pd.testing.assert_index_equal(result, expected)


def test_next_td_respects_strict_parameter():
    trading_days = pd.DatetimeIndex(
        pd.to_datetime(["2025-01-02", "2025-01-03", "2025-01-06"])
    )

    assert cfs.next_td(
        trading_days, pd.Timestamp("2025-01-03"), strict=False
    ) == pd.Timestamp("2025-01-03")
    assert cfs.next_td(
        trading_days, pd.Timestamp("2025-01-03"), strict=True
    ) == pd.Timestamp("2025-01-06")


def test_next_td_returns_nat_when_after_calendar_end():
    trading_days = pd.DatetimeIndex(pd.to_datetime(["2025-01-02", "2025-01-03"]))

    result = cfs.next_td(trading_days, pd.Timestamp("2025-01-10"), strict=False)

    assert pd.isna(result)


def test_compute_trade_date_maps_non_trading_days_forward():
    trading_days = pd.DatetimeIndex(
        pd.to_datetime(["2025-01-02", "2025-01-03", "2025-01-06"])
    )
    dates = pd.Series(pd.to_datetime(["2025-01-02", "2025-01-04", "2025-01-06"]))

    result = cfs.compute_trade_date(trading_days, dates)

    expected = pd.Series(pd.to_datetime(["2025-01-02", "2025-01-06", "2025-01-06"]))
    pd.testing.assert_series_equal(
        result.reset_index(drop=True), expected, check_names=False
    )


def test_main_builds_and_writes_daily_scores(monkeypatch, tmp_path):
    outputs_df = pd.DataFrame(
        {
            "custom_id": ["rp-1", "rp-2", "rp-3", "rp-4"],
            "label": ["YES", "NO", "YES", "UNKNOWN"],
            "score": [1, -1, 1, 0],
        }
    )
    mapping_df = pd.DataFrame(
        {
            "custom_id": ["rp-1", "rp-2", "rp-3", "rp-4"],
            "ticker": ["AAPL", "AAPL", "MSFT", "MSFT"],
            "entity_name": ["Apple", "Apple", "Microsoft", "Microsoft"],
            "date": pd.to_datetime(
                ["2025-01-04", "2025-01-04", "2025-01-03", "2025-01-04"]
            ).date,
        }
    )
    trading_days = pd.DatetimeIndex(pd.to_datetime(["2025-01-03", "2025-01-06"]))

    captured = {}

    def fake_to_parquet(self, path, index=False):
        captured["df"] = self.copy()
        captured["path"] = path
        captured["index"] = index

    monkeypatch.setattr(cfs, "load_outputs", lambda: outputs_df)
    monkeypatch.setattr(cfs, "load_mapping", lambda: mapping_df)
    monkeypatch.setattr(cfs, "load_trading_days", lambda: trading_days)
    monkeypatch.setattr(pd.DataFrame, "to_parquet", fake_to_parquet)

    out_path = tmp_path / "daily_headline_polarity.parquet"
    monkeypatch.setattr(cfs, "OUT_PATH", out_path)

    cfs.main()

    assert captured["path"] == out_path
    assert captured["index"] is False

    result = captured["df"].sort_values(["ticker", "date"]).reset_index(drop=True)

    assert len(result) == 3

    # AAPL has +1 and -1 on a non-trading day, both map forward to 2025-01-06.
    assert result.loc[0, "ticker"] == "AAPL"
    assert str(result.loc[0, "date"]) == "2025-01-06"
    assert result.loc[0, "n_headlines"] == 2
    assert result.loc[0, "score_sum"] == 0
    assert result.loc[0, "score"] == 0

    # MSFT has one score on a trading day and one UNKNOWN on a non-trading day.
    assert result.loc[1, "ticker"] == "MSFT"
    assert str(result.loc[1, "date"]) == "2025-01-03"
    assert result.loc[1, "n_headlines"] == 1
    assert result.loc[1, "score_sum"] == 1
    assert result.loc[1, "score"] == 1

    assert result.loc[2, "ticker"] == "MSFT"
    assert str(result.loc[2, "date"]) == "2025-01-06"
    assert result.loc[2, "n_headlines"] == 1
    assert result.loc[2, "score_sum"] == 0
    assert result.loc[2, "score"] == 0
