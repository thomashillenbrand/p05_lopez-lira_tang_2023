import sys
from pathlib import Path

import pandas as pd
import pytest

sys.path.append(str(Path(__file__).resolve().parents[1] / "src"))

import clean_ravenpack as cr


def test_norm_ticker_series_normalizes_and_handles_missing_markers():
    s = pd.Series([" aapl ", " msft", "", "NaN", "none", "NULL", None])

    result = cr._norm_ticker_series(s)

    expected = pd.Series(
        ["AAPL", "MSFT", pd.NA, pd.NA, pd.NA, pd.NA, pd.NA], dtype="object"
    )
    pd.testing.assert_series_equal(result, expected)


def test_norm_headline_lowercases_and_normalizes_whitespace():
    assert cr._norm_headline("  Big   NEWS\nTODAY ") == "big news today"
    assert cr._norm_headline("") == ""


def test_osa_dedupe_firm_day_drops_near_duplicates():
    g = pd.DataFrame(
        {
            "rp_entity_id": ["E1", "E1", "E1", "E1"],
            "rpa_date_utc": pd.to_datetime(
                ["2025-01-02", "2025-01-02", "2025-01-02", "2025-01-02"]
            ),
            "timestamp_utc": pd.to_datetime(
                [
                    "2025-01-02 08:00:00",
                    "2025-01-02 08:05:00",
                    "2025-01-02 08:06:00",
                    "2025-01-02 08:10:00",
                ]
            ),
            "headline": [
                "Apple posts record earnings",
                "Apple posts record earnings",
                "Apple to post record earnings",  # duplicate
                "Company announces new product",  # distinct
            ],
            "event_relevance": [0.9, 0.8, 0.85, 0.7],
        }
    )

    result = cr._osa_dedupe_firm_day(g, threshold=0.60)

    assert len(result) == 2
    assert "Apple posts record earnings" in set(result["headline"])
    assert "Company announces new product" in set(result["headline"])


def test_apply_osa_dedupe_firm_day_dedupes_only_multi_headline_groups():
    rp_filt = pd.DataFrame(
        {
            "rp_entity_id": ["E1", "E1", "E2"],
            "rpa_date_utc": pd.to_datetime(["2025-01-02", "2025-01-02", "2025-01-02"]),
            "timestamp_utc": pd.to_datetime(
                ["2025-01-02 08:00:00", "2025-01-02 08:05:00", "2025-01-02 09:00:00"]
            ),
            "headline": ["Same headline", "Same headline", "Unique headline"],
            "map_ticker": ["AAPL", "AAPL", "MSFT"],
            "entity_name": ["Apple", "Apple", "Microsoft"],
            "event_relevance": [0.9, 0.8, 0.7],
        }
    )

    result = cr.apply_osa_dedupe_firm_day(rp_filt)

    # E1 duplicates collapse to one row; E2 singleton remains.
    assert len(result) == 2
    assert set(result["headline"]) == {"Same headline", "Unique headline"}


def test_apply_osa_dedupe_firm_day_raises_when_required_columns_missing():
    rp_filt = pd.DataFrame(
        {
            "rp_entity_id": ["E1"],
            "map_ticker": ["AAPL"],
            # missing required: rpa_date_utc, timestamp_utc, headline
        }
    )

    with pytest.raises(KeyError, match="Cannot OSA-dedupe"):
        cr.apply_osa_dedupe_firm_day(rp_filt)


def test_align_headlines_to_dates_filters_intraday_and_assigns_headline_date():
    df = pd.DataFrame(
        {
            "rp_entity_id": ["E1", "E1", "E2"],
            "map_ticker": ["AAPL", "AAPL", "MSFT"],
            "entity_name": ["Apple", "Apple", "Microsoft"],
            "timestamp_utc": pd.to_datetime(
                [
                    "2025-01-02 13:00:00+00:00",  # 08:00 ET -> keep, same date
                    "2025-01-02 17:00:00+00:00",  # 12:00 ET -> drop intraday
                    "2025-01-02 21:30:00+00:00",  # 16:30 ET -> keep, next date
                ]
            ),
            "rpa_date_utc": pd.to_datetime(["2025-01-02", "2025-01-02", "2025-01-02"]),
            "headline": ["h1", "h2", "h3"],
        }
    )

    result = cr.align_headlines_to_dates(df)

    assert list(result.columns) == cr.FINAL_COLUMN_LIST
    assert len(result) == 2

    aapl_row = result[result["map_ticker"] == "AAPL"].iloc[0]
    assert str(aapl_row["headline_date"]) == "2025-01-02"

    msft_row = result[result["map_ticker"] == "MSFT"].iloc[0]
    assert str(msft_row["headline_date"]) == "2025-01-03"


def test_align_headlines_to_dates_drops_rows_with_invalid_timestamp():
    df = pd.DataFrame(
        {
            "rp_entity_id": ["E1"],
            "map_ticker": ["AAPL"],
            "entity_name": ["Apple"],
            "timestamp_utc": ["not-a-timestamp"],
            "rpa_date_utc": pd.to_datetime(["2025-01-02"]),
            "headline": ["h1"],
        }
    )

    result = cr.align_headlines_to_dates(df)

    assert list(result.columns) == cr.FINAL_COLUMN_LIST
    assert result.empty


def test_main_raises_when_crsp_tickers_file_missing(tmp_path, monkeypatch):
    missing_crsp = tmp_path / "missing_crsp_unique_tickers.parquet"
    rp_path = tmp_path / "RAVENPACK.parquet"
    rp_path.touch()

    monkeypatch.setattr(cr, "CRSP_TICKERS_FILE", missing_crsp)
    monkeypatch.setattr(cr, "RAVENPACK_FILE", rp_path)

    with pytest.raises(FileNotFoundError, match="Missing CRSP tickers file"):
        cr.main()


def test_main_raises_when_ravenpack_file_missing(tmp_path, monkeypatch):
    crsp_path = tmp_path / "CRSP_unique_tickers.parquet"
    missing_rp = tmp_path / "missing_ravenpack.parquet"
    crsp_path.touch()

    monkeypatch.setattr(cr, "CRSP_TICKERS_FILE", crsp_path)
    monkeypatch.setattr(cr, "RAVENPACK_FILE", missing_rp)

    with pytest.raises(FileNotFoundError, match="Missing RavenPack file"):
        cr.main()


def test_main_raises_when_crsp_ticker_column_missing(tmp_path, monkeypatch):
    crsp_path = tmp_path / "CRSP_unique_tickers.parquet"
    rp_path = tmp_path / "RAVENPACK.parquet"
    out_path = tmp_path / "RAVENPACK_cleaned.parquet"
    crsp_path.touch()
    rp_path.touch()

    monkeypatch.setattr(cr, "CRSP_TICKERS_FILE", crsp_path)
    monkeypatch.setattr(cr, "RAVENPACK_FILE", rp_path)
    monkeypatch.setattr(cr, "OUTPUT_FILE", out_path)

    crsp_df = pd.DataFrame({"permno": [1, 2]})
    rp_df = pd.DataFrame({"map_ticker": ["AAPL"]})

    def fake_read_parquet(path):
        if path == crsp_path:
            return crsp_df
        if path == rp_path:
            return rp_df
        raise AssertionError(f"Unexpected path: {path}")

    monkeypatch.setattr(cr.pd, "read_parquet", fake_read_parquet)

    with pytest.raises(KeyError, match="must contain column 'ticker'"):
        cr.main()


def test_main_raises_when_ravenpack_map_ticker_missing(tmp_path, monkeypatch):
    crsp_path = tmp_path / "CRSP_unique_tickers.parquet"
    rp_path = tmp_path / "RAVENPACK.parquet"
    out_path = tmp_path / "RAVENPACK_cleaned.parquet"
    crsp_path.touch()
    rp_path.touch()

    monkeypatch.setattr(cr, "CRSP_TICKERS_FILE", crsp_path)
    monkeypatch.setattr(cr, "RAVENPACK_FILE", rp_path)
    monkeypatch.setattr(cr, "OUTPUT_FILE", out_path)

    crsp_df = pd.DataFrame({"ticker": ["AAPL"]})
    rp_df = pd.DataFrame({"headline": ["x"]})

    def fake_read_parquet(path):
        if path == crsp_path:
            return crsp_df
        if path == rp_path:
            return rp_df
        raise AssertionError(f"Unexpected path: {path}")

    monkeypatch.setattr(cr.pd, "read_parquet", fake_read_parquet)

    with pytest.raises(KeyError, match="'map_ticker' not found"):
        cr.main()


def test_main_raises_when_required_osa_columns_missing(tmp_path, monkeypatch):
    crsp_path = tmp_path / "CRSP_unique_tickers.parquet"
    rp_path = tmp_path / "RAVENPACK.parquet"
    out_path = tmp_path / "RAVENPACK_cleaned.parquet"
    crsp_path.touch()
    rp_path.touch()

    monkeypatch.setattr(cr, "CRSP_TICKERS_FILE", crsp_path)
    monkeypatch.setattr(cr, "RAVENPACK_FILE", rp_path)
    monkeypatch.setattr(cr, "OUTPUT_FILE", out_path)

    crsp_df = pd.DataFrame({"ticker": ["AAPL"]})
    rp_df = pd.DataFrame(
        {
            "map_ticker": ["AAPL"],
            "rp_entity_id": ["E1"],
            # missing required cols: rpa_date_utc, timestamp_utc, headline
        }
    )

    def fake_read_parquet(path):
        if path == crsp_path:
            return crsp_df
        if path == rp_path:
            return rp_df
        raise AssertionError(f"Unexpected path: {path}")

    monkeypatch.setattr(cr.pd, "read_parquet", fake_read_parquet)

    with pytest.raises(KeyError, match="Cannot OSA-dedupe"):
        cr.main()


def test_main_filters_dedupes_applies_timing_and_writes_output(tmp_path, monkeypatch):
    crsp_path = tmp_path / "CRSP_unique_tickers.parquet"
    rp_path = tmp_path / "RAVENPACK.parquet"
    out_path = tmp_path / "RAVENPACK_cleaned.parquet"
    crsp_path.touch()
    rp_path.touch()

    monkeypatch.setattr(cr, "CRSP_TICKERS_FILE", crsp_path)
    monkeypatch.setattr(cr, "RAVENPACK_FILE", rp_path)
    monkeypatch.setattr(cr, "OUTPUT_FILE", out_path)

    crsp_df = pd.DataFrame({"ticker": ["AAPL", "MSFT"]})

    # UTC times selected so ET hours are: 08, 08, 12 (intraday drop), 16 (keep and +1 day)
    rp_df = pd.DataFrame(
        {
            "rp_entity_id": ["E1", "E1", "E1", "E2"],
            "map_ticker": ["AAPL", "AAPL", "AAPL", "MSFT"],
            "entity_name": ["Apple", "Apple", "Apple", "Microsoft"],
            "timestamp_utc": pd.to_datetime(
                [
                    "2025-01-02 13:00:00+00:00",
                    "2025-01-02 13:05:00+00:00",  # duplicate headline, should be deduped
                    "2025-01-02 17:00:00+00:00",  # 12 ET -> intraday, dropped
                    "2025-01-02 21:30:00+00:00",  # 16:30 ET -> kept, headline_date +1 day
                ]
            ),
            "rpa_date_utc": pd.to_datetime(
                ["2025-01-02", "2025-01-02", "2025-01-02", "2025-01-02"]
            ),
            "headline": [
                "Apple beats earnings",
                "Apple beats earnings",
                "Apple midday comment",
                "Microsoft after-close update",
            ],
            "event_relevance": [0.9, 0.8, 0.7, 0.95],
        }
    )

    def fake_read_parquet(path):
        if path == crsp_path:
            return crsp_df
        if path == rp_path:
            return rp_df
        raise AssertionError(f"Unexpected path: {path}")

    captured = {}

    def fake_to_parquet(self, path, index=False):
        captured["df"] = self.copy()
        captured["path"] = path
        captured["index"] = index

    monkeypatch.setattr(cr.pd, "read_parquet", fake_read_parquet)
    monkeypatch.setattr(pd.DataFrame, "to_parquet", fake_to_parquet)

    cr.main()

    assert captured["path"] == out_path
    assert captured["index"] is False

    result = captured["df"].reset_index(drop=True)
    assert list(result.columns) == cr.FINAL_COLUMN_LIST

    # Expect 2 rows: one deduped AAPL 08:00 ET + one MSFT after-close row.
    assert len(result) == 2

    # AAPL row keeps same date (08 ET)
    aapl_row = result[result["map_ticker"] == "AAPL"].iloc[0]
    assert str(aapl_row["headline_date"]) == "2025-01-02"

    # MSFT after 16 ET maps to next day
    msft_row = result[result["map_ticker"] == "MSFT"].iloc[0]
    assert str(msft_row["headline_date"]) == "2025-01-03"
