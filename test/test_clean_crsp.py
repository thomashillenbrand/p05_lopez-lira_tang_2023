import sys
from pathlib import Path

import pandas as pd
import pytest

sys.path.append(str(Path(__file__).resolve().parents[1] / "src"))

import clean_crsp as cc


def test_norm_ticker_series_normalizes_and_handles_missing_markers():
    s = pd.Series([" aapl ", "brk.b", "RDS/A", "", "NaN", "none", "NULL", None])

    result = cc._norm_ticker_series(s)

    expected = pd.Series(["AAPL", "BRKB", "RDSA", pd.NA, pd.NA, pd.NA, pd.NA, pd.NA], dtype="object")
    pd.testing.assert_series_equal(result, expected)


def test_main_raises_when_ravenpack_file_missing(tmp_path, monkeypatch):
    missing_rp = tmp_path / "missing_rp.parquet"
    crsp_path = tmp_path / "crsp.parquet"
    crsp_path.touch()

    monkeypatch.setattr(cc, "RAVENPACK_CLEAN_FILE", missing_rp)
    monkeypatch.setattr(cc, "CRSP_RAW_FILE", crsp_path)

    with pytest.raises(FileNotFoundError, match="Missing RavenPack cleaned file"):
        cc.main()


def test_main_raises_when_crsp_file_missing(tmp_path, monkeypatch):
    rp_path = tmp_path / "rp.parquet"
    missing_crsp = tmp_path / "missing_crsp.parquet"
    rp_path.touch()

    monkeypatch.setattr(cc, "RAVENPACK_CLEAN_FILE", rp_path)
    monkeypatch.setattr(cc, "CRSP_RAW_FILE", missing_crsp)

    with pytest.raises(FileNotFoundError, match="Missing CRSP file"):
        cc.main()


def test_main_raises_when_ticker_column_missing(tmp_path, monkeypatch):
    rp_path = tmp_path / "rp.parquet"
    crsp_path = tmp_path / "crsp.parquet"
    out_path = tmp_path / "out.parquet"
    rp_path.touch()
    crsp_path.touch()

    monkeypatch.setattr(cc, "RAVENPACK_CLEAN_FILE", rp_path)
    monkeypatch.setattr(cc, "CRSP_RAW_FILE", crsp_path)
    monkeypatch.setattr(cc, "OUTPUT_FILE", out_path)

    rp_df = pd.DataFrame({"map_ticker": ["AAPL"]})
    crsp_df = pd.DataFrame({"permno": [1]})

    def fake_read_parquet(path, columns=None):
        if path == rp_path:
            return rp_df
        if path == crsp_path:
            return crsp_df
        raise AssertionError(f"Unexpected path: {path}")

    monkeypatch.setattr(cc.pd, "read_parquet", fake_read_parquet)

    with pytest.raises(KeyError, match="CRSP file missing 'ticker'"):
        cc.main()


def test_main_filters_to_ravenpack_universe_and_writes_output(tmp_path, monkeypatch):
    rp_path = tmp_path / "rp.parquet"
    crsp_path = tmp_path / "crsp.parquet"
    out_path = tmp_path / "out.parquet"
    rp_path.touch()
    crsp_path.touch()

    monkeypatch.setattr(cc, "RAVENPACK_CLEAN_FILE", rp_path)
    monkeypatch.setattr(cc, "CRSP_RAW_FILE", crsp_path)
    monkeypatch.setattr(cc, "OUTPUT_FILE", out_path)

    rp_df = pd.DataFrame({"map_ticker": [" aapl ", "msft", "brk.b"]})
    crsp_df = pd.DataFrame(
        {
            "ticker": ["AAPL", "MSFT", "GOOG", "BRK/B", "TSLA"],
            "date": pd.to_datetime(["2025-01-02"] * 5),
            "dlyclose": [100, 200, 300, 400, 500],
        }
    )

    def fake_read_parquet(path, columns=None):
        if path == rp_path:
            assert columns == ["map_ticker"]
            return rp_df
        if path == crsp_path:
            return crsp_df
        raise AssertionError(f"Unexpected path: {path}")

    captured = {}

    def fake_to_parquet(self, path, index=False):
        captured["df"] = self.copy()
        captured["path"] = path
        captured["index"] = index

    monkeypatch.setattr(cc.pd, "read_parquet", fake_read_parquet)
    monkeypatch.setattr(pd.DataFrame, "to_parquet", fake_to_parquet)

    cc.main()

    assert captured["path"] == out_path
    assert captured["index"] is False

    result = captured["df"].reset_index(drop=True)
    assert list(result["ticker"]) == ["AAPL", "MSFT", "BRK/B"]
    assert list(result["ticker_norm"]) == ["AAPL", "MSFT", "BRKB"]
