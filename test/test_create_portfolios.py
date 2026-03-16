import sys
from pathlib import Path

import pandas as pd
import pytest

sys.path.append(str(Path(__file__).resolve().parents[1] / "src"))

import create_portfolios as cp


def test_norm_ticker_uppercases_and_trims():
    s = pd.Series([" aapl ", "msft", "Brk.b"])

    result = cp.norm_ticker(s)

    expected = pd.Series(["AAPL", "MSFT", "BRK.B"])
    pd.testing.assert_series_equal(result, expected)


def test_is_nyse_exchange_recognizes_supported_values():
    s = pd.Series(["N", "nyse", "1", "Q", "2", None])

    result = cp.is_nyse_exchange(s)

    expected = pd.Series([True, True, True, False, False, False])
    pd.testing.assert_series_equal(result, expected)


def test_value_weighted_return_handles_normal_and_edge_cases():
    df = pd.DataFrame({"ret": [0.10, -0.05, 0.00], "w": [2.0, 1.0, 1.0]})
    assert cp.value_weighted_return(df, "ret", "w") == pytest.approx(0.0375)

    zero_weight_df = pd.DataFrame({"ret": [0.10, -0.05], "w": [0.0, 0.0]})
    assert pd.isna(cp.value_weighted_return(zero_weight_df, "ret", "w"))

    nan_df = pd.DataFrame({"ret": [None], "w": [None]})
    assert pd.isna(cp.value_weighted_return(nan_df, "ret", "w"))


def test_main_raises_when_signal_columns_missing(monkeypatch):
    bad_sig = pd.DataFrame({"ticker": ["AAA"], "date": ["2024-01-01"]})

    monkeypatch.setattr(cp.pd, "read_parquet", lambda _: bad_sig)

    with pytest.raises(KeyError, match="must include"):
        cp.main()


def test_main_raises_when_crsp_columns_missing(monkeypatch):
    sig = pd.DataFrame({"ticker": ["AAA"], "date": ["2024-01-02"], "score": [1]})
    bad_crsp = pd.DataFrame(
        {"ticker": ["AAA"], "date": ["2024-01-02"], "dlyopen": [10.0]}
    )

    def fake_read_parquet(path):
        if path == cp.SCORES_PATH:
            return sig
        if path == cp.CRSP_PATH:
            return bad_crsp
        raise AssertionError(f"Unexpected parquet path: {path}")

    monkeypatch.setattr(cp.pd, "read_parquet", fake_read_parquet)

    with pytest.raises(KeyError, match="must include"):
        cp.main()


def test_main_builds_expected_trade_logic_and_writes_output(monkeypatch, tmp_path):
    sig = pd.DataFrame(
        {
            "ticker": ["AAA", "CCC", "AAA", "CCC", "BBB", "DDD", "BBB", "ZZZ"],
            "date": pd.to_datetime(
                [
                    "2024-01-02",  # long-only day setup (2 pos, 0 neg)
                    "2024-01-02",
                    "2024-01-03",  # both-legs day setup (2 pos, 2 neg)
                    "2024-01-03",
                    "2024-01-03",
                    "2024-01-03",
                    "2024-01-04",  # short-only day setup (0 pos, 1 neg)
                    "2024-01-05",  # neutral-only day
                ]
            ),
            "score": [1, 1, 1, 1, -1, -1, -1, 0],
        }
    )

    crsp = pd.DataFrame(
        {
            "permno": [
                1,
                1,
                1,
                2,
                2,
                2,
                3,
                3,
                4,
                4,
            ],
            "ticker": [
                "AAA",
                "AAA",
                "AAA",
                "CCC",
                "CCC",
                "CCC",
                "BBB",
                "BBB",
                "DDD",
                "DDD",
            ],
            "date": pd.to_datetime(
                [
                    "2024-01-01",
                    "2024-01-02",
                    "2024-01-03",
                    "2024-01-01",
                    "2024-01-02",
                    "2024-01-03",
                    "2024-01-02",
                    "2024-01-03",
                    "2024-01-02",
                    "2024-01-03",
                ]
            ),
            "dlyopen": [10.0, 10.5, 11.0, 20.0, 20.5, 21.0, 30.0, 30.5, 40.0, 40.5],
            "dlyclose": [10.0, 11.0, 11.2, 20.0, 21.0, 22.0, 30.0, 29.9, 40.0, 39.0],
            "dlycap": [100, 110, 120, 200, 210, 220, 300, 290, 400, 390],
            "primaryexch": ["N"] * 10,
        }
    )

    def fake_read_parquet(path):
        if path == cp.SCORES_PATH:
            return sig
        if path == cp.CRSP_PATH:
            return crsp
        raise AssertionError(f"Unexpected parquet path: {path}")

    monkeypatch.setattr(cp.pd, "read_parquet", fake_read_parquet)

    out_path = tmp_path / "portfolio_daily_returns.parquet"
    monkeypatch.setattr(cp, "OUT_PATH", out_path)

    captured = {}

    def fake_to_parquet(self, path, index=False):
        captured["df"] = self.copy()
        captured["path"] = path
        captured["index"] = index

    monkeypatch.setattr(pd.DataFrame, "to_parquet", fake_to_parquet)

    cp.main()

    assert captured["path"] == out_path
    assert captured["index"] is False

    out = captured["df"].copy()
    out["date"] = pd.to_datetime(out["date"])

    d_long_only = pd.Timestamp("2024-01-02")
    d_both = pd.Timestamp("2024-01-03")
    d_short_only = pd.Timestamp("2024-01-04")
    d_neutral = pd.Timestamp("2024-01-05")

    row_long = out.loc[out["date"] == d_long_only].iloc[0]
    row_both = out.loc[out["date"] == d_both].iloc[0]
    row_short = out.loc[out["date"] == d_short_only].iloc[0]
    row_neutral = out.loc[out["date"] == d_neutral].iloc[0]

    # Signal counts
    assert row_long["n_pos"] == 2 and row_long["n_neg"] == 0 and row_long["n_neu"] == 0
    assert row_both["n_pos"] == 2 and row_both["n_neg"] == 2 and row_both["n_neu"] == 0
    assert (
        row_short["n_pos"] == 0 and row_short["n_neg"] == 1 and row_short["n_neu"] == 0
    )
    assert (
        row_neutral["n_pos"] == 0
        and row_neutral["n_neg"] == 0
        and row_neutral["n_neu"] == 1
    )

    # Trade flags from Table 1 logic
    assert (
        row_long["trade_long"] and not row_long["trade_short"] and row_long["trade_ls"]
    )
    assert row_both["trade_long"] and row_both["trade_short"] and row_both["trade_ls"]
    assert (
        (not row_short["trade_long"])
        and row_short["trade_short"]
        and row_short["trade_ls"]
    )
    assert (
        (not row_neutral["trade_long"])
        and (not row_neutral["trade_short"])
        and (not row_neutral["trade_ls"])
    )

    # LS return composition rules
    assert row_long["ret_ls"] == pytest.approx(row_long["ret_long"])
    assert row_long["ret_ir_ls"] == pytest.approx(row_long["ret_ir_long"])

    assert row_both["ret_ls"] == pytest.approx(
        row_both["ret_long"] + row_both["ret_short"]
    )
    assert row_both["ret_ir_ls"] == pytest.approx(
        row_both["ret_ir_long"] + row_both["ret_ir_short"]
    )

    assert row_short["ret_ls"] == pytest.approx(row_short["ret_short"])
    assert row_short["ret_ir_ls"] == pytest.approx(row_short["ret_ir_short"])

    assert row_neutral["ret_ls"] == pytest.approx(0.0)
    assert row_neutral["ret_ir_ls"] == pytest.approx(0.0)

    # Figure 5 restricted series requires both long and short legs (no fallback).
    assert row_long["ret_ls_restricted"] == pytest.approx(0.0)
    assert row_short["ret_ls_restricted"] == pytest.approx(0.0)
