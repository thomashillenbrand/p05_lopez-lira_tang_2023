import sys
from pathlib import Path

import numpy as np
import pandas as pd
import pytest

sys.path.append(str(Path(__file__).resolve().parents[1] / "src"))

import create_table1 as ct1


def test_annualized_sharpe_computes_expected_value():
    x = pd.Series([0.01, 0.02, -0.01, 0.00])

    result = ct1.annualized_sharpe(x)

    expected = np.sqrt(252) * x.mean() / x.std(ddof=1)
    assert result == pytest.approx(expected)


def test_annualized_sharpe_returns_nan_for_too_few_or_zero_std():
    assert pd.isna(ct1.annualized_sharpe(pd.Series([0.01])))
    assert pd.isna(ct1.annualized_sharpe(pd.Series([0.01, 0.01, 0.01])))


def test_hit_rate_and_mean_pct_handle_empty_and_valid_input():
    empty = pd.Series([np.nan, None])
    x = pd.Series([0.01, -0.02, 0.03])

    assert pd.isna(ct1.hit_rate(empty))
    assert pd.isna(ct1.mean_pct(empty))
    assert ct1.hit_rate(x) == pytest.approx((2 / 3) * 100)
    assert ct1.mean_pct(x) == pytest.approx(x.mean() * 100)


def test_summarize_portfolio_uses_only_traded_rows():
    df = pd.DataFrame(
        {
            "ret_ir_ls": [0.10, 0.20, -0.10],
            "ret_ls": [0.01, 0.02, -0.03],
            "trade_ls": [True, False, True],
        }
    )

    result = ct1.summarize_portfolio(
        df=df,
        label="Long-Short Portfolio",
        ir_col="ret_ir_ls",
        drift_col="ret_ls",
        trade_col="trade_ls",
    )

    assert result["Portfolio"] == "Long-Short Portfolio"
    assert result["Trading Days"] == 2
    assert result["Initial Reaction Hit Rate (%)"] == pytest.approx(50.0)
    assert result["Initial Reaction Mean Return (%)"] == pytest.approx(0.0)
    assert result["Drift Mean Return (%)"] == pytest.approx(-1.0)
    assert pd.isna(result["Firm-Day Observations"])


def test_build_table_creates_three_portfolios_plus_summary_and_rounds():
    df = pd.DataFrame(
        {
            "date": pd.to_datetime(["2024-01-02", "2024-01-02", "2024-01-03"]),
            "n_total": [10, 20, 30],
            "ret_ir_ls": [0.0033334, -0.0022222, 0.0011111],
            "ret_ls": [0.0051234, -0.0011111, 0.0002345],
            "trade_ls": [True, True, False],
            "ret_ir_long": [0.001, 0.002, 0.003],
            "ret_long": [0.002, 0.001, 0.000],
            "trade_long": [True, False, True],
            "ret_ir_short": [-0.001, -0.002, 0.001],
            "ret_short": [-0.002, -0.003, 0.001],
            "trade_short": [False, True, True],
        }
    )

    result = ct1.build_table(df)

    assert list(result["Portfolio"]) == [
        "Long-Short Portfolio",
        "Long-Only Portfolio",
        "Short-Only Portfolio",
        "Sample Summary",
    ]

    summary = result.loc[result["Portfolio"] == "Sample Summary"].iloc[0]
    assert summary["Trading Days"] == 2
    assert summary["Firm-Day Observations"] == 60

    metric_cols = [
        "Initial Reaction Hit Rate (%)",
        "Initial Reaction Mean Return (%)",
        "Drift Hit Rate (%)",
        "Drift Mean Return (%)",
        "Drift Sharpe Ratio",
    ]
    portfolio_rows = result[result["Portfolio"] != "Sample Summary"]
    for col in metric_cols:
        vals = portfolio_rows[col].dropna()
        # Rounded to 3 decimals: value * 1000 should be (close to) integer.
        assert np.allclose(vals * 1000, np.round(vals * 1000))


def test_filter_window_applies_inclusive_date_bounds():
    df = pd.DataFrame(
        {
            "date": pd.to_datetime(["2024-01-01", "2024-01-02", "2024-01-03"]),
            "x": [1, 2, 3],
        }
    )

    result = ct1.filter_window(
        df,
        start=pd.Timestamp("2024-01-02"),
        end=pd.Timestamp("2024-01-03"),
    )

    assert list(result["x"]) == [2, 3]


def test_main_reads_builds_and_writes_paper_and_full_tables(monkeypatch, tmp_path):
    input_df = pd.DataFrame(
        {
            "date": pd.to_datetime(["2021-09-30", "2021-10-01", "2024-05-31", "2024-06-01"]),
            "n_total": [1, 2, 3, 4],
            "ret_ir_ls": [0.01, 0.02, -0.01, 0.00],
            "ret_ls": [0.001, 0.002, -0.001, 0.000],
            "trade_ls": [True, True, True, False],
            "ret_ir_long": [0.01, 0.01, 0.01, 0.01],
            "ret_long": [0.002, 0.002, 0.002, 0.002],
            "trade_long": [True, False, True, False],
            "ret_ir_short": [-0.01, -0.01, 0.01, 0.01],
            "ret_short": [-0.002, -0.002, 0.002, 0.002],
            "trade_short": [False, True, True, True],
        }
    )

    monkeypatch.setattr(ct1.pd, "read_parquet", lambda _: input_df)

    out_paper = tmp_path / "table1_overnight_paper_sample.csv"
    out_full = tmp_path / "table1_overnight_full_sample.csv"
    monkeypatch.setattr(ct1, "OUT_PAPER", out_paper)
    monkeypatch.setattr(ct1, "OUT_FULL", out_full)

    writes = {}

    def fake_to_csv(self, path, index=False):
        writes[Path(path)] = self.copy()
        assert index is False

    monkeypatch.setattr(pd.DataFrame, "to_csv", fake_to_csv)

    ct1.main()

    assert out_paper in writes
    assert out_full in writes

    paper_df = writes[out_paper]
    full_df = writes[out_full]

    paper_summary = paper_df.loc[paper_df["Portfolio"] == "Sample Summary"].iloc[0]
    full_summary = full_df.loc[full_df["Portfolio"] == "Sample Summary"].iloc[0]

    # Paper window includes only 2021-10-01 through 2024-05-31.
    assert paper_summary["Trading Days"] == 2
    assert paper_summary["Firm-Day Observations"] == 5

    # Full sample keeps all valid dates.
    assert full_summary["Trading Days"] == 4
    assert full_summary["Firm-Day Observations"] == 10


def test_main_raises_when_required_columns_are_missing(monkeypatch):
    bad_df = pd.DataFrame({"date": pd.to_datetime(["2024-01-01"])})
    monkeypatch.setattr(ct1.pd, "read_parquet", lambda _: bad_df)

    with pytest.raises(KeyError, match="must include"):
        ct1.main()