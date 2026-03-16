import sys
from pathlib import Path

import pandas as pd
import pytest

sys.path.append(str(Path(__file__).resolve().parents[1] / "src"))

import create_portfolio_size as cps


def test_norm_ticker_and_is_nyse_exchange():
    s = pd.Series([" aapl ", "NySe", "q", None])

    norm = cps.norm_ticker(s)
    nyse = cps.is_nyse_exchange(pd.Series(["N", "NYSE", "1", "Q"]))

    pd.testing.assert_series_equal(norm, pd.Series(["AAPL", "NYSE", "Q", "NONE"]))
    pd.testing.assert_series_equal(nyse, pd.Series([True, True, True, False]))


def test_counts_for_subset_builds_expected_columns_and_totals():
    df = pd.DataFrame(
        {
            "date": pd.to_datetime(["2024-01-02", "2024-01-02", "2024-01-03"]).date,
            "score": [1, -1, 1],
        }
    )

    out = cps.counts_for_subset(df, "x").sort_values("date").reset_index(drop=True)

    assert list(out.columns) == ["portfolio", "date", "n_long", "n_short", "n_total"]
    assert out.loc[0, "n_long"] == 1
    assert out.loc[0, "n_short"] == 1
    assert out.loc[0, "n_total"] == 2
    assert out.loc[1, "n_long"] == 1
    assert out.loc[1, "n_short"] == 0
    assert out.loc[1, "n_total"] == 1


def test_build_daily_counts_returns_all_expected_portfolios():
    df = pd.DataFrame(
        {
            "date": pd.to_datetime(["2024-01-02", "2024-01-02", "2024-01-03", "2024-01-03"]).date,
            "score": [1, -1, 1, -1],
            "eligible_restricted": [True, False, True, True],
            "eligible_not_small": [True, True, False, True],
            "eligible_price_gt_5": [False, True, True, True],
        }
    )

    out = cps.build_daily_counts(df)

    expected = {
        "table1_unrestricted",
        "table1_long_only",
        "table1_short_only",
        "table1_long_short",
        "figure5_restricted",
        "figure5_not_small",
        "figure5_price_gt_5",
    }
    assert set(out["portfolio"].unique()) == expected


def test_build_summary_aggregates_by_portfolio():
    daily_counts = pd.DataFrame(
        {
            "portfolio": ["p1", "p1", "p2"],
            "date": pd.to_datetime(["2024-01-02", "2024-01-03", "2024-01-02"]).date,
            "n_long": [2, 4, 1],
            "n_short": [1, 3, 2],
            "n_total": [3, 7, 3],
        }
    )

    out = cps.build_summary(daily_counts).sort_values("portfolio").reset_index(drop=True)

    p1 = out.loc[out["portfolio"] == "p1"].iloc[0]
    assert p1["trading_days"] == 2
    assert p1["avg_n_long"] == pytest.approx(3.0)
    assert p1["median_n_short"] == pytest.approx(2.0)
    assert p1["max_n_total"] == 7


def test_load_base_df_raises_on_missing_signal_columns(monkeypatch):
    bad_sig = pd.DataFrame({"ticker": ["AAA"], "date": ["2024-01-01"]})

    monkeypatch.setattr(cps.pd, "read_parquet", lambda _: bad_sig)

    with pytest.raises(KeyError, match="must include"):
        cps.load_base_df()


def test_load_base_df_raises_on_missing_crsp_columns(monkeypatch):
    sig = pd.DataFrame({"ticker": ["AAA"], "date": ["2024-01-02"], "score": [1]})
    bad_crsp = pd.DataFrame({"ticker": ["AAA"], "date": ["2024-01-02"], "permno": [1]})

    def fake_read_parquet(path):
        if path == cps.SCORES_PATH:
            return sig
        if path == cps.CRSP_PATH:
            return bad_crsp
        raise AssertionError(f"Unexpected parquet path: {path}")

    monkeypatch.setattr(cps.pd, "read_parquet", fake_read_parquet)

    with pytest.raises(KeyError, match="must include"):
        cps.load_base_df()


def test_load_base_df_builds_expected_merge_and_flags(monkeypatch):
    sig = pd.DataFrame(
        {
            "ticker": ["AAA", "BBB", "CCC", "ZZZ"],
            "date": pd.to_datetime(["2024-01-02", "2024-01-03", "2024-01-03", "2024-01-03"]),
            "score": [1, -1, 1, 0],
        }
    )

    crsp = pd.DataFrame(
        {
            "permno": [1, 1, 2, 2, 3, 3],
            "ticker": ["AAA", "AAA", "BBB", "BBB", "CCC", "CCC"],
            "date": pd.to_datetime(["2024-01-01", "2024-01-02", "2024-01-02", "2024-01-03", "2024-01-02", "2024-01-03"]),
            "dlyopen": [10.0, 10.5, 20.0, 20.5, 30.0, 30.5],
            "dlyclose": [10.0, 11.0, 20.0, 21.0, 30.0, 30.1],
            "dlycap": [100, 110, 200, 210, 300, 290],
            "primaryexch": ["N", "N", "N", "N", "N", "N"],
        }
    )

    def fake_read_parquet(path):
        if path == cps.SCORES_PATH:
            return sig
        if path == cps.CRSP_PATH:
            return crsp
        raise AssertionError(f"Unexpected parquet path: {path}")

    monkeypatch.setattr(cps.pd, "read_parquet", fake_read_parquet)

    out = cps.load_base_df().sort_values(["date", "ticker"]).reset_index(drop=True)

    # neutral score row should be removed; non-mapped ticker should drop in merge.
    assert set(out["score"].unique()) == {1, -1}
    assert set(out["ticker"].unique()) == {"AAA", "BBB", "CCC"}
    assert "eligible_restricted" in out.columns
    assert "eligible_not_small" in out.columns
    assert "eligible_price_gt_5" in out.columns


def test_plot_functions_write_files(tmp_path):
    daily_counts = pd.DataFrame(
        {
            "portfolio": [
                "table1_long_short",
                "table1_long_short",
                "table1_long_only",
                "table1_short_only",
                "figure5_restricted",
                "figure5_not_small",
                "figure5_price_gt_5",
            ],
            "date": pd.to_datetime(
                [
                    "2024-01-02",
                    "2024-01-03",
                    "2024-01-02",
                    "2024-01-02",
                    "2024-01-02",
                    "2024-01-02",
                    "2024-01-02",
                ]
            ).date,
            "n_long": [2, 3, 2, 0, 1, 1, 1],
            "n_short": [1, 2, 0, 1, 1, 1, 1],
            "n_total": [3, 5, 2, 1, 2, 2, 2],
        }
    )

    out_all = tmp_path / "overlay.png"
    out_grid = tmp_path / "grid.png"

    cps.make_overlay_plot(daily_counts, out_all)
    cps.make_grid_plot(daily_counts, out_grid)

    assert out_all.exists()
    assert out_grid.exists()


def test_main_runs_end_to_end_with_monkeypatched_dependencies(monkeypatch, tmp_path):
    base_df = pd.DataFrame(
        {
            "portfolio": ["placeholder"],
            "date": pd.to_datetime(["2024-01-02"]).date,
            "score": [1],
            "eligible_restricted": [True],
            "eligible_not_small": [True],
            "eligible_price_gt_5": [True],
        }
    )
    daily_counts = pd.DataFrame(
        {
            "portfolio": ["table1_long_short"],
            "date": pd.to_datetime(["2024-01-02"]).date,
            "n_long": [2],
            "n_short": [1],
            "n_total": [3],
        }
    )
    summary = pd.DataFrame(
        {
            "portfolio": ["table1_long_short"],
            "start_date": pd.to_datetime(["2024-01-02"]).date,
            "end_date": pd.to_datetime(["2024-01-02"]).date,
            "trading_days": [1],
            "avg_n_long": [2.0],
            "median_n_long": [2.0],
            "min_n_long": [2],
            "max_n_long": [2],
            "avg_n_short": [1.0],
            "median_n_short": [1.0],
            "min_n_short": [1],
            "max_n_short": [1],
            "avg_n_total": [3.0],
            "median_n_total": [3.0],
            "min_n_total": [3],
            "max_n_total": [3],
        }
    )

    monkeypatch.setattr(cps, "load_base_df", lambda: base_df)
    monkeypatch.setattr(cps, "build_daily_counts", lambda _: daily_counts)
    monkeypatch.setattr(cps, "build_summary", lambda _: summary)

    out_counts = tmp_path / "counts.csv"
    out_summary = tmp_path / "summary.csv"
    out_all = tmp_path / "all.png"
    out_grid = tmp_path / "grid.png"

    monkeypatch.setattr(cps, "OUT_COUNTS_CSV", out_counts)
    monkeypatch.setattr(cps, "OUT_SUMMARY_CSV", out_summary)
    monkeypatch.setattr(cps, "OUT_PLOT_ALL", out_all)
    monkeypatch.setattr(cps, "OUT_PLOT_GRID", out_grid)

    saved_csvs = []
    saved_plots = []

    def fake_to_csv(self, path, index=False):
        saved_csvs.append((path, index, self.copy()))

    monkeypatch.setattr(pd.DataFrame, "to_csv", fake_to_csv)
    monkeypatch.setattr(cps, "make_overlay_plot", lambda d, p: saved_plots.append(("overlay", p, d.copy())))
    monkeypatch.setattr(cps, "make_grid_plot", lambda d, p: saved_plots.append(("grid", p, d.copy())))

    cps.main()

    assert len(saved_csvs) == 2
    assert saved_csvs[0][0] == out_counts
    assert saved_csvs[0][1] is False
    assert saved_csvs[1][0] == out_summary
    assert saved_csvs[1][1] is False

    assert len(saved_plots) == 2
    assert saved_plots[0][0] == "overlay"
    assert saved_plots[0][1] == out_all
    assert saved_plots[1][0] == "grid"
    assert saved_plots[1][1] == out_grid