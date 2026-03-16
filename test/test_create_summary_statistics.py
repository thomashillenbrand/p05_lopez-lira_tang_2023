import sys
from pathlib import Path

import pandas as pd
import pytest

sys.path.append(str(Path(__file__).resolve().parents[1] / "src"))

import create_summary_statistics as css


def test_norm_ticker_uppercases_and_trims():
    s = pd.Series([" aapl ", "MsFt", None])

    result = css.norm_ticker(s)

    expected = pd.Series(["AAPL", "MSFT", "NONE"])
    pd.testing.assert_series_equal(result, expected)


def test_percentile_and_summarize_series_handle_normal_and_empty_input():
    x = pd.Series([1, 2, 3, 4])
    empty = pd.Series([None, float("nan")])

    assert css.percentile(x, 0.25) == pytest.approx(1.75)
    assert pd.isna(css.percentile(empty, 0.5))

    summary = css.summarize_series(x)
    assert summary["Mean"] == pytest.approx(2.5)
    assert summary["P25"] == pytest.approx(1.75)
    assert summary["Median"] == pytest.approx(2.5)
    assert summary["P75"] == pytest.approx(3.25)


def test_classify_timing_uses_new_york_market_hours():
    # Winter dates to avoid DST ambiguity in expected values.
    ts = pd.Series(
        pd.to_datetime(
            [
                "2024-01-02 15:00:00+00:00",  # 10:00 ET -> Intraday
                "2024-01-02 21:30:00+00:00",  # 16:30 ET -> Overnight
                "2024-01-02 14:30:00+00:00",  # 09:30 ET -> Intraday (inclusive)
                "2024-01-02 21:00:00+00:00",  # 16:00 ET -> Overnight (exclusive close)
            ]
        )
    )

    out = css.classify_timing(ts)

    assert list(out) == ["Intraday", "Overnight", "Intraday", "Overnight"]


def test_create_crsp_summary_builds_two_metric_rows(monkeypatch):
    crsp = pd.DataFrame(
        {
            "ticker": ["aaa", "bbb", "ccc", None],
            "date": ["2024-01-02", "2024-01-02", "2024-01-03", "2024-01-03"],
            "dlycap": [2_000_000, 1_000_000, 500_000, 1_000_000],
            "dlyopen": [10.0, 20.0, 0.0, 10.0],
            "dlyclose": [11.0, 19.0, 10.0, 10.0],
        }
    )

    monkeypatch.setattr(css.pd, "read_parquet", lambda _: crsp)

    out = css.create_crsp_summary()

    assert list(out["Metric"]) == ["Market Cap ($M)", "Daily Return (%)"]
    mc = out.loc[out["Metric"] == "Market Cap ($M)"].iloc[0]
    dr = out.loc[out["Metric"] == "Daily Return (%)"].iloc[0]

    # Valid rows after filtering include AAA, BBB, and None->"NONE" ticker row.
    assert mc["Mean"] == pytest.approx((2.0 + 1.0 + 1.0) / 3)
    assert dr["Mean"] == pytest.approx(
        (((11 / 10 - 1) * 100) + ((19 / 20 - 1) * 100) + 0.0) / 3
    )


def test_create_crsp_summary_raises_on_missing_columns(monkeypatch):
    bad = pd.DataFrame({"ticker": ["AAA"], "date": ["2024-01-01"]})
    monkeypatch.setattr(css.pd, "read_parquet", lambda _: bad)

    with pytest.raises(KeyError, match="missing required columns"):
        css.create_crsp_summary()


def test_create_news_by_year_builds_expected_aggregates(monkeypatch):
    rp = pd.DataFrame(
        {
            "rp_entity_id": [1, 1, 2, 3],
            "rpa_date_utc": [
                "2024-01-02 10:00:00",
                "2024-01-02 11:00:00",
                "2024-01-03 10:00:00",
                "2025-01-03 10:00:00",
            ],
            "headline": ["h1", "h2", "h3", "h4"],
        }
    )

    monkeypatch.setattr(css.pd, "read_parquet", lambda _: rp)

    out = css.create_news_by_year().sort_values("Year").reset_index(drop=True)

    y2024 = out.loc[out["Year"] == 2024].iloc[0]
    assert y2024["Headlines"] == 3
    assert y2024["Firms"] == 2
    assert y2024["Days"] == 2
    assert y2024["Per Day"] == pytest.approx(1.5)
    assert y2024["Per Firm"] == pytest.approx(1.5)


def test_create_news_by_year_raises_on_missing_columns(monkeypatch):
    bad = pd.DataFrame({"rp_entity_id": [1], "headline": ["h"]})
    monkeypatch.setattr(css.pd, "read_parquet", lambda _: bad)

    with pytest.raises(KeyError, match="missing required columns"):
        css.create_news_by_year()


def test_create_news_by_timing_builds_ordered_output(monkeypatch):
    rp = pd.DataFrame(
        {
            "rp_entity_id": [1, 1, 2, 2],
            "timestamp_utc": [
                "2024-01-02 15:00:00+00:00",  # Intraday
                "2024-01-02 22:00:00+00:00",  # Overnight
                "2024-01-03 15:00:00+00:00",  # Intraday
                "2024-01-03 22:00:00+00:00",  # Overnight
            ],
            "headline": ["h1", "h2", "h3", "h4"],
        }
    )

    monkeypatch.setattr(css.pd, "read_parquet", lambda _: rp)

    out = css.create_news_by_timing().reset_index(drop=True)

    assert list(out["Timing"]) == ["Overnight", "Intraday"]
    assert out.loc[0, "Headlines"] == 2
    assert out.loc[1, "Headlines"] == 2
    assert out["% Total"].sum() == pytest.approx(100.0)


def test_create_news_by_timing_raises_on_missing_columns(monkeypatch):
    bad = pd.DataFrame({"rp_entity_id": [1], "timestamp_utc": ["2024-01-02"]})
    monkeypatch.setattr(css.pd, "read_parquet", lambda _: bad)

    with pytest.raises(KeyError, match="missing required columns"):
        css.create_news_by_timing()


def test_main_writes_three_csv_outputs(monkeypatch, tmp_path):
    crsp_summary = pd.DataFrame({"Metric": ["M"], "Mean": [1.0]})
    news_year = pd.DataFrame(
        {
            "Year": [2024],
            "Headlines": [1],
            "Firms": [1],
            "Days": [1],
            "Per Day": [1.0],
            "Per Firm": [1.0],
        }
    )
    news_timing = pd.DataFrame(
        {
            "Timing": ["Overnight"],
            "Headlines": [1],
            "% Total": [100.0],
            "Firms": [1],
            "Per Day": [1.0],
        }
    )

    monkeypatch.setattr(css, "create_crsp_summary", lambda: crsp_summary)
    monkeypatch.setattr(css, "create_news_by_year", lambda: news_year)
    monkeypatch.setattr(css, "create_news_by_timing", lambda: news_timing)

    out_crsp = tmp_path / "crsp.csv"
    out_year = tmp_path / "year.csv"
    out_timing = tmp_path / "timing.csv"
    monkeypatch.setattr(css, "OUT_CRSP_CSV", out_crsp)
    monkeypatch.setattr(css, "OUT_NEWS_YEAR_CSV", out_year)
    monkeypatch.setattr(css, "OUT_NEWS_TIMING_CSV", out_timing)

    writes = []

    def fake_to_csv(self, path, index=False):
        writes.append((path, index, self.copy()))

    monkeypatch.setattr(pd.DataFrame, "to_csv", fake_to_csv)

    css.main()

    assert len(writes) == 3
    assert writes[0][0] == out_crsp
    assert writes[1][0] == out_year
    assert writes[2][0] == out_timing
    assert all(index is False for _, index, _ in writes)
