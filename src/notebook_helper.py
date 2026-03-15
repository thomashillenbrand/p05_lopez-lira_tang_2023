from pathlib import Path
from tempfile import TemporaryDirectory

import pandas as pd
from generate_batched_requests import OPENAI_MODEL, make_requests_jsonl


def get_rp_timing_stats(rp: pd.DataFrame) -> pd.DataFrame:
    if not rp.empty:
        x = rp
        x["timestamp_et"] = pd.to_datetime(x["timestamp_et"], errors="coerce")
        x["headline_date"] = pd.to_datetime(x["headline_date"], errors="coerce").dt.date

        stats = pd.Series({
            "n_rows": len(x),
            "n_tickers": x["map_ticker"].nunique(dropna=True),
            "min_ts": x["timestamp_et"].min(),
            "max_ts": x["timestamp_et"].max(),
            "min_headline_date": x["headline_date"].min(),
            "max_headline_date": x["headline_date"].max(),
        })
        # display(stats)

        intraday = x["timestamp_et"].dt.hour.between(9, 15).sum()
        print("Intraday rows remaining (expected ~0):", int(intraday))

        cutoff = pd.to_datetime("2024-05-31").date()

        x_stats = x.copy()
        x_stats["headline_date"] = pd.to_datetime(x_stats["headline_date"], errors="coerce").dt.date
        x_stats["rpa_date"] = pd.to_datetime(x_stats["rpa_date_utc"], errors="coerce").dt.date
        x_stats["headline_gt_rpa"] = x_stats["headline_date"] > x_stats["rpa_date"]

        samples = {
            "Oct 2021 - May 2024": x_stats["headline_date"] <= cutoff,
            "Oct 2021 - Feb 2026": pd.Series(True, index=x_stats.index),
        }

        rows = []
        for sample_name, mask in samples.items():
            s = x_stats.loc[mask]
            rows.append(
                {
                    "sample": sample_name,
                    "rows total": len(s),
                    "headline dates rolled over": int(s["headline_gt_rpa"].fillna(False).sum()),
                }
            )

        return stats, pd.DataFrame(rows)
    else:
        print("RAVENPACK_cleaned.parquet not found.")


def generate_single_request_jsonl(rp: pd.DataFrame):
    if rp.empty:
        print("RAVENPACK_cleaned.parquet not found or empty.")
    else:
        one_row = rp.head(1).copy()
        model_name = OPENAI_MODEL

        with TemporaryDirectory() as tmp_dir:
            tmp_path = Path(tmp_dir)
            requests_file = tmp_path / "openai_headline_requests.single.jsonl"
            mapping_file = tmp_path / "id_to_row_mapping.single.json"

            make_requests_jsonl(
                one_row,
                model=model_name,
                requests_output_dir=requests_file,
                id_row_json_output_dir=mapping_file,
                starting_row_idx=0,
            )

            jsonl_content = requests_file.read_text(encoding="utf-8")
            mapping_content = mapping_file.read_text(encoding="utf-8")
            return jsonl_content, mapping_content
            