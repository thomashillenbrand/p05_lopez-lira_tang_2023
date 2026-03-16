from __future__ import annotations

import json
import re
from pathlib import Path

import pandas as pd

from settings import config

DATA_DIR = Path(config("DATA_DIR"))
OUTPUT_DIR = Path(config("OUTPUT_DIR"))

OUT_CSV = OUTPUT_DIR / "openai_output_label_proportions.csv"

PAPER_START = pd.Timestamp("2021-10-01")
PAPER_END = pd.Timestamp("2024-05-31")

LABELS = ["YES", "NO", "UNKNOWN"]


def iter_jsonl(path: Path):
    with path.open("r", encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if line:
                yield json.loads(line)


def extract_label(content: str) -> str | None:
    if not content:
        return None
    first_line = content.strip().splitlines()[0].strip().upper()
    token = re.split(r"\W+", first_line)[0]
    if token in LABELS:
        return token
    for lab in LABELS:
        if lab in first_line:
            return lab
    return None


def load_mapping() -> pd.DataFrame:
    mapping_files = sorted(DATA_DIR.glob("id_to_row_mapping.*.json"))
    if not mapping_files:
        raise FileNotFoundError(
            f"No mapping files found in {DATA_DIR} matching id_to_row_mapping.*.json"
        )

    rows = []
    for path in mapping_files:
        with path.open("r", encoding="utf-8") as f:
            obj = json.load(f)

        # supports either dict[custom_id] -> row_info or list-like records
        if isinstance(obj, dict):
            for custom_id, meta in obj.items():
                rec = {"custom_id": str(custom_id)}
                if isinstance(meta, dict):
                    rec.update(meta)
                rows.append(rec)
        elif isinstance(obj, list):
            for meta in obj:
                if not isinstance(meta, dict):
                    continue
                custom_id = meta.get("custom_id") or meta.get("id")
                if custom_id is None:
                    continue
                rec = {"custom_id": str(custom_id)}
                rec.update(meta)
                rows.append(rec)

    out = pd.DataFrame(rows).drop_duplicates(subset=["custom_id"], keep="first")
    if "date" not in out.columns:
        raise KeyError("Mapping file must contain a 'date' field for each custom_id.")
    out["date"] = pd.to_datetime(out["date"], errors="coerce")
    out = out.dropna(subset=["date"])
    return out


def load_outputs() -> pd.DataFrame:
    output_files = sorted(DATA_DIR.glob("openai_headline_batch_output*.jsonl"))
    if not output_files:
        raise FileNotFoundError(
            f"No output files found in {DATA_DIR} matching openai_headline_batch_output*.jsonl"
        )

    rows = []
    for path in output_files:
        for obj in iter_jsonl(path):
            custom_id = obj.get("custom_id")
            if custom_id is None:
                continue

            body = (obj.get("response") or {}).get("body") or {}
            choices = body.get("choices") or []
            if not choices:
                continue

            message = choices[0].get("message") or {}
            content = message.get("content") or ""
            label = extract_label(content)
            if label is None:
                continue

            rows.append({"custom_id": str(custom_id), "label": label})

    out = pd.DataFrame(rows).drop_duplicates(subset=["custom_id"], keep="first")
    return out


def make_prop_table(df: pd.DataFrame) -> pd.DataFrame:
    full_counts = df["label"].value_counts().reindex(LABELS, fill_value=0)
    full_props = full_counts / full_counts.sum()

    sample_df = df[(df["date"] >= PAPER_START) & (df["date"] <= PAPER_END)].copy()
    sample_counts = sample_df["label"].value_counts().reindex(LABELS, fill_value=0)
    sample_props = sample_counts / sample_counts.sum()

    out = pd.DataFrame(
        {
            "label": LABELS,
            "proportion_2021_2024_sample_period": [
                sample_props.get(l, 0.0) for l in LABELS
            ],
            "proportion_full_sample": [full_props.get(l, 0.0) for l in LABELS],
            "count_2021_2024_sample_period": [sample_counts.get(l, 0) for l in LABELS],
            "count_full_sample": [full_counts.get(l, 0) for l in LABELS],
        }
    )
    return out


def main() -> None:
    mapping = load_mapping()
    outputs = load_outputs()

    df = outputs.merge(mapping[["custom_id", "date"]], on="custom_id", how="inner")

    table = make_prop_table(df)
    table.to_csv(OUT_CSV, index=False)

    print(table.to_string(index=False))
    print(f"\nWrote {OUT_CSV}")


if __name__ == "__main__":
    main()
