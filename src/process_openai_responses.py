"""
File containing methods for processing the OpenAI batch job outputs, including
parsing the raw responses and building a DataFrame of daily headline scores per ticker.
"""

import json
import re
from pathlib import Path

import pandas as pd
from settings import config

DATA_DIR = Path(config("DATA_DIR"))
OUTPUT_DIR = Path(config("OUTPUT_DIR"))

SCORES_PARQUET = DATA_DIR / "daily_headline_polarity.parquet"
BATCH_OUTPUT_JSONL = OUTPUT_DIR / "openai_headline_batch_output.jsonl"
ID_ROW_JSON = OUTPUT_DIR / "id_to_row_mapping.json"


def extract_response(content: str) -> str | None:
    """Helper method to extract YES/NO/UNKNOWN value from OpenAI response content.

    Args:
        content (str): string content of OpenAI response message

    Returns:
        str | None: The extracted YES/NO/UNKNOWN value, or None if not found.
    """
    if not content:
        return None
    first_line = content.strip().splitlines()[0].strip().upper()
    first_token = re.split(r"\W+", first_line)[0] if first_line else ""
    if first_token in {"YES", "NO", "UNKNOWN"}:
        return first_token
    if "YES" in first_line:
        return "YES"
    if "NO" in first_line:
        return "NO"
    if "UNKNOWN" in first_line:
        return "UNKNOWN"
    return None


def response_to_score(label: str) -> int:
    """Convert YES/NO/UNKNOWN label to numeric score.
    
    Args:
        label (str): The YES/NO/UNKNOWN label to convert.
    Returns:
        int: 1 for YES, -1 for NO, 0 for UNKNOWN or anything else.
    """
    if label == "YES":
        return 1
    if label == "NO":
        return -1
    return 0


def build_scores_df(id_to_row: dict[str, dict[str, str]]) -> pd.DataFrame:
    """Build a DataFrame of daily headline scores per ticker from OpenAI batch output.
    This method reads the batch output JSONL file, extracts the YES/NO/UNKNOWN labels,
    converts them to scores, and aggregates to daily ticker-level polarity scores.
    
    Args:
        id_to_row (dict): Mapping of custom_id to original row data (ticker, date, entity_name).
        
    Returns:
        pd.DataFrame: DataFrame with columns [ticker, date, n_headlines, score_sum].
    """
    rows = []
    with BATCH_OUTPUT_JSONL.open("r", encoding="utf-8") as f:
        for line in f:
            obj = json.loads(line)
            custom_id = obj.get("custom_id")
            if custom_id not in id_to_row.index:
                print(f"Custom ID {custom_id} not found in id_to_row mapping")
                continue

            response = obj.get("response", {})
            body = response.get("body", {})
            choices = body.get("choices", [])
            if not choices:
                print(f"No choices found in response for Custom ID {custom_id}")
                continue

            message = choices[0].get("message", {})
            content = message.get("content", "")
            if not content:
                print(f"No content found in message for Custom ID {custom_id}")
                continue

            label = extract_response(content)
            if label is None:
                print(f"Could not extract response from content for Custom ID {custom_id}: {content}")
                continue

            rows.append(
                {
                    "ticker": id_to_row.loc[custom_id]["ticker"],
                    "date": id_to_row.loc[custom_id]["date"],
                    "headline_label": label,
                    "headline_score": response_to_score(label),
                }
            )

    scored = pd.DataFrame(rows)
    if scored.empty:
        raise ValueError("No parseable batch outputs found. Check output/error jsonl files.")

    scored["date"] = pd.to_datetime(scored["date"]).dt.date

    daily = (
        scored.groupby(["ticker", "date"], as_index=False)
        .agg(
            n_headlines=("headline_score", "size"),
            score_sum=("headline_score", "sum"),
        )
        .sort_values(["ticker", "date"])
    )

    return daily[[
        "ticker",
        "date",
        "n_headlines",
        "score_sum",
    ]]


def main():
    """Main method to drive processing of data"""
    id_to_row = pd.read_json(ID_ROW_JSON, orient="index")
    scores_df = build_scores_df(id_to_row)
    scores_df.to_parquet(SCORES_PARQUET, index=False)
    print(f"Saved daily headline scores to {SCORES_PARQUET}")    
    

if __name__ == "__main__":
    main()
    