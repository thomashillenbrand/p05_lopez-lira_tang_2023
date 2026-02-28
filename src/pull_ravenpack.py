# src/pull_ravenpack.py
from datetime import datetime
from pathlib import Path

import pandas as pd
import wrds

from settings import config

DATA_DIR = Path(config("DATA_DIR"))
WRDS_USERNAME = config("WRDS_USERNAME")

# Dates mentioned in the paper: Oct 2021 to May 2024
START_DATE = datetime.strptime("2021-10-01", "%Y-%m-%d")
END_DATE = datetime.strptime("2024-05-31", "%Y-%m-%d")


def _pick_first_existing_column(db: wrds.Connection, schema: str, table: str, candidates: list[str]) -> str | None:
    """
    Return the first column in `candidates` that exists in schema.table, else None.
    """
    q = f"""
    SELECT column_name
    FROM information_schema.columns
    WHERE table_schema = '{schema}'
      AND table_name = '{table}'
    """
    cols = set(db.raw_sql(q)["column_name"].tolist())
    for c in candidates:
        if c in cols:
            return c
    return None


# Two main steps to filter with SQL:
# - keep only relevance = 100 per the paper
# - remove duplicate news stories tagged with same event similarity key (via ROW_NUMBER)
def pull_ravenpack(wrds_username: str = WRDS_USERNAME) -> pd.DataFrame:
    print("Pulling Ravenpack data from WRDS...", flush=True)

    start = START_DATE.strftime("%Y-%m-%d")
    end = END_DATE.strftime("%Y-%m-%d")

    db = wrds.Connection(wrds_username=wrds_username)

    # --- Find a usable "entity name" column from the mappings table (WRDS schema can vary) ---
    mapping_schema = "ravenpack_common"
    mapping_table = "wrds_rpa_company_mappings"

    entity_name_candidates = [
        "entity_name",
        "company_name",
        "name",
        "rpa_entity_name",
        "issuer_name",
        "security_name",
    ]
    entity_name_col = _pick_first_existing_column(
        db,
        schema=mapping_schema,
        table=mapping_table,
        candidates=entity_name_candidates,
    )

    if entity_name_col is None:
        # Minimal-change fallback: still pull data; downstream can fall back to ticker as "name"
        print(
            f"WARNING: Could not find an entity name column in {mapping_schema}.{mapping_table}. "
            f"Tried: {entity_name_candidates}. Proceeding without entity_name.",
            flush=True,
        )
        entity_name_select = "NULL::text AS entity_name"
    else:
        entity_name_select = f"{entity_name_col}::text AS entity_name"
        print(f"Using entity name column from mappings: {mapping_schema}.{mapping_table}.{entity_name_col}", flush=True)

    # NOTE:
    # - We REMOVE rp.entity_id (it is not present in at least some yearly tables on WRDS).
    # - We ADD entity_name from the mappings table to support your OpenAI prompting step.
    # - UNION ALL branches are kept identical across years for stability.
    query = f"""
    WITH id AS (
      SELECT
        rp_entity_id,
        ticker,
        {entity_name_select}
      FROM {mapping_schema}.{mapping_table}
      WHERE entity_type = 'COMP'
    ),
    rp AS (
      SELECT
        rp_entity_id,
        rpa_date_utc,
        timestamp_utc,
        headline,
        relevance,
        event_similarity_key,
        event_similarity_days,
        news_type,
        category,
        "group"
      FROM ravenpack_dj.rpa_djpr_equities_2021
      UNION ALL
      SELECT
        rp_entity_id,
        rpa_date_utc,
        timestamp_utc,
        headline,
        relevance,
        event_similarity_key,
        event_similarity_days,
        news_type,
        category,
        "group"
      FROM ravenpack_dj.rpa_djpr_equities_2022
      UNION ALL
      SELECT
        rp_entity_id,
        rpa_date_utc,
        timestamp_utc,
        headline,
        relevance,
        event_similarity_key,
        event_similarity_days,
        news_type,
        category,
        "group"
      FROM ravenpack_dj.rpa_djpr_equities_2023
      UNION ALL
      SELECT
        rp_entity_id,
        rpa_date_utc,
        timestamp_utc,
        headline,
        relevance,
        event_similarity_key,
        event_similarity_days,
        news_type,
        category,
        "group"
      FROM ravenpack_dj.rpa_djpr_equities_2024
    ),
    ranked AS (
      SELECT
        rp.rp_entity_id,
        rp.rpa_date_utc,
        rp.timestamp_utc,
        id.ticker AS map_ticker,
        id.entity_name AS entity_name,
        rp.headline,
        ROW_NUMBER() OVER (
          PARTITION BY rp.rp_entity_id, rp.event_similarity_key
          ORDER BY rp.timestamp_utc
        ) AS rn
      FROM rp
      LEFT JOIN id ON rp.rp_entity_id = id.rp_entity_id
      WHERE rp.rpa_date_utc BETWEEN '{start}'::date AND '{end}'::date
        AND rp.relevance = 100
        AND rp.event_similarity_days > 90
        AND rp.news_type IN ('PRESS-RELEASE', 'FULL-ARTICLE')
        AND (rp.category IS NULL OR rp.category NOT IN ('stock-gain', 'stock-loss'))
        AND (rp."group" IS NULL OR rp."group" <> 'stock-prices')
        AND id.rp_entity_id IS NOT NULL
        AND rp.headline IS NOT NULL
        AND rp.timestamp_utc IS NOT NULL
    )
    SELECT
      rp_entity_id,
      rpa_date_utc,
      timestamp_utc,
      map_ticker,
      entity_name,
      headline
    FROM ranked
    WHERE rn = 1;
    """

    df = db.raw_sql(query, date_cols=["rpa_date_utc", "timestamp_utc"])
    db.close()
    return df


def load_ravenpack(data_dir: Path = DATA_DIR) -> pd.DataFrame:
    path = Path(data_dir) / "RAVENPACK.parquet"
    return pd.read_parquet(path)


if __name__ == "__main__":
    df = pull_ravenpack()
    print("Saving Ravenpack data to parquet file...", flush=True)
    path = Path(DATA_DIR) / "RAVENPACK.parquet"
    df.to_parquet(path, index=False)
    print(f"Saved: {path}")