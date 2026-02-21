from datetime import datetime
from pathlib import Path

import numpy as np
import pandas as pd
import wrds
from dateutil.relativedelta import relativedelta

from settings import config

DATA_DIR = Path(config("DATA_DIR"))
WRDS_USERNAME = config("WRDS_USERNAME")

#these are the dates they mention in the paper, oct 2021 to may 2024
START_DATE = datetime.strptime("2021-10-01", "%Y-%m-%d")
END_DATE = datetime.strptime("2024-05-31", "%Y-%m-%d")

db = wrds.Connection(wrds_username=WRDS_USERNAME)

#two main steps to filter with sql: using only relevance = 100 per the paper
#and removing duplicate news stories tagged with the same event similarity key
def pull_ravenpack(wrds_username=WRDS_USERNAME):
    print("Pulling Ravenpack data from WRDS...", flush=True)

    start = START_DATE.strftime("%Y-%m-%d")
    end = END_DATE.strftime("%Y-%m-%d")

    query = f"""
    WITH id AS (
      SELECT rp_entity_id, ticker
      FROM ravenpack_common.wrds_rpa_company_mappings
      WHERE entity_type = 'COMP'
    ),
    rp AS (
      SELECT rp_entity_id,
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
      SELECT rp_entity_id, rpa_date_utc, timestamp_utc, headline,
             relevance, event_similarity_key, event_similarity_days,
             news_type, category, "group"
      FROM ravenpack_dj.rpa_djpr_equities_2022
      UNION ALL
      SELECT rp_entity_id, rpa_date_utc, timestamp_utc, headline,
             relevance, event_similarity_key, event_similarity_days,
             news_type, category, "group"
      FROM ravenpack_dj.rpa_djpr_equities_2023
      UNION ALL
      SELECT rp_entity_id, rpa_date_utc, timestamp_utc, headline,
             relevance, event_similarity_key, event_similarity_days,
             news_type, category, "group"
      FROM ravenpack_dj.rpa_djpr_equities_2024
    ),
    ranked AS (
      SELECT
        rp.rp_entity_id,
        rp.rpa_date_utc,
        rp.timestamp_utc,
        id.ticker AS map_ticker,
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
    SELECT rp_entity_id,
           rpa_date_utc,
           timestamp_utc,
           map_ticker,
           headline
    FROM ranked
    WHERE rn = 1;
    """

    db = wrds.Connection(wrds_username=wrds_username)
    df = db.raw_sql(query, date_cols=["rpa_date_utc", "timestamp_utc"])
    db.close()

    return df

##this time only pull minimal columns that we need, hopefully helps things run a bit faster!

##there are still many more observations in the ravenpack data than in the paper
##we can filter out some of companies keeping those that are present in the CRSP data
##we can also try to filter out some headlines which describe the same/very similar events


def load_ravenpack(data_dir=DATA_DIR):
    path = Path(data_dir) / "RAVENPACK.parquet"
    df = pd.read_parquet(path)
    return df
   
if __name__ == "__main__":
    df = pull_ravenpack()
    print("Saving Ravenpack data to parquet file...", flush=True)
    path = Path(DATA_DIR) / "RAVENPACK.parquet"
    df.to_parquet(path)

