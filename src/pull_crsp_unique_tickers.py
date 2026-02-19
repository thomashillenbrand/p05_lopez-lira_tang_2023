from datetime import datetime
from pathlib import Path

import pandas as pd
import wrds
from settings import config

DATA_DIR = Path(config("DATA_DIR"))
WRDS_USERNAME = config("WRDS_USERNAME")
START_DATE = datetime.strptime("2021-10-01", "%Y-%m-%d")
END_DATE = datetime.strptime("2024-05-31", "%Y-%m-%d")


def pull_crsp_unique_tickers(
    start_date=START_DATE,
    end_date=END_DATE,
    wrds_username=WRDS_USERNAME,
):
    """
    Pulls unique CRSP tickers over a given date range.

    Parameters:
    - start_date (str or datetime): Start date (inclusive)
    - end_date (str or datetime): End date (inclusive)
    - wrds_username (str): WRDS username

    Returns:
    - pandas.DataFrame with unique tickers
    """

    start_date = start_date.date() if isinstance(start_date, datetime) else start_date
    end_date = end_date.date() if isinstance(end_date, datetime) else end_date

    query = f"""
    SELECT DISTINCT ticker
    FROM CRSPM.DSF_V2
    WHERE
        primaryexch IN ('N', 'A', 'Q')
        AND conditionaltype = 'RW'
        AND tradingstatusflg = 'A'
        AND dlycaldt >= '{start_date}'
        AND dlycaldt <= '{end_date}'
        AND ticker IS NOT NULL
    ;
    """

    print("Pulling unique CRSP tickers from WRDS...")
    print(f"Start date: {start_date}, End date: {end_date}")

    db = wrds.Connection(wrds_username=wrds_username)
    df = db.raw_sql(query)

    db.close()

    df["ticker"] = df["ticker"].str.strip().str.upper()

    return df


if __name__ == "__main__":
    tickers_df = pull_crsp_unique_tickers()
    path = Path(DATA_DIR) / "CRSP_unique_tickers.parquet"
    tickers_df.to_parquet(path)

def load_crsp_unique_tickers(data_dir=DATA_DIR):
    path = Path(data_dir) / "CRSP_unique_tickers.parquet"
    df = pd.read_parquet(path)
    return df