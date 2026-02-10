"""
Function(s) to pull Daily Stock data from CRSP via WRDS.

"""

from datetime import datetime
from pathlib import Path

import pandas as pd
import wrds
from settings import config

DATA_DIR = Path(config("DATA_DIR"))
WRDS_USERNAME = config("WRDS_USERNAME")
START_DATE = config("START_DATE")
END_DATE = config("END_DATE")

def pull_crsp_daily_file(
    start_date=START_DATE, end_date=END_DATE, permnos=None, wrds_username=WRDS_USERNAME
):
    """
    Pulls daily CRSP stock data from a specified start date to end date.
    Optionally, can filter by a list of permnos.

    Parameters:
    - start_date (str or datetime): The start date for the data pull (inclusive).
    - end_date (str or datetime): The end date for the data pull (inclusive).
    - permnos (list of int): A list of permnos to filter the data. If None, pulls all stocks.
    - wrds_username (str): The WRDS username for authentication, pulled from .env file by default

    Returns:
    - pandas.DataFrame: A DataFrame containing the pulled CRSP daily stock data.
    """

    start_date = start_date.date() if isinstance(start_date, datetime) else start_date
    end_date = end_date.date() if isinstance(end_date, datetime) else end_date
    permno_filter = f"AND PERMNO IN ({', '.join(map(str, permnos))})" if permnos else ""

    query = f"""
    SELECT
        permno,
        permco,
        ticker,
        primaryexch,
        dlycaldt,
        dlycap,
        dlyprc,
        dlyopen,
        dlyhigh,
        dlylow,
        dlyclose,
        dlyfacprc,
        dlyret,
        dlyretx
    FROM CRSPM.DSF_V2
    WHERE 
    ( 
        primaryexch IN ( 'N', 'A', 'Q' ) AND
        conditionaltype = 'RW' AND
        tradingstatusflg = 'A' AND
        dlycaldt >= '{start_date}' AND
        dlycaldt <= '{end_date}'
        {permno_filter}
    ) ; 
    """

    print("Pulling CRSP daily data from WRDS...")
    print(f"Start date: {start_date}, End date: {end_date}")
    print(f"Permnos filter: {permno_filter if permnos else 'None'}")

    db = wrds.Connection(wrds_username=wrds_username)
    df = db.raw_sql(query, date_cols=["dlycaldt"])
    
    df['daily_cum_price_adj_factor'] = df['dlyfacprc'].cumprod()
    df['dlyprc_adj'] = df['dlyprc'] * df['daily_cum_price_adj_factor']
    df['dlyopen_adj'] = df['dlyopen'] * df['daily_cum_price_adj_factor']
    df['dlyhigh_adj'] = df['dlyhigh'] * df['daily_cum_price_adj_factor']
    df['dlylow_adj'] = df['dlylow'] * df['daily_cum_price_adj_factor']
    df['dlyclose_adj'] = df['dlyclose'] * df['daily_cum_price_adj_factor']
    
    db.close()
    return df


def load_crsp_daily_file(data_dir=DATA_DIR):
    """
    Method to load the CRSP daily stock data from a parquet file.
    
    Parameters:
    - data_dir (str or Path): The directory where the CRSP daily stock parquet file is located.

    Returns:
    - pandas.DataFrame: A DataFrame containing the CRSP daily stock data loaded from the parquet file.
    """
    path = Path(data_dir) / "CRSP_stock_daily.parquet"
    df = pd.read_parquet(path)
    return df


if __name__ == "__main__":
    # hardcoding these three permnos for now (HW3), but will want to pull all stocks for replication
    crsp_df = pull_crsp_daily_file(permnos=[10107, 93436, 14593])
    crsp_path = Path(DATA_DIR) / "CRSP_stock_daily.parquet"
    crsp_df.to_parquet(crsp_path)
