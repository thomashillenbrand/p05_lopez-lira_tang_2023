from pathlib import Path

import pandas as pd
import plotly.express as px
from pull_CRSP_stock import load_crsp_daily_file
from settings import config

DATA_DIR = Path(config("DATA_DIR"))
OUTPUT_DIR = Path(config("OUTPUT_DIR"))


def plot_crsp_data(data_dir=DATA_DIR, price_column="dlyclose_adj", output_file="crsp_plot.html"):
    """
    Loads CRSP daily stock data from a parquet file, plots the closing prices grouped by PERMNO using Plotly,
    and saves the plot to an HTML file.

    Parameters:
    - data_dir (str or Path): The directory where the CRSP daily stock parquet file is located.
    - price_column (str): The column name for the price data to plot.
    - output_file (str): The name of the HTML file to save the plot.

    Returns:
    - None: Saves the plot to an HTML file.
    """
    try:
        df = load_crsp_daily_file(data_dir)

        if "dlycaldt" not in df.columns or price_column not in df.columns or "permno" not in df.columns:
            raise ValueError(f"The required columns 'dlycaldt', '{price_column}', and 'permno' are missing from the data.")

        df["dlycaldt"] = pd.to_datetime(df["dlycaldt"], errors="coerce")
        df = df.dropna(subset=["dlycaldt", price_column])
        df = df.sort_values(by="dlycaldt")

        # Create the plot using Plotly
        fig = px.line(
            df,
            x="dlycaldt",
            y=price_column,
            color="ticker",
            title="CRSP Daily Closing Prices",
            labels={"dlycaldt": "Date", price_column: "Closing Price", "ticker": "TICKER"}
        )

        fig.write_html(output_file, include_plotlyjs="cdn")
        print(f"Plot saved to {output_file}")

    except FileNotFoundError:
        print(f"The file 'CRSP_stock_daily.parquet' was not found in the directory: {data_dir}")
    except Exception as e:
        print(f"An error occurred: {e}")


if __name__ == "__main__":
    print("Loading CRSP daily stock data and plotting closing prices...")
    plot_crsp_data(data_dir=DATA_DIR, price_column="dlyclose_adj", output_file=OUTPUT_DIR / "crsp_daily_closing_prices.html")
