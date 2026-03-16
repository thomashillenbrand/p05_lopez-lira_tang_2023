# Dataframe: `SLTH:crsp_daily_stock` - 

This dataframe contains daily stock data from CRSP, including closing prices and other relevant fields.


## DataFrame Glimpse

```
Rows: 7467730
Columns: 8
$ permno               <i64> 10026
$ permco               <i64> 7976
$ ticker               <str> 'JJSF'
$ primaryexch          <str> 'Q'
$ date        <datetime[ns]> 2021-10-01 00:00:00
$ dlycap               <f64> 2932065.76
$ dlyopen              <f64> 153.85
$ dlyclose             <f64> 153.64


```

## Dataframe Manifest

| Dataframe Name                 |                                                    |
|--------------------------------|--------------------------------------------------------------------------------------|
| Dataframe ID                   | [crsp_daily_stock](../dataframes/SLTH/crsp_daily_stock.md)                                       |
| Data Sources                   | WRDS                                        |
| Data Providers                 | CRSP                                      |
| Links to Providers             | https://wrds-www.wharton.upenn.edu/                             |
| Topic Tags                     |                                           |
| Type of Data Access            |                                   |
| How is data pulled?            | src/pull_CRSP_stock.py                                                    |
| Data available up to (min)     | N/A (large file)                                                             |
| Data available up to (max)     | N/A (large file)                                                             |
| Dataframe Path                 | /home/tomhi/finmath/finm-32900/p05_lopez-lira_tang_2023/_data/CRSP_stock_daily.parquet                                                   |


**Linked Charts:**

- None


## Pipeline Manifest

| Pipeline Name                   | ChatGPT Price Forecasting                       |
|---------------------------------|--------------------------------------------------------|
| Pipeline ID                     | [SLTH](../index.md)              |
| Lead Pipeline Developer         | Tom and Sophie             |
| Contributors                    | Sophie Lara and Tom Hillenbrand           |
| Git Repo URL                    | github.com/thomashillenbrand/p05_lopez-lira_tang_2023                        |
| Pipeline Web Page               | <a href="file:///home/tomhi/finmath/finm-32900/p05_lopez-lira_tang_2023/docs/index.html">Pipeline Web Page      |
| Date of Last Code Update        | 2026-03-15 20:29:03           |
| OS Compatibility                |  |
| Linked Dataframes               |  [SLTH:ravenpack](../dataframes/SLTH/ravenpack.md)<br>  [SLTH:crsp_daily_stock](../dataframes/SLTH/crsp_daily_stock.md)<br>  [SLTH:daily_headline_polarity](../dataframes/SLTH/daily_headline_polarity.md)<br>  [SLTH:portfolio_returns](../dataframes/SLTH/portfolio_returns.md)<br>  [SLTH:table1_paper_sample](../dataframes/SLTH/table1_paper_sample.md)<br>  [SLTH:table1_full_sample](../dataframes/SLTH/table1_full_sample.md)<br>  [SLTH:openai_label_proportions](../dataframes/SLTH/openai_label_proportions.md)<br>  |


