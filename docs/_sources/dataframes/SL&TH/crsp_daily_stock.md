# Dataframe: `SL&TH:crsp_daily_stock` - 

This dataframe contains daily stock data from CRSP, including closing prices and other relevant fields.


## DataFrame Glimpse

```
Rows: 24536
Columns: 20
$ permno                              <i64> 93436
$ permco                              <i64> 53453
$ ticker                              <str> 'TSLA'
$ primaryexch                         <str> 'Q'
$ dlycaldt                   <datetime[ns]> 2024-12-31 00:00:00
$ dlycap                              <f64> 1298958225.28
$ dlyprc                              <f64> 403.84
$ dlyopen                             <f64> 423.79
$ dlyhigh                             <f64> 427.93
$ dlylow                              <f64> 402.54
$ dlyclose                            <f64> 403.84
$ dlyfacprc                           <f64> 1.0
$ dlyret                              <f64> -0.03251
$ dlyretx                             <f64> -0.03251
$ daily_cum_price_adj_factor          <f64> 967680.0
$ dlyprc_adj                          <f64> 390787891.2
$ dlyopen_adj                         <f64> 410093107.20000005
$ dlyhigh_adj                         <f64> 414099302.40000004
$ dlylow_adj                          <f64> 389529907.20000005
$ dlyclose_adj                        <f64> 390787891.2


```

## Dataframe Manifest

| Dataframe Name                 |                                                    |
|--------------------------------|--------------------------------------------------------------------------------------|
| Dataframe ID                   | [crsp_daily_stock](../dataframes/SL&TH/crsp_daily_stock.md)                                       |
| Data Sources                   | WRDS                                        |
| Data Providers                 | CRSP                                      |
| Links to Providers             |                              |
| Topic Tags                     |                                           |
| Type of Data Access            |                                   |
| How is data pulled?            | src/pull_CRSP_stock.py                                                    |
| Data available up to (min)     | 2024-12-31 00:00:00                                                             |
| Data available up to (max)     | 2024-12-31 00:00:00                                                             |
| Dataframe Path                 | /Users/sophielara/Library/CloudStorage/OneDrive-TheUniversityofChicago/UChicago/2025/winter/FINM 32900/hw3/p05_lopez-lira_tang_2023/chatgpt_price_forecasting/_data/CRSP_stock_daily.parquet                                                   |


**Linked Charts:**


- [SL&TH:crsp_daily_stock](../../charts/SL&TH.crsp_daily_stock.md)



## Pipeline Manifest

| Pipeline Name                   | ChatGPT Price Forecasting                       |
|---------------------------------|--------------------------------------------------------|
| Pipeline ID                     | [SL&TH](../index.md)              |
| Lead Pipeline Developer         | Tom&Sophie             |
| Contributors                    | Tom&Sophie           |
| Git Repo URL                    | github.com/thomashillenbrand/p05_lopez-lira_tang_2023                        |
| Pipeline Web Page               | <a href="file:///Users/sophielara/Library/CloudStorage/OneDrive-TheUniversityofChicago/UChicago/2025/winter/FINM 32900/hw3/p05_lopez-lira_tang_2023/chatgpt_price_forecasting/docs/index.html">Pipeline Web Page      |
| Date of Last Code Update        | 2026-02-10 17:19:47           |
| OS Compatibility                |  |
| Linked Dataframes               |  [SL&TH:ravenpack](../dataframes/SL&TH/ravenpack.md)<br>  [SL&TH:crsp_daily_stock](../dataframes/SL&TH/crsp_daily_stock.md)<br>  |


