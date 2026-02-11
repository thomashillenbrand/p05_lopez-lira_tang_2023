# Dataframe: `SL&TH:crsp_daily_stock` - 

This dataframe contains daily stock data from CRSP, including closing prices and other relevant fields.


## DataFrame Glimpse

```
Rows: 2010
Columns: 20
$ permno                              <i64> 93436
$ permco                              <i64> 53453
$ ticker                              <str> 'TSLA'
$ primaryexch                         <str> 'Q'
$ dlycaldt                   <datetime[ns]> 2024-05-31 00:00:00
$ dlycap                              <f64> 567932023.68
$ dlyprc                              <f64> 178.08
$ dlyopen                             <f64> 178.5
$ dlyhigh                             <f64> 180.32
$ dlylow                              <f64> 173.82
$ dlyclose                            <f64> 178.08
$ dlyfacprc                           <f64> 1.0
$ dlyret                              <f64> -0.003971
$ dlyretx                             <f64> -0.003971
$ daily_cum_price_adj_factor          <f64> 3.0
$ dlyprc_adj                          <f64> 534.24
$ dlyopen_adj                         <f64> 535.5
$ dlyhigh_adj                         <f64> 540.96
$ dlylow_adj                          <f64> 521.46
$ dlyclose_adj                        <f64> 534.24


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
| Data available up to (min)     | 2024-05-31 00:00:00                                                             |
| Data available up to (max)     | 2024-05-31 00:00:00                                                             |
| Dataframe Path                 | /home/tomhillenbrand/finmath/finm-32900/p05_lopez-lira_tang_2023/_data/CRSP_stock_daily.parquet                                                   |


**Linked Charts:**


- [SL&TH:crsp_daily_stock](../../charts/SL&TH.crsp_daily_stock.md)



## Pipeline Manifest

| Pipeline Name                   | ChatGPT Price Forecasting                       |
|---------------------------------|--------------------------------------------------------|
| Pipeline ID                     | [SL&TH](../index.md)              |
| Lead Pipeline Developer         | Tom&Sophie             |
| Contributors                    | Tom&Sophie           |
| Git Repo URL                    | github.com/thomashillenbrand/p05_lopez-lira_tang_2023                        |
| Pipeline Web Page               | <a href="file:///home/tomhillenbrand/finmath/finm-32900/p05_lopez-lira_tang_2023/docs/index.html">Pipeline Web Page      |
| Date of Last Code Update        | 2026-02-10 19:39:21           |
| OS Compatibility                |  |
| Linked Dataframes               |  [SL&TH:ravenpack](../dataframes/SL&TH/ravenpack.md)<br>  [SL&TH:crsp_daily_stock](../dataframes/SL&TH/crsp_daily_stock.md)<br>  |


