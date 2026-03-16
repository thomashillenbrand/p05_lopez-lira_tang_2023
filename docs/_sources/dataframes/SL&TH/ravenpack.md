# Dataframe: `SL&TH:ravenpack` - 

Dataframe showing Ravenpack data split out as intraday or overnight news


## DataFrame Glimpse

```
Rows: 1143333
Columns: 6
$ rp_entity_id           <str> 'WZXP28'
$ rpa_date_utc  <datetime[ns]> 2023-03-13 00:00:00
$ timestamp_utc <datetime[ns]> 2023-03-13 12:30:07.068000
$ map_ticker             <str> 'CRGO'
$ entity_name            <str> 'Freightos Ltd.'
$ headline               <str> 'Freightos Announces Record Fiscal Year 2022 Results'


```

## Dataframe Manifest

| Dataframe Name                 |                                                    |
|--------------------------------|--------------------------------------------------------------------------------------|
| Dataframe ID                   | [ravenpack](../dataframes/SL&TH/ravenpack.md)                                       |
| Data Sources                   | WRDS                                        |
| Data Providers                 | RavenPack                                      |
| Links to Providers             |                              |
| Topic Tags                     |                                           |
| Type of Data Access            |                                   |
| How is data pulled?            | src/pull_ravenpack.py                                                    |
| Data available up to (min)     | None                                                             |
| Data available up to (max)     | None                                                             |
| Dataframe Path                 | /home/tomhi/finmath/finm-32900/p05_lopez-lira_tang_2023/_data/RAVENPACK.parquet                                                   |


**Linked Charts:**


- [SL&TH:ravenpack_news_timing](../../charts/SL&TH.ravenpack_news_timing.md)



## Pipeline Manifest

| Pipeline Name                   | ChatGPT Price Forecasting                       |
|---------------------------------|--------------------------------------------------------|
| Pipeline ID                     | [SL&TH](../index.md)              |
| Lead Pipeline Developer         | Tom&Sophie             |
| Contributors                    | Tom&Sophie           |
| Git Repo URL                    | github.com/thomashillenbrand/p05_lopez-lira_tang_2023                        |
| Pipeline Web Page               | <a href="file:///home/tomhi/finmath/finm-32900/p05_lopez-lira_tang_2023/docs/index.html">Pipeline Web Page      |
| Date of Last Code Update        | 2026-03-15 19:32:07           |
| OS Compatibility                |  |
| Linked Dataframes               |  [SL&TH:ravenpack](../dataframes/SL&TH/ravenpack.md)<br>  [SL&TH:crsp_daily_stock](../dataframes/SL&TH/crsp_daily_stock.md)<br>  [SL&TH:daily_headline_polarity](../dataframes/SL&TH/daily_headline_polarity.md)<br>  [SL&TH:portfolio_returns](../dataframes/SL&TH/portfolio_returns.md)<br>  [SL&TH:table1_paper_sample](../dataframes/SL&TH/table1_paper_sample.md)<br>  [SL&TH:table1_full_sample](../dataframes/SL&TH/table1_full_sample.md)<br>  [SL&TH:openai_label_proportions](../dataframes/SL&TH/openai_label_proportions.md)<br>  |


