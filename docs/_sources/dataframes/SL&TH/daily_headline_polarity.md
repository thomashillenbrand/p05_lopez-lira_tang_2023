# Dataframe: `SL&TH:daily_headline_polarity` - 

Firm-day sentiment scores derived from OpenAI classifications of RavenPack headlines.


## DataFrame Glimpse

```
Rows: 176968
Columns: 5
$ ticker       <str> 'ZYME'
$ date        <date> 2026-01-06
$ n_headlines  <i64> 1
$ score_sum    <i64> 0
$ score        <i64> 0


```

## Dataframe Manifest

| Dataframe Name                 |                                                    |
|--------------------------------|--------------------------------------------------------------------------------------|
| Dataframe ID                   | [daily_headline_polarity](../dataframes/SL&TH/daily_headline_polarity.md)                                       |
| Data Sources                   | RavenPack, OpenAI                                        |
| Data Providers                 | RavenPack, OpenAI                                      |
| Links to Providers             |                              |
| Topic Tags                     |                                           |
| Type of Data Access            |                                   |
| How is data pulled?            | src/create_firmday_score.py                                                    |
| Data available up to (min)     | 2026-01-30 00:00:00                                                             |
| Data available up to (max)     | 2026-01-30 00:00:00                                                             |
| Dataframe Path                 | /home/tomhi/finmath/finm-32900/p05_lopez-lira_tang_2023/_data/daily_headline_polarity.parquet                                                   |


**Linked Charts:**

- None


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


