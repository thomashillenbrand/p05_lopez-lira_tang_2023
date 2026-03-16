# Dataframe: `SL&TH:openai_label_proportions` - 

Distribution of OpenAI headline labels across the paper sample and the full sample.


## DataFrame Glimpse

```
Rows: 3
Columns: 5
$ label                              <str> 'UNKNOWN'
$ proportion_2021_2024_sample_period <f64> 0.6719238780388449
$ proportion_full_sample             <f64> 0.6654236803204425
$ count_2021_2024_sample_period      <i64> 87422
$ count_full_sample                  <i64> 139546


```

## Dataframe Manifest

| Dataframe Name                 |                                                    |
|--------------------------------|--------------------------------------------------------------------------------------|
| Dataframe ID                   | [openai_label_proportions](../dataframes/SL&TH/openai_label_proportions.md)                                       |
| Data Sources                   | RavenPack, OpenAI                                        |
| Data Providers                 | RavenPack, OpenAI                                      |
| Links to Providers             | https://wrds-www.wharton.upenn.edu/                             |
| Topic Tags                     |                                           |
| Type of Data Access            |                                   |
| How is data pulled?            | src/create_openai_responses_table.py                                                    |
| Data available up to (min)     | None                                                             |
| Data available up to (max)     | None                                                             |
| Dataframe Path                 | /home/tomhi/finmath/finm-32900/p05_lopez-lira_tang_2023/_output/openai_output_label_proportions.csv                                                   |


**Linked Charts:**

- None


## Pipeline Manifest

| Pipeline Name                   | ChatGPT Price Forecasting                       |
|---------------------------------|--------------------------------------------------------|
| Pipeline ID                     | [SL&TH](../index.md)              |
| Lead Pipeline Developer         | Tom & Sophie             |
| Contributors                    | Sophie Lara & Tom Hillenbrand           |
| Git Repo URL                    | github.com/thomashillenbrand/p05_lopez-lira_tang_2023                        |
| Pipeline Web Page               | <a href="file:///home/tomhi/finmath/finm-32900/p05_lopez-lira_tang_2023/docs/index.html">Pipeline Web Page      |
| Date of Last Code Update        | 2026-03-15 20:19:58           |
| OS Compatibility                |  |
| Linked Dataframes               |  [SL&TH:ravenpack](../dataframes/SL&TH/ravenpack.md)<br>  [SL&TH:crsp_daily_stock](../dataframes/SL&TH/crsp_daily_stock.md)<br>  [SL&TH:daily_headline_polarity](../dataframes/SL&TH/daily_headline_polarity.md)<br>  [SL&TH:portfolio_returns](../dataframes/SL&TH/portfolio_returns.md)<br>  [SL&TH:table1_paper_sample](../dataframes/SL&TH/table1_paper_sample.md)<br>  [SL&TH:table1_full_sample](../dataframes/SL&TH/table1_full_sample.md)<br>  [SL&TH:openai_label_proportions](../dataframes/SL&TH/openai_label_proportions.md)<br>  |


