# Dataframe: `SL&TH:table1_paper_sample` - 

Replication of Table 1 using the paper sample period from 2021-10-01 to 2024-05-31.


## DataFrame Glimpse

```
Rows: 4
Columns: 8
$ Portfolio                        <str> 'Sample Summary'
$ Initial Reaction Hit Rate (%)    <f64> null
$ Initial Reaction Mean Return (%) <f64> null
$ Drift Hit Rate (%)               <f64> null
$ Drift Mean Return (%)            <f64> null
$ Drift Sharpe Ratio               <f64> null
$ Trading Days                     <i64> 670
$ Firm-Day Observations            <f64> 112092.0


```

## Dataframe Manifest

| Dataframe Name                 |                                                    |
|--------------------------------|--------------------------------------------------------------------------------------|
| Dataframe ID                   | [table1_paper_sample](../dataframes/SL&TH/table1_paper_sample.md)                                       |
| Data Sources                   | CRSP, RavenPack, OpenAI                                        |
| Data Providers                 | WRDS, OpenAI                                      |
| Links to Providers             | https://wrds-www.wharton.upenn.edu/                             |
| Topic Tags                     |                                           |
| Type of Data Access            |                                   |
| How is data pulled?            | src/create_table1.py                                                    |
| Data available up to (min)     | None                                                             |
| Data available up to (max)     | None                                                             |
| Dataframe Path                 | /home/tomhi/finmath/finm-32900/p05_lopez-lira_tang_2023/_output/table1_overnight_paper_sample.csv                                                   |


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
| Date of Last Code Update        | 2026-03-15 19:50:22           |
| OS Compatibility                |  |
| Linked Dataframes               |  [SL&TH:ravenpack](../dataframes/SL&TH/ravenpack.md)<br>  [SL&TH:crsp_daily_stock](../dataframes/SL&TH/crsp_daily_stock.md)<br>  [SL&TH:daily_headline_polarity](../dataframes/SL&TH/daily_headline_polarity.md)<br>  [SL&TH:portfolio_returns](../dataframes/SL&TH/portfolio_returns.md)<br>  [SL&TH:table1_paper_sample](../dataframes/SL&TH/table1_paper_sample.md)<br>  [SL&TH:table1_full_sample](../dataframes/SL&TH/table1_full_sample.md)<br>  [SL&TH:openai_label_proportions](../dataframes/SL&TH/openai_label_proportions.md)<br>  |


