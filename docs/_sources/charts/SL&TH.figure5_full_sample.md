---
date: 2026-03-15 19:13:18
tags: CRSP, RavenPack, OpenAI
category: 
---

# Chart: Figure 5: Portfolio Performance (Full Sample)
Cumulative portfolio value for the long-short strategy and benchmark variants over the full sample.

## Chart
```{raw} html
<iframe src="../_static/SL&TH/figure5_full_sample.html" height="500px" width="100%"></iframe>

<p style="text-align: center;">Sources: CRSP, RavenPack, OpenAI</p>
```
[Full Screen Chart](../download_chart/SL&TH/figure5_full_sample.html)





Replication of Figure 5 over the full available sample.


## Chart Specs

| Chart Name             | Figure 5: Portfolio Performance (Full Sample)                                             |
|------------------------|------------------------------------------------------------|
| Chart ID               | figure5_full_sample                                               |
| Topic Tags             |                                 |
| Data Series Start Date |                                  |
| Data Frequency         |                                          |
| Observation Period     |                                      |
| Lag in Data Release    |                                     |
| Data Release Timing    |                                     |
| Seasonal Adjustment    |                                     |
| Units                  |                                                   |
| HTML Chart             | [HTML](../download_chart/SL&TH/figure5_full_sample.html)    |


## Dataframe Manifest

| Dataframe Name                 |                                                    |
|--------------------------------|--------------------------------------------------------------------------------------|
| Dataframe ID                   | [portfolio_returns](../dataframes/SL&TH/portfolio_returns.md)                                       |
| Data Sources                   | CRSP, RavenPack, OpenAI                                        |
| Data Providers                 | WRDS, OpenAI                                      |
| Links to Providers             |                              |
| Topic Tags                     |                                           |
| Type of Data Access            |                                   |
| How is data pulled?            | src/create_portfolios.py                                                    |
| Data available up to (min)     |                                                              |
| Data available up to (max)     |                                                              |
| Dataframe Path                 | /home/tomhi/finmath/finm-32900/p05_lopez-lira_tang_2023/_data/portfolio_daily_returns.parquet                                                   |


**Linked Charts:**


- [SL&TH:figure5_paper_sample](../../charts/SL&TH.figure5_paper_sample.md)

- [SL&TH:figure5_full_sample](../../charts/SL&TH.figure5_full_sample.md)

- [SL&TH:portfolio_size_diagnostics](../../charts/SL&TH.portfolio_size_diagnostics.md)



## Pipeline Manifest

| Pipeline Name                   | ChatGPT Price Forecasting                       |
|---------------------------------|--------------------------------------------------------|
| Pipeline ID                     | [SL&TH](../index.md)              |
| Lead Pipeline Developer         | Tom&Sophie             |
| Contributors                    | Tom&Sophie           |
| Git Repo URL                    | github.com/thomashillenbrand/p05_lopez-lira_tang_2023                        |
| Pipeline Web Page               | <a href="file:///home/tomhi/finmath/finm-32900/p05_lopez-lira_tang_2023/docs/index.html">Pipeline Web Page      |
| Date of Last Code Update        | 2026-03-15 19:13:18           |
| OS Compatibility                |  |
| Linked Dataframes               |  [SL&TH:ravenpack](../dataframes/SL&TH/ravenpack.md)<br>  [SL&TH:crsp_daily_stock](../dataframes/SL&TH/crsp_daily_stock.md)<br>  [SL&TH:daily_headline_polarity](../dataframes/SL&TH/daily_headline_polarity.md)<br>  [SL&TH:portfolio_returns](../dataframes/SL&TH/portfolio_returns.md)<br>  [SL&TH:table1_paper_sample](../dataframes/SL&TH/table1_paper_sample.md)<br>  [SL&TH:table1_full_sample](../dataframes/SL&TH/table1_full_sample.md)<br>  [SL&TH:openai_label_proportions](../dataframes/SL&TH/openai_label_proportions.md)<br>  |

