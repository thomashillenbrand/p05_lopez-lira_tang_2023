---
date: 2026-03-15 21:42:08
tags: CRSP, RavenPack, OpenAI
category: 
---

# Chart: Portfolio Size Diagnostics
Number of stocks in the long and short portfolios through time.

## Chart
```{raw} html
<iframe src="../_static/SLTH/portfolio_size_diagnostics.html" height="500px" width="100%"></iframe>

<p style="text-align: center;">Sources: CRSP, RavenPack, OpenAI</p>
```
[Full Screen Chart](../download_chart/SLTH/portfolio_size_diagnostics.html)





Daily long and short portfolio counts for the main strategy and restricted variants.


## Chart Specs

| Chart Name             | Portfolio Size Diagnostics                                             |
|------------------------|------------------------------------------------------------|
| Chart ID               | portfolio_size_diagnostics                                               |
| Topic Tags             |                                 |
| Data Series Start Date |                                  |
| Data Frequency         |                                          |
| Observation Period     |                                      |
| Lag in Data Release    |                                     |
| Data Release Timing    |                                     |
| Seasonal Adjustment    |                                     |
| Units                  |                                                   |
| HTML Chart             | [HTML](../download_chart/SLTH/portfolio_size_diagnostics.html)    |


## Dataframe Manifest

| Dataframe Name                 |                                                    |
|--------------------------------|--------------------------------------------------------------------------------------|
| Dataframe ID                   | [portfolio_returns](../dataframes/SLTH/portfolio_returns.md)                                       |
| Data Sources                   | CRSP, RavenPack, OpenAI                                        |
| Data Providers                 | WRDS, OpenAI                                      |
| Links to Providers             | https://wrds-www.wharton.upenn.edu/                             |
| Topic Tags                     |                                           |
| Type of Data Access            |                                   |
| How is data pulled?            | src/create_portfolios.py                                                    |
| Data available up to (min)     |                                                              |
| Data available up to (max)     |                                                              |
| Dataframe Path                 | /home/tomhi/finmath/finm-32900/p05_lopez-lira_tang_2023/_data/portfolio_daily_returns.parquet                                                   |


**Linked Charts:**


- [SLTH:figure5_paper_sample](../../charts/SLTH.figure5_paper_sample.md)

- [SLTH:figure5_full_sample](../../charts/SLTH.figure5_full_sample.md)

- [SLTH:portfolio_size_diagnostics](../../charts/SLTH.portfolio_size_diagnostics.md)



## Pipeline Manifest

| Pipeline Name                   | ChatGPT Price Forecasting                       |
|---------------------------------|--------------------------------------------------------|
| Pipeline ID                     | [SLTH](../index.md)              |
| Lead Pipeline Developer         | Tom and Sophie             |
| Contributors                    | Sophie Lara and Tom Hillenbrand           |
| Git Repo URL                    | github.com/thomashillenbrand/p05_lopez-lira_tang_2023                        |
| Pipeline Web Page               | <a href="file:///home/tomhi/finmath/finm-32900/p05_lopez-lira_tang_2023/docs/index.html">Pipeline Web Page      |
| Date of Last Code Update        | 2026-03-15 21:42:08           |
| OS Compatibility                |  |
| Linked Dataframes               |  [SLTH:ravenpack](../dataframes/SLTH/ravenpack.md)<br>  [SLTH:crsp_daily_stock](../dataframes/SLTH/crsp_daily_stock.md)<br>  [SLTH:daily_headline_polarity](../dataframes/SLTH/daily_headline_polarity.md)<br>  [SLTH:portfolio_returns](../dataframes/SLTH/portfolio_returns.md)<br>  [SLTH:table1_paper_sample](../dataframes/SLTH/table1_paper_sample.md)<br>  [SLTH:table1_full_sample](../dataframes/SLTH/table1_full_sample.md)<br>  [SLTH:openai_label_proportions](../dataframes/SLTH/openai_label_proportions.md)<br>  |

