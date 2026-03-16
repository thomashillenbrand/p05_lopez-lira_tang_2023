# Dataframe: `SLTH:table1_full_sample` - 

Replication of Table 1 using the full available sample.


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
$ Trading Days                     <i64> 1087
$ Firm-Day Observations            <f64> 176968.0


```

## Dataframe Manifest

| Dataframe Name                 |                                                    |
|--------------------------------|--------------------------------------------------------------------------------------|
| Dataframe ID                   | [table1_full_sample](../dataframes/SLTH/table1_full_sample.md)                                       |
| Data Sources                   | CRSP, RavenPack, OpenAI                                        |
| Data Providers                 | WRDS, OpenAI                                      |
| Links to Providers             | https://wrds-www.wharton.upenn.edu/                             |
| Topic Tags                     |                                           |
| Type of Data Access            |                                   |
| How is data pulled?            | src/create_table1.py                                                    |
| Data available up to (min)     | None                                                             |
| Data available up to (max)     | None                                                             |
| Dataframe Path                 | /home/tomhi/finmath/finm-32900/p05_lopez-lira_tang_2023/_output/table1_overnight_full_sample.csv                                                   |


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
| Date of Last Code Update        | 2026-03-15 21:42:08           |
| OS Compatibility                |  |
| Linked Dataframes               |  [SLTH:ravenpack](../dataframes/SLTH/ravenpack.md)<br>  [SLTH:crsp_daily_stock](../dataframes/SLTH/crsp_daily_stock.md)<br>  [SLTH:daily_headline_polarity](../dataframes/SLTH/daily_headline_polarity.md)<br>  [SLTH:portfolio_returns](../dataframes/SLTH/portfolio_returns.md)<br>  [SLTH:table1_paper_sample](../dataframes/SLTH/table1_paper_sample.md)<br>  [SLTH:table1_full_sample](../dataframes/SLTH/table1_full_sample.md)<br>  [SLTH:openai_label_proportions](../dataframes/SLTH/openai_label_proportions.md)<br>  |


