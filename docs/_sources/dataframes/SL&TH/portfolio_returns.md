# Dataframe: `SL&TH:portfolio_returns` - 

Daily returns of headline-based trading portfolios constructed from RavenPack sentiment signals.


## DataFrame Glimpse

```
Rows: 1087
Columns: 20
$ date              <date> 2026-01-30
$ n_neg              <i64> 10
$ n_neu              <i64> 119
$ n_pos              <i64> 57
$ n_total            <i64> 186
$ ret_long           <f64> -0.010953181157434494
$ n_long             <i64> 56
$ n_short            <i64> 8
$ ret_short          <f64> 0.007552368516700658
$ ret_ir_long        <f64> -0.005508344181657134
$ ret_ir_short       <f64> 0.02189939219278135
$ ret_ls_restricted  <f64> -0.004339941519376619
$ ret_ls_not_small   <f64> -0.006024064773692914
$ ret_ls_price_gt_5  <f64> -0.0005269772387315533
$ ret_mkt_vw         <f64> -0.0030984310276356876
$ trade_long        <bool> True
$ trade_short       <bool> True
$ trade_ls          <bool> True
$ ret_ls             <f64> -0.0034008126407338354
$ ret_ir_ls          <f64> 0.016391048011124218


```

## Dataframe Manifest

| Dataframe Name                 |                                                    |
|--------------------------------|--------------------------------------------------------------------------------------|
| Dataframe ID                   | [portfolio_returns](../dataframes/SL&TH/portfolio_returns.md)                                       |
| Data Sources                   | CRSP, RavenPack, OpenAI                                        |
| Data Providers                 | WRDS, OpenAI                                      |
| Links to Providers             | https://wrds-www.wharton.upenn.edu/                             |
| Topic Tags                     |                                           |
| Type of Data Access            |                                   |
| How is data pulled?            | src/create_portfolios.py                                                    |
| Data available up to (min)     | 2026-01-30 00:00:00                                                             |
| Data available up to (max)     | 2026-01-30 00:00:00                                                             |
| Dataframe Path                 | /home/tomhi/finmath/finm-32900/p05_lopez-lira_tang_2023/_data/portfolio_daily_returns.parquet                                                   |


**Linked Charts:**


- [SL&TH:figure5_paper_sample](../../charts/SL&TH.figure5_paper_sample.md)

- [SL&TH:figure5_full_sample](../../charts/SL&TH.figure5_full_sample.md)

- [SL&TH:portfolio_size_diagnostics](../../charts/SL&TH.portfolio_size_diagnostics.md)



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


