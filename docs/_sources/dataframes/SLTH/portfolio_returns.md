# Dataframe: `SLTH:portfolio_returns` - 

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
| Dataframe ID                   | [portfolio_returns](../dataframes/SLTH/portfolio_returns.md)                                       |
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
| Date of Last Code Update        | 2026-03-15 20:50:48           |
| OS Compatibility                |  |
| Linked Dataframes               |  [SLTH:ravenpack](../dataframes/SLTH/ravenpack.md)<br>  [SLTH:crsp_daily_stock](../dataframes/SLTH/crsp_daily_stock.md)<br>  [SLTH:daily_headline_polarity](../dataframes/SLTH/daily_headline_polarity.md)<br>  [SLTH:portfolio_returns](../dataframes/SLTH/portfolio_returns.md)<br>  [SLTH:table1_paper_sample](../dataframes/SLTH/table1_paper_sample.md)<br>  [SLTH:table1_full_sample](../dataframes/SLTH/table1_full_sample.md)<br>  [SLTH:openai_label_proportions](../dataframes/SLTH/openai_label_proportions.md)<br>  |


