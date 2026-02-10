# Dataframe: `SL&TH:ravenpack` - 

Dataframe showing Ravenpack data split out as intraday or overnight news


## DataFrame Glimpse

```
Rows: 1738788
Columns: 59
$ rpa_date_utc                      <str> '2023-02-01'
$ rpa_time_utc                      <str> '11:15:38'
$ timestamp_utc            <datetime[ns]> 2023-02-01 11:15:37.880000
$ rp_story_id                       <str> 'E56C0D7C056807A4A56FF60635B82CB7'
$ rp_entity_id                      <str> '000127'
$ entity_type                       <str> 'COMP'
$ entity_name                       <str> 'PT Bank Syariah Indonesia Tbk'
$ country_code                      <str> 'ID'
$ relevance                         <f64> 100.0
$ event_sentiment_score             <f64> 0.29
$ event_relevance                   <f64> 100.0
$ event_similarity_key              <str> '3FF9DCE990FBDE92AD5BAE9B4EA6D594'
$ event_similarity_days             <f64> 365.0
$ topic                             <str> 'business'
$ group                             <str> 'credit-ratings'
$ type                              <str> 'credit-rating-change'
$ sub_type                          <str> 'affirmation'
$ property                          <str> null
$ fact_level                        <str> 'fact'
$ rp_position_id                    <str> null
$ position_name                     <str> null
$ evaluation_method                 <str> null
$ maturity                          <str> null
$ earnings_type                     <str> null
$ event_start_date_utc     <datetime[ns]> null
$ event_end_date_utc       <datetime[ns]> null
$ reporting_period                  <str> null
$ reporting_start_date_utc <datetime[ns]> null
$ reporting_end_date_utc   <datetime[ns]> null
$ related_entity                    <str> null
$ relationship                      <str> null
$ category                          <str> 'credit-rating-affirmation'
$ event_text                        <str> "Fitch Affirms Bank Syariah Indonesia at 'AA"
$ news_type                         <str> 'PRESS-RELEASE'
$ rp_source_id                      <str> 'B5569E'
$ source_name                       <str> 'Dow Jones Newswires'
$ css                               <f64> 0.04
$ nip                               <f64> -0.4
$ peq                               <f64> 0.0
$ bee                               <f64> 0.0
$ bmq                               <f64> 1.0
$ bam                               <f64> 0.0
$ bca                               <f64> 0.0
$ ber                               <f64> 0.0
$ anl_chg                           <f64> 0.0
$ mcq                               <f64> 1.0
$ rp_story_event_index              <f64> 1.0
$ rp_story_event_count              <f64> 13.0
$ product_key                       <str> 'RPA'
$ provider_id                       <str> 'DJ'
$ provider_story_id                 <str> 'DN20230201003325'
$ headline                          <str> "Press Release: Fitch Affirms Bank Syariah Indonesia at 'AA(idn)'; Outlook Stable"
$ map_entity_type                   <str> 'COMP'
$ map_entity_name                   <str> 'PT Bank Syariah Indonesia Tbk'
$ map_ticker                        <str> 'BRIS.JK'
$ map_cusip                         <str> 'Y0R8KR105'
$ map_isin                          <str> 'ID1000142904'
$ rn                                <i64> 1
$ __index_level_0__                 <i64> 0


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
| Data available up to (min)     | N/A (large file)                                                             |
| Data available up to (max)     | N/A (large file)                                                             |
| Dataframe Path                 | /Users/sophielara/Library/CloudStorage/OneDrive-TheUniversityofChicago/UChicago/2025/winter/FINM 32900/hw3/p05_lopez-lira_tang_2023/chatgpt_price_forecasting/_data/RAVENPACK.parquet                                                   |


**Linked Charts:**


- [SL&TH:ravenpack_news_timing](../../charts/SL&TH.ravenpack_news_timing.md)



## Pipeline Manifest

| Pipeline Name                   | ChatGPT Price Forecasting                       |
|---------------------------------|--------------------------------------------------------|
| Pipeline ID                     | [SL&TH](../index.md)              |
| Lead Pipeline Developer         | Tom&Sophie             |
| Contributors                    | Tom&Sophie           |
| Git Repo URL                    | github.com/thomashillenbrand/p05_lopez-lira_tang_2023                        |
| Pipeline Web Page               | <a href="file:///Users/sophielara/Library/CloudStorage/OneDrive-TheUniversityofChicago/UChicago/2025/winter/FINM 32900/hw3/p05_lopez-lira_tang_2023/chatgpt_price_forecasting/docs/index.html">Pipeline Web Page      |
| Date of Last Code Update        | 2026-02-10 17:19:47           |
| OS Compatibility                |  |
| Linked Dataframes               |  [SL&TH:ravenpack](../dataframes/SL&TH/ravenpack.md)<br>  [SL&TH:crsp_daily_stock](../dataframes/SL&TH/crsp_daily_stock.md)<br>  |


