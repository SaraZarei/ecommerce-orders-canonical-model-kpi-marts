-- 6) mart_kpi_freshness_lag (optional)
-- Grain: ingest_date × source_system
-- ------------------------------------------------------------
/*
This table is a pipeline freshness + lateness monitoring mart. It tells you how “on time” your data arrives and whether you’re loading old orders late.
Grain: One row per ingest_date × source_system
So you can answer: “For each day we ingested data from Amazon/Shopify, how fresh was it?”
*/
create or replace table `analytics_500550161.mart_kpi_freshness_lag`
partition by ingest_date
cluster by source_system
as
select
  ingest_date,
  source_system,
  -- Metrics it calculates
  count(*) as rows_loaded, --Volume of the load. total rows ingested that day (sanity check)
  count(distinct global_order_id) as orders_loaded,-- Volume of the load. distinct orders ingested that day

  avg(date_diff(ingest_date, order_date, day)) as avg_lag_days,/*Lag between business event and ingestion.Example:
  if order_date=2025-12-28 and ingest_date=2025-12-30, lag = 2 days.*/
  max(date_diff(ingest_date, order_date, day)) as max_lag_days,-- worst case lag in that ingested batch

  countif(date_diff(ingest_date, order_date, day) > 2) as late_orders_gt_2d,-- Late arrivals.how many rows have lag > 2 days
  safe_divide(countif(date_diff(ingest_date, order_date, day) > 2), nullif(count(*), 0)) as late_rate_gt_2d --Late arrivals.percentage of late rows

from `analytics_500550161.mart_fact_orders`
where ingest_date is not null and order_date is not null -- Because lag only makes sense when you have both dates.
group by 1,2;

/*
Why you need it
1) Detect broken or delayed pipelines early
If yesterday you normally have avg_lag_days ~ 0–1 but today it jumps to 5+, you know:
ingestion failed for multiple days and then backfilled
API/report availability changed
scheduling issues happened
2) Trust & SLA monitoring
It helps you answer:
“How fresh is our reporting data?”
“Can the business trust today’s dashboard?”
3) Debugging
If you see a dashboard mismatch (“why revenue is low today?”), this mart can quickly show:
maybe you didn’t ingest today’s orders yet
or you are ingesting mostly older orders
4) Backfill visibility
Late-arrival rate spikes usually indicate backfills/reprocessing (not necessarily bad, but should be visible).
In short: it’s not for business KPIs — it’s for data reliability and timeliness monitoring.
*/