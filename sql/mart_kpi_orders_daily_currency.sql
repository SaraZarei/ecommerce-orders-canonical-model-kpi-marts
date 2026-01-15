-- 2) mart_kpi_orders_daily_currency
-- Grain: order_date × source_system × currency_canon
-- ------------------------------------------------------------
/*
This query builds a daily KPI summary table for orders, separated by currency (and by source). It turns your row-level fact table into an aggregated dataset that dashboards can read fast and consistently.
*/
create or replace table `analytics_500550161.mart_kpi_orders_daily_currency` --Creates an aggregated KPI mart.It creates a new table that contains one row per day × source_system × currency
partition by order_date --Partition by order_date: queries like “last 30 days” scan less data → cheaper/faster.
cluster by source_system, currency_canon --Cluster by source_system, currency_canon: speeds up filters/grouping by source/currency.
as
select
  order_date,
  source_system,
  currency_canon,

  count(*) as rows_in_fact, -- total rows contributing (good for sanity checks)
  count(distinct global_order_id) as orders_total,-- distinct orders that day
  count(distinct global_customer_id) as customers_total,-- distinct customers that day

  countif(is_revenue_eligible) as orders_revenue_eligible,--counts orders that are not test/cancelled/refund
  sum(revenue_amount) as revenue,--(already 0 for excluded orders)

  safe_divide(sum(revenue_amount), nullif(countif(is_revenue_eligible), 0)) as aov,--average order value = revenue / eligible orders.to avoid division-by-zero errors

  countif(coalesce(is_cancelled,false)) as cancelled_orders,
  countif(coalesce(is_refund_or_return,false)) as refunded_or_returned_orders,

  safe_divide(countif(coalesce(dq_has_issue,false)), nullif(count(*), 0)) as dq_issue_rate --Data quality monitoring.% of rows with DQ issues that day

from `analytics_500550161.mart_fact_orders`
group by 1,2,3; --Groups the data by day/source/currency.So each row is a daily KPI slice, like:2025-12-29, amazon, EUR, 2025-12-29, shopify, USD

/*
Why you need it
1) Multi-currency correctness
You cannot safely sum revenue across currencies. This table forces revenue to be reported per currency (unless you later do FX conversion).
2) Faster and cheaper dashboards
BI tools querying raw facts do expensive scans repeatedly. This KPI mart is pre-aggregated, so dashboards load quickly.
3) Consistent KPI definitions everywhere
It uses is_revenue_eligible and revenue_amount from your fact table, so every report follows the same business rules.
4) Monitoring + alerting
rows_in_fact, dq_issue_rate, cancelled/refund counts help you notice:
broken loads (sudden drop to zero)
spikes in cancellations/refunds
pipeline quality problems
*/
