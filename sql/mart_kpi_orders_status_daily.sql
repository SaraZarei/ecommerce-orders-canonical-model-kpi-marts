-- 4) mart_kpi_orders_status_daily
-- Grain: order_date × source_system × order_status_canon
-- ------------------------------------------------------------
-- This builds a daily KPI table broken down by order status (per source). It’s mainly for operational monitoring and “funnel-ish” views (how orders move through statuses).
/*
Creates an aggregated status KPI mart.Each row represents:one day × one source_system × one canonical order status
Grain:
order_date × source_system × order_status_canon
*/
create or replace table `analytics_500550161.mart_kpi_orders_status_daily`
partition by order_date
cluster by source_system, order_status_canon
as
select
  order_date,
  source_system,
  order_status_canon,
  -- Computes status-level metrics
  count(distinct global_order_id) as orders,-- how many distinct orders were in that status (for that day)
  count(distinct global_customer_id) as customers, -- distinct customers associated with those orders

  -- revenue uses revenue_amount, so cancelled/refund/test already contribute 0
  sum(revenue_amount) as revenue_eligible_only

from `analytics_500550161.mart_fact_orders`
group by 1,2,3;

/*
Why you need it
1) Operational monitoring
It answers questions like:
“Are cancellations spiking today?”
“Why are so many orders stuck in PENDING?”
“How many orders are SHIPPED vs UNSHIPPED per day?”
This is often used by Ops/Customer Support/Supply chain teams.
2) Early warning signals
If suddenly:
PENDING jumps,
SHIPPED drops,
CANCELLED spikes,
it’s a strong signal something changed (fulfillment issues, payment issues, ingestion issues).
3) Easier dashboards
A BI chart “orders by status over time” becomes a simple read from this pre-aggregated table rather than heavy grouping on the fact table.
4) Consistency across sources
Because you use order_status_canon, Amazon and Shopify are comparable (as much as your mapping allows).
So this mart is your daily status distribution table: great for monitoring and understanding order lifecycle
*/