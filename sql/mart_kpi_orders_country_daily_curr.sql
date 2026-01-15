-- 3) mart_kpi_orders_country_daily_currency
-- Grain: order_date × source_system × shipping_country_canon × currency_canon
-- ------------------------------------------------------------
-- This builds a geo breakdown KPI table: daily orders + revenue by country and currency, per source.
-- Creates an aggregated mart for country-level KPIs.It produces a table where each row represents:
-- ne day × one source_system × one shipping country × one currency
-- That’s the “grain”:order_date × source_system × shipping_country_canon × currency_canon
create or replace table `analytics_500550161.mart_kpi_orders_country_daily_currency`
partition by order_date --Partition by date → fast/cheap time filtering
cluster by source_system, shipping_country_canon, currency_canon --Cluster by source/country/currency → faster filtering and grouping for geo dashboards
as
select
  order_date,
  source_system,
  shipping_country_canon,
  currency_canon,

  count(distinct global_order_id) as orders_total,-- distinct orders shipped to that country that day
  count(distinct global_customer_id) as customers_total,-- distinct customers in that slice

  countif(is_revenue_eligible) as orders_revenue_eligible,-- only orders that pass your revenue rule (not test/cancelled/refund)
  sum(revenue_amount) as revenue,

  safe_divide(sum(revenue_amount), nullif(countif(is_revenue_eligible), 0)) as aov

from `analytics_500550161.mart_fact_orders`
group by 1,2,3,4;

/*
Why you need it
1) Geo reporting
This is the table you use for questions like:
“Which countries generate the most revenue?”
“Revenue by country over time”
“Top shipping destinations per channel (Amazon vs Shopify)”
“AOV by country”
2) Multi-currency safety
Revenue is grouped by currency as well as country. That prevents incorrect “mixed currency” totals.
3) Faster dashboards
Country breakdowns can be heavy if done directly on the fact table. Pre-aggregating makes maps and top-country charts fast.
4) Business monitoring
Big changes in a specific country (e.g., sudden drop in orders to DE) can indicate:
shipping/config issues
data ingestion problems for a region
marketplace changes
So this mart is basically your daily geo KPI layer, already aligned to your revenue definition and safe for multiple currencies.
*/