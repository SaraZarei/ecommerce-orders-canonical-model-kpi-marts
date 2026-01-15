-- 5) mart_dim_customers
-- Grain: global_customer_id
-- Lifetime metrics computed using only revenue-eligible orders
-- Includes primary_currency + multi_currency flag (based on eligible orders)
/*
This creates a customer dimension table: one row per customer, with lifetime stats. It’s used for customer analytics (repeat buyers, LTV, recency) and for joining customer attributes to facts.
*/
-- ------------------------------------------------------------
/*Build mart_dim_customers with grain = one row per global_customer_id (and per source_system, because customer IDs are source-specific).
*/
create or replace table `analytics_500550161.mart_dim_customers`
cluster by source_system
as
with base as (
  select
    global_customer_id,
    source_system,
    email,
    currency_canon,
    order_date,
    is_revenue_eligible,
    revenue_amount
  from `analytics_500550161.mart_fact_orders`
  where global_customer_id is not null
),
agg as ( -- main lifetime metrics. It produces per-customer:
  select
    global_customer_id,
    source_system,

    any_value(email) as any_email,-- just keeps one email value if available (mostly Shopify)

    min(order_date) as first_order_date_any,-- first/last order date (including cancelled/refund/test)
    max(order_date) as last_order_date_any,

    min(if(is_revenue_eligible, order_date, null)) as first_order_date_revenue_eligible,-- first/last revenue-eligible order date (more meaningful for “real customers”)
    max(if(is_revenue_eligible, order_date, null)) as last_order_date_revenue_eligible,

    countif(is_revenue_eligible) as lifetime_orders_revenue_eligible,--lifetime_revenue (because revenue_amount is 0 for excluded orders, this matches your revenue definition)
    sum(revenue_amount) as lifetime_revenue

  from base
  group by 1,2
),

currency_counts as (-- Counts, per customer, how many revenue-eligible orders they placed in each currency.
  select
    global_customer_id,
    source_system,
    currency_canon,
    countif(is_revenue_eligible) as eligible_orders_in_currency
  from base
  group by 1,2,3
),

primary_currency as ( -- Chooses the most frequently used currency per customer (among revenue-eligible orders).
  select * except(rn)
  from (
    select
      global_customer_id,
      source_system,
      currency_canon as primary_currency,
      eligible_orders_in_currency,
      row_number() over (    -- Why use row_number? Because you want one “winner” currency per customer.
        partition by global_customer_id, source_system
        order by eligible_orders_in_currency desc, currency_canon
      ) as rn
    from currency_counts
    where eligible_orders_in_currency > 0
  )
  where rn = 1
),

multi_currency as ( -- Counts how many different currencies the customer used (again only looking at eligible orders).
  select
    global_customer_id,
    source_system,
    countif(eligible_orders_in_currency > 0) as num_currencies_used
  from currency_counts
  group by 1,2
)
/*
Adds:primary_currency, is_multi_currency_customer, days_since_last_order_any, primary_currency, num_currencies_used,is_multi_currency_customer
recency: days since last order
*/
select 
  a.*,
  p.primary_currency,
  m.num_currencies_used,
  (m.num_currencies_used > 1) as is_multi_currency_customer,
  date_diff(current_date(), a.last_order_date_any, day) as days_since_last_order_any
from agg a
left join primary_currency p using (global_customer_id, source_system)
left join multi_currency m using (global_customer_id, source_system);

/*
Why you need it
1) Customer analytics becomes easy
Instead of recalculating lifetime metrics every time, you can just query:
“top customers by lifetime revenue”
“repeat customers”
“customers inactive for 90+ days”
“new customers per month” (using first_order_date)
2) It respects your revenue definition
Because metrics use is_revenue_eligible and revenue_amount, cancelled/refund/test don’t inflate lifetime revenue.
3) Handles multi-currency reality
Customers can buy in multiple currencies. This table tells you:
what currency they mostly use
whether they’re multi-currency
4) Good BI model design
Facts (mart_fact_orders) are order-level.
Dimensions (mart_dim_customers) are customer-level.
That separation is exactly how star schemas work and keeps reporting clean and fast.
*/
