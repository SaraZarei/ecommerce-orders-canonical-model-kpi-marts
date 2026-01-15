-- 1) mart_fact_orders
-- ------------------------------------------------------------
create or replace table `analytics_500550161.mart_fact_orders`
partition by order_date
cluster by source_system, global_customer_id
as
select
  -- keys
  global_order_id,
  source_system,
  order_id,
  global_customer_id,

  -- (optional) keep source customer/email for traceability
  customer_id,
  email,

  -- dates/times
  order_date,
  order_created_at,
  order_processed_at,
  order_updated_at,
  order_event_ts,
  order_event_date,
  ingest_date,
  report_date,

  -- statuses (raw + canonical)
  order_status,
  financial_status,
  fulfillment_status,
  order_status_canon,
  payment_status,
  fulfillment_status_canon,

  -- geo/currency
  currency,
  currency_canon,
  shipping_country,
  shipping_country_canon,
  fulfillment_channel,

  -- money (already NUMERIC in your canonical table)
  order_total,
  subtotal_price,
  total_discounts,
  total_tax,
  order_total_num,
  subtotal_price_num,
  total_discounts_num,
  total_tax_num,

  -- flags
  is_test,
  tags,
  is_test_order,
  is_cancelled,
  is_refund_or_return,
  dq_global_order_id_is_null,
  dq_order_id_is_null,
  dq_order_created_at_is_null,
  dq_order_total_is_null,
  dq_order_total_is_negative,
  dq_currency_invalid_format,
  dq_shipping_country_invalid_format,
  dq_has_issue,
/*
revenue eligibility (exclude cancelled + refunded/returned + test)
This block creates one consistent rule for “what counts as revenue” and then turns that rule into:
a boolean flag you can filter on everywhere (is_revenue_eligible)
a numeric amount you can safely sum (revenue_amount)
*/

  (
    coalesce(is_test_order, false) = false
    and coalesce(is_cancelled, false) = false
    and coalesce(is_refund_or_return, false) = false
    and order_total_num is not null
    and order_total_num >= 0
  ) as is_revenue_eligible, /* It marks an order as eligible to be counted as revenue only if:it’s not a test order,it’s not cancelled,it’s not refunded/returned,
it has a valid numeric total (order_total_num not null and not negative)
coalesce(flag, false) is used because those flags might be NULL for some rows; this treats NULL as “false” so the logic stays stable.
  */

  case
    when
      coalesce(is_test_order, false) = false
      and coalesce(is_cancelled, false) = false
      and coalesce(is_refund_or_return, false) = false
      and order_total_num is not null
      and order_total_num >= 0
    then order_total_num
    else cast(0 as numeric)
  end as revenue_amount --If the order is revenue-eligible, revenue_amount = order_total_num.Otherwise, set it to 0.



from `analytics_500550161.int_orders_canonical`
where rn is null or rn = 1;  -- safety: if rn exists, keep only the chosen row

/*
Why you need it
1) Prevents wrong revenue in dashboards
If you sum order_total_num directly, you’ll accidentally include:
cancelled orders
refunded/returned orders
test orders
bad/negative values
2) Makes KPI queries simple and consistent
Instead of rewriting complicated filters in every KPI mart, you can just:
sum(revenue_amount) for revenue
countif(is_revenue_eligible) for revenue-eligible orders
aov = sum(revenue_amount) / countif(is_revenue_eligible)
That means every report uses the same definition.
3) Keeps the fact table flexible
You still keep the original status fields and totals, but you add a standardized “business metric version” used for analytics.
When you’d change it
If later you decide refunds should subtract revenue (negative) instead of zero, you’d change revenue_amount rule. But your current requirement was: exclude cancelled + refunded/returned, so setting to 0 is correct for that definition.
*/
