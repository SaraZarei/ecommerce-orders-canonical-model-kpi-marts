create or replace table `analytics_500550161.int_orders_canonical`
partition by order_date
cluster by source_system, order_id
as
with base as (
  select *
  from `analytics_500550161.int_amazon_shopify_orders`
),
/*
--Enforce grain + dedupe (1 row per global_order_id).Keep the latest record per order (latest update timestamp, else latest created time, else latest ingestion time)
Goal: 1 row per global_order_id.
Amazon can have multiple versions of the same order across runs (status updates).
Keep the “latest” row based on order_updated_at (or ingested_at as fallback).
Shopify can also have updates; keep latest if you have updated_at / processed_at.
Output rule:
global_order_id is unique.
*/
deduped as ( 
  select *
  from (
    select
      b.*,
      row_number() over (
        partition by global_order_id
        order by
          coalesce(order_updated_at, order_processed_at, order_created_at) desc,
          ingested_at desc
      ) as rn
    from base b
  )
  where rn = 1
),

with_customer_key as ( --Add unified customer key
  select
    *,
    case
      when customer_id is null then null
      else concat(source_system, ':', cast(customer_id as string))
    end as global_customer_id
  from deduped
),
/*
Standardize statuses (separate meanings).
Keep status semantics clean: order_status_canon = one canonical “overall” label you can filter on, payment_status = Shopify financial status (Amazon often NULL)
fulfillment_status_canon = shipping/fulfillment state (Amazon uses order_status)
Decide and standardize “status” semantics
Right now you may have:
Amazon: order_status (shipping-ish)
Shopify: financial_status and fulfillment_status
In the canonical model, keep two standardized columns instead of forcing one meaning:
payment_status (Shopify financial status; Amazon often NULL)
fulfillment_status (Shopify fulfillment; Amazon fulfillment channel/status)
optionally keep order_status as “source-native canonical”
This avoids mixing “paid/unpaid” with “shipped/unshipped”.
*/

status_standardized as (
  select
    *,

    case
      when order_status is null then null
      when upper(trim(order_status)) in ('CANCELLED','CANCELED') then 'CANCELLED'
      when upper(trim(order_status)) in ('COMPLETED','COMPLETE') then 'COMPLETED'
      when upper(trim(order_status)) = 'PENDING' then 'PENDING'
      when upper(trim(order_status)) = 'REFUNDED' then 'REFUNDED'
      when upper(trim(order_status)) = 'RETURNED' then 'RETURNED'
      when upper(trim(order_status)) = 'SHIPPED' then 'SHIPPED'
      when upper(trim(order_status)) = 'UNSHIPPED' then 'UNSHIPPED'
      else upper(trim(order_status))
    end as order_status_canon,

    case
      when financial_status is null then null
      else upper(trim(financial_status))
    end as payment_status,

    case
      when source_system = 'amazon' then upper(trim(order_status))
      when fulfillment_status is not null then upper(trim(fulfillment_status))
      else null
    end as fulfillment_status_canon

  from with_customer_key
),
/*
Standardize money fields (NUMERIC + rounding)
Standardize money fields + “amount semantics”
Make sure these are consistent across sources:
order_total as NUMERIC everywhere
choose whether it means gross (includes tax/shipping) or not
Shopify total_price is typically gross; Amazon order_total depends on report definition.
In canonical:
store order_total
store optional components when available (subtotal, discount, tax)
add amount_type or a note field if semantics differ (optional but helpful)
*/
money_standardized as (
  select
    *,
    round(cast(order_total as numeric), 2) as order_total_num,
    round(cast(subtotal_price as numeric), 2) as subtotal_price_num,
    round(cast(total_discounts as numeric), 2) as total_discounts_num,
    round(cast(total_tax as numeric), 2) as total_tax_num
  from status_standardized
),
/*
Standardize currency & country (final form)
Standardize currency & country (final form)
You already cleaned them, but canonical step should ensure:
currency is always ISO-3 or NULL
shipping_country always ISO-2 or NULL
optionally rename columns to final business names (e.g., country_code)
*/

geo_standardized as (
  select
    *,
    upper(nullif(trim(currency), '')) as currency_canon,
    upper(nullif(trim(shipping_country), '')) as shipping_country_canon
  from money_standardized
),
/*
Business filters/flags (don’t delete—flag)
Common filters applied here:
remove Shopify test orders (is_test = true) OR keep them but flag them (recommended: keep + flag)
optionally exclude cancelled orders from revenue marts later (don’t delete; keep status)
Add:
is_test_order (true/false)
is_cancelled (based on canonical status)
is_refund/returned if you track those
*/
business_flags as (
  select
    *,
    (source_system = 'shopify' and is_test = true) as is_test_order,
    (order_status_canon = 'CANCELLED') as is_cancelled,
    (order_status_canon in ('REFUNDED','RETURNED')) as is_refund_or_return
  from geo_standardized
),
/*
DQ rollup flag.Uses only canonical fields you’ll always have
Add a single “row quality” rollup flag
You already have many DQ flags in staging. In canonical, create:
dq_has_issue (true if any critical flag is true)
optionally dq_issue_count
Then marts can easily exclude/monitor.
*/

dq_rollup as (
  select
    *,

    (global_order_id is null) as dq_global_order_id_is_null,
    (order_id is null) as dq_order_id_is_null,
    (order_created_at is null) as dq_order_created_at_is_null,
    (order_total_num is null) as dq_order_total_is_null,
    (order_total_num is not null and order_total_num < 0) as dq_order_total_is_negative,

    (currency_canon is not null and not regexp_contains(currency_canon, r'^[A-Z]{3}$')) as dq_currency_invalid_format,
    (shipping_country_canon is not null and not regexp_contains(shipping_country_canon, r'^[A-Z]{2}$')) as dq_shipping_country_invalid_format,

    (
      global_order_id is null
      or order_id is null
      or order_created_at is null
      or order_total_num is null
      or (order_total_num is not null and order_total_num < 0)
      or (currency_canon is not null and not regexp_contains(currency_canon, r'^[A-Z]{3}$'))
      or (shipping_country_canon is not null and not regexp_contains(shipping_country_canon, r'^[A-Z]{2}$'))
    ) as dq_has_issue

  from business_flags
)

select
  *,
  /*
Pick “best timestamps” (optional but common)
Pick “best timestamps” for analytics
You currently have:
order_created_at
order_updated_at (Amazon only in your union)
ingest_date (Amazon only)
Canonical should:
define order_timestamp = coalesce(order_created_at, ...)
keep both created and updated
keep ingest_date where present, else NULL (or derive if you have Shopify ingestion metadata later)
  */

  coalesce(order_created_at, order_processed_at, order_updated_at) as order_event_ts,
  date(coalesce(order_created_at, order_processed_at, order_updated_at)) as order_event_date

from dq_rollup;
