create or replace table `analytics_500550161.int_amazon_shopify_orders`
partition by order_date
cluster by source_system, order_id
as

-- AMAZON mapped into unified schema
select
   date(purchase_date) as order_date,           -- required for partitioning
  'amazon' as source_system,

  case
    when nullif(trim(amazon_order_id), '') is null then null
    else concat('amazon:', nullif(trim(amazon_order_id), ''))
  end as global_order_id,

  nullif(trim(amazon_order_id), '') as order_id,

  nullif(trim(buyer_id), '') as customer_id,
  cast(null as string) as email,

  purchase_date as order_created_at,
  cast(null as timestamp) as order_processed_at,
  last_updated_date as order_updated_at,

  -- canonical order status (use your cleaned one if it exists; fallback to upper(trim()))
  coalesce(order_status_cleaned, upper(nullif(trim(order_status), ''))) as order_status,
  cast(null as string) as financial_status,
  cast(null as string) as fulfillment_status,

  -- fulfillment channel canonical (use cleaned if it exists; fallback to upper(trim()))
  coalesce(fulfillment_channel_cleaned, upper(nullif(trim(fulfillment_channel), ''))) as fulfillment_channel,

  coalesce(currency_cleaned, upper(nullif(trim(currency), ''))) as currency,
  cast(order_total as numeric) as order_total,
  cast(null as numeric) as subtotal_price,
  cast(null as numeric) as total_discounts,
  cast(null as numeric) as total_tax,

  coalesce(ship_country_cleaned, upper(nullif(trim(ship_country), ''))) as shipping_country,

  source_file_cleaned as source_file,
  ingested_at,
  ingest_date,
  report_date,

  cast(null as bool) as is_test,
  cast(null as string) as tags

from `analytics_500550161.stg_amazon_orders`

union all

-- SHOPIFY mapped into unified schema
select
  date(created_at) as order_date,              -- required for partitioning
  'shopify' as source_system,

  case
    when id_cleaned is null then null
    else concat('shopify:', id_cleaned)
  end as global_order_id,

  id_cleaned as order_id,

  customer_id_cleaned as customer_id,
  email_cleaned as email,

  created_at as order_created_at,
  processed_at as order_processed_at,
  cast(null as timestamp) as order_updated_at,   -- (you can map a better Shopify "updated_at" if you have it)

  -- pick a single "order_status" meaning for the unified table:
  -- here we use financial status as the main one
  financial_status_cleaned as order_status,
  financial_status_cleaned as financial_status,
  fulfillment_status_cleaned as fulfillment_status,

  cast(null as string) as fulfillment_channel,

  currency_cleaned as currency,
  total_price_numeric as order_total,
  subtotal_price_numeric as subtotal_price,
  total_discounts_numeric as total_discounts,
  total_tax_numeric as total_tax,

  shipping_country_cleaned as shipping_country,

  cast(null as string) as source_file,
  cast(null as timestamp) as ingested_at,
  cast(null as date) as ingest_date,
  cast(null as date) as report_date,

  test as is_test,
  tags as tags

from `analytics_500550161.stg_shopify_orders`;
