create or replace table `analytics_500550161.stg_shopify_orders`
partition by order_date
cluster by id
as
with src as (
  select *
  from `analytics_500550161.very_raw_shopify_orders`
),

clean as (
  select
    *,

    -- ids
    nullif(trim(id), '') as id_cleaned,
    (nullif(trim(id), '') is null) as dq_id_is_null,

    nullif(trim(customer_id), '') as customer_id_cleaned,
    (nullif(trim(customer_id), '') is null) as dq_customer_id_is_null,

    nullif(trim(email), '') as email_cleaned,

    -- dates
    date(created_at) as order_date,
    (created_at is null) as dq_created_at_is_null,
    (created_at is not null and date(created_at) > current_date()) as dq_created_at_is_future,
    (created_at is not null and date(created_at) < date '2010-01-01') as dq_created_at_is_too_old,

    -- money (float -> numeric)
    cast(total_price as numeric) as total_price_numeric,
    cast(subtotal_price as numeric) as subtotal_price_numeric,
    cast(total_discounts as numeric) as total_discounts_numeric,
    cast(total_tax as numeric) as total_tax_numeric,

    (total_price is null) as dq_total_price_is_null,
    (total_price is not null and total_price < 0) as dq_total_price_is_negative,

    -- currency
    upper(nullif(trim(currency), '')) as currency_cleaned,
    (upper(nullif(trim(currency), '')) is null) as dq_currency_is_null,
    (
      upper(nullif(trim(currency), '')) is not null
      and not regexp_contains(upper(nullif(trim(currency), '')), r'^[A-Z]{3}$')
    ) as dq_currency_invalid_format,

    -- shipping country
    upper(nullif(trim(shipping_country), '')) as shipping_country_cleaned,
    (upper(nullif(trim(shipping_country), '')) is null) as dq_shipping_country_is_null,
    (
      upper(nullif(trim(shipping_country), '')) is not null
      and not regexp_contains(upper(nullif(trim(shipping_country), '')), r'^[A-Z]{2}$')
    ) as dq_shipping_country_invalid_format,

    -- statuses (light cleaning; you can add canonical mapping later if needed)
    nullif(trim(financial_status), '') as financial_status_raw,
    upper(nullif(trim(financial_status), '')) as financial_status_cleaned,

    nullif(trim(fulfillment_status), '') as fulfillment_status_raw,
    upper(nullif(trim(fulfillment_status), '')) as fulfillment_status_cleaned

  from src
)

select * from clean;
