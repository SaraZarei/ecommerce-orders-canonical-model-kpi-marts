create or replace table analytics_500550161.stg_amazon_orders  
partition by ingest_date
cluster by amazon_order_id
as
with src as (
  select *
  from analytics_500550161.very_raw_amazon_orders
),
clean as(
  select *,
  -- order_id
  nullif(trim(amazon_order_id), '') as amazon_order_id_cleaned,--trim, empty → null,
  (nullif(trim(amazon_order_id), '') is null) as dq_amazon_order_id_is_null,--NULL CHECK FLAG

  -- order_total
  (order_total is null) as dq_order_total_is_null,--NULL CHECK FLAG
  (order_total is not null and order_total < 0) as dq_order_total_is_negative,--NEGATIVE VALUE VALIDATION FLAG
  cast(order_total as numeric) as order_total_numeric,--TYPE CAST / NUMERIC CONVERSION

  -- purchase_date
  (purchase_date is null) as dq_purchase_date_is_null,--NULL CHECK FLAG
  (purchase_date is not null and date(purchase_date) > current_date()) as dq_purchase_date_is_future,--FUTURE DATE VALIDATION FLAG
  (purchase_date is not null and date(purchase_date) < date '2010-01-01') as dq_purchase_date_is_too_old,--TOO-OLD DATE VALIDATION FLAG
  date(purchase_date) as order_date, -- (business analytics) --DATE DERIVATION

 -- ingested_at
  date(ingested_at) as ingest_date,-- (pipeline partitioning + monitoring) -- DATE DERIVATION
  (ingested_at is null) as dq_ingested_at_is_null,-- NULL CHECK FLAG
  -- CROSS-FIELD TEMPORAL ORDER VALIDATION FLAG
  (ingested_at is not null and purchase_date is not null and ingested_at < purchase_date) as dq_ingested_at_is_before_purchase_date,
  

  -- last_updated_date
  (last_updated_date is null) as dq_last_updated_date_is_null,--NULL CHECK FLAG
  -- CROSS-FIELD TEMPORAL ORDER VALIDATION FLAG
  (last_updated_date is not null and purchase_date is not null and  last_updated_date < purchase_date) as dq_last_updated_date_is_before_purchase_date,

  -- source_file
  nullif(trim(source_file), '') as source_file_cleaned,--trim, empty → null,
  (nullif(trim(source_file), '') is null) as dq_source_file_is_null,--NULL CHECK FLAG
  safe_cast(regexp_extract(source_file, r'\d{4}-\d{2}-\d{2}') as date) as source_file_date,--REGEX EXTRACT (extract date pattern)
   -- safe_cast() returns null instead of error
   -- SAFE_CAST to DATE (parse safely)
  (
    date(ingested_at) is not null
    and source_file is not null
    and date(ingested_at) != safe_cast(regexp_extract(source_file, r'\d{4}-\d{2}-\d{2}') as date)--DATE DERIVATION
  ) as dq_source_file_date_mismatch_ingest_date, --MISMATCH VALIDATION FLAG

 -- currency
    upper(nullif(trim(currency), '')) as currency_cleaned,
    (upper(nullif(trim(currency), '')) is null) as dq_currency_is_null,
    (
      upper(nullif(trim(currency), '')) is not null
      and not regexp_contains(upper(nullif(trim(currency), '')), r'^[A-Z]{3}$')
    ) as dq_currency_invalid_format,

from src
),

order_status_mapped as ( 
  select *,
  nullif(trim(order_status), '') as order_status_raw,--trim, empty → null,

  --CANONICAL MAPPING (synonyms/variants → standard values
  case
    when nullif(trim(order_status), '') is null then null
    when lower(trim(order_status)) in ('cancelled', 'canceled') then 'CANCELLED'--LOWERCASE normalization(for matching)
    when lower(trim(order_status)) in ('complete', 'completed') then 'COMPLETED'
    when lower(trim(order_status)) in ('pending') then 'PENDING'
    when lower(trim(order_status)) in ('refunded') then 'REFUNDED'
    when lower(trim(order_status)) in ('returned') then 'RETURNED'
    when lower(trim(order_status)) in ('shipped') then 'SHIPPED'
    when lower(trim(order_status)) in ('unshipped') then 'UNSHIPPED'
    else upper(trim(order_status))--UPPERCASE normalization
  end as order_status_cleaned

  from clean
),
order_status_flags as (
select *,
  (order_status_raw is null) as dq_order_status_is_null,--NULL CHECK FLAG
  --LLOWED-LIST VALIDATION FLAG
  (order_status_cleaned is not null and order_status_cleaned not in ('CANCELLED', 'COMPLETED', 'PENDING', 'REFUNDED', 'RETURNED', 'SHIPPED', 'UNSHIPPED')) as   dq_order_status_is_invalid

from order_status_mapped
),

-- fulfillment_channel
fulfillment_channel_mapped as ( 
  select *,
  nullif(trim(fulfillment_channel), '') as fulfillment_channel_raw,--trim, empty → null,

  --mapping to a canonical set
case
  when nullif(trim(fulfillment_channel), '') is null then null
  when upper(trim(fulfillment_channel)) = 'AFN' then 'AMAZON'   -- or 'FBA'
  when upper(trim(fulfillment_channel)) = 'MFN' then 'MERCHANT' -- or 'FBM'
  else upper(trim(fulfillment_channel))
end as fulfillment_channel_cleaned
from order_status_flags
),

-- flags
fulfillment_channel_flags as (
select *,

(fulfillment_channel_cleaned is null) as dq_fulfillment_channel_is_null,--NULL CHECK FLAG
(
  fulfillment_channel_cleaned is not null
  and fulfillment_channel_cleaned not in ('AMAZON','MERCHANT')--ALLOWED-LIST VALIDATION
) as dq_fulfillment_channel_invalid

from fulfillment_channel_mapped
),
-- ship_country
ship_country_clean as (
  select *,
    upper(nullif(trim(ship_country), '')) as ship_country_cleaned,--trim, empty → null, uppercase
    --flag null + invalid format
    (upper(nullif(trim(ship_country), '')) is null) as dq_ship_country_is_null,--NULL CHECK FLAG
    (
    upper(nullif(trim(ship_country), '')) is not null
    and not regexp_contains(upper(nullif(trim(ship_country), '')), r'^[A-Z]{2}$')--validate it looks like ISO-2 (two letters)
    )as dq_ship_country_invalid
 
  from fulfillment_channel_flags
)

 

select * from ship_country_clean











