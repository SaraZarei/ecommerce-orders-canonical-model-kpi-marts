# Ecommerce-Orders-Canonical-Model-KPI-Marts (Amazon + Shopify → Canonical Orders → Marts)

## Contents

### Overview
- [Goal](#goal)
- [High-level layers & table lineage](#high-level-layers--table-lineage)
  - [Tables (as implemented)](#tables-as-implemented)
  - [What each layer means](#what-each-layer-means)

### Per-source staging
- [Amazon staging](#detailed-implementation-amazon-staging)
  - [`very_raw_amazon_orders`](#1-very_raw_amazon_orders-raw-landing-table)
  - [`stg_amazon_orders`](#2-stg_amazon_orders-clean--validate-amazon-still-source-shaped)
- [Shopify staging](#detailed-implementation-shopify-staging)
  - [`very_raw_shopify_orders`](#1-very_raw_shopify_orders-raw-landing-table)
  - [`stg_shopify_orders`](#2-stg_shopify_orders-clean--validate-shopify-still-source-shaped)

### Cross-source integration & canonicalization
- [`int_amazon_shopify_orders` (UNION ALL)](#cross-source-integration-int_amazon_shopify_orders-aligned-selects--union-all)
- [`int_orders_canonical` (dedupe + business rules + standardized fields)](#canonical-intermediate-int_orders_canonical-dedupe--business-rules--standardized-fields)

### Marts
- [Marts built on `int_orders_canonical`](#marts-built-on-int_orders_canonical)
  - [`mart_fact_orders` (order-level fact + revenue rule)](#1-mart_fact_orders-order-level-analytics-fact--revenue-rule)
  - [`mart_kpi_orders_daily_currency`](#2-mart_kpi_orders_daily_currency-daily-kpis-by-source--currency)
  - [`mart_kpi_orders_country_daily_currency`](#3-mart_kpi_orders_country_daily_currency-daily-kpis-by-country--currency)
  - [`mart_kpi_orders_status_daily`](#4-mart_kpi_orders_status_daily-daily-order-counts-by-canonical-status)
  - [`mart_dim_customers` (customer dimension)](#5-mart_dim_customers-customer-dimension-with-lifetime-metrics)
  - [`mart_kpi_freshness_lag` (optional)](#6-mart_kpi_freshness_lag-optional-pipeline-freshness--lateness-monitoring)


## Goal
Order data lives in multiple disconnected systems (e.g., **Amazon**, **Shopify**, WooCommerce, Klaviyo).  
This pipeline creates a **single consistent view of orders** so KPIs (orders, revenue, status rates, country mix, currency-normalized metrics, freshness/lag) can be tracked reliably across sources.

---

## High-level layers & table lineage

### Tables (as implemented)
- `very_raw_amazon_orders`
- `stg_amazon_orders`
- `very_raw_shopify_orders`
- `stg_shopify_orders`
- `int_amazon_shopify_orders`
- `int_orders_canonical`
- `mart_fact_orders`
- `mart_kpi_orders_daily_currency`
- `mart_kpi_orders_country_daily_currency`
- `mart_kpi_orders_status_daily`
- `mart_dim_customers`
- `mart_kpi_freshness_lag` *(optional)*

### What each layer means
**Staging (per-source, still source-shaped)**
- `very_raw_*`: raw landed tables (minimal transformation, mostly “as received”)
- `stg_*`: cleaned & validated tables per source (adds *cleaned fields* + *data quality flags*), but still source-shaped

**Intermediate staging (cross-source integration)**
- `int_amazon_shopify_orders`: unioned / “just stacked” view across sources (may still contain duplicates and inconsistent semantics)
- `int_orders_canonical`: standardized, deduped, business-defined canonical orders (final rules for keys, grain, statuses, money fields, test-order filtering). This becomes the base for marts.

**Marts**
- Facts/dims and KPI summary tables built on the canonical model.

---

## Detailed implementation: Amazon staging

### 1) `very_raw_amazon_orders` (raw landing table)

**Purpose**
- Store Amazon orders exactly as ingested from source files (e.g., S3 report exports), with minimal/no business logic.

**Schema (raw)**
- `ingested_at` (TIMESTAMP)
- `source_file` (STRING)
- `report_date` (DATE)
- `amazon_order_id` (STRING)
- `purchase_date` (TIMESTAMP)
- `last_updated_date` (TIMESTAMP)
- `order_status` (STRING)
- `fulfillment_channel` (STRING)
- `order_total` (FLOAT)
- `currency` (STRING)
- `ship_country` (STRING)
- `buyer_id` (STRING)

**Typical raw issues observed (examples)**
- missing `last_updated_date` values
- negative `order_total` values possible
- inconsistent text casing/formatting (statuses, currency codes, country codes)
- duplicates across multiple report ingestions (same `amazon_order_id` appearing on different ingest dates)

---

### 2) `stg_amazon_orders` (clean + validate Amazon, still source-shaped)

**Purpose**
- Keep Amazon semantics, but make fields consistently formatted and measurable:
  - add **cleaned columns**
  - add **DQ (data quality) flags**
  - derive key analytics dates (`order_date`, `ingest_date`)
  - normalize enums (`order_status`, `fulfillment_channel`)
  - standardize money + identifiers

**Storage optimizations**
- Materialized as a table.
- **Partitioned by:** `ingest_date` (DATE derived from `ingested_at`)
- **Clustered by:** `amazon_order_id`

---

## Transformations applied in `stg_amazon_orders`

### A) Order ID cleaning + null checks
**Created**
- `amazon_order_id_cleaned`: `trim(amazon_order_id)` and convert empty strings to `NULL`
- `dq_amazon_order_id_is_null`: flag when cleaned order id is `NULL`

**Why**
- Stable keys are required for deduplication, joins, and downstream canonical keys.

---

### B) Order total validation + numeric casting
**Created**
- `dq_order_total_is_null`: flag when `order_total` is `NULL`
- `dq_order_total_is_negative`: flag when `order_total < 0`
- `order_total_numeric`: cast from FLOAT → NUMERIC

**Why**
- FLOAT is risky for currency math; NUMERIC is safer for financial aggregation.
- Negative totals are a common anomaly (refund edge cases vs. true data errors) and need explicit visibility.

---

### C) Purchase date validation + business date derivation
**Created**
- `dq_purchase_date_is_null`: purchase timestamp is missing
- `dq_purchase_date_is_future`: purchase date is after `CURRENT_DATE()`
- `dq_purchase_date_is_too_old`: purchase date earlier than `2010-01-01`
- `order_date`: `DATE(purchase_date)` used for analytics grain (daily KPIs)

**Why**
- Ensures time fields are usable for time-series KPIs and helps detect broken source exports.

---

### D) Ingestion timestamp → ingestion date + cross-field temporal validation
**Created**
- `ingest_date`: `DATE(ingested_at)` (used for partitioning + monitoring)
- `dq_ingested_at_is_null`: ingested timestamp missing
- `dq_ingested_at_is_before_purchase_date`: flag when `ingested_at < purchase_date` (if both present)

**Why**
- `ingest_date` supports incremental loading, partition pruning, and “data freshness” monitoring.
- Temporal ordering checks catch timezone/clock/parsing issues.

---

### E) Last updated timestamp checks
**Created**
- `dq_last_updated_date_is_null`
- `dq_last_updated_date_is_before_purchase_date`: flag when `last_updated_date < purchase_date` (if both present)

**Why**
- Helps validate event ordering and detect malformed records.

---

### F) Source file cleaning + extracting a file-date + mismatch detection
**Created**
- `source_file_cleaned`: trim + empty → NULL
- `dq_source_file_is_null`
- `source_file_date`: extracted via regex pattern `\d{4}-\d{2}-\d{2}` and `SAFE_CAST(... AS DATE)`
- `dq_source_file_date_mismatch_ingest_date`: flag when:
  - `DATE(ingested_at)` is present
  - `source_file_date` is present
  - and they **do not match**

**Why**
- File-name date is useful for auditability and debugging ingestion windows.
- Mismatch flags catch delayed uploads, wrong file routing, or backfills.

---

### G) Currency standardization + format validation
**Created**
- `currency_cleaned`: `UPPER(trim(currency))`, empty → NULL
- `dq_currency_is_null`
- `dq_currency_invalid_format`: flag when currency is present but does **not** match `^[A-Z]{3}$` (ISO-4217 style)

**Why**
- Currency normalization is required before cross-source union and FX conversion.

---

### H) Order status normalization to canonical values + allowed-list validation
**Created**
- `order_status_raw`: trimmed original
- `order_status_cleaned`: canonical mapping (case-insensitive matching), then normalized to standard uppercase values:
  - `cancelled/canceled` → `CANCELLED`
  - `complete/completed` → `COMPLETED`
  - `pending` → `PENDING`
  - `refunded` → `REFUNDED`
  - `returned` → `RETURNED`
  - `shipped` → `SHIPPED`
  - `unshipped` → `UNSHIPPED`
  - otherwise `UPPER(trim(order_status))`
- `dq_order_status_is_null`
- `dq_order_status_is_invalid`: flag if `order_status_cleaned` not in:
  - `CANCELLED, COMPLETED, PENDING, REFUNDED, RETURNED, SHIPPED, UNSHIPPED`

**Why**
- Status values must be consistent before building status-based KPIs (cancel rate, shipped rate, etc.).

---

### I) Fulfillment channel canonical mapping + allowed-list validation
**Created**
- `fulfillment_channel_raw`: trimmed original
- `fulfillment_channel_cleaned`: mapped into a canonical set:
  - `AFN` → `AMAZON` *(e.g., FBA)*
  - `MFN` → `MERCHANT` *(e.g., FBM)*
  - else `UPPER(trim(fulfillment_channel))`
- `dq_fulfillment_channel_is_null`
- `dq_fulfillment_channel_invalid`: flag if present but not in `AMAZON, MERCHANT`

**Why**
- Standardizes fulfillment semantics for channel-level KPIs and cross-source consistency.

---

### J) Ship-to country standardization + ISO-2 validation
**Created**
- `ship_country_cleaned`: `UPPER(trim(ship_country))`, empty → NULL
- `dq_ship_country_is_null`
- `dq_ship_country_invalid`: flag when present but does **not** match `^[A-Z]{2}$` (ISO-2 style)

**Why**
- Enables country-level KPI slices and reduces join/mapping issues.

---

## `stg_amazon_orders` output columns (what exists after staging)

### Original fields retained
- `ingested_at`, `source_file`, `report_date`, `amazon_order_id`, `purchase_date`, `last_updated_date`,
  `order_status`, `fulfillment_channel`, `order_total`, `currency`, `ship_country`, `buyer_id`

### Cleaned / derived fields added
- `amazon_order_id_cleaned`
- `order_total_numeric`
- `order_date`
- `ingest_date`
- `source_file_cleaned`
- `source_file_date`
- `currency_cleaned`
- `order_status_raw`, `order_status_cleaned`
- `fulfillment_channel_raw`, `fulfillment_channel_cleaned`
- `ship_country_cleaned`

### Data quality flags added (booleans)
- `dq_amazon_order_id_is_null`
- `dq_order_total_is_null`
- `dq_order_total_is_negative`
- `dq_purchase_date_is_null`
- `dq_purchase_date_is_future`
- `dq_purchase_date_is_too_old`
- `dq_ingested_at_is_null`
- `dq_ingested_at_is_before_purchase_date`
- `dq_last_updated_date_is_null`
- `dq_last_updated_date_is_before_purchase_date`
- `dq_source_file_is_null`
- `dq_source_file_date_mismatch_ingest_date`
- `dq_currency_is_null`
- `dq_currency_invalid_format`
- `dq_order_status_is_null`
- `dq_order_status_is_invalid`
- `dq_fulfillment_channel_is_null`
- `dq_fulfillment_channel_invalid`
- `dq_ship_country_is_null`
- `dq_ship_country_invalid`

---

## Next layers (documented intent)
- `int_amazon_shopify_orders`: union Amazon + Shopify orders (still “stacked”; duplicates/semantic differences may remain)
- `int_orders_canonical`: enforce final business rules (keys, grain, dedup, standardized money/statuses, test-order filters)
- marts: build fact/dim and KPI summary tables from the canonical orders model

> If you send the Shopify staging screenshots + `int_*` / `mart_*` code the same way, I can extend this README with equally precise, table-by-table documentation for the rest of the pipeline.

---

## Detailed implementation: Shopify staging

### 1) `very_raw_shopify_orders` (raw landing table)

**Purpose**
- Store Shopify orders as ingested from the source with minimal transformation.

**Schema (raw)**
- `id` (STRING)
- `created_at` (TIMESTAMP)
- `processed_at` (TIMESTAMP)
- `financial_status` (STRING)
- `fulfillment_status` (STRING)
- `currency` (STRING)
- `total_price` (FLOAT)
- `subtotal_price` (FLOAT)
- `total_discounts` (FLOAT)
- `total_tax` (FLOAT)
- `shipping_country` (STRING)
- `customer_id` (STRING)
- `email` (STRING)
- `tags` (STRING)
- `test` (BOOLEAN)

**Typical raw issues observed (examples)**
- missing timestamps (`processed_at` can be `NULL`)
- inconsistent casing/format in currency and country codes
- money fields are FLOAT (not ideal for currency math)
- possible test orders (via `test` flag and/or tags)

---

### 2) `stg_shopify_orders` (clean + validate Shopify, still source-shaped)

**Purpose**
- Keep Shopify semantics, but make fields consistent and measurable:
  - add **cleaned columns**
  - add **DQ flags**
  - derive an **analytics date** (`order_date`)
  - standardize money + text formatting

**Storage optimizations**
- Materialized as a table.
- **Partitioned by:** `order_date` (DATE derived from `created_at`)
- **Clustered by:** `id`

---

## Transformations applied in `stg_shopify_orders`

### A) ID cleaning + null checks
**Created**
- `id_cleaned`: `trim(id)` and convert empty strings to `NULL`
- `dq_id_is_null`: flag when cleaned `id` is `NULL`

**Why**
- Stable order identifiers are required for joins, deduplication, and cross-source canonical keys.

---

### B) Customer + email cleaning
**Created**
- `customer_id_cleaned`: `trim(customer_id)` and empty → `NULL`
- `dq_customer_id_is_null`: flag when cleaned `customer_id` is `NULL`
- `email_cleaned`: `trim(email)` and empty → `NULL`

**Why**
- Prepares customer fields for downstream customer dimension modeling and identity stitching.

---

### C) Order date derivation + created_at validation
**Created**
- `order_date`: `DATE(created_at)` (used for partitioning and daily KPIs)
- `dq_created_at_is_null`
- `dq_created_at_is_future`: `DATE(created_at) > CURRENT_DATE()`
- `dq_created_at_is_too_old`: `DATE(created_at) < DATE '2010-01-01'`

**Why**
- Establishes a consistent daily grain for KPI tables and detects broken timestamps.

> Note: `processed_at` is retained as-is, but the analytics date in staging is driven by `created_at`.

---

### D) Money fields: FLOAT → NUMERIC casting + validations
**Created**
- `total_price_numeric`: `CAST(total_price AS NUMERIC)`
- `subtotal_price_numeric`: `CAST(subtotal_price AS NUMERIC)`
- `total_discounts_numeric`: `CAST(total_discounts AS NUMERIC)`
- `total_tax_numeric`: `CAST(total_tax AS NUMERIC)`
- `dq_total_price_is_null`
- `dq_total_price_is_negative`: flag when `total_price < 0`

**Why**
- NUMERIC is safer than FLOAT for financial aggregation.
- Negative totals are explicitly flagged for review (refunds vs data errors).

---

### E) Currency standardization + format validation
**Created**
- `currency_cleaned`: `UPPER(trim(currency))`, empty → `NULL`
- `dq_currency_is_null`
- `dq_currency_invalid_format`: flag when present but does **not** match `^[A-Z]{3}$` (ISO-4217 style)

**Why**
- Ensures currency values can be used reliably for cross-source union + FX conversion.

---

### F) Shipping country standardization + ISO-2 validation
**Created**
- `shipping_country_cleaned`: `UPPER(trim(shipping_country))`, empty → `NULL`
- `dq_shipping_country_is_null`
- `dq_shipping_country_invalid_format`: flag when present but does **not** match `^[A-Z]{2}$` (ISO-2 style)

**Why**
- Enables country-level KPI slices and reduces downstream mapping issues.

---

### G) Status fields: light normalization (no business mapping yet)
**Created**
- `financial_status_raw`: trimmed original (`NULL` if empty)
- `financial_status_cleaned`: `UPPER(financial_status_raw)`
- `fulfillment_status_raw`: trimmed original (`NULL` if empty)
- `fulfillment_status_cleaned`: `UPPER(fulfillment_status_raw)`

**Why**
- Standardizes casing and empties, while postponing full canonical mapping until the cross-source canonical layer (where Amazon+Shopify status semantics are aligned).

---

## `stg_shopify_orders` output columns (what exists after staging)

### Original fields retained
- `id`, `created_at`, `processed_at`, `financial_status`, `fulfillment_status`, `currency`,
  `total_price`, `subtotal_price`, `total_discounts`, `total_tax`,
  `shipping_country`, `customer_id`, `email`, `tags`, `test`

### Cleaned / derived fields added
- `id_cleaned`
- `customer_id_cleaned`
- `email_cleaned`
- `order_date`
- `total_price_numeric`
- `subtotal_price_numeric`
- `total_discounts_numeric`
- `total_tax_numeric`
- `currency_cleaned`
- `shipping_country_cleaned`
- `financial_status_raw`, `financial_status_cleaned`
- `fulfillment_status_raw`, `fulfillment_status_cleaned`

### Data quality flags added (booleans)
- `dq_id_is_null`
- `dq_customer_id_is_null`
- `dq_created_at_is_null`
- `dq_created_at_is_future`
- `dq_created_at_is_too_old`
- `dq_total_price_is_null`
- `dq_total_price_is_negative`
- `dq_currency_is_null`
- `dq_currency_invalid_format`
- `dq_shipping_country_is_null`
- `dq_shipping_country_invalid_format`

---
---

---

## Cross-source integration: `int_amazon_shopify_orders` (aligned selects + `UNION ALL`)

### Purpose
This step creates a single **“all orders”** table by:
- defining a **unified schema** (standard field names shared across sources)
- creating a **global/composite key** (`global_order_id`) so IDs don’t collide across systems
- mapping each source into the same column list (filling missing fields with `NULL`)
- stacking records using `UNION ALL` (no dedup yet — just integration)

This table is intentionally **intermediate**: it enables cross-source KPI tracking while keeping lineage fields for audit/debugging. Full standardization + dedup happens later in `int_orders_canonical`.

---

### Storage optimizations
- Materialized as a table: `analytics_500550161.int_amazon_shopify_orders`
- **Partitioned by:** `order_date`
- **Clustered by:** `source_system`, `order_id`

---

## Unified schema (final column set in this table)

### Core identifiers & dates
- `order_date` *(DATE; required for partitioning)*
- `source_system` *(STRING; e.g., `amazon`, `shopify`)*
- `global_order_id` *(STRING; composite key `source_system:order_id`)*
- `order_id` *(STRING; source-native order id normalized)*
- `customer_id` *(STRING; buyer/customer identifier aligned across sources)*
- `email` *(STRING; available in Shopify, NULL for Amazon)*

### Timestamps (event fields)
- `order_created_at` *(TIMESTAMP)*
- `order_processed_at` *(TIMESTAMP; Shopify only, NULL for Amazon)*
- `order_updated_at` *(TIMESTAMP; Amazon last_updated, NULL placeholder for Shopify unless available)*

### Status fields (kept separately for later canonical rules)
- `order_status` *(STRING; single “main status” in this integrated view)*
- `financial_status` *(STRING; Shopify-specific, NULL for Amazon)*
- `fulfillment_status` *(STRING; Shopify-specific, NULL for Amazon)*
- `fulfillment_channel` *(STRING; Amazon-specific, NULL for Shopify)*

### Commerce fields
- `currency` *(STRING; cleaned/standardized where available)*
- `order_total` *(NUMERIC)*
- `subtotal_price` *(NUMERIC)*
- `total_discounts` *(NUMERIC)*
- `total_tax` *(NUMERIC)*
- `shipping_country` *(STRING; cleaned/standardized where available)*

### Lineage / ingestion fields
- `source_file` *(STRING; Amazon only)*
- `ingested_at` *(TIMESTAMP; Amazon only)*
- `ingest_date` *(DATE; Amazon only)*
- `report_date` *(DATE; Amazon only)*

### Test / tagging fields
- `is_test` *(BOOLEAN; Shopify `test`, NULL for Amazon)*
- `tags` *(STRING; Shopify tags, NULL for Amazon)*

---

## Source-to-unified mapping

### A) Amazon → unified schema
Source table: `analytics_500550161.stg_amazon_orders`

**Partitioning date**
- `order_date = DATE(purchase_date)` *(explicitly used for partitioning)*

**Source identifier**
- `source_system = 'amazon'`

**Global/composite key**
- `global_order_id`:
  - `NULL` if `amazon_order_id` is blank/NULL
  - else `CONCAT('amazon:', amazon_order_id_cleaned)`
- Ensures uniqueness across sources even if raw `order_id` formats overlap.

**IDs**
- `order_id = NULLIF(TRIM(amazon_order_id), '')`
- `customer_id = NULLIF(TRIM(buyer_id), '')`
- `email = NULL` (Amazon extract does not supply email here)

**Timestamps**
- `order_created_at = purchase_date`
- `order_processed_at = NULL` *(not present for Amazon)*
- `order_updated_at = last_updated_date`

**Statuses**
- `order_status` uses cleaned canonical value when available:
  - `order_status = COALESCE(order_status_cleaned, UPPER(NULLIF(TRIM(order_status), '')))`
- `financial_status = NULL`
- `fulfillment_status = NULL`

**Fulfillment channel**
- `fulfillment_channel = COALESCE(fulfillment_channel_cleaned, UPPER(NULLIF(TRIM(fulfillment_channel), '')))`
  - This keeps a canonical set where possible (e.g., `AMAZON` vs `MERCHANT`)

**Commerce fields**
- `currency = COALESCE(currency_cleaned, UPPER(NULLIF(TRIM(currency), '')))`
- `order_total = CAST(order_total AS NUMERIC)`
- `subtotal_price = NULL`
- `total_discounts = NULL`
- `total_tax = NULL`
- `shipping_country = COALESCE(ship_country_cleaned, UPPER(NULLIF(TRIM(ship_country), '')))`

**Lineage fields retained (Amazon-only)**
- `source_file = source_file_cleaned`
- `ingested_at`, `ingest_date`, `report_date`

**Test/Tags**
- `is_test = NULL`
- `tags = NULL`

---

### B) Shopify → unified schema
Source table: `analytics_500550161.stg_shopify_orders`

**Partitioning date**
- `order_date = DATE(created_at)` *(explicitly used for partitioning)*

**Source identifier**
- `source_system = 'shopify'`

**Global/composite key**
- `global_order_id`:
  - `NULL` if `id_cleaned` is NULL
  - else `CONCAT('shopify:', id_cleaned)`

**IDs**
- `order_id = id_cleaned`
- `customer_id = customer_id_cleaned`
- `email = email_cleaned`

**Timestamps**
- `order_created_at = created_at`
- `order_processed_at = processed_at`
- `order_updated_at = NULL` *(placeholder; comment notes you can map a better Shopify updated timestamp if available)*

**Statuses**
To pick a single “order_status” meaning for the unified table:
- `order_status = financial_status_cleaned` *(chosen as the main one here)*
- `financial_status = financial_status_cleaned`
- `fulfillment_status = fulfillment_status_cleaned`

**Fulfillment channel**
- `fulfillment_channel = NULL` *(Shopify doesn’t provide Amazon-like AFN/MFN in this extract)*

**Commerce fields (NUMERIC from staging)**
- `currency = currency_cleaned`
- `order_total = total_price_numeric`
- `subtotal_price = subtotal_price_numeric`
- `total_discounts = total_discounts_numeric`
- `total_tax = total_tax_numeric`
- `shipping_country = shipping_country_cleaned`

**Lineage fields (Shopify-only NULLs in this unified table)**
- `source_file = NULL`
- `ingested_at = NULL`
- `ingest_date = NULL`
- `report_date = NULL`

**Test/Tags**
- `is_test = test`
- `tags = tags`

---

## What the result looks like 
After the `UNION ALL`, rows from both sources appear together with:
- `source_system` distinguishing Amazon vs Shopify
- `global_order_id` values like `amazon:A-3003` and `shopify:S-2003`
- Shopify rows populated for `email`, `financial_status`, `fulfillment_status`, `tags`, `is_test`
- Amazon rows populated for `fulfillment_channel`, `source_file`, `ingested_at`, `ingest_date`, `report_date`
- Shared commerce fields (`currency`, `order_total`, etc.) aligned under the same names, with NULLs where a source doesn’t have that field

---
---

## Canonical intermediate: `int_orders_canonical` (dedupe + business rules + standardized fields)

### Purpose
`int_orders_canonical` is built from `int_amazon_shopify_orders` and becomes the **official standardized intermediate orders table** that marts should use. In this step you:
- enforce the correct **grain** (1 row per `global_order_id`)
- standardize **status semantics** (separate overall vs payment vs fulfillment meanings)
- standardize **money** (NUMERIC + rounding)
- standardize **geo fields** (currency ISO-3, country ISO-2)
- add **business flags** (test orders, cancelled, refund/return)
- add **DQ rollups** (single place to flag problematic rows)
- create a “best timestamp” (`order_event_ts`) for analytics

### Storage optimizations
- Materialized as: `analytics_500550161.int_orders_canonical`
- **Partitioned by:** `order_date`
- **Clustered by:** `source_system`, `order_id`

---

## Step-by-step logic (CTEs)

### 1) `base`
Reads the integrated “all orders” table:
- `base AS (SELECT * FROM analytics_500550161.int_amazon_shopify_orders)`

---

### 2) `deduped` (enforce grain: 1 row per `global_order_id`)
You deduplicate multiple versions of the same order (common in Amazon re-ingestions / status updates).

- Window logic:
  - `ROW_NUMBER() OVER (PARTITION BY global_order_id ORDER BY
      COALESCE(order_updated_at, order_processed_at, order_created_at) DESC,
      ingested_at DESC
    ) AS rn`
- Keep only the latest record:
  - `WHERE rn = 1`

**Result:** one “best/latest” row per `global_order_id`.

---

### 3) `with_customer_key` (create a unified customer key)
Adds a cross-source-safe customer identifier:

- `global_customer_id`:
  - `NULL` if `customer_id` is NULL
  - else `CONCAT(source_system, ':', CAST(customer_id AS STRING))`

**Why:** avoids collisions if Amazon + Shopify have overlapping customer ids.

---

### 4) `status_standardized` (separate meanings of status)
You keep status semantics clean by creating **three standardized columns**:

- **`order_status_canon`** (overall/canonical label you can filter on)
  - canonical mapping of `order_status`:
    - cancelled/canceled → `CANCELLED`
    - complete/completed → `COMPLETED`
    - pending → `PENDING`
    - refunded → `REFUNDED`
    - returned → `RETURNED`
    - shipped → `SHIPPED`
    - unshipped → `UNSHIPPED`
    - else uppercase normalized

- **`payment_status`**
  - `UPPER(TRIM(financial_status))` (Shopify payment semantics; Amazon is usually NULL)

- **`fulfillment_status_canon`**
  - if `source_system = 'amazon'` → uses Amazon `order_status` (shipping-ish)
  - else if Shopify `fulfillment_status` exists → `UPPER(TRIM(fulfillment_status))`
  - else NULL

**Why:** prevents mixing *paid/unpaid* with *shipped/unshipped* into one messy field.

---

### 5) `money_standardized` (NUMERIC + rounding)
Creates consistent numeric fields used in KPIs:

- `order_total_num = ROUND(CAST(order_total AS NUMERIC), 2)`
- `subtotal_price_num = ROUND(CAST(subtotal_price AS NUMERIC), 2)`
- `total_discounts_num = ROUND(CAST(total_discounts AS NUMERIC), 2)`
- `total_tax_num = ROUND(CAST(total_tax AS NUMERIC), 2)`

**Why:** consistent money type + stable rounding across sources.

---

### 6) `geo_standardized` (final currency + country format)
Final normalization (even though staging already cleans most of it):

- `currency_canon = UPPER(NULLIF(TRIM(currency), ''))`  *(ISO-3 or NULL)*
- `shipping_country_canon = UPPER(NULLIF(TRIM(shipping_country), ''))` *(ISO-2 or NULL)*

---

### 7) `business_flags`
Adds analytics-friendly flags (don’t delete rows; **flag** them):

- `is_test_order = (source_system = 'shopify' AND is_test = TRUE)`
- `is_cancelled = (order_status_canon = 'CANCELLED')`
- `is_refund_or_return = (order_status_canon IN ('REFUNDED','RETURNED'))`

---

### 8) `dq_rollup` (DQ flags + single rollup boolean)
Creates canonical DQ flags that marts can use consistently:

- `dq_global_order_id_is_null`
- `dq_order_id_is_null`
- `dq_order_created_at_is_null`
- `dq_order_total_is_null` (based on `order_total_num`)
- `dq_order_total_is_negative`
- `dq_currency_invalid_format` (not matching `^[A-Z]{3}$`)
- `dq_shipping_country_invalid_format` (not matching `^[A-Z]{2}$`)

And one combined flag:
- `dq_has_issue` = TRUE if **any** critical DQ condition is true.

---

### 9) Final select: create “best event timestamp” for analytics
Adds a unified timestamp/date you can use for event-based KPIs:

- `order_event_ts = COALESCE(order_created_at, order_processed_at, order_updated_at)`
- `order_event_date = DATE(order_event_ts)`

---

## Key outputs added by this step
- `rn` (dedupe rank, kept for transparency)
- `global_customer_id`
- `order_status_canon`, `payment_status`, `fulfillment_status_canon`
- `order_total_num`, `subtotal_price_num`, `total_discounts_num`, `total_tax_num`
- `currency_canon`, `shipping_country_canon`
- `is_test_order`, `is_cancelled`, `is_refund_or_return`
- `dq_*` flags + `dq_has_issue`
- `order_event_ts`, `order_event_date`

---
---

# Marts built on `int_orders_canonical`

All marts below are built on top of `int_orders_canonical` (via `mart_fact_orders`).  
The design pattern is:

- **Canonical intermediate (`int_orders_canonical`)**: enforce grain + business rules + DQ flags
- **Fact (`mart_fact_orders`)**: “analytics-ready” order-level table (adds revenue rule fields)
- **KPI marts (`mart_kpi_*`)**: pre-aggregations for fast dashboards (daily / by country / by status)
- **Customer dim (`mart_dim_customers`)**: one row per customer with lifetime metrics
- **Freshness/lag mart (optional)**: pipeline timeliness monitoring

---

## 1) `mart_fact_orders` (order-level analytics fact + revenue rule)

### Purpose
Creates the **final order-level fact table** used by downstream KPI marts and BI.  
It mostly *selects through* fields from `int_orders_canonical`, and adds a consistent **revenue eligibility rule** so all KPIs use the same definition of “revenue”.

### Storage optimizations
- Materialized as: `analytics_500550161.mart_fact_orders`
- **Partitioned by:** `order_date`
- **Clustered by:** `source_system`, `global_customer_id`

### What it includes
This fact table keeps:

**Keys**
- `global_order_id`, `source_system`, `order_id`, `global_customer_id`
- (optionally) `customer_id`, `email` for traceability

**Dates/timestamps**
- `order_date`
- `order_created_at`, `order_processed_at`, `order_updated_at`
- `order_event_ts`, `order_event_date`
- `ingest_date`, `report_date` (when available)

**Statuses (raw + canonical)**
- `order_status`, `financial_status`, `fulfillment_status`
- `order_status_canon`, `payment_status`, `fulfillment_status_canon`

**Geo / currency**
- `currency`, `currency_canon`
- `shipping_country`, `shipping_country_canon`
- `fulfillment_channel`

**Money**
- raw money fields: `order_total`, `subtotal_price`, `total_discounts`, `total_tax`
- standardized numeric money fields: `order_total_num`, `subtotal_price_num`, `total_discounts_num`, `total_tax_num`

**Flags + DQ**
- `is_test`, `tags`, `is_test_order`, `is_cancelled`, `is_refund_or_return`
- DQ flags: `dq_*` and `dq_has_issue`

### Revenue definition (the main “business logic” added here)
You convert your revenue definition into two fields:

1) **`is_revenue_eligible`**
An order is revenue-eligible only if:
- not a test order
- not cancelled
- not refunded/returned
- `order_total_num` is present
- `order_total_num` is not negative

You use `COALESCE(flag, FALSE)` so NULL flags don’t break logic.

2) **`revenue_amount`**
- if revenue-eligible → `revenue_amount = order_total_num`
- else → `revenue_amount = 0`

**Why this matters**
- Prevents accidental revenue inflation in dashboards (cancelled/refund/test won’t count)
- Makes all KPI marts consistent (they just sum `revenue_amount` and count `is_revenue_eligible`)
- Keeps the original fields but provides a “business-safe” metric layer

**Source**
- built from `analytics_500550161.int_orders_canonical`
- includes safety condition: `WHERE rn IS NULL OR rn = 1`

---

## 2) `mart_kpi_orders_daily_currency` (daily KPIs by source + currency)

### Purpose
Creates a fast, dashboard-friendly **daily KPI summary** at grain:

> `order_date × source_system × currency_canon`

This prevents incorrect “mixed currency” totals and avoids expensive repeated scans of the raw fact.

### Storage optimizations
- Materialized as: `analytics_500550161.mart_kpi_orders_daily_currency`
- **Partitioned by:** `order_date`
- **Clustered by:** `source_system`, `currency_canon`

### Metrics produced (per day, per source, per currency)
- `rows_in_fact` = `COUNT(*)` (sanity check volume)
- `orders_total` = `COUNT(DISTINCT global_order_id)`
- `customers_total` = `COUNT(DISTINCT global_customer_id)`
- `orders_revenue_eligible` = `COUNTIF(is_revenue_eligible)`
- `revenue` = `SUM(revenue_amount)`
- `aov` = `SAFE_DIVIDE(SUM(revenue_amount), NULLIF(COUNTIF(is_revenue_eligible), 0))`
- `cancelled_orders` = `COUNTIF(COALESCE(is_cancelled, FALSE))`
- `refunded_or_returned_orders` = `COUNTIF(COALESCE(is_refund_or_return, FALSE))`
- `dq_issue_rate` = `SAFE_DIVIDE(COUNTIF(COALESCE(dq_has_issue, FALSE)), NULLIF(COUNT(*), 0))`

**Source**
- `FROM analytics_500550161.mart_fact_orders`
- `GROUP BY order_date, source_system, currency_canon`

---

## 3) `mart_kpi_orders_country_daily_currency` (daily KPIs by country + currency)

### Purpose
Geo breakdown KPI mart at grain:

> `order_date × source_system × shipping_country_canon × currency_canon`

Used for questions like:
- “Revenue by country over time”
- “Top shipping destinations”
- “AOV by country”
…and remains currency-safe.

### Storage optimizations
- Materialized as: `analytics_500550161.mart_kpi_orders_country_daily_currency`
- **Partitioned by:** `order_date`
- **Clustered by:** `source_system`, `shipping_country_canon`, `currency_canon`

### Metrics produced
- `orders_total` = `COUNT(DISTINCT global_order_id)`
- `customers_total` = `COUNT(DISTINCT global_customer_id)`
- `orders_revenue_eligible` = `COUNTIF(is_revenue_eligible)`
- `revenue` = `SUM(revenue_amount)`
- `aov` = `SAFE_DIVIDE(SUM(revenue_amount), NULLIF(COUNTIF(is_revenue_eligible), 0))`

**Source**
- `FROM analytics_500550161.mart_fact_orders`
- `GROUP BY order_date, source_system, shipping_country_canon, currency_canon`

---

## 4) `mart_kpi_orders_status_daily` (daily order counts by canonical status)

### Purpose
Operational/funnel monitoring mart at grain:

> `order_date × source_system × order_status_canon`

Good for:
- cancellation spikes
- pending backlog
- shipped/unshipped trends
- general lifecycle monitoring

### Storage optimizations
- Materialized as: `analytics_500550161.mart_kpi_orders_status_daily`
- **Partitioned by:** `order_date`
- **Clustered by:** `source_system`, `order_status_canon`

### Metrics produced
- `orders` = `COUNT(DISTINCT global_order_id)`
- `customers` = `COUNT(DISTINCT global_customer_id)`
- `revenue_eligible_only` = `SUM(revenue_amount)`
  - (since cancelled/refund/test already contribute 0, this stays consistent)

**Source**
- `FROM analytics_500550161.mart_fact_orders`
- `GROUP BY order_date, source_system, order_status_canon`

---

## 5) `mart_dim_customers` (customer dimension with lifetime metrics)

### Purpose
Creates a **customer-level dimension table** with lifetime metrics and currency attributes.
Grain:

> `global_customer_id × source_system`

(kept per source because customer IDs are source-specific; you already prevent collisions using `global_customer_id`)

### Storage optimizations
- Materialized as: `analytics_500550161.mart_dim_customers`
- **Clustered by:** `source_system`

### Step-by-step logic
**A) `base`**
Selects customer/order attributes from the fact table:
- `global_customer_id`, `source_system`, `email`, `currency_canon`, `order_date`, `is_revenue_eligible`, `revenue_amount`
- filters out NULL customers: `WHERE global_customer_id IS NOT NULL`

**B) `agg` (main lifetime metrics)**
Per customer:
- `any_email` = `ANY_VALUE(email)` (keeps one if available)
- `first_order_date_any` = `MIN(order_date)` (includes test/cancel/refund)
- `last_order_date_any` = `MAX(order_date)` (includes test/cancel/refund)
- `first_order_date_revenue_eligible` = `MIN(IF(is_revenue_eligible, order_date, NULL))`
- `last_order_date_revenue_eligible` = `MAX(IF(is_revenue_eligible, order_date, NULL))`
- `lifetime_orders_revenue_eligible` = `COUNTIF(is_revenue_eligible)`
- `lifetime_revenue` = `SUM(revenue_amount)` (safe because excluded orders contribute 0)

**C) `currency_counts`**
Counts how many revenue-eligible orders a customer placed in each currency:
- `eligible_orders_in_currency = COUNTIF(is_revenue_eligible)`
- grouped by customer + currency

**D) `primary_currency`**
Chooses the most frequent currency per customer (among eligible orders):
- uses `ROW_NUMBER()` ordered by `eligible_orders_in_currency DESC, currency_canon`

**E) `multi_currency`**
Counts how many distinct currencies the customer used (eligible orders only):
- `num_currencies_used = COUNTIF(eligible_orders_in_currency > 0)`

**Final select adds**
- `primary_currency`
- `num_currencies_used`
- `is_multi_currency_customer = (num_currencies_used > 1)`
- `days_since_last_order_any = DATE_DIFF(CURRENT_DATE(), last_order_date_any, DAY)`

---

## 6) `mart_kpi_freshness_lag` (optional: pipeline freshness & lateness monitoring)

### Purpose
Pipeline observability mart at grain:

> `ingest_date × source_system`

Answers:
- “How fresh is today’s data load?”
- “Are we backfilling older orders?”
- “Did ingestion stall or delay?”

### Storage optimizations
- Materialized as: `analytics_500550161.mart_kpi_freshness_lag`
- **Partitioned by:** `ingest_date`
- **Clustered by:** `source_system`

### Metrics produced
- `rows_loaded` = `COUNT(*)` (volume sanity check)
- `orders_loaded` = `COUNT(DISTINCT global_order_id)`
- `avg_lag_days` = `AVG(DATE_DIFF(ingest_date, order_date, DAY))`
- `max_lag_days` = `MAX(DATE_DIFF(ingest_date, order_date, DAY))`
- `late_orders_gt_2d` = `COUNTIF(DATE_DIFF(ingest_date, order_date, DAY) > 2)`
- `late_rate_gt_2d` = `SAFE_DIVIDE(late_orders_gt_2d, NULLIF(COUNT(*), 0))`

**Filters**
- only rows with both dates present:
  - `WHERE ingest_date IS NOT NULL AND order_date IS NOT NULL`

**Source**
- `FROM analytics_500550161.mart_fact_orders`
- `GROUP BY ingest_date, source_system`

---
