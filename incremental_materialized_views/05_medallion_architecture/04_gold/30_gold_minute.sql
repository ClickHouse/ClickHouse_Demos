-- ================================================
-- FastMart Demo: GOLD Layer - Minute Aggregations
-- ================================================
-- Purpose: Real-time aggregations for operational dashboards
-- Pattern: AggregatingMergeTree with State functions
-- Key Feature: Pre-aggregated metrics updated in real-time!
-- ================================================

USE fastmart_demo;

-- ================================================
-- GOLD: Sales by Minute
-- ================================================
-- Pre-aggregated metrics refreshed every minute
-- Powers real-time dashboards with millisecond query times
-- Uses State functions for incremental aggregation

DROP TABLE IF EXISTS sales_by_minute;

CREATE TABLE sales_by_minute (
    minute DateTime,
    category LowCardinality(String),

    -- Aggregate functions stored as state
    total_orders AggregateFunction(count, UUID),
    total_revenue AggregateFunction(sum, Decimal64(2)),
    total_profit AggregateFunction(sum, Decimal64(2)),
    unique_customers AggregateFunction(uniq, UInt64),
    unique_products AggregateFunction(uniq, UInt64),
    avg_order_value AggregateFunction(avg, Decimal64(2)),

    -- Distribution metrics
    min_order_value AggregateFunction(min, Decimal64(2)),
    max_order_value AggregateFunction(max, Decimal64(2))
)
ENGINE = AggregatingMergeTree()
PARTITION BY toYYYYMM(minute)
ORDER BY (minute, category)
TTL minute + INTERVAL 90 DAY  -- Gold minute data: 90 day retention
COMMENT 'Gold layer: 1-minute sales aggregations';

-- ================================================
-- DEMO TALKING POINT #1
-- ================================================
-- "AggregatingMergeTree is the SECRET SAUCE for real-time analytics!
-- It stores pre-aggregated STATE, not final values.
-- This enables:
--  1. Incremental updates (no full recalculation)
--  2. Cascading aggregations (minute → hour → day)
--  3. Blazing fast queries (milliseconds for complex metrics)"

-- ================================================
-- INCREMENTAL MV: Populate sales_by_minute
-- ================================================

DROP VIEW IF EXISTS sales_by_minute_mv;

CREATE MATERIALIZED VIEW sales_by_minute_mv
TO sales_by_minute
AS
SELECT
    toStartOfMinute(order_time) AS minute,
    category,

    -- Use State functions to create aggregate state
    countState(order_id) AS total_orders,
    sumState(total_amount) AS total_revenue,
    sumState(profit_margin) AS total_profit,
    uniqState(customer_id) AS unique_customers,
    uniqState(product_id) AS unique_products,
    avgState(total_amount) AS avg_order_value,

    minState(total_amount) AS min_order_value,
    maxState(total_amount) AS max_order_value
FROM orders_enriched
GROUP BY minute, category;

-- ================================================
-- DEMO TALKING POINT #2
-- ================================================
-- "Notice the 'State' suffix on aggregation functions:
--  - countState() instead of count()
--  - sumState() instead of sum()
--  This stores INCREMENTAL state, not final values.
--  When new orders arrive, the state is MERGED, not recalculated!"

-- ================================================
-- GOLD: Sales by Minute and Brand
-- ================================================

DROP TABLE IF EXISTS sales_by_minute_brand;

CREATE TABLE sales_by_minute_brand (
    minute DateTime,
    category LowCardinality(String),
    brand LowCardinality(String),

    total_orders AggregateFunction(count, UUID),
    total_revenue AggregateFunction(sum, Decimal64(2)),
    unique_customers AggregateFunction(uniq, UInt64)
)
ENGINE = AggregatingMergeTree()
PARTITION BY toYYYYMM(minute)
ORDER BY (minute, category, brand)
TTL minute + INTERVAL 90 DAY
COMMENT 'Gold layer: 1-minute sales by brand';

DROP VIEW IF EXISTS sales_by_minute_brand_mv;

CREATE MATERIALIZED VIEW sales_by_minute_brand_mv
TO sales_by_minute_brand
AS
SELECT
    toStartOfMinute(order_time) AS minute,
    category,
    brand,

    countState(order_id) AS total_orders,
    sumState(total_amount) AS total_revenue,
    uniqState(customer_id) AS unique_customers
FROM orders_enriched
GROUP BY minute, category, brand;

-- ================================================
-- GOLD: Customer Tier Performance by Minute
-- ================================================

DROP TABLE IF EXISTS sales_by_minute_tier;

CREATE TABLE sales_by_minute_tier (
    minute DateTime,
    customer_tier LowCardinality(String),

    total_orders AggregateFunction(count, UUID),
    total_revenue AggregateFunction(sum, Decimal64(2)),
    total_profit AggregateFunction(sum, Decimal64(2)),
    unique_customers AggregateFunction(uniq, UInt64)
)
ENGINE = AggregatingMergeTree()
PARTITION BY toYYYYMM(minute)
ORDER BY (minute, customer_tier)
TTL minute + INTERVAL 90 DAY
COMMENT 'Gold layer: 1-minute sales by customer tier';

DROP VIEW IF EXISTS sales_by_minute_tier_mv;

CREATE MATERIALIZED VIEW sales_by_minute_tier_mv
TO sales_by_minute_tier
AS
SELECT
    toStartOfMinute(order_time) AS minute,
    customer_tier,

    countState(order_id) AS total_orders,
    sumState(total_amount) AS total_revenue,
    sumState(profit_margin) AS total_profit,
    uniqState(customer_id) AS unique_customers
FROM orders_enriched
GROUP BY minute, customer_tier;

-- NOTE: Do NOT insert data here. Hourly/daily MVs must be created first.
-- Sample data will be inserted in 31_gold_hourly.sql after ALL MVs exist.

SELECT '[OK] Gold minute tables and MVs created' AS step;

-- ================================================
-- Query examples (will show data after 31_gold_hourly.sql runs)
-- ================================================

SELECT '--- Real-Time Dashboard Query Example ---' AS section;

SELECT
    minute,
    category,
    countMerge(total_orders) AS orders,
    round(sumMerge(total_revenue), 2) AS revenue,
    round(sumMerge(total_profit), 2) AS profit,
    uniqMerge(unique_customers) AS customers,
    uniqMerge(unique_products) AS products,
    round(avgMerge(avg_order_value), 2) AS avg_order_val
FROM sales_by_minute
WHERE minute >= now() - INTERVAL 5 MINUTE
GROUP BY minute, category
ORDER BY minute DESC, revenue DESC
LIMIT 10;

-- ================================================
-- DEMO TALKING POINT #3
-- ================================================
-- "Notice the 'Merge' suffix when querying:
--  - countMerge() to get final count from countState()
--  - sumMerge() to get final sum from sumState()
--  This is FAST because we're just merging pre-aggregated state!
--  Query time: single-digit milliseconds for complex metrics."

-- ================================================
-- Performance comparison
-- ================================================

SELECT '--- Performance: Aggregated vs Raw ---' AS section;

-- Query 1: From Gold (pre-aggregated) - FAST
SELECT
    'Gold layer (pre-aggregated)' AS source,
    count() AS metric_rows
FROM (
    SELECT
        minute,
        category,
        sumMerge(total_revenue) AS revenue
    FROM sales_by_minute
    WHERE minute >= now() - INTERVAL 1 HOUR
    GROUP BY minute, category
);

-- Query 2: From Silver (raw orders) - SLOWER
SELECT
    'Silver layer (raw orders)' AS source,
    count() AS rows_scanned
FROM (
    SELECT
        toStartOfMinute(order_time) AS minute,
        category,
        sum(total_amount) AS revenue
    FROM orders_enriched
    WHERE order_time >= now() - INTERVAL 1 HOUR
    GROUP BY minute, category
);

-- ================================================
-- DEMO TALKING POINT #4
-- ================================================
-- "Expected speedup: 10-100x depending on data volume
-- Why so fast?
--  1. Pre-aggregated: No aggregation at query time
--  2. Smaller dataset: Minutes instead of individual orders
--  3. Optimized storage: AggregatingMergeTree compression
--  4. Incremental: Only new data processed, not full scans"

-- ================================================
-- Business Intelligence Queries
-- ================================================

SELECT '--- Top Categories (Last Hour) ---' AS section;

SELECT
    category,
    countMerge(total_orders) AS orders,
    round(sumMerge(total_revenue), 2) AS revenue,
    round(sumMerge(total_profit), 2) AS profit,
    round((sumMerge(total_profit) / sumMerge(total_revenue)) * 100, 2) AS profit_margin_pct
FROM sales_by_minute
WHERE minute >= now() - INTERVAL 1 HOUR
GROUP BY category
ORDER BY revenue DESC;

SELECT '--- Top Brands (Last Hour) ---' AS section;

SELECT
    brand,
    category,
    countMerge(total_orders) AS orders,
    round(sumMerge(total_revenue), 2) AS revenue
FROM sales_by_minute_brand
WHERE minute >= now() - INTERVAL 1 HOUR
GROUP BY brand, category
ORDER BY revenue DESC
LIMIT 10;

SELECT '--- Customer Tier Performance (Last Hour) ---' AS section;

SELECT
    customer_tier,
    countMerge(total_orders) AS orders,
    round(sumMerge(total_revenue), 2) AS revenue,
    uniqMerge(unique_customers) AS customers,
    round(sumMerge(total_revenue) / uniqMerge(unique_customers), 2) AS revenue_per_customer
FROM sales_by_minute_tier
WHERE minute >= now() - INTERVAL 1 HOUR
GROUP BY customer_tier
ORDER BY revenue DESC;

-- ================================================
-- Monitor Gold layer statistics
-- ================================================

SELECT '--- Gold Layer Table Statistics ---' AS section;

SELECT
    table,
    engine,
    formatReadableSize(sum(bytes)) AS size,
    sum(rows) AS rows,
    count() AS parts
FROM system.parts
WHERE database = 'fastmart_demo'
  AND table LIKE 'sales_by_minute%'
  AND active
GROUP BY table, engine
ORDER BY table;

-- ================================================
-- NEXT STEPS
-- ================================================
SELECT
    'Gold minute aggregations created' AS status,
    'Real-time dashboard metrics ready' AS result,
    'Query time: milliseconds for complex aggregations' AS performance,
    'Next: Create hourly cascading aggregations' AS next_step,
    'File: sql/gold/31_gold_hourly.sql' AS next_file;
