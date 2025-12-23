-- ================================================
-- Example 3: Demo Queries
-- ================================================
-- Demonstrates State/Merge pattern and correct COUNT DISTINCT
-- ================================================

USE mv_demo_aggregating;

-- ================================================
-- Step 1: Insert first batch of orders
-- ================================================
SELECT '--- Step 1: Inserting first batch (5 orders, 4 customers) ---' AS step;

INSERT INTO orders_raw (customer_id, product_category, quantity, amount) VALUES
    (1001, 'Electronics', 1, 299.99),
    (1002, 'Electronics', 2, 149.99),
    (1001, 'Electronics', 1, 49.99),   -- Same customer 1001!
    (1003, 'Clothing', 3, 89.99),
    (1004, 'Clothing', 1, 129.99);

-- ================================================
-- Step 2: Query with Merge functions
-- ================================================
SELECT '--- Step 2: Query with *Merge() functions ---' AS step;

SELECT
    hour,
    category,
    countMerge(order_count) AS orders,
    round(sumMerge(total_revenue), 2) AS revenue,
    round(avgMerge(avg_order_value), 2) AS avg_order,
    uniqMerge(unique_customers) AS unique_customers
FROM hourly_sales
GROUP BY hour, category
ORDER BY category;

-- ================================================
-- TALKING POINT
-- ================================================
-- "Notice unique_customers correctly shows:
--   Electronics: 2 customers (1001, 1002) - even though 1001 ordered twice!
--   Clothing: 2 customers (1003, 1004)
--
-- This is the power of uniqState/uniqMerge - true COUNT DISTINCT!"

-- ================================================
-- Step 3: Insert second batch with OVERLAPPING customers
-- ================================================
SELECT '--- Step 3: Inserting second batch (same customers!) ---' AS step;

INSERT INTO orders_raw (customer_id, product_category, quantity, amount) VALUES
    (1001, 'Electronics', 1, 199.99),   -- Customer 1001 again!
    (1005, 'Electronics', 1, 79.99),    -- New customer
    (1003, 'Clothing', 2, 59.99);       -- Customer 1003 again!

-- ================================================
-- Step 4: Observe correct COUNT DISTINCT across batches
-- ================================================
SELECT '--- Step 4: After second INSERT (correct unique count!) ---' AS step;

SELECT
    hour,
    category,
    countMerge(order_count) AS orders,
    round(sumMerge(total_revenue), 2) AS revenue,
    round(avgMerge(avg_order_value), 2) AS avg_order,
    uniqMerge(unique_customers) AS unique_customers
FROM hourly_sales
GROUP BY hour, category
ORDER BY category;

-- ================================================
-- KEY INSIGHT
-- ================================================
SELECT '--- Key Insight: Correct COUNT DISTINCT ---' AS step;

SELECT
    'Electronics' AS category,
    '5 orders total' AS orders,
    '3 unique customers (1001, 1002, 1005)' AS unique_customers,
    'Customer 1001 ordered 3 times but counted once!' AS explanation
UNION ALL
SELECT
    'Clothing' AS category,
    '3 orders total' AS orders,
    '2 unique customers (1003, 1004)' AS unique_customers,
    'Customer 1003 ordered 2 times but counted once!' AS explanation;

-- ================================================
-- TALKING POINT
-- ================================================
-- "This is IMPOSSIBLE with SummingMergeTree!
-- SummingMergeTree would just add the counts from each batch.
-- AggregatingMergeTree with uniqState/uniqMerge correctly deduplicates."

-- ================================================
-- Step 5: Compare State vs regular aggregation
-- ================================================
SELECT '--- Comparison: State/Merge vs Raw Query ---' AS step;

-- Query 1: From AggregatingMergeTree (fast, pre-aggregated)
SELECT
    'AggregatingMergeTree' AS source,
    category,
    countMerge(order_count) AS orders,
    uniqMerge(unique_customers) AS unique_customers
FROM hourly_sales
GROUP BY category
ORDER BY category;

-- Query 2: From raw data (slower, full scan)
SELECT
    'Raw orders_raw' AS source,
    product_category AS category,
    count() AS orders,
    uniq(customer_id) AS unique_customers
FROM orders_raw
GROUP BY category
ORDER BY category;

-- Both should match!

-- ================================================
-- TRANSITION TO EXAMPLE 4
-- ================================================
SELECT '--- Transition to Medallion Architecture ---' AS step;

SELECT
    'You now understand the building blocks' AS summary,
    'Example 1: MVs are INSERT triggers' AS concept_1,
    'Example 2: SummingMergeTree auto-sums' AS concept_2,
    'Example 3: AggregatingMergeTree + State/Merge for complex aggregations' AS concept_3,
    'Next: Combine all patterns in Medallion Architecture!' AS next_step;

SELECT '[OK] Demo complete. Next: Example 4 - Medallion Architecture' AS status;
