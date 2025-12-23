-- ================================================
-- Example 3: Create MV with State Functions
-- ================================================
-- KEY CONCEPT: Use *State() functions when inserting,
--              Use *Merge() functions when querying
-- ================================================

USE mv_demo_aggregating;

-- ================================================
-- MV: Transform orders into hourly sales with State functions
-- ================================================
DROP VIEW IF EXISTS hourly_sales_mv;

CREATE MATERIALIZED VIEW hourly_sales_mv
TO hourly_sales
AS
SELECT
    toStartOfHour(order_time) AS hour,
    product_category AS category,

    -- STATE functions: create intermediate aggregate state
    countState(order_id) AS order_count,       -- NOT count()!
    sumState(amount) AS total_revenue,         -- NOT sum()!
    avgState(amount) AS avg_order_value,       -- NOT avg()!
    uniqState(customer_id) AS unique_customers -- NOT uniq()!
FROM orders_raw
GROUP BY hour, category;

-- ================================================
-- TALKING POINT
-- ================================================
-- "Notice the *State suffix on every aggregation function:
--   countState() instead of count()
--   sumState() instead of sum()
--   avgState() instead of avg()
--   uniqState() instead of uniq()
--
-- These create MERGEABLE state that can be combined across batches.
-- When querying, we'll use *Merge() to finalize the results."

SELECT '[OK] State MV created: hourly_sales_mv -> hourly_sales' AS status;
SELECT 'Use *State() for insert, *Merge() for query' AS pattern;
