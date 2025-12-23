-- ================================================
-- Example 3: Create AggregatingMergeTree Table
-- ================================================
-- KEY CONCEPT: AggregateFunction columns store intermediate aggregate STATE
-- This enables COUNT DISTINCT (uniq), AVG, and other complex aggregations
-- that SummingMergeTree cannot handle
-- ================================================

USE mv_demo_aggregating;

-- ================================================
-- Target Table: Sales metrics with AggregateFunction columns
-- ================================================
DROP TABLE IF EXISTS hourly_sales;

CREATE TABLE hourly_sales (
    hour DateTime,
    category String,

    -- AggregateFunction columns - store STATE, not final values!
    order_count AggregateFunction(count, UUID),
    total_revenue AggregateFunction(sum, Decimal64(2)),
    avg_order_value AggregateFunction(avg, Decimal64(2)),
    unique_customers AggregateFunction(uniq, UInt64)  -- COUNT DISTINCT!
)
ENGINE = AggregatingMergeTree()
ORDER BY (hour, category);

-- ================================================
-- TALKING POINT
-- ================================================
-- "Notice the column types: AggregateFunction(count, UUID), AggregateFunction(sum, ...), etc.
-- These columns don't store numbers - they store INTERMEDIATE STATE.
-- This state can be merged incrementally without losing accuracy.
-- This is how we achieve COUNT DISTINCT across multiple INSERT batches!"

SELECT '[OK] AggregatingMergeTree table created: hourly_sales' AS status;
SELECT 'Columns store aggregate STATE, not final values' AS key_concept;
