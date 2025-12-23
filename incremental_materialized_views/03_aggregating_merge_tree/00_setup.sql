-- ================================================
-- Example 3: AggregatingMergeTree with State/Merge Pattern
-- ================================================
-- Scenario: E-commerce metrics with unique customers
-- Key Concept: State functions store intermediate aggregate state,
--              enabling COUNT DISTINCT and AVG across batches
-- ================================================

-- Create database
CREATE DATABASE IF NOT EXISTS mv_demo_aggregating;
USE mv_demo_aggregating;

-- ================================================
-- Source Table: Raw orders
-- ================================================
DROP TABLE IF EXISTS orders_raw;

CREATE TABLE orders_raw (
    order_id UUID DEFAULT generateUUIDv4(),
    customer_id UInt64,
    product_category String,
    quantity UInt32,
    amount Decimal64(2),
    order_time DateTime DEFAULT now()
)
ENGINE = MergeTree()
ORDER BY (order_time, order_id);

SELECT '[OK] Source table created: orders_raw' AS status;
