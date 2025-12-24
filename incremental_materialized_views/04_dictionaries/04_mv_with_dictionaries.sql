-- ================================================
-- Materialized Views with Dictionaries
-- ================================================
-- Combine MVs + dictGet() for real-time data enrichment
-- Orders are automatically enriched as they arrive
-- ================================================

USE mv_demo_dictionaries;

-- ================================================
-- Target Table: Enriched Orders
-- ================================================

DROP TABLE IF EXISTS orders_enriched;
CREATE TABLE orders_enriched (
    order_id UUID,
    order_time DateTime,
    -- Customer fields (from dictionary)
    customer_id UInt32,
    customer_name String,
    customer_tier String,
    customer_country String,
    -- Product fields (from dictionary)
    product_id UInt32,
    product_name String,
    category String,
    brand String,
    -- Calculated fields
    quantity UInt32,
    unit_price Decimal64(2),
    unit_cost Decimal64(2),
    total_amount Decimal64(2),
    total_cost Decimal64(2),
    profit Decimal64(2)
)
ENGINE = MergeTree()
ORDER BY (order_time, order_id);

-- ================================================
-- Materialized View: Auto-Enrich Orders
-- ================================================

DROP VIEW IF EXISTS orders_enrichment_mv;
CREATE MATERIALIZED VIEW orders_enrichment_mv
TO orders_enriched
AS
SELECT
    order_id,
    order_time,
    -- Customer enrichment via dictionary
    customer_id,
    dictGet('customers_dict', 'customer_name', customer_id) AS customer_name,
    dictGet('customers_dict', 'tier', customer_id) AS customer_tier,
    dictGet('customers_dict', 'country', customer_id) AS customer_country,
    -- Product enrichment via dictionary
    product_id,
    dictGet('products_dict', 'product_name', product_id) AS product_name,
    dictGet('products_dict', 'category', product_id) AS category,
    dictGet('products_dict', 'brand', product_id) AS brand,
    -- Calculated fields
    quantity,
    dictGet('products_dict', 'unit_price', product_id) AS unit_price,
    dictGet('products_dict', 'unit_cost', product_id) AS unit_cost,
    quantity * dictGet('products_dict', 'unit_price', product_id) AS total_amount,
    quantity * dictGet('products_dict', 'unit_cost', product_id) AS total_cost,
    quantity * (dictGet('products_dict', 'unit_price', product_id) -
                dictGet('products_dict', 'unit_cost', product_id)) AS profit
FROM orders_raw;

SELECT '[OK] Enrichment MV created' AS status;

-- ================================================
-- Test: Insert New Orders
-- ================================================

SELECT '-- Inserting new orders (will auto-enrich):' AS info;

-- New orders to test auto-enrichment
INSERT INTO orders_raw (customer_id, product_id, quantity) VALUES
    (1001, 8, 1),
    (1002, 10, 2),
    (1003, 1, 1),
    (1007, 4, 1),
    (1009, 9, 2);

-- ================================================
-- Query Enriched Data
-- ================================================

SELECT '-- Enriched orders (auto-populated by MV):' AS info;

SELECT
    customer_name,
    customer_tier,
    product_name,
    category,
    quantity,
    unit_price,
    total_amount,
    profit
FROM orders_enriched
ORDER BY order_time DESC
LIMIT 10;

-- ================================================
-- Analytics on Enriched Data
-- ================================================

SELECT '-- Revenue by customer tier:' AS info;

SELECT
    customer_tier,
    count() AS order_count,
    sum(total_amount) AS total_revenue,
    sum(profit) AS total_profit,
    round(avg(total_amount), 2) AS avg_order_value
FROM orders_enriched
GROUP BY customer_tier
ORDER BY total_revenue DESC;

SELECT '-- Revenue by category:' AS info;

SELECT
    category,
    count() AS order_count,
    sum(quantity) AS units_sold,
    sum(total_amount) AS total_revenue,
    sum(profit) AS total_profit,
    round(sum(profit) / sum(total_amount) * 100, 1) AS profit_margin_pct
FROM orders_enriched
GROUP BY category
ORDER BY total_revenue DESC;

SELECT '[OK] MV with dictionaries demo complete' AS status;
