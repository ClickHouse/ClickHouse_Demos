-- ================================================
-- JOIN vs dictGet() Comparison
-- ================================================
-- Demonstrates why dictionaries are preferred for dimension lookups
-- ================================================

USE mv_demo_dictionaries;

-- ================================================
-- Insert Sample Orders
-- ================================================

INSERT INTO orders_raw (customer_id, product_id, quantity) VALUES
    (1001, 1, 1),   -- Alice buys Laptop
    (1001, 2, 2),   -- Alice buys 2 Wireless Mice
    (1002, 4, 1),   -- Bob buys Keyboard
    (1003, 5, 2),   -- Carol buys 2 Monitors
    (1003, 6, 1),   -- Carol buys Desk Chair
    (1004, 3, 3),   -- David buys 3 USB-C Hubs
    (1005, 1, 1),   -- Eva buys Laptop
    (1005, 9, 1),   -- Eva buys Headphones
    (1006, 7, 1),   -- Frank buys Standing Desk
    (1007, 1, 2);   -- Grace buys 2 Laptops

SELECT '[OK] Sample orders inserted' AS status;
SELECT 'Orders count:' AS info, count() AS count FROM orders_raw;

-- ================================================
-- Method 1: Traditional JOIN (slower)
-- ================================================

SELECT '-- Method 1: JOIN approach:' AS info;

SELECT
    o.order_id,
    c.customer_name,
    c.tier AS customer_tier,
    p.product_name,
    p.category,
    o.quantity,
    p.unit_price,
    o.quantity * p.unit_price AS total_amount
FROM orders_raw o
JOIN dim_customers c ON o.customer_id = c.customer_id
JOIN dim_products p ON o.product_id = p.product_id
ORDER BY o.order_time DESC
LIMIT 5;

-- ================================================
-- Method 2: dictGet() (faster, preferred)
-- ================================================

SELECT '-- Method 2: dictGet() approach:' AS info;

SELECT
    order_id,
    dictGet('customers_dict', 'customer_name', customer_id) AS customer_name,
    dictGet('customers_dict', 'tier', customer_id) AS customer_tier,
    dictGet('products_dict', 'product_name', product_id) AS product_name,
    dictGet('products_dict', 'category', product_id) AS category,
    quantity,
    dictGet('products_dict', 'unit_price', product_id) AS unit_price,
    quantity * dictGet('products_dict', 'unit_price', product_id) AS total_amount
FROM orders_raw
ORDER BY order_time DESC
LIMIT 5;

-- ================================================
-- Why dictGet() is Better
-- ================================================
-- 1. O(1) lookup vs O(n) or O(log n) for JOINs
-- 2. No shuffle/sort operations needed
-- 3. Dictionary stays in memory - instant access
-- 4. Works seamlessly in Materialized Views
-- 5. Reduces query complexity
-- ================================================

SELECT '[OK] JOIN vs dictGet comparison complete' AS status;
