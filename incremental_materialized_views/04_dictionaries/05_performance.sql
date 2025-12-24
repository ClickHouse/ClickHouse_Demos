-- ================================================
-- Example 4: Performance Comparison
-- ================================================
-- Compare: dictGet() O(1) lookup vs JOIN operation
-- Goal: Show identical results with different performance characteristics
-- ================================================

USE mv_demo_dictionaries;

-- ================================================
-- Step 1: Insert large dataset
-- ================================================
SELECT '##############################################################################' AS _;
SELECT '#                  EXAMPLE 4: DICTIONARY PERFORMANCE TEST                   #' AS _;
SELECT '##############################################################################' AS _;

SELECT 'Inserting 500,000 orders for performance test...' AS status;

INSERT INTO orders_raw (customer_id, product_id, quantity)
SELECT
    1001 + (rand() % 10) AS customer_id,
    1 + (rand() % 10) AS product_id,
    (rand() % 5) + 1 AS quantity
FROM numbers(500000)
SETTINGS max_block_size = 100000;

SELECT 'Done. Total orders: ' || toString(count()) AS status FROM orders_raw;

-- ================================================
-- Dictionary Status
-- ================================================
SELECT '============================================================================' AS _;
SELECT '                       DICTIONARY STATUS                                   ' AS _;
SELECT '============================================================================' AS _;

SELECT
    name AS dictionary,
    status,
    element_count AS entries,
    formatReadableSize(bytes_allocated) AS memory_used
FROM system.dictionaries
WHERE database = 'mv_demo_dictionaries'
ORDER BY name;

SYSTEM FLUSH LOGS;

-- ================================================
-- Test 1: Enrichment Query (100K rows)
-- ================================================
SELECT '============================================================================' AS _;
SELECT '        QUERY: Enrich Orders with Customer & Product Info (100K rows)      ' AS _;
SELECT '============================================================================' AS _;

-- Method 1: dictGet()
SELECT '>>> APPROACH A: dictGet() - O(1) hash lookup per row' AS _;
SELECT '    Dictionaries are pre-loaded in memory' AS _;
SELECT
    customer_id,
    dictGet('mv_demo_dictionaries.customers_dict', 'customer_name', toUInt64(customer_id)) AS customer_name,
    dictGet('mv_demo_dictionaries.customers_dict', 'tier', toUInt64(customer_id)) AS customer_tier,
    product_id,
    dictGet('mv_demo_dictionaries.products_dict', 'product_name', toUInt32(product_id)) AS product_name,
    dictGet('mv_demo_dictionaries.products_dict', 'category', toUInt32(product_id)) AS category,
    quantity,
    round(quantity * dictGet('mv_demo_dictionaries.products_dict', 'unit_price', toUInt32(product_id)), 2) AS total_amount
FROM orders_raw
LIMIT 5;


-- Method 2: JOIN
SELECT '>>> APPROACH B: JOIN - Must scan dimension tables' AS _;
SELECT '    Dimension tables read from disk for each query' AS _;
SELECT
    o.customer_id,
    c.customer_name,
    c.tier AS customer_tier,
    o.product_id,
    p.product_name,
    p.category,
    o.quantity,
    round(o.quantity * p.unit_price, 2) AS total_amount
FROM orders_raw o
JOIN dim_customers c ON o.customer_id = c.customer_id
JOIN dim_products p ON o.product_id = p.product_id
LIMIT 5;

SELECT '(Results are identical - both approaches return the same enriched data)' AS _;

-- Now run the full 100K for performance measurement (FORMAT Null = don't display)
SELECT 'Running full 100K enrichment for performance measurement...' AS _;

SELECT
    customer_id,
    dictGet('mv_demo_dictionaries.customers_dict', 'customer_name', toUInt64(customer_id)) AS customer_name,
    dictGet('mv_demo_dictionaries.customers_dict', 'tier', toUInt64(customer_id)) AS customer_tier,
    product_id,
    dictGet('mv_demo_dictionaries.products_dict', 'product_name', toUInt32(product_id)) AS product_name,
    dictGet('mv_demo_dictionaries.products_dict', 'category', toUInt32(product_id)) AS category,
    quantity,
    quantity * dictGet('mv_demo_dictionaries.products_dict', 'unit_price', toUInt32(product_id)) AS total_amount
FROM orders_raw
LIMIT 100000
FORMAT Null;

SELECT
    o.customer_id,
    c.customer_name,
    c.tier AS customer_tier,
    o.product_id,
    p.product_name,
    p.category,
    o.quantity,
    o.quantity * p.unit_price AS total_amount
FROM orders_raw o
JOIN dim_customers c ON o.customer_id = c.customer_id
JOIN dim_products p ON o.product_id = p.product_id
LIMIT 100000
FORMAT Null;

SYSTEM FLUSH LOGS;

-- ================================================
-- Performance Comparison: Enrichment
-- ================================================
SELECT '============================================================================' AS _;
SELECT '              PERFORMANCE: Enrichment Query (100K rows)                    ' AS _;
SELECT '============================================================================' AS _;
SELECT '+---------------------------+------------+---------------+--------------+' AS _;
SELECT '| Approach                  | Rows Read  | Data Scanned  | Query Time   |' AS _;
SELECT '+---------------------------+------------+---------------+--------------+' AS _;

-- Get the most recent query for each approach
WITH ranked AS (
    SELECT
        CASE
            WHEN query LIKE '%dictGet%' AND query NOT LIKE '%JOIN%' THEN 'dictGet() - O(1) lookup'
            WHEN query LIKE '%JOIN dim_customers%' THEN 'JOIN - table scan'
            ELSE 'Other'
        END AS approach,
        read_rows,
        read_bytes,
        query_duration_ms,
        row_number() OVER (PARTITION BY
            CASE
                WHEN query LIKE '%dictGet%' AND query NOT LIKE '%JOIN%' THEN 1
                WHEN query LIKE '%JOIN dim_customers%' THEN 2
                ELSE 3
            END
            ORDER BY event_time DESC
        ) AS rn
    FROM system.query_log
    WHERE
        type = 'QueryFinish'
        AND query_kind = 'Select'
        AND event_date = today()
        AND query LIKE '%orders_raw%'
        AND query LIKE '%customer_name%'
        AND query LIKE '%FORMAT Null%'
        AND query NOT LIKE '%system.query_log%'
)
SELECT
    '| ' || leftPad(approach, 25) || ' | ' ||
    leftPad(toString(read_rows), 10) || ' | ' ||
    leftPad(formatReadableSize(read_bytes), 13) || ' | ' ||
    leftPad(toString(round(query_duration_ms, 1)) || ' ms', 12) || ' |' AS _
FROM ranked
WHERE rn = 1 AND approach != 'Other'
ORDER BY
    CASE approach
        WHEN 'JOIN - table scan' THEN 1
        ELSE 2
    END;

SELECT '+---------------------------+------------+---------------+--------------+' AS _;

-- ================================================
-- Test 2: Aggregation with Enrichment
-- ================================================
SELECT '============================================================================' AS _;
SELECT '              QUERY: Revenue by Category (with enrichment)                 ' AS _;
SELECT '============================================================================' AS _;

SYSTEM FLUSH LOGS;

-- Method 1: dictGet()
SELECT '>>> APPROACH A: dictGet() for category lookup' AS _;
SELECT
    dictGet('mv_demo_dictionaries.products_dict', 'category', toUInt32(product_id)) AS category,
    count() AS order_count,
    sum(quantity) AS total_quantity,
    round(sum(quantity * dictGet('mv_demo_dictionaries.products_dict', 'unit_price', toUInt32(product_id))), 2) AS revenue
FROM orders_raw
GROUP BY category
ORDER BY revenue DESC;


-- Method 2: JOIN
SELECT '>>> APPROACH B: JOIN for category lookup' AS _;
SELECT
    p.category,
    count() AS order_count,
    sum(o.quantity) AS total_quantity,
    round(sum(o.quantity * p.unit_price), 2) AS revenue
FROM orders_raw o
JOIN dim_products p ON o.product_id = p.product_id
GROUP BY p.category
ORDER BY revenue DESC;

SYSTEM FLUSH LOGS;

-- ================================================
-- Performance Comparison: Aggregation
-- ================================================
SELECT '============================================================================' AS _;
SELECT '                 PERFORMANCE: Aggregation Query                            ' AS _;
SELECT '============================================================================' AS _;
SELECT '+---------------------------+------------+---------------+--------------+' AS _;
SELECT '| Approach                  | Rows Read  | Data Scanned  | Query Time   |' AS _;
SELECT '+---------------------------+------------+---------------+--------------+' AS _;

-- Get the most recent query for each approach
WITH ranked AS (
    SELECT
        CASE
            WHEN query LIKE '%dictGet%' AND query LIKE '%GROUP BY category%' THEN 'dictGet() aggregation'
            WHEN query LIKE '%JOIN dim_products%' AND query LIKE '%GROUP BY p.category%' THEN 'JOIN aggregation'
            ELSE 'Other'
        END AS approach,
        read_rows,
        read_bytes,
        query_duration_ms,
        row_number() OVER (PARTITION BY
            CASE
                WHEN query LIKE '%dictGet%' AND query LIKE '%GROUP BY category%' THEN 1
                WHEN query LIKE '%JOIN dim_products%' AND query LIKE '%GROUP BY p.category%' THEN 2
                ELSE 3
            END
            ORDER BY event_time DESC
        ) AS rn
    FROM system.query_log
    WHERE
        type = 'QueryFinish'
        AND query_kind = 'Select'
        AND event_date = today()
        AND query LIKE '%revenue%'
        AND query LIKE '%ORDER BY revenue%'
        AND query NOT LIKE '%system.query_log%'
        AND query NOT LIKE '%FORMAT Null%'
)
SELECT
    '| ' || leftPad(approach, 25) || ' | ' ||
    leftPad(toString(read_rows), 10) || ' | ' ||
    leftPad(formatReadableSize(read_bytes), 13) || ' | ' ||
    leftPad(toString(round(query_duration_ms, 1)) || ' ms', 12) || ' |' AS _
FROM ranked
WHERE rn = 1 AND approach != 'Other'
ORDER BY
    CASE approach
        WHEN 'JOIN aggregation' THEN 1
        ELSE 2
    END;

SELECT '+---------------------------+------------+---------------+--------------+' AS _;

-- ================================================
-- Key Takeaway
-- ================================================
SELECT '============================================================================' AS _;
SELECT '                           KEY TAKEAWAY                                    ' AS _;
SELECT '============================================================================' AS _;
SELECT 'dictGet() vs JOIN:' AS _;
SELECT '  - dictGet(): Dictionary loaded ONCE into memory, O(1) hash lookup' AS _;
SELECT '  - JOIN:      Dimension table scanned for EVERY query' AS _;
SELECT 'When to use dictionaries:' AS _;
SELECT '  - Dimension tables (products, customers, stores, etc.)' AS _;
SELECT '  - Foreign key lookups in Materialized Views' AS _;
SELECT '  - Any frequently-accessed lookup data' AS _;
SELECT 'At scale (1M orders x 100K products):' AS _;
SELECT '  - JOIN: Scans 100K products per query' AS _;
SELECT '  - dictGet(): Instant lookup regardless of dimension size' AS _;

SELECT '[OK] Performance comparison complete' AS status;
