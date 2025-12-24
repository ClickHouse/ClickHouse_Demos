-- ================================================
-- Example 3: Performance Comparison
-- ================================================
-- Compare: AggregatingMergeTree with State/Merge vs Raw COUNT DISTINCT
-- Goal: Show identical results with massive performance difference
-- ================================================

USE mv_demo_aggregating;

-- ================================================
-- Step 1: Insert large dataset
-- ================================================
SELECT '##############################################################################' AS _;
SELECT '#              EXAMPLE 3: AGGREGATINGMERGETREE PERFORMANCE TEST             #' AS _;
SELECT '##############################################################################' AS _;

SELECT 'Inserting 1,000,000 orders for performance test...' AS status;
SELECT '(50,000 unique customers, 10 categories - realistic scenario)' AS _;

INSERT INTO orders_raw (customer_id, product_category, quantity, amount)
SELECT
    (rand() % 50000) + 1 AS customer_id,
    arrayElement(
        ['Electronics', 'Clothing', 'Home', 'Sports', 'Books',
         'Toys', 'Food', 'Beauty', 'Auto', 'Garden'],
        (rand() % 10) + 1
    ) AS product_category,
    (rand() % 5) + 1 AS quantity,
    round((rand() % 50000) / 100 + 10, 2) AS amount
FROM numbers(1000000)
SETTINGS max_block_size = 100000;

SELECT sleep(2);
SELECT 'Done. Total orders: ' || toString(count()) AS status FROM orders_raw;

-- ================================================
-- Storage Comparison
-- ================================================
SELECT '============================================================================' AS _;
SELECT '                         STORAGE COMPARISON                                ' AS _;
SELECT '============================================================================' AS _;

SELECT
    'Raw Orders Table' AS table_name,
    (SELECT count() FROM orders_raw) AS row_count,
    'All individual orders' AS description;

SELECT
    'AggregatingMergeTree' AS table_name,
    (SELECT count() FROM hourly_sales) AS row_count,
    'Pre-aggregated states' AS description;

SYSTEM FLUSH LOGS;

-- ================================================
-- Test 1: COUNT DISTINCT (unique customers by category)
-- ================================================
SELECT '============================================================================' AS _;
SELECT '           QUERY: Unique Customers & Revenue by Category                   ' AS _;
SELECT '============================================================================' AS _;

-- Method 1: AggregatingMergeTree
SELECT '>>> APPROACH A: AggregatingMergeTree + uniqMerge()' AS _;
SELECT '    Query: SELECT category, uniqMerge(unique_customers), sumMerge(total_revenue)' AS _;
SELECT '           FROM hourly_sales GROUP BY category' AS _;
SELECT
    category,
    countMerge(order_count) AS total_orders,
    uniqMerge(unique_customers) AS unique_customers,
    round(sumMerge(total_revenue), 2) AS revenue
FROM hourly_sales
GROUP BY category
ORDER BY revenue DESC
LIMIT 5;


-- Method 2: Raw table
SELECT '>>> APPROACH B: Raw Table + uniq() (on-the-fly COUNT DISTINCT)' AS _;
SELECT '    Query: SELECT product_category, uniq(customer_id), sum(amount)' AS _;
SELECT '           FROM orders_raw GROUP BY product_category' AS _;
SELECT
    product_category AS category,
    count() AS total_orders,
    uniq(customer_id) AS unique_customers,
    round(sum(amount), 2) AS revenue
FROM orders_raw
GROUP BY product_category
ORDER BY revenue DESC
LIMIT 5;

SYSTEM FLUSH LOGS;

-- ================================================
-- Performance Comparison: COUNT DISTINCT
-- ================================================
SELECT '============================================================================' AS _;
SELECT '              PERFORMANCE: COUNT DISTINCT Query                            ' AS _;
SELECT '============================================================================' AS _;
SELECT '+--------------------------------+------------+---------------+--------------+' AS _;
SELECT '| Approach                       | Rows Read  | Data Scanned  | Query Time   |' AS _;
SELECT '+--------------------------------+------------+---------------+--------------+' AS _;

-- Get the most recent query for each approach
WITH ranked AS (
    SELECT
        CASE
            WHEN query LIKE '%hourly_sales%' AND query LIKE '%uniqMerge%' THEN 'AggregatingMT + uniqMerge()'
            WHEN query LIKE '%orders_raw%' AND query LIKE '%uniq(customer_id)%' THEN 'Raw Table + uniq()'
            ELSE 'Other'
        END AS approach,
        read_rows,
        read_bytes,
        query_duration_ms,
        row_number() OVER (PARTITION BY
            CASE
                WHEN query LIKE '%hourly_sales%' AND query LIKE '%uniqMerge%' THEN 1
                WHEN query LIKE '%orders_raw%' AND query LIKE '%uniq(customer_id)%' THEN 2
                ELSE 3
            END
            ORDER BY event_time DESC
        ) AS rn
    FROM system.query_log
    WHERE
        type = 'QueryFinish'
        AND query_kind = 'Select'
        AND event_date = today()
        AND (
            (query LIKE '%hourly_sales%' AND query LIKE '%uniqMerge%')
            OR (query LIKE '%orders_raw%' AND query LIKE '%uniq(customer_id)%')
        )
        AND query NOT LIKE '%system.query_log%'
)
SELECT
    '| ' || leftPad(approach, 30) || ' | ' ||
    leftPad(toString(read_rows), 10) || ' | ' ||
    leftPad(formatReadableSize(read_bytes), 13) || ' | ' ||
    leftPad(toString(round(query_duration_ms, 1)) || ' ms', 12) || ' |' AS _
FROM ranked
WHERE rn = 1 AND approach != 'Other'
ORDER BY
    CASE approach
        WHEN 'Raw Table + uniq()' THEN 1
        ELSE 2
    END;

SELECT '+--------------------------------+------------+---------------+--------------+' AS _;

-- Calculate speedup

WITH
    agg_stats AS (
        SELECT read_rows, read_bytes, query_duration_ms
        FROM system.query_log
        WHERE type = 'QueryFinish' AND query_kind = 'Select' AND event_date = today()
            AND query LIKE '%hourly_sales%' AND query LIKE '%uniqMerge%'
            AND query NOT LIKE '%system.query_log%'
        ORDER BY event_time DESC LIMIT 1
    ),
    raw_stats AS (
        SELECT read_rows, read_bytes, query_duration_ms
        FROM system.query_log
        WHERE type = 'QueryFinish' AND query_kind = 'Select' AND event_date = today()
            AND query LIKE '%orders_raw%' AND query LIKE '%uniq(customer_id)%'
            AND query NOT LIKE '%system.query_log%'
        ORDER BY event_time DESC LIMIT 1
    )
SELECT
    'RESULT: Same output! Scanned ' ||
    toString(round(raw_stats.read_rows / agg_stats.read_rows, 0)) || 'x fewer rows, ' ||
    toString(round(raw_stats.read_bytes / agg_stats.read_bytes, 0)) || 'x less data' AS summary
FROM agg_stats, raw_stats;

-- ================================================
-- Test 2: AVG Order Value
-- ================================================
SELECT '============================================================================' AS _;
SELECT '                QUERY: Average Order Value by Category                     ' AS _;
SELECT '============================================================================' AS _;

SYSTEM FLUSH LOGS;

-- Method 1: AggregatingMergeTree
SELECT '>>> APPROACH A: AggregatingMergeTree + avgMerge()' AS _;
SELECT
    category,
    round(avgMerge(avg_order_value), 2) AS avg_order
FROM hourly_sales
GROUP BY category
ORDER BY avg_order DESC
LIMIT 5;


-- Method 2: Raw table
SELECT '>>> APPROACH B: Raw Table + avg()' AS _;
SELECT
    product_category AS category,
    round(avg(amount), 2) AS avg_order
FROM orders_raw
GROUP BY product_category
ORDER BY avg_order DESC
LIMIT 5;

SYSTEM FLUSH LOGS;

-- ================================================
-- Performance Comparison: AVG
-- ================================================
SELECT '============================================================================' AS _;
SELECT '                  PERFORMANCE: AVG Query                                   ' AS _;
SELECT '============================================================================' AS _;
SELECT '+--------------------------------+------------+---------------+--------------+' AS _;
SELECT '| Approach                       | Rows Read  | Data Scanned  | Query Time   |' AS _;
SELECT '+--------------------------------+------------+---------------+--------------+' AS _;

-- Get the most recent query for each approach
WITH ranked AS (
    SELECT
        CASE
            WHEN query LIKE '%hourly_sales%' AND query LIKE '%avgMerge%' THEN 'AggregatingMT + avgMerge()'
            WHEN query LIKE '%orders_raw%' AND query LIKE '%avg(amount)%' THEN 'Raw Table + avg()'
            ELSE 'Other'
        END AS approach,
        read_rows,
        read_bytes,
        query_duration_ms,
        row_number() OVER (PARTITION BY
            CASE
                WHEN query LIKE '%hourly_sales%' AND query LIKE '%avgMerge%' THEN 1
                WHEN query LIKE '%orders_raw%' AND query LIKE '%avg(amount)%' THEN 2
                ELSE 3
            END
            ORDER BY event_time DESC
        ) AS rn
    FROM system.query_log
    WHERE
        type = 'QueryFinish'
        AND query_kind = 'Select'
        AND event_date = today()
        AND (
            (query LIKE '%hourly_sales%' AND query LIKE '%avgMerge%')
            OR (query LIKE '%orders_raw%' AND query LIKE '%avg(amount)%')
        )
        AND query NOT LIKE '%system.query_log%'
)
SELECT
    '| ' || leftPad(approach, 30) || ' | ' ||
    leftPad(toString(read_rows), 10) || ' | ' ||
    leftPad(formatReadableSize(read_bytes), 13) || ' | ' ||
    leftPad(toString(round(query_duration_ms, 1)) || ' ms', 12) || ' |' AS _
FROM ranked
WHERE rn = 1 AND approach != 'Other'
ORDER BY
    CASE approach
        WHEN 'Raw Table + avg()' THEN 1
        ELSE 2
    END;

SELECT '+--------------------------------+------------+---------------+--------------+' AS _;

-- ================================================
-- Key Takeaway
-- ================================================
SELECT '============================================================================' AS _;
SELECT '                           KEY TAKEAWAY                                    ' AS _;
SELECT '============================================================================' AS _;
SELECT 'The State/Merge Pattern:' AS _;
SELECT '  - INSERT time: *State() functions store intermediate aggregate states' AS _;
SELECT '  - QUERY time:  *Merge() functions combine states to get final result' AS _;
SELECT 'Why this matters for COUNT DISTINCT:' AS _;
SELECT '  - Raw table:   Must scan ALL 1M rows to find unique values' AS _;
SELECT '  - Aggregating: Reads ~10-20 pre-computed state rows' AS _;
SELECT 'Scale Impact:' AS _;
SELECT '  - 1M rows today    -> Raw: slow,    Aggregating: fast' AS _;
SELECT '  - 100M rows later  -> Raw: SLOWER,  Aggregating: still fast!' AS _;

SELECT '[OK] Performance comparison complete' AS status;
