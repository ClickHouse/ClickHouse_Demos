-- ================================================
-- Example 5: Medallion Architecture Performance Comparison
-- ================================================
-- Compare: Gold (pre-aggregated) vs Silver (enriched) vs Bronze (raw)
-- Goal: Show identical results with dramatically different performance
-- ================================================

USE fastmart_demo;

SELECT '##############################################################################' AS _;
SELECT '#              EXAMPLE 5: MEDALLION ARCHITECTURE PERFORMANCE TEST           #' AS _;
SELECT '##############################################################################' AS _;

-- ================================================
-- Data Volume by Layer
-- ================================================
SELECT '============================================================================' AS _;
SELECT '                    DATA VOLUME BY LAYER                                   ' AS _;
SELECT '============================================================================' AS _;
SELECT '+---------------------------+--------------+---------------+' AS _;
SELECT '| Layer                     | Row Count    | Storage Size  |' AS _;
SELECT '+---------------------------+--------------+---------------+' AS _;

SELECT
    '| Bronze: events_raw        | ' ||
    leftPad(toString((SELECT count() FROM events_raw)), 12) || ' | ' ||
    leftPad((SELECT formatReadableSize(sum(data_compressed_bytes)) FROM system.parts
             WHERE database = 'fastmart_demo' AND table = 'events_raw' AND active), 13) || ' |' AS _;

SELECT
    '| Silver: orders_enriched   | ' ||
    leftPad(toString((SELECT count() FROM orders_enriched)), 12) || ' | ' ||
    leftPad((SELECT formatReadableSize(sum(data_compressed_bytes)) FROM system.parts
             WHERE database = 'fastmart_demo' AND table = 'orders_enriched' AND active), 13) || ' |' AS _;

SELECT
    '| Gold: sales_by_minute     | ' ||
    leftPad(toString((SELECT count() FROM sales_by_minute)), 12) || ' | ' ||
    leftPad((SELECT formatReadableSize(sum(data_compressed_bytes)) FROM system.parts
             WHERE database = 'fastmart_demo' AND table = 'sales_by_minute' AND active), 13) || ' |' AS _;

SELECT
    '| Gold: sales_by_hour       | ' ||
    leftPad(toString((SELECT count() FROM sales_by_hour)), 12) || ' | ' ||
    leftPad((SELECT formatReadableSize(sum(data_compressed_bytes)) FROM system.parts
             WHERE database = 'fastmart_demo' AND table = 'sales_by_hour' AND active), 13) || ' |' AS _;

SELECT
    '| Gold: sales_by_day        | ' ||
    leftPad(toString((SELECT count() FROM sales_by_day)), 12) || ' | ' ||
    leftPad((SELECT formatReadableSize(sum(data_compressed_bytes)) FROM system.parts
             WHERE database = 'fastmart_demo' AND table = 'sales_by_day' AND active), 13) || ' |' AS _;

SELECT '+---------------------------+--------------+---------------+' AS _;

SYSTEM FLUSH LOGS;

-- ================================================
-- Test 1: Daily Revenue Query (4 approaches)
-- ================================================
SELECT '============================================================================' AS _;
SELECT '         QUERY: Daily Revenue by Category (Last 7 Days, Top 5)             ' AS _;
SELECT '============================================================================' AS _;

-- Method 1: Gold (Day)
SELECT '>>> APPROACH A: Gold Layer (sales_by_day) - Pre-aggregated daily' AS _;
SELECT
    day,
    category,
    countMerge(total_orders) AS orders,
    round(sumMerge(total_revenue), 2) AS revenue,
    uniqMerge(unique_customers) AS unique_customers
FROM sales_by_day
WHERE day >= today() - 7
GROUP BY day, category
ORDER BY day DESC, revenue DESC
LIMIT 5;


-- Method 2: Gold (Hour)
SELECT '>>> APPROACH B: Gold Layer (sales_by_hour) - Aggregate from hourly' AS _;
SELECT
    toDate(hour) AS day,
    category,
    countMerge(total_orders) AS orders,
    round(sumMerge(total_revenue), 2) AS revenue,
    uniqMerge(unique_customers) AS unique_customers
FROM sales_by_hour
WHERE hour >= today() - 7
GROUP BY day, category
ORDER BY day DESC, revenue DESC
LIMIT 5;


-- Method 3: Silver
SELECT '>>> APPROACH C: Silver Layer (orders_enriched) - Row-level aggregation' AS _;
SELECT
    toDate(order_time) AS day,
    category,
    count() AS orders,
    round(sum(total_amount), 2) AS revenue,
    uniq(customer_id) AS unique_customers
FROM orders_enriched
WHERE order_time >= today() - 7
GROUP BY day, category
ORDER BY day DESC, revenue DESC
LIMIT 5;


-- Method 4: Bronze
SELECT '>>> APPROACH D: Bronze Layer (events_raw) - JSON parsing + dict lookup' AS _;
SELECT
    toDate(event_time) AS day,
    dictGet('fastmart_demo.products_dict', 'category',
        JSONExtract(payload, 'product_id', 'UInt64')) AS category,
    count() AS orders,
    round(sum(
        JSONExtract(payload, 'quantity', 'UInt32') *
        JSONExtract(payload, 'price', 'Decimal64(2)')
    ), 2) AS revenue,
    uniq(JSONExtract(payload, 'customer_id', 'UInt64')) AS unique_customers
FROM events_raw
WHERE event_type = 'order' AND event_time >= today() - 7
GROUP BY day, category
ORDER BY day DESC, revenue DESC
LIMIT 5;

SYSTEM FLUSH LOGS;

-- ================================================
-- Performance Comparison: Daily Query
-- ================================================
SELECT '============================================================================' AS _;
SELECT '              PERFORMANCE: Daily Revenue Query                             ' AS _;
SELECT '============================================================================' AS _;
SELECT '+---------------------------+------------+---------------+--------------+' AS _;
SELECT '| Layer                     | Rows Read  | Data Scanned  | Query Time   |' AS _;
SELECT '+---------------------------+------------+---------------+--------------+' AS _;

-- Get the most recent query for each approach
WITH ranked AS (
    SELECT
        CASE
            WHEN query LIKE '%sales_by_day%' THEN 'Gold (Day) - FASTEST'
            WHEN query LIKE '%sales_by_hour%' THEN 'Gold (Hour)'
            WHEN query LIKE '%orders_enriched%' AND query NOT LIKE '%toStartOfMinute%' THEN 'Silver (Enriched)'
            WHEN query LIKE '%events_raw%' AND query LIKE '%JSONExtract%' THEN 'Bronze (Raw) - SLOWEST'
            ELSE 'Other'
        END AS approach,
        read_rows,
        read_bytes,
        query_duration_ms,
        row_number() OVER (PARTITION BY
            CASE
                WHEN query LIKE '%sales_by_day%' THEN 1
                WHEN query LIKE '%sales_by_hour%' THEN 2
                WHEN query LIKE '%orders_enriched%' AND query NOT LIKE '%toStartOfMinute%' THEN 3
                WHEN query LIKE '%events_raw%' AND query LIKE '%JSONExtract%' THEN 4
                ELSE 5
            END
            ORDER BY event_time DESC
        ) AS rn
    FROM system.query_log
    WHERE
        type = 'QueryFinish'
        AND query_kind = 'Select'
        AND event_date = today()
        AND query LIKE '%revenue%'
        AND query LIKE '%unique_customers%'
        AND query LIKE '%ORDER BY day DESC%'
        AND query NOT LIKE '%system.query_log%'
        AND query NOT LIKE '%APPROACH%'
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
        WHEN 'Bronze (Raw) - SLOWEST' THEN 1
        WHEN 'Silver (Enriched)' THEN 2
        WHEN 'Gold (Hour)' THEN 3
        WHEN 'Gold (Day) - FASTEST' THEN 4
        ELSE 5
    END;

SELECT '+---------------------------+------------+---------------+--------------+' AS _;

-- ================================================
-- Test 2: Real-Time Dashboard Query
-- ================================================
SELECT '============================================================================' AS _;
SELECT '          QUERY: Real-Time Dashboard (Last Hour, by Minute)                ' AS _;
SELECT '============================================================================' AS _;

SYSTEM FLUSH LOGS;

-- Method 1: Gold (Minute)
SELECT '>>> APPROACH A: Gold Layer (sales_by_minute) - Real-time optimized' AS _;
SELECT
    minute,
    category,
    countMerge(total_orders) AS orders,
    round(sumMerge(total_revenue), 2) AS revenue
FROM sales_by_minute
WHERE minute >= now() - INTERVAL 1 HOUR
GROUP BY minute, category
ORDER BY minute DESC, revenue DESC
LIMIT 5;


-- Method 2: Silver
SELECT '>>> APPROACH B: Silver Layer (orders_enriched) - Must scan all orders' AS _;
SELECT
    toStartOfMinute(order_time) AS minute,
    category,
    count() AS orders,
    round(sum(total_amount), 2) AS revenue
FROM orders_enriched
WHERE order_time >= now() - INTERVAL 1 HOUR
GROUP BY minute, category
ORDER BY minute DESC, revenue DESC
LIMIT 5;

SYSTEM FLUSH LOGS;

-- ================================================
-- Performance Comparison: Real-Time Dashboard
-- ================================================
SELECT '============================================================================' AS _;
SELECT '              PERFORMANCE: Real-Time Dashboard Query                       ' AS _;
SELECT '============================================================================' AS _;
SELECT '+---------------------------+------------+---------------+--------------+' AS _;
SELECT '| Approach                  | Rows Read  | Data Scanned  | Query Time   |' AS _;
SELECT '+---------------------------+------------+---------------+--------------+' AS _;

-- Get the most recent query for each approach
WITH ranked AS (
    SELECT
        CASE
            WHEN query LIKE '%sales_by_minute%' THEN 'Gold (Minute) - FAST'
            WHEN query LIKE '%orders_enriched%' AND query LIKE '%toStartOfMinute%' THEN 'Silver - ON-THE-FLY'
            ELSE 'Other'
        END AS approach,
        read_rows,
        read_bytes,
        query_duration_ms,
        row_number() OVER (PARTITION BY
            CASE
                WHEN query LIKE '%sales_by_minute%' THEN 1
                WHEN query LIKE '%orders_enriched%' AND query LIKE '%toStartOfMinute%' THEN 2
                ELSE 3
            END
            ORDER BY event_time DESC
        ) AS rn
    FROM system.query_log
    WHERE
        type = 'QueryFinish'
        AND query_kind = 'Select'
        AND event_date = today()
        AND query LIKE '%INTERVAL 1 HOUR%'
        AND query LIKE '%minute%'
        AND query NOT LIKE '%system.query_log%'
        AND query NOT LIKE '%APPROACH%'
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
        WHEN 'Silver - ON-THE-FLY' THEN 1
        ELSE 2
    END;

SELECT '+---------------------------+------------+---------------+--------------+' AS _;

-- ================================================
-- Data Reduction Analysis
-- ================================================
SELECT '============================================================================' AS _;
SELECT '                      DATA REDUCTION ANALYSIS                              ' AS _;
SELECT '============================================================================' AS _;

WITH
    bronze AS (SELECT count() AS cnt FROM events_raw),
    silver AS (SELECT count() AS cnt FROM orders_enriched),
    gold_min AS (SELECT count() AS cnt FROM sales_by_minute),
    gold_hr AS (SELECT count() AS cnt FROM sales_by_hour),
    gold_day AS (SELECT count() AS cnt FROM sales_by_day)
SELECT
    'Bronze -> Gold (Day): ' ||
    toString(bronze.cnt) || ' rows -> ' || toString(gold_day.cnt) || ' rows = ' ||
    toString(round(bronze.cnt / gold_day.cnt, 0)) || 'x reduction' AS reduction_summary
FROM bronze, gold_day;

SELECT '+---------------------------+--------------+-------------------+' AS _;
SELECT '| Layer                     | Row Count    | Reduction Ratio   |' AS _;
SELECT '+---------------------------+--------------+-------------------+' AS _;

SELECT
    '| Bronze (events_raw)       | ' ||
    leftPad(toString((SELECT count() FROM events_raw)), 12) ||
    ' | 1x (baseline)       |' AS _;

SELECT
    '| Silver (orders_enriched)  | ' ||
    leftPad(toString((SELECT count() FROM orders_enriched)), 12) ||
    ' | ' || leftPad(toString(round((SELECT count() FROM events_raw) / nullIf((SELECT count() FROM orders_enriched), 0), 1)) || 'x', 17) || ' |' AS _;

SELECT
    '| Gold (sales_by_minute)    | ' ||
    leftPad(toString((SELECT count() FROM sales_by_minute)), 12) ||
    ' | ' || leftPad(toString(round((SELECT count() FROM events_raw) / nullIf((SELECT count() FROM sales_by_minute), 0), 1)) || 'x', 17) || ' |' AS _;

SELECT
    '| Gold (sales_by_hour)      | ' ||
    leftPad(toString((SELECT count() FROM sales_by_hour)), 12) ||
    ' | ' || leftPad(toString(round((SELECT count() FROM events_raw) / nullIf((SELECT count() FROM sales_by_hour), 0), 1)) || 'x', 17) || ' |' AS _;

SELECT
    '| Gold (sales_by_day)       | ' ||
    leftPad(toString((SELECT count() FROM sales_by_day)), 12) ||
    ' | ' || leftPad(toString(round((SELECT count() FROM events_raw) / nullIf((SELECT count() FROM sales_by_day), 0), 1)) || 'x', 17) || ' |' AS _;

SELECT '+---------------------------+--------------+-------------------+' AS _;

-- ================================================
-- Key Takeaway
-- ================================================
SELECT '============================================================================' AS _;
SELECT '                           KEY TAKEAWAY                                    ' AS _;
SELECT '============================================================================' AS _;
SELECT 'Medallion Architecture Query Strategy:' AS _;
SELECT '  DASHBOARDS & REPORTS -> Query Gold Layer (pre-aggregated)' AS _;
SELECT '    - Fastest response time' AS _;
SELECT '    - Minimal data scanned' AS _;
SELECT '    - Limited to pre-defined dimensions' AS _;
SELECT '  AD-HOC ANALYSIS -> Query Silver Layer (enriched)' AS _;
SELECT '    - Good balance of speed and flexibility' AS _;
SELECT '    - Data already cleaned and enriched' AS _;
SELECT '    - Can query any dimension' AS _;
SELECT '  DEBUGGING & AUDIT -> Query Bronze Layer (raw)' AS _;
SELECT '    - Access to original data' AS _;
SELECT '    - Full flexibility' AS _;
SELECT '    - Slowest (JSON parsing, no pre-aggregation)' AS _;
SELECT 'At scale (10x-100x current data):' AS _;
SELECT '  - Gold queries stay fast (row count grows slowly)' AS _;
SELECT '  - Bronze queries become 10x-100x slower (row count grows linearly)' AS _;

SELECT '[OK] Medallion performance comparison complete' AS status;
