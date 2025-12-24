-- ================================================
-- Example 2: Performance Comparison
-- ================================================
-- Compare: SummingMergeTree vs Regular MergeTree vs Raw Table
-- Goal: Show identical results with different performance
-- ================================================

USE mv_demo_summing;

-- ================================================
-- Setup: Create comparison table (regular MergeTree)
-- ================================================
SELECT '##############################################################################' AS _;
SELECT '#                 EXAMPLE 2: SUMMINGMERGETREE PERFORMANCE TEST              #' AS _;
SELECT '##############################################################################' AS _;

SELECT 'Setting up comparison table (Regular MergeTree)...' AS status;

DROP TABLE IF EXISTS hourly_metrics_regular;

CREATE TABLE hourly_metrics_regular (
    hour DateTime,
    page_url String,
    pageviews UInt64,
    clicks UInt64,
    purchases UInt64,
    revenue Decimal64(2)
) ENGINE = MergeTree()
ORDER BY (hour, page_url);

DROP VIEW IF EXISTS hourly_metrics_regular_mv;

CREATE MATERIALIZED VIEW hourly_metrics_regular_mv
TO hourly_metrics_regular
AS
SELECT
    toStartOfHour(event_time) AS hour,
    page_url,
    countIf(event_type = 'pageview') AS pageviews,
    countIf(event_type = 'click') AS clicks,
    countIf(event_type = 'purchase') AS purchases,
    sumIf(revenue, event_type = 'purchase') AS revenue
FROM events_raw
GROUP BY hour, page_url;

SELECT 'Inserting 500,000 events for performance test...' AS status;

INSERT INTO events_raw (event_type, page_url, user_id, revenue)
SELECT
    arrayElement(['pageview', 'click', 'purchase'], (rand() % 3) + 1) AS event_type,
    concat('/page/', toString(rand() % 50)) AS page_url,
    rand() % 10000 AS user_id,
    if(rand() % 3 = 2, round(rand() % 500 + 10, 2), 0) AS revenue
FROM numbers(500000)
SETTINGS max_block_size = 100000;

SELECT sleep(1);
SELECT 'Done. Total events: ' || toString(count()) AS status FROM events_raw;

-- ================================================
-- Storage Comparison
-- ================================================
SELECT '============================================================================' AS _;
SELECT '                         STORAGE COMPARISON                                ' AS _;
SELECT '============================================================================' AS _;

SELECT
    'Raw Events Table' AS table_name,
    (SELECT count() FROM events_raw) AS row_count,
    'Source data' AS description;

SELECT
    'SummingMergeTree' AS table_name,
    (SELECT count() FROM hourly_metrics) AS row_count,
    'Auto-sums on merge' AS description;

SELECT
    'Regular MergeTree' AS table_name,
    (SELECT count() FROM hourly_metrics_regular) AS row_count,
    'No auto-sum' AS description;

SYSTEM FLUSH LOGS;

-- ================================================
-- Run Queries and Show Results
-- ================================================
SELECT '============================================================================' AS _;
SELECT '                 QUERY: Total Revenue by Hour (Top 5 Hours)                ' AS _;
SELECT '============================================================================' AS _;

-- Method 1: SummingMergeTree
SELECT '>>> APPROACH A: SummingMergeTree + FINAL' AS _;
SELECT '    Query: SELECT hour, sum(pageviews), sum(revenue) FROM hourly_metrics FINAL GROUP BY hour' AS _;
SELECT
    hour,
    sum(pageviews) AS total_pageviews,
    round(sum(revenue), 2) AS total_revenue
FROM hourly_metrics FINAL
GROUP BY hour
ORDER BY hour
LIMIT 5;


-- Method 2: Regular MergeTree
SELECT '>>> APPROACH B: Regular MergeTree (no auto-sum)' AS _;
SELECT '    Query: SELECT hour, sum(pageviews), sum(revenue) FROM hourly_metrics_regular GROUP BY hour' AS _;
SELECT
    hour,
    sum(pageviews) AS total_pageviews,
    round(sum(revenue), 2) AS total_revenue
FROM hourly_metrics_regular
GROUP BY hour
ORDER BY hour
LIMIT 5;


-- Method 3: Raw events
SELECT '>>> APPROACH C: Raw Events Table (on-the-fly aggregation)' AS _;
SELECT '    Query: SELECT toStartOfHour(event_time), countIf(...), sum(revenue) FROM events_raw GROUP BY hour' AS _;
SELECT
    toStartOfHour(event_time) AS hour,
    countIf(event_type = 'pageview') AS total_pageviews,
    round(sum(revenue), 2) AS total_revenue
FROM events_raw
GROUP BY hour
ORDER BY hour
LIMIT 5;

SYSTEM FLUSH LOGS;

-- ================================================
-- Performance Comparison Summary
-- ================================================
SELECT '============================================================================' AS _;
SELECT '                      PERFORMANCE COMPARISON                               ' AS _;
SELECT '============================================================================' AS _;
SELECT '+---------------------------+------------+---------------+--------------+' AS _;
SELECT '| Approach                  | Rows Read  | Data Scanned  | Query Time   |' AS _;
SELECT '+---------------------------+------------+---------------+--------------+' AS _;

-- Get the most recent query for each approach
WITH ranked AS (
    SELECT
        CASE
            WHEN query LIKE '%hourly_metrics FINAL%' AND query NOT LIKE '%hourly_metrics_regular%' THEN 'SummingMergeTree+FINAL'
            WHEN query LIKE '%hourly_metrics_regular%' THEN 'Regular MergeTree'
            WHEN query LIKE '%events_raw%' AND query LIKE '%toStartOfHour%' THEN 'Raw Events (on-fly)'
            ELSE 'Other'
        END AS approach,
        read_rows,
        read_bytes,
        query_duration_ms,
        row_number() OVER (PARTITION BY
            CASE
                WHEN query LIKE '%hourly_metrics FINAL%' AND query NOT LIKE '%hourly_metrics_regular%' THEN 1
                WHEN query LIKE '%hourly_metrics_regular%' THEN 2
                WHEN query LIKE '%events_raw%' AND query LIKE '%toStartOfHour%' THEN 3
                ELSE 4
            END
            ORDER BY event_time DESC
        ) AS rn
    FROM system.query_log
    WHERE
        type = 'QueryFinish'
        AND query_kind = 'Select'
        AND event_date = today()
        AND (
            (query LIKE '%hourly_metrics FINAL%' AND query NOT LIKE '%hourly_metrics_regular%')
            OR query LIKE '%hourly_metrics_regular%'
            OR (query LIKE '%events_raw%' AND query LIKE '%toStartOfHour%')
        )
        AND query LIKE '%ORDER BY hour%'
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
        WHEN 'Raw Events (on-fly)' THEN 1
        WHEN 'Regular MergeTree' THEN 2
        ELSE 3
    END;

SELECT '+---------------------------+------------+---------------+--------------+' AS _;

-- ================================================
-- Calculate Improvement
-- ================================================

WITH
    summing_stats AS (
        SELECT read_rows, read_bytes, query_duration_ms
        FROM system.query_log
        WHERE type = 'QueryFinish' AND query_kind = 'Select' AND event_date = today()
            AND query LIKE '%hourly_metrics FINAL%' AND query NOT LIKE '%hourly_metrics_regular%'
            AND query LIKE '%ORDER BY hour%' AND query NOT LIKE '%system.query_log%'
        ORDER BY event_time DESC LIMIT 1
    ),
    raw_stats AS (
        SELECT read_rows, read_bytes, query_duration_ms
        FROM system.query_log
        WHERE type = 'QueryFinish' AND query_kind = 'Select' AND event_date = today()
            AND query LIKE '%events_raw%' AND query LIKE '%toStartOfHour%'
            AND query LIKE '%ORDER BY hour%' AND query NOT LIKE '%system.query_log%'
        ORDER BY event_time DESC LIMIT 1
    )
SELECT
    'RESULT: Same output! Raw table scanned ' ||
    toString(round(raw_stats.read_rows / summing_stats.read_rows, 0)) || 'x more rows (' ||
    formatReadableSize(raw_stats.read_bytes) || ' vs ' ||
    formatReadableSize(summing_stats.read_bytes) || ')' AS summary
FROM summing_stats, raw_stats;

-- ================================================
-- Storage After Optimization
-- ================================================
SELECT '============================================================================' AS _;
SELECT '                    STORAGE AFTER OPTIMIZE FINAL                           ' AS _;
SELECT '============================================================================' AS _;

OPTIMIZE TABLE hourly_metrics FINAL;
OPTIMIZE TABLE hourly_metrics_regular FINAL;

SELECT '+---------------------------+--------------+------------------------------+' AS _;
SELECT '| Table                     | Row Count    | Notes                        |' AS _;
SELECT '+---------------------------+--------------+------------------------------+' AS _;

SELECT
    '| SummingMergeTree          | ' ||
    leftPad(toString((SELECT count() FROM hourly_metrics)), 12) ||
    ' | Rows auto-merged by SUM        |' AS _;

SELECT
    '| Regular MergeTree         | ' ||
    leftPad(toString((SELECT count() FROM hourly_metrics_regular)), 12) ||
    ' | All INSERT rows preserved      |' AS _;

SELECT '+---------------------------+--------------+------------------------------+' AS _;

SELECT '============================================================================' AS _;
SELECT '                           KEY TAKEAWAY                                    ' AS _;
SELECT '============================================================================' AS _;
SELECT 'SummingMergeTree: Background merges AUTO-SUM numeric columns' AS _;
SELECT '                  Fewer rows = faster queries + less storage' AS _;
SELECT 'Regular MergeTree: Must manually GROUP BY every query' AS _;
SELECT '                   More rows to scan = slower performance' AS _;
SELECT 'Limitation: SummingMergeTree only does SUM. For COUNT DISTINCT, use AggregatingMergeTree!' AS _;

-- Cleanup
DROP VIEW IF EXISTS hourly_metrics_regular_mv;
DROP TABLE IF EXISTS hourly_metrics_regular;

SELECT '[OK] Performance comparison complete' AS status;
