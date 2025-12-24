-- ================================================
-- Example 1: Performance Comparison
-- ================================================
-- Compare: Pre-aggregated MV target vs Raw table aggregation
-- Goal: Show identical results with different performance
-- ================================================

USE mv_demo_basic;

-- ================================================
-- Step 1: Insert large dataset for meaningful comparison
-- ================================================
SELECT '##############################################################################' AS _;
SELECT '#                    EXAMPLE 1: BASIC MV PERFORMANCE TEST                   #' AS _;
SELECT '##############################################################################' AS _;

SELECT 'Inserting 100,000 page views for performance test...' AS status;

-- Generate 100K page views across 100 URLs
INSERT INTO page_views_raw (page_url, user_id)
SELECT
    concat('/page/', toString(rand() % 100)) AS page_url,
    rand() % 10000 AS user_id
FROM numbers(100000);

SELECT 'Done. Total rows in page_views_raw: ' || toString(count()) AS status FROM page_views_raw;

-- ================================================
-- Step 2: Show Storage Comparison
-- ================================================
SELECT '============================================================================' AS _;
SELECT '                         STORAGE COMPARISON                                ' AS _;
SELECT '============================================================================' AS _;

SELECT
    'Raw Table (page_views_raw)' AS table_name,
    (SELECT count() FROM page_views_raw) AS row_count,
    (SELECT formatReadableSize(sum(data_compressed_bytes)) FROM system.parts
     WHERE database = 'mv_demo_basic' AND table = 'page_views_raw' AND active) AS storage_size;

SELECT
    'MV Target (page_views_count)' AS table_name,
    (SELECT count() FROM page_views_count) AS row_count,
    (SELECT formatReadableSize(sum(data_compressed_bytes)) FROM system.parts
     WHERE database = 'mv_demo_basic' AND table = 'page_views_count' AND active) AS storage_size;

-- Clear query log for accurate measurement
SYSTEM FLUSH LOGS;

-- ================================================
-- Step 3: Run Both Queries and Show Results
-- ================================================
SELECT '============================================================================' AS _;
SELECT '                    QUERY: Top 10 Most Viewed Pages                        ' AS _;
SELECT '============================================================================' AS _;

-- Method 1: Query pre-aggregated MV target table
SELECT '>>> APPROACH A: Using MV Target (Pre-aggregated)' AS _;
SELECT '    Query: SELECT page_url, sum(view_count) FROM page_views_count GROUP BY page_url' AS _;
SELECT
    page_url,
    sum(view_count) AS total_views
FROM page_views_count
GROUP BY page_url
ORDER BY total_views DESC
LIMIT 10;


-- Method 2: Aggregate from raw table
SELECT '>>> APPROACH B: Using Raw Table (On-the-fly aggregation)' AS _;
SELECT '    Query: SELECT page_url, count() FROM page_views_raw GROUP BY page_url' AS _;
SELECT
    page_url,
    count() AS total_views
FROM page_views_raw
GROUP BY page_url
ORDER BY total_views DESC
LIMIT 10;

-- Wait for query log to be flushed
SYSTEM FLUSH LOGS;

-- ================================================
-- Step 4: Performance Comparison Summary
-- ================================================
SELECT '============================================================================' AS _;
SELECT '                      PERFORMANCE COMPARISON                               ' AS _;
SELECT '============================================================================' AS _;
SELECT '+-------------------------+------------+---------------+--------------+' AS _;
SELECT '| Approach                | Rows Read  | Data Scanned  | Query Time   |' AS _;
SELECT '+-------------------------+------------+---------------+--------------+' AS _;

-- Get the most recent query for each approach
WITH ranked AS (
    SELECT
        CASE
            WHEN query LIKE '%page_views_count%' THEN 'MV Target (pre-agg)'
            WHEN query LIKE '%page_views_raw%' THEN 'Raw Table (on-fly)'
            ELSE 'Other'
        END AS approach,
        read_rows,
        read_bytes,
        query_duration_ms,
        row_number() OVER (PARTITION BY
            CASE
                WHEN query LIKE '%page_views_count%' THEN 1
                WHEN query LIKE '%page_views_raw%' THEN 2
                ELSE 3
            END
            ORDER BY event_time DESC
        ) AS rn
    FROM system.query_log
    WHERE
        type = 'QueryFinish'
        AND query_kind = 'Select'
        AND event_date = today()
        AND (query LIKE '%page_views_count%' OR query LIKE '%page_views_raw%')
        AND query LIKE '%ORDER BY total_views DESC%'
        AND query NOT LIKE '%system.query_log%'
)
SELECT
    '| ' || leftPad(approach, 23) || ' | ' ||
    leftPad(toString(read_rows), 10) || ' | ' ||
    leftPad(formatReadableSize(read_bytes), 13) || ' | ' ||
    leftPad(toString(round(query_duration_ms, 1)) || ' ms', 12) || ' |' AS _
FROM ranked
WHERE rn = 1 AND approach != 'Other'
ORDER BY
    CASE approach
        WHEN 'Raw Table (on-fly)' THEN 1
        ELSE 2
    END;

SELECT '+-------------------------+------------+---------------+--------------+' AS _;

-- ================================================
-- Step 5: Calculate Speedup
-- ================================================

WITH
    mv_stats AS (
        SELECT read_rows, read_bytes, query_duration_ms
        FROM system.query_log
        WHERE type = 'QueryFinish' AND query_kind = 'Select' AND event_date = today()
            AND query LIKE '%page_views_count%' AND query LIKE '%ORDER BY total_views DESC%'
            AND query NOT LIKE '%system.query_log%'
        ORDER BY event_time DESC LIMIT 1
    ),
    raw_stats AS (
        SELECT read_rows, read_bytes, query_duration_ms
        FROM system.query_log
        WHERE type = 'QueryFinish' AND query_kind = 'Select' AND event_date = today()
            AND query LIKE '%page_views_raw%' AND query LIKE '%ORDER BY total_views DESC%'
            AND query NOT LIKE '%system.query_log%'
        ORDER BY event_time DESC LIMIT 1
    )
SELECT
    'RESULT: Same output, ' ||
    toString(round(raw_stats.read_rows / mv_stats.read_rows, 0)) || 'x fewer rows scanned, ' ||
    toString(round(raw_stats.read_bytes / mv_stats.read_bytes, 0)) || 'x less data read' AS summary
FROM mv_stats, raw_stats;

SELECT '============================================================================' AS _;
SELECT '                           KEY TAKEAWAY                                    ' AS _;
SELECT '============================================================================' AS _;
SELECT 'MV Target Table:  Aggregation done at INSERT time (once)' AS _;
SELECT 'Raw Table:        Aggregation done at QUERY time (every time)' AS _;
SELECT 'As data grows to millions/billions of rows, MV approach stays fast!' AS _;

SELECT '[OK] Performance comparison complete' AS status;
