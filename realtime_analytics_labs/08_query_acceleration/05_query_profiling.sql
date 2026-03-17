-- ============================================================
-- Section 08: Demo 5 — Query Profiling
-- ============================================================
-- Scenario : Use ClickHouse built-in tools to diagnose,
--            understand, and optimise slow queries
-- Concepts : EXPLAIN levels, system.query_log, query metrics,
--            read_rows, read_bytes, memory_usage,
--            finding slow queries, index effectiveness
-- ============================================================

USE nyc_taxi_perf;


-- ============================================================
-- 5.1  EXPLAIN levels — from syntax to pipeline
-- ============================================================
SELECT '==== 5.1  EXPLAIN levels ====' AS step;

-- Level 1: Logical plan
SELECT '---- EXPLAIN (logical plan) ----' AS level;
EXPLAIN
SELECT pickup_ntaname, count(), avg(fare_amount)
FROM trips
WHERE pickup_datetime >= '2013-06-01'
GROUP BY pickup_ntaname;

-- Level 2: Include index information
SELECT '---- EXPLAIN indexes=1 ----' AS level;
EXPLAIN indexes = 1
SELECT pickup_ntaname, count(), avg(fare_amount)
FROM trips
WHERE pickup_datetime >= '2013-06-01'
GROUP BY pickup_ntaname;

-- Level 3: Physical pipeline (parallelism)
SELECT '---- EXPLAIN PIPELINE ----' AS level;
EXPLAIN PIPELINE
SELECT pickup_ntaname, count(), avg(fare_amount)
FROM trips
WHERE pickup_datetime >= '2013-06-01'
GROUP BY pickup_ntaname;

-- ============================================================
-- TALKING POINT
-- ============================================================
-- Three key things to look for in EXPLAIN output:
--
--   1. Granules read (indexes=1):
--      "Granules: 50/977" = only 5% of data read → good
--      "Granules: 977/977" = full scan → consider skip index
--
--   2. Parallel threads (PIPELINE):
--      Multiple MergeTreeThread nodes → parallel reads
--      Number of threads = min(parts, max_threads setting)
--
--   3. Aggregation method:
--      "GroupingAggregated" → memory-based hash aggregation
--      Look for "Two-level" aggregation for large GROUP BY results
-- ============================================================


-- ============================================================
-- 5.2  system.query_log — historical query analysis
-- ============================================================
SELECT '==== 5.2  Profiling via system.query_log ====' AS step;

-- Flush recent queries to the log
SYSTEM FLUSH LOGS;

-- Show the most recent queries from this session
SELECT
    query_id,
    left(query, 80)                         AS query_preview,
    read_rows,
    formatReadableQuantity(read_rows)       AS read_rows_fmt,
    formatReadableSize(read_bytes)          AS read_bytes_fmt,
    formatReadableSize(memory_usage)        AS memory_used,
    round(query_duration_ms, 0)             AS duration_ms,
    ProfileEvents['SelectedParts']          AS parts_selected,
    ProfileEvents['SelectedGranules']       AS granules_selected
FROM system.query_log
WHERE type = 'QueryFinish'
  AND databases = ['nyc_taxi_perf']
ORDER BY event_time DESC
LIMIT 15
FORMAT Vertical;

-- ============================================================
-- TALKING POINT
-- ============================================================
-- Key metrics to watch:
--   read_rows        → rows scanned (lower is better)
--   read_bytes       → bytes from disk (shows compression benefit)
--   memory_usage     → peak memory for this query
--   duration_ms      → wall-clock execution time
--   SelectedGranules → how many granules the index selected
--                      (vs. total granules = ~977)
-- ============================================================


-- ============================================================
-- 5.3  Find the slowest recent queries
-- ============================================================
SELECT '==== 5.3  Top 10 slowest queries ====' AS step;

SYSTEM FLUSH LOGS;

SELECT
    left(query, 100)                AS query_preview,
    round(query_duration_ms, 0)     AS duration_ms,
    formatReadableQuantity(read_rows) AS rows_read,
    formatReadableSize(read_bytes)  AS bytes_read,
    formatReadableSize(memory_usage) AS memory
FROM system.query_log
WHERE type = 'QueryFinish'
  AND databases = ['nyc_taxi_perf']
  AND query_duration_ms > 0
ORDER BY query_duration_ms DESC
LIMIT 10;


-- ============================================================
-- 5.4  Diagnose index effectiveness
-- ============================================================
SELECT '==== 5.4  Index effectiveness: key vs. non-key queries ====' AS step;

SYSTEM FLUSH LOGS;

-- Run two queries with different selectivity
SELECT count() FROM trips
WHERE pickup_datetime BETWEEN '2013-06-01' AND '2013-06-30';  -- key-aligned

SELECT count() FROM trips
WHERE total_amount BETWEEN 10 AND 15;  -- not key-aligned

SYSTEM FLUSH LOGS;

-- Compare them side by side
SELECT
    left(query, 80)                 AS query_preview,
    ProfileEvents['SelectedGranules'] AS granules_read,
    977                             AS total_granules,
    round(ProfileEvents['SelectedGranules'] / 977.0 * 100, 1) AS pct_scanned,
    round(query_duration_ms, 1)     AS ms
FROM system.query_log
WHERE type = 'QueryFinish'
  AND databases = ['nyc_taxi_perf']
  AND (query LIKE '%pickup_datetime BETWEEN%'
    OR query LIKE '%total_amount BETWEEN%')
ORDER BY event_time DESC
LIMIT 4;

-- ============================================================
-- TALKING POINT
-- ============================================================
-- The key-aligned query should show pct_scanned ≈ 5–10%
-- (only the June 2013 granules).
-- The total_amount query should show pct_scanned ≈ 100%
-- (no index → full scan).
-- This visualises exactly WHY query acceleration matters:
-- full scans on 8M rows work fine today — but at 800M rows,
-- a 100% scan is 100x more expensive.
-- ============================================================


-- ============================================================
-- 5.5  Profile a complex query — track improvements
-- ============================================================
SELECT '==== 5.5  Before/after optimisation comparison ====' AS step;

-- BEFORE: unoptimised query (no date filter, wide aggregation)
SELECT
    pickup_ntaname,
    payment_type,
    count()                     AS trips,
    round(avg(fare_amount), 2)  AS avg_fare
FROM trips
GROUP BY pickup_ntaname, payment_type
ORDER BY trips DESC
LIMIT 20;

-- AFTER: optimised with date filter (leverages primary index)
SELECT
    pickup_ntaname,
    payment_type,
    count()                     AS trips,
    round(avg(fare_amount), 2)  AS avg_fare
FROM trips
WHERE pickup_datetime >= '2013-01-01'
  AND pickup_datetime <  '2014-01-01'
GROUP BY pickup_ntaname, payment_type
ORDER BY trips DESC
LIMIT 20;

SYSTEM FLUSH LOGS;

SELECT
    left(query, 100)                AS query,
    round(query_duration_ms, 0)     AS ms,
    formatReadableQuantity(read_rows) AS rows_read,
    ProfileEvents['SelectedGranules'] AS granules
FROM system.query_log
WHERE type = 'QueryFinish'
  AND databases = ['nyc_taxi_perf']
  AND query LIKE '%pickup_ntaname, payment_type%'
ORDER BY event_time DESC
LIMIT 2;

-- ============================================================
-- TALKING POINT
-- ============================================================
-- Summary of all four acceleration techniques:
--
--   1. Primary Key (ORDER BY):
--      → Zero overhead, free index, best first step
--      → Only helps with range/equality on key columns
--
--   2. Skip Indexes (minmax, set, bloom_filter, ngrambf):
--      → Helps non-key columns, adds storage overhead
--      → Best when data clusters along the primary key
--
--   3. Projections:
--      → Hidden sorted copy, automatic query routing
--      → Best for a few alternative access patterns
--      → ~2x storage cost per projection
--
--   4. Materialized Views:
--      → Pre-aggregated, explicit target table
--      → Best for dashboard queries, real-time analytics
--      → Requires backfill, changes query (or alias target)
-- ============================================================

SELECT '[OK] Demo 5 complete: Query Profiling.' AS status;
