-- ============================================================
-- Section 08: Demo 2 — Skip Indexes
-- ============================================================
-- Scenario : Accelerate queries on non-primary-key columns
-- Concepts : minmax, set, bloom_filter, ngrambf_v1 skip indexes,
--            ALTER TABLE ADD INDEX, MATERIALIZE INDEX,
--            measuring effectiveness with system.query_log
-- ============================================================

USE nyc_taxi_perf;

-- ============================================================
-- TALKING POINT
-- ============================================================
-- Skip indexes (also called "data skipping indexes") are stored
-- per granule (8192 rows). They let ClickHouse decide to skip
-- an entire granule WITHOUT reading its rows.
-- Unlike a traditional B-tree index, they don't locate an exact
-- row — they eliminate granules that CANNOT contain a match.
--
-- Four main types:
--   minmax     → stores min/max value per granule (numerics, dates)
--   set(N)     → stores up to N distinct values per granule (low-cardinality)
--   bloom_filter → probabilistic, high-cardinality equality/IN checks
--   ngrambf_v1 → n-gram bloom filter for LIKE / token searches
-- ============================================================


-- ============================================================
-- 2.1  Baseline: query without skip index
-- ============================================================
SELECT '==== 2.1  Baseline query (no skip index on fare_amount) ====' AS step;

SELECT count(), avg(trip_distance), avg(total_amount)
FROM trips
WHERE fare_amount > 200;

-- ============================================================
-- TALKING POINT
-- ============================================================
-- read_rows ≈ 3,000,000 — the full table was scanned.
-- We'll compare this number after adding the skip index.
-- ============================================================


-- ============================================================
-- 2.2  minmax skip index — numeric range queries
-- ============================================================
SELECT '==== 2.2  Adding minmax skip index on fare_amount ====' AS step;

ALTER TABLE trips
    ADD INDEX idx_fare_minmax (fare_amount) TYPE minmax GRANULARITY 1;

-- Materialize: apply the index to existing data
ALTER TABLE trips MATERIALIZE INDEX idx_fare_minmax;

-- ============================================================
-- TALKING POINT
-- ============================================================
-- GRANULARITY 1 means: store one index entry per 1 table granule.
-- Setting it to 4 would store one entry per 4 granules (coarser,
-- less memory, but fewer skips possible).
-- MATERIALIZE INDEX is needed for existing data. New inserts
-- automatically build the index.
-- ============================================================

-- Now run the same query
SELECT count(), avg(trip_distance), avg(total_amount)
FROM trips
WHERE fare_amount > 200;

-- ============================================================
-- TALKING POINT
-- ============================================================
-- Compare read_rows BEFORE and AFTER the index.
-- For fare_amount > 50 (a small fraction of trips), the minmax
-- index can skip granules where max(fare_amount) <= 50.
-- The speedup depends on data correlation with the sort key —
-- if fare_amount is random relative to pickup_datetime (our sort
-- key), granule-level min/max values are wide, and skipping is limited.
-- This is why minmax works best on columns that are CORRELATED
-- with the primary key column order.
-- ============================================================


-- ============================================================
-- 2.3  set skip index — low-cardinality equality filter
-- ============================================================
SELECT '==== 2.3  set skip index on payment_type ====' AS step;

ALTER TABLE trips
    ADD INDEX idx_payment_set (payment_type) TYPE set(5) GRANULARITY 1;

ALTER TABLE trips MATERIALIZE INDEX idx_payment_set;

-- Query filtering on payment_type
SELECT count(), avg(fare_amount), avg(tip_amount)
FROM trips
WHERE payment_type = 'CRE';

EXPLAIN indexes = 1
SELECT count(), avg(fare_amount), avg(tip_amount)
FROM trips
WHERE payment_type = 'CRE';

-- ============================================================
-- TALKING POINT
-- ============================================================
-- Run this and note that Granules: 366/366 — a full scan. This is intentional. Payment types are randomly distributed across time, so every granule contains a mix of all five values. The set index can never rule out any granule. Skip indexes are not a universal fix — they only help when the indexed column's values cluster within granules, which happens naturally when the column correlates with the sort key.
-- ============================================================


-- ============================================================
-- 2.4  bloom_filter — high-cardinality string equality
-- ============================================================
SELECT '==== 2.4  bloom_filter skip index on pickup_ntaname ====' AS step;

ALTER TABLE trips
    ADD INDEX idx_pickup_bloom (pickup_ntaname) TYPE bloom_filter(0.01) GRANULARITY 1;

ALTER TABLE trips MATERIALIZE INDEX idx_pickup_bloom;

-- Query filtering on a specific neighborhood
SELECT count(), avg(fare_amount), avg(total_amount)
FROM trips_key_b
WHERE pickup_ntaname = 'Midtown-Midtown South';

EXPLAIN indexes = 1
SELECT count(), avg(fare_amount)
FROM trips_key_b
WHERE pickup_ntaname = 'Midtown-Midtown South';

-- ============================================================
-- TALKING POINT
-- ============================================================
-- bloom_filter(false_positive_rate) — 0.01 = 1% false positive rate.
-- A lower FPR means more accurate skipping but larger index size.
-- The bloom filter answers: "does this granule DEFINITELY NOT
-- contain pickup_ntaname = 'X'?" — if yes, skip it.
-- Best for: high-cardinality string columns used in equality/IN.
-- NOT helpful for: LIKE '%substring%' searches (use ngrambf_v1).
-- ============================================================


SELECT '[OK] Demo 2 complete: Skip Indexes.' AS status;
