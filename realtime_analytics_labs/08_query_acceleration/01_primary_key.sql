-- ============================================================
-- Section 08: Demo 1 — Primary Key Optimization
-- ============================================================
-- Scenario : Understand how ORDER BY drives query performance
-- Concepts : Sparse primary index, granule scanning,
--            EXPLAIN indexes=1, key column ordering,
--            cardinality and selectivity trade-offs
-- ============================================================

USE nyc_taxi_perf;


-- ============================================================
-- 1.1  How many granules does our table have?
-- ============================================================
SELECT '==== 1.1  Table granule statistics ====' AS step;

SELECT
    table,
    sum(marks)                          AS total_granules,
    sum(rows)                           AS total_rows,
    round(sum(rows) / sum(marks), 0)    AS rows_per_granule,
    formatReadableSize(sum(data_compressed_bytes)) AS size_on_disk
FROM system.parts
WHERE database = 'nyc_taxi_perf'
  AND table = 'trips'
  AND active = 1
GROUP BY table;

-- ============================================================
-- TALKING POINT
-- ============================================================
-- The sparse primary index has ONE entry per granule (~8192 rows).
-- For an 8M-row table: 3,000,000 / 8192 ≈ 367 granules total.
-- When you filter on the first ORDER BY column, ClickHouse binary-
-- searches this small index to find the relevant granule range.
-- The index itself fits in memory (typically just a few KB–MB).
-- ============================================================


-- ============================================================
-- 1.2  Query aligned with the primary key (FAST)
-- ============================================================
SELECT '==== 1.2  Primary-key-aligned query ====' AS step;

-- This query filters on pickup_datetime — our first ORDER BY column
SELECT count(), avg(fare_amount)
FROM trips
WHERE pickup_datetime >= '2015-08-01'
  AND pickup_datetime <  '2015-09-01';

-- Inspect how many granules were read
EXPLAIN indexes = 1
SELECT count(), avg(fare_amount)
FROM trips
WHERE pickup_datetime >= '2015-08-01'
  AND pickup_datetime <  '2015-09-01';

-- ============================================================
-- TALKING POINT
-- ============================================================
-- Look at "Granules: X/366" in the EXPLAIN output.
-- Only the granules that COULD contain June 2015 data are read.
-- Everything else is skipped at the index level — zero disk I/O
-- for the skipped granules.
-- ============================================================


-- ============================================================
-- 1.3  Query NOT aligned with the primary key (SLOW)
-- ============================================================
SELECT '==== 1.3  Non-key query — full scan ====' AS step;

-- This query filters on fare_amount — NOT in our ORDER BY
SELECT count(), avg(trip_distance)
FROM trips
WHERE fare_amount > 50;

EXPLAIN indexes = 1
SELECT count(), avg(trip_distance)
FROM trips
WHERE fare_amount > 50;

-- ============================================================
-- TALKING POINT
-- ============================================================
-- "Granules: 977/977" — ALL granules must be read because
-- fare_amount is not part of our primary key. ClickHouse can't
-- know which granules might have fare_amount > 50 without reading them.
-- This is where SKIP INDEXES (Demo 2) help.
-- ============================================================


-- ============================================================
-- 1.4  Composite key — column order matters
-- ============================================================
SELECT '==== 1.4  Composite key: column order effects ====' AS step;

-- Create two comparison tables with different key ordering
DROP TABLE IF EXISTS trips_key_a;
DROP TABLE IF EXISTS trips_key_b;

-- Table A: pickup_datetime first (good for time-range queries)
CREATE TABLE trips_key_a AS trips ENGINE = MergeTree
ORDER BY (pickup_datetime, pickup_ntaname, payment_type);

-- Table B: pickup_ntaname first (good for zone-filter queries)
CREATE TABLE trips_key_b AS trips ENGINE = MergeTree
ORDER BY (pickup_ntaname, pickup_datetime, payment_type);

-- Populate both tables
INSERT INTO trips_key_a SELECT * FROM trips;
INSERT INTO trips_key_b SELECT * FROM trips;

-- Query A: time-range filter — which table is faster?
SELECT '---- Query: Time-range filter ----' AS test;

EXPLAIN indexes = 1
SELECT count() FROM trips_key_a
WHERE pickup_datetime >= '2015-06-01' AND pickup_datetime < '2015-09-01';

EXPLAIN indexes = 1
SELECT count() FROM trips_key_b
WHERE pickup_datetime >= '2015-06-01' AND pickup_datetime < '2015-09-01';

-- Query B: zone filter — which table is faster?
SELECT '---- Query: Zone filter ----' AS test;

EXPLAIN indexes = 1
SELECT count() FROM trips_key_a
WHERE pickup_ntaname = 'Midtown-Midtown South';

EXPLAIN indexes = 1
SELECT count() FROM trips_key_b
WHERE pickup_ntaname = 'Midtown-Midtown South';

-- ============================================================
-- TALKING POINT
-- ============================================================
-- The first column in ORDER BY provides the MOST granule-skipping.
-- Subsequent columns only help when earlier columns are also filtered.

--Search Algorithm: binary search — the classic fast path. Because pickup_datetime is the first key column and the data is sorted purely by it, ClickHouse can do a single binary search to find the exact start and end granule of the June–September range.
-- Search Algorithm: generic exclusion search — a slower fallback. Because pickup_ntaname comes first in the sort order, the data is not globally sorted by pickup_datetime. Trips from June–September are scattered across all neighborhoods. ClickHouse must scan through each neighborhood's sub-range to find qualifying granules.
--
-- For time-series data: pickup_datetime as the first key column
-- is almost always the right choice.
-- ============================================================


-- ============================================================
-- 1.5  Clean up comparison tables
-- ============================================================
DROP TABLE IF EXISTS trips_key_a;
DROP TABLE IF EXISTS trips_key_b;

SELECT '[OK] Demo 1 complete: Primary Key Optimization.' AS status;
