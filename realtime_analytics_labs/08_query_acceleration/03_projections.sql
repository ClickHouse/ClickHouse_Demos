-- ============================================================
-- Section 08: Demo 3 — Projections
-- ============================================================
-- Scenario : Pre-sort data for queries that filter on columns
--            not in the primary key
-- Concepts : ADD PROJECTION, MATERIALIZE PROJECTION,
--            automatic projection selection, aggregate projections
-- ============================================================

USE nyc_taxi_perf;

-- ============================================================
-- TALKING POINT
-- ============================================================
-- A Projection is a hidden sub-table stored alongside the main table.
-- It has its own sort order (and optionally pre-aggregated columns).
-- When you run a query, ClickHouse automatically picks the best
-- projection (or the base table) to answer it.
--
-- Key differences from skip indexes:
--   Skip index:  skips granules → still reads the main table data
--   Projection:  reads from an entirely different sorted copy
--                → optimal for queries with a DIFFERENT ORDER BY
--
-- Storage cost: each projection roughly doubles storage for those columns.
-- ============================================================


-- ============================================================
-- 3.1  Baseline: query on dropoff_ntaname (not in ORDER BY)
-- ============================================================
SELECT '==== 3.1  Baseline: filtering on dropoff_ntaname ====' AS step;

-- Without a projection, this reads ALL granules
SELECT count(), avg(fare_amount), avg(total_amount)
FROM trips
WHERE dropoff_ntaname = 'JFK Airport';

EXPLAIN indexes = 1
SELECT count(), avg(fare_amount)
FROM trips
WHERE dropoff_ntaname = 'JFK Airport';

-- ============================================================
-- TALKING POINT
-- ============================================================
-- dropoff_ntaname is NOT in our ORDER BY (pickup_datetime, dropoff_datetime).
-- The primary index can't help — all granules are read.
-- A skip index (bloom_filter) would help a bit, but a projection
-- gives us a fully sorted copy optimised for this access pattern.
-- ============================================================


-- ============================================================
-- 3.2  Add a row-level projection (different sort order)
-- ============================================================
SELECT '==== 3.2  Adding projection sorted by dropoff_ntaname ====' AS step;

ALTER TABLE trips ADD PROJECTION proj_by_dropoff
(
    SELECT *
    ORDER BY (dropoff_ntaname, pickup_datetime)
);

-- Materialise: apply to existing data
ALTER TABLE trips MATERIALIZE PROJECTION proj_by_dropoff;

-- ============================================================
-- TALKING POINT
-- ============================================================
-- MATERIALIZE runs as a background mutation — it builds the sorted
-- copy from existing data. For a live table, new inserts
-- automatically populate all active projections.
-- Watch progress with:
--   SELECT * FROM system.mutations
--   WHERE database = 'nyc_taxi_perf' AND table = 'trips'
-- ============================================================

-- Wait for materialisation to finish (check mutations)
SELECT
    command,
    is_done,
    parts_to_do
FROM system.mutations
WHERE database = 'nyc_taxi_perf'
  AND table = 'trips'
ORDER BY create_time DESC
LIMIT 5;


-- ============================================================
-- 3.3  Query after projection — ClickHouse auto-selects it
-- ============================================================
SELECT '==== 3.3  Same query — now uses projection ====' AS step;

SELECT count(), avg(fare_amount), avg(total_amount)
FROM trips
WHERE dropoff_ntaname = 'JFK Airport';

EXPLAIN indexes = 1
SELECT count(), avg(fare_amount)
FROM trips
WHERE dropoff_ntaname = 'JFK Airport';

-- ============================================================
-- TALKING POINT
-- ============================================================
-- Look at the EXPLAIN output — it now shows "Projection: proj_by_dropoff".
-- ClickHouse estimated that reading from the projection (sorted by
-- dropoff_ntaname) is faster than scanning the base table.
-- No query changes needed — it's fully automatic.
-- ============================================================


SELECT '[OK] Demo 3 complete: Projections.' AS status;
