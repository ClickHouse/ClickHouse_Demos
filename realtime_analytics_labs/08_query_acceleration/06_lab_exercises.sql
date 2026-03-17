-- ============================================================
-- Section 08: Lab Exercises — Query Acceleration Techniques
-- ============================================================
-- Complete each exercise independently using Demos 1–5 as a guide.
-- Answer keys are in the commented block at the bottom.
-- ============================================================

USE nyc_taxi_perf;

SELECT '============================================================' AS info;
SELECT 'SECTION 08 LAB EXERCISES — Write your code below each prompt.' AS info;
SELECT '============================================================' AS info;


-- ============================================================
-- Exercise 1 ★☆☆  (Primary Key / EXPLAIN)
-- ============================================================
-- Run EXPLAIN indexes=1 on the two queries below and compare
-- the number of granules read. For each query, write a comment
-- explaining WHY the index helps (or doesn't help).
--
-- Query A:
--   SELECT count() FROM trips
--   WHERE pickup_datetime BETWEEN '2013-03-01' AND '2013-03-31';
--
-- Query B:
--   SELECT count() FROM trips
--   WHERE trip_distance > 20;
--
-- Question: Which query benefits from the primary index? Why?
-- ============================================================

-- YOUR EXPLAIN STATEMENTS HERE:




-- ============================================================
-- Exercise 2 ★★☆  (Skip Index)
-- ============================================================
-- Add a minmax skip index on the trip_distance column.
-- Name it: idx_distance_minmax
-- Granularity: 1
-- After adding and materialising it, run EXPLAIN indexes=1 on:
--   SELECT count() FROM trips WHERE trip_distance > 20;
-- How many granules are read now vs. before the index?
-- ============================================================

-- YOUR ALTER TABLE STATEMENT HERE:




-- ============================================================
-- Exercise 3 ★★☆  (Projections)
-- ============================================================
-- Create a projection named proj_by_fare that sorts data by:
--   (fare_amount DESC, pickup_datetime)
-- This will accelerate queries looking for high-fare trips.
-- After materialising, run:
--   EXPLAIN indexes=1 SELECT * FROM trips ORDER BY fare_amount DESC LIMIT 10;
-- Verify the projection is used in the plan.
-- ============================================================

-- YOUR ALTER TABLE ... ADD PROJECTION STATEMENT HERE:




-- ============================================================
-- Exercise 4 ★★★  (Materialized View)
-- ============================================================
-- Create a materialized view that pre-aggregates DAILY revenue
-- per pickup neighborhood. Use the following spec:
--
-- Target table: trips_daily_revenue
--   Columns: day (Date), pickup_ntaname, trips (UInt64),
--            total_revenue (Float64), total_fare (Float64)
--   Engine:  SummingMergeTree
--   ORDER BY: (day, pickup_ntaname)
--
-- MV name: mv_daily_revenue
-- MV SELECT: aggregate trips table by day and pickup_ntaname
--
-- After creating the MV:
--   1. Backfill existing data with INSERT INTO ... SELECT ...
--   2. Query trips_daily_revenue to find the highest-revenue day
--      for 'Midtown-Midtown South'
-- ============================================================

-- YOUR CREATE TABLE, CREATE MATERIALIZED VIEW, and INSERT HERE:





-- ============================================================
-- ============================================================
-- ANSWER KEY — Review after attempting exercises independently
-- ============================================================
-- ============================================================

/*

-- Exercise 1 Answer

-- Query A — primary key aligned (pickup_datetime range)
EXPLAIN indexes = 1
SELECT count() FROM trips
WHERE pickup_datetime BETWEEN '2013-03-01' AND '2013-03-31';
-- Expected: Granules: ~80/977 (only March 2013 granules read)
-- WHY: pickup_datetime is the FIRST column in ORDER BY.
--      The sparse index stores pickup_datetime per granule,
--      so ClickHouse can binary-search and skip non-March granules.

-- Query B — NOT in primary key
EXPLAIN indexes = 1
SELECT count() FROM trips
WHERE trip_distance > 20;
-- Expected: Granules: 977/977 (full scan)
-- WHY: trip_distance is NOT in the ORDER BY.
--      ClickHouse has no index on this column, so it must
--      check every granule.


-- Exercise 2 Answer
ALTER TABLE trips
    ADD INDEX idx_distance_minmax (trip_distance) TYPE minmax GRANULARITY 1;

ALTER TABLE trips MATERIALIZE INDEX idx_distance_minmax;

EXPLAIN indexes = 1
SELECT count() FROM trips WHERE trip_distance > 20;
-- Expected: Granules should be fewer than 977 now.
-- Note: effectiveness depends on how trip_distance correlates
-- with pickup_datetime (our sort key). If high-distance trips
-- are clustered in time, more granules can be skipped.


-- Exercise 3 Answer
ALTER TABLE trips ADD PROJECTION proj_by_fare
(
    SELECT *
    ORDER BY (fare_amount DESC, pickup_datetime)
);

ALTER TABLE trips MATERIALIZE PROJECTION proj_by_fare;

EXPLAIN indexes = 1
SELECT * FROM trips ORDER BY fare_amount DESC LIMIT 10;
-- Look for "Projection: proj_by_fare" in the EXPLAIN output.


-- Exercise 4 Answer
DROP TABLE IF EXISTS trips_daily_revenue;
CREATE TABLE trips_daily_revenue
(
    day             Date,
    pickup_ntaname  LowCardinality(String),
    trips           UInt64,
    total_revenue   Float64,
    total_fare      Float64
)
ENGINE = SummingMergeTree
ORDER BY (day, pickup_ntaname);

DROP VIEW IF EXISTS mv_daily_revenue;
CREATE MATERIALIZED VIEW mv_daily_revenue
TO trips_daily_revenue
AS
SELECT
    toDate(pickup_datetime)     AS day,
    pickup_ntaname,
    count()                     AS trips,
    sum(total_amount)           AS total_revenue,
    sum(fare_amount)            AS total_fare
FROM trips
WHERE pickup_ntaname != ''
GROUP BY day, pickup_ntaname;

-- Backfill
INSERT INTO trips_daily_revenue
SELECT
    toDate(pickup_datetime)     AS day,
    pickup_ntaname,
    count()                     AS trips,
    sum(total_amount)           AS total_revenue,
    sum(fare_amount)            AS total_fare
FROM trips
WHERE pickup_ntaname != ''
GROUP BY day, pickup_ntaname;

-- Find highest revenue day for Midtown
SELECT
    day,
    sum(trips)          AS trips,
    sum(total_revenue)  AS revenue
FROM trips_daily_revenue
WHERE pickup_ntaname = 'Midtown-Midtown South'
GROUP BY day
ORDER BY revenue DESC
LIMIT 5;


*/
