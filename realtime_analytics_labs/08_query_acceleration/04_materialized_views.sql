-- ============================================================
-- Section 08: Demo 4 — Materialized Views
-- ============================================================
-- Scenario : Pre-aggregate data so dashboards return instantly
-- Concepts : CREATE MATERIALIZED VIEW, SummingMergeTree,
--            AggregatingMergeTree, MV as INSERT trigger,
--            querying MV target tables
-- ============================================================

USE nyc_taxi_perf;

-- ============================================================
-- TALKING POINT
-- ============================================================
-- A Materialized View (MV) in ClickHouse is NOT a cached query.
-- It is an INSERT trigger:
--   1. Data arrives in the source table
--   2. The MV SELECT runs on each new batch
--   3. Results are written to a separate TARGET table
--
-- Key benefits:
--   • Pre-aggregated results → instant dashboard queries
--   • Continuous, zero-latency updates
--   • No cron jobs, no Airflow, no batch ETL
--
-- Difference from projections:
--   • MV → explicitly queried from a named target table
--   • Projection → transparent, ClickHouse auto-selects
-- ============================================================


-- ============================================================
-- 4.1  Create the target table (SummingMergeTree)
-- ============================================================
SELECT '==== 4.1  Create MV target: hourly trips per zone ====' AS step;

-- Target table: pre-aggregated by hour + pickup zone
DROP TABLE IF EXISTS trips_hourly_zone;

CREATE TABLE trips_hourly_zone
(
    hour                DateTime,
    pickup_ntaname      LowCardinality(String),
    trips               UInt64,
    total_fare          Float64,
    total_tip           Float64,
    total_amount        Float64
)
ENGINE = SummingMergeTree
ORDER BY (hour, pickup_ntaname);

-- ============================================================
-- TALKING POINT
-- ============================================================
-- SummingMergeTree automatically SUMS numeric columns when it merges
-- parts with the same ORDER BY key. This makes it perfect as a
-- materialized view target — even if multiple MV batches write
-- to the same (hour, zone) key, the values will be merged correctly.
--
-- Important: use sum() and count() in queries on this table
-- (not avg directly) because multiple parts may not yet be merged:
--   avg_fare = sum(total_fare) / sum(trips)  ✓
--   avg(fare_amount)                          ✗ (not stored here)
-- ============================================================


-- ============================================================
-- 4.2  Create the Materialized View
-- ============================================================
SELECT '==== 4.2  Create the Materialized View ====' AS step;

DROP VIEW IF EXISTS mv_hourly_zone;

CREATE MATERIALIZED VIEW mv_hourly_zone
TO trips_hourly_zone
AS
SELECT
    toStartOfHour(pickup_datetime)  AS hour,
    pickup_ntaname,
    count()                         AS trips,
    sum(fare_amount)                AS total_fare,
    sum(tip_amount)                 AS total_tip,
    sum(total_amount)               AS total_amount
FROM trips
WHERE pickup_ntaname != ''
GROUP BY hour, pickup_ntaname;

-- ============================================================
-- TALKING POINT
-- ============================================================
-- The MV definition is the transformation SELECT.
-- It only fires on NEW inserts to `trips`. It does NOT
-- retroactively process existing data.
-- We'll backfill existing data manually in the next step.
-- ============================================================


-- ============================================================
-- 4.3  Backfill: populate MV target from existing data
-- ============================================================
SELECT '==== 4.3  Backfilling existing data into MV target ====' AS step;

INSERT INTO trips_hourly_zone
SELECT
    toStartOfHour(pickup_datetime)  AS hour,
    pickup_ntaname,
    count()                         AS trips,
    sum(fare_amount)                AS total_fare,
    sum(tip_amount)                 AS total_tip,
    sum(total_amount)               AS total_amount
FROM trips
WHERE pickup_ntaname != ''
GROUP BY hour, pickup_ntaname;

SELECT
    formatReadableQuantity(count())     AS rows_in_mv_target,
    min(hour)                           AS earliest_hour,
    max(hour)                           AS latest_hour
FROM trips_hourly_zone;

-- ============================================================
-- TALKING POINT
-- ============================================================
-- Backfilling is a manual INSERT INTO ... SELECT.
-- After this, the MV target has pre-aggregated summaries.
-- New inserts to `trips` will automatically update the MV target.
-- ============================================================


-- ============================================================
-- 4.4  Performance comparison: raw vs. MV target
-- ============================================================
SELECT '==== 4.4  Performance: raw table vs. MV target ====' AS step;

-- Query 1: Hourly revenue for a specific zone — RAW TABLE
SELECT '---- Query on raw trips table ----' AS method;
SELECT
    hour,
    trips,
    round(total_fare / trips, 2)    AS avg_fare,
    round(total_amount, 0)          AS total_revenue
FROM (
    SELECT
        toStartOfHour(pickup_datetime)  AS hour,
        count()                         AS trips,
        sum(fare_amount)                AS total_fare,
        sum(total_amount)               AS total_amount
    FROM trips
    WHERE pickup_ntaname = 'Midtown-Midtown South'
    GROUP BY hour
)
ORDER BY hour
LIMIT 24;

-- Query 2: Same data — MV TARGET
SELECT '---- Query on MV target table ----' AS method;
SELECT
    hour,
    sum(trips)                              AS trips_total,
    round(sum(total_fare) / sum(trips), 2)  AS avg_fare,
    round(sum(total_amount), 0)             AS total_revenue
FROM trips_hourly_zone
WHERE pickup_ntaname = 'Midtown-Midtown South'
GROUP BY hour
ORDER BY hour
LIMIT 24;

-- ============================================================
-- TALKING POINT
-- ============================================================
-- The results are identical, but the MV query runs on a
-- fraction of the data (one row per hour per zone vs. 33M rows).
-- For dashboards refreshed every 30 seconds, this difference
-- means the query runs in microseconds instead of seconds.
-- ============================================================


-- ============================================================
-- 4.5  Verify MV fires on new inserts
-- ============================================================
SELECT '==== 4.5  Verify MV triggers on INSERT ====' AS step;

-- Count current rows in MV for a future timestamp
SELECT count() AS existing_rows_for_future_hour
FROM trips_hourly_zone
WHERE hour = '2023-01-01 12:00:00';

-- Insert synthetic "future" trips
INSERT INTO trips (trip_id, pickup_datetime, dropoff_datetime, fare_amount,
                   tip_amount, total_amount, payment_type, pickup_ntaname,
                   dropoff_ntaname, passenger_count, trip_distance)
SELECT
    number + 9000000                            AS trip_id,
    '2023-01-01 12:30:00'                       AS pickup_datetime,
    '2023-01-01 12:45:00'                       AS dropoff_datetime,
    10.0 + (number % 5)                         AS fare_amount,
    2.0                                         AS tip_amount,
    13.5                                        AS total_amount,
    'CRE'                                       AS payment_type,
    'Midtown-Midtown South'                     AS pickup_ntaname,
    'JFK Airport'                               AS dropoff_ntaname,
    1                                           AS passenger_count,
    5.0                                         AS trip_distance
FROM numbers(100);

-- Check MV target — should now have rows for 2023-01-01 12:00
SELECT
    hour,
    pickup_ntaname,
    sum(trips)          AS trips,
    sum(total_amount)   AS revenue
FROM trips_hourly_zone
WHERE hour = '2023-01-01 12:00:00'
GROUP BY hour, pickup_ntaname;

-- ============================================================
-- TALKING POINT
-- ============================================================
-- The MV fired automatically on the INSERT — no scheduler,
-- no cron, no Airflow needed. The aggregated results appeared
-- in trips_hourly_zone instantly.
-- ClickHouse rounds pickup_datetime to toStartOfHour,
-- so 12:30 → 12:00:00 in the MV target.
-- ============================================================


-- ============================================================
-- 4.6  AggregatingMergeTree — for averages and exact aggregates
-- ============================================================
SELECT '==== 4.6  AggregatingMergeTree for exact aggregates ====' AS step;

-- When you need avg(), uniq(), or other non-summable aggregates,
-- use AggregatingMergeTree with *State and *Merge functions.

DROP TABLE IF EXISTS trips_daily_agg;
DROP VIEW IF EXISTS mv_daily_agg;

CREATE TABLE trips_daily_agg
(
    day                 Date,
    pickup_ntaname      LowCardinality(String),
    trips               AggregateFunction(count),
    total_fare          AggregateFunction(sum, Float32),
    avg_fare_state      AggregateFunction(avg, Float32),
    unique_dropoffs     AggregateFunction(uniq, LowCardinality(String))
)
ENGINE = AggregatingMergeTree
ORDER BY (day, pickup_ntaname);

CREATE MATERIALIZED VIEW mv_daily_agg
TO trips_daily_agg
AS
SELECT
    toDate(pickup_datetime)         AS day,
    pickup_ntaname,
    countState()                    AS trips,
    sumState(fare_amount)           AS total_fare,
    avgState(fare_amount)           AS avg_fare_state,
    uniqState(dropoff_ntaname)      AS unique_dropoffs
FROM trips
WHERE pickup_ntaname != ''
GROUP BY day, pickup_ntaname;

-- Backfill
INSERT INTO trips_daily_agg
SELECT
    toDate(pickup_datetime)         AS day,
    pickup_ntaname,
    countState()                    AS trips,
    sumState(fare_amount)           AS total_fare,
    avgState(fare_amount)           AS avg_fare_state,
    uniqState(dropoff_ntaname)      AS unique_dropoffs
FROM trips
WHERE pickup_ntaname != ''
GROUP BY day, pickup_ntaname;

-- Query using *Merge functions to finalise aggregates
SELECT
    day,
    pickup_ntaname,
    countMerge(trips)               AS total_trips,
    round(sumMerge(total_fare), 2)  AS total_fare,
    round(avgMerge(avg_fare_state), 2) AS avg_fare,
    uniqMerge(unique_dropoffs)      AS unique_dropoff_zones
FROM trips_daily_agg
WHERE pickup_ntaname = 'Midtown-Midtown South'
GROUP BY day, pickup_ntaname
ORDER BY day
LIMIT 10;

-- ============================================================
-- TALKING POINT
-- ============================================================
-- *State() functions store partial aggregation state (a binary blob).
-- *Merge() functions finalise the state into a final value.
-- This pattern supports exact avg(), uniq(), quantile() in MVs
-- which are not natively summable like SUM or COUNT.
-- AggregatingMergeTree merges these state blobs automatically.
-- ============================================================

SELECT '[OK] Demo 4 complete: Materialized Views.' AS status;
