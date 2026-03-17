-- ============================================================
-- Section 08: Query Acceleration Techniques — Setup
-- ============================================================
-- Dataset  : NYC Taxi Rides (public S3, ~8M rows)
-- Database : nyc_taxi_perf
-- Note     : This creates a FRESH database separate from Section 05
--            so we can add indexes/projections without affecting
--            the Section 05 analysis environment.
-- ============================================================

-- ------------------------------------------------------------
-- Step 1: Create database
-- ------------------------------------------------------------
SELECT '---- Step 1: Creating database nyc_taxi_perf ----' AS step;

CREATE DATABASE IF NOT EXISTS nyc_taxi_perf;
USE nyc_taxi_perf;


-- ------------------------------------------------------------
-- Step 2: Create baseline trips table (no extra indexes yet)
-- ------------------------------------------------------------
-- We deliberately start with a MINIMAL schema so we can add
-- indexes, projections, and MVs incrementally in later demos.
-- ------------------------------------------------------------
SELECT '---- Step 2: Creating baseline trips table ----' AS step;

DROP TABLE IF EXISTS trips;

CREATE TABLE trips
(
    trip_id             UInt32,
    pickup_datetime     DateTime,
    dropoff_datetime    DateTime,
    pickup_longitude    Nullable(Float64),
    pickup_latitude     Nullable(Float64),
    dropoff_longitude   Nullable(Float64),
    dropoff_latitude    Nullable(Float64),
    passenger_count     UInt8,
    trip_distance       Float32,
    fare_amount         Float32,
    extra               Float32,
    tip_amount          Float32,
    tolls_amount        Float32,
    total_amount        Float32,
    payment_type        Enum('CSH' = 1, 'CRE' = 2, 'NOC' = 3, 'DIS' = 4, 'UNK' = 5),
    pickup_ntaname      LowCardinality(String),
    dropoff_ntaname     LowCardinality(String)
)
ENGINE = MergeTree
ORDER BY (pickup_datetime, dropoff_datetime)
SETTINGS index_granularity = 8192;     -- default: one index entry per 8192 rows

-- ============================================================
-- TALKING POINT
-- ============================================================
-- index_granularity = 8192 means ClickHouse stores one primary
-- index entry for every 8192 rows. This is the default and
-- works well for most use cases. A lower value means finer
-- granularity (more index entries, more memory usage).
-- Our table with ~8M rows will have roughly 8M/8192 ≈ 977 granules.
-- ============================================================


-- ------------------------------------------------------------
-- Step 3: Load data from S3
-- ------------------------------------------------------------
SELECT '---- Step 3: Loading data from S3 (3–5 minutes) ----' AS step;

INSERT INTO trips
SELECT *
FROM s3(
    'https://datasets-documentation.s3.eu-west-3.amazonaws.com/nyc-taxi/trips_{0..2}.gz',
    'TabSeparated',
    'trip_id             UInt32,
     pickup_datetime     DateTime,
     dropoff_datetime    DateTime,
     pickup_longitude    Nullable(Float64),
     pickup_latitude     Nullable(Float64),
     dropoff_longitude   Nullable(Float64),
     dropoff_latitude    Nullable(Float64),
     passenger_count     UInt8,
     trip_distance       Float32,
     fare_amount         Float32,
     extra               Float32,
     tip_amount          Float32,
     tolls_amount        Float32,
     total_amount        Float32,
     payment_type        Enum(''CSH'' = 1, ''CRE'' = 2, ''NOC'' = 3, ''DIS'' = 4, ''UNK'' = 5),
     pickup_ntaname      LowCardinality(String),
     dropoff_ntaname     LowCardinality(String)'
)
SETTINGS max_insert_threads = 4;


-- ------------------------------------------------------------
-- Step 4: Verify load and show granule statistics
-- ------------------------------------------------------------
SELECT '---- Step 4: Verifying data and granule count ----' AS step;

SELECT
    formatReadableQuantity(count())     AS total_rows,
    (SELECT count() FROM system.parts
     WHERE database = 'nyc_taxi_perf' AND table = 'trips' AND active)
                                        AS active_parts,
    (SELECT sum(marks)
     FROM system.parts
     WHERE database = 'nyc_taxi_perf' AND table = 'trips' AND active)
                                        AS total_granules,
    formatReadableSize(
        (SELECT sum(data_compressed_bytes)
         FROM system.parts
         WHERE database = 'nyc_taxi_perf' AND table = 'trips' AND active)
    )                                   AS compressed_size
FROM trips;

-- ============================================================
-- TALKING POINT
-- ============================================================
-- The total_granules count (≈ rows / 8192) is key for understanding
-- query performance. Each granule is the smallest unit of data
-- ClickHouse can skip during a query. A query that reads N granules
-- out of M total reads (N/M) × 100% of the data.
-- Our goal in this section is to minimise this ratio for common queries.
-- ============================================================

SELECT '[OK] Setup complete. nyc_taxi_perf.trips is ready.' AS status;
