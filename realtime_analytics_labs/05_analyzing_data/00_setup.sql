-- ============================================================
-- Section 05: Analyzing Data — Setup
-- ============================================================
-- Dataset  : NYC Taxi Rides (public S3, ~8M rows)
-- Database : nyc_taxi_analytics
-- Run time : ~3–5 minutes (data loading from S3)
-- ============================================================

-- ------------------------------------------------------------
-- Step 1: Create database
-- ------------------------------------------------------------
SELECT '---- Step 1: Creating database ----' AS step;

CREATE DATABASE IF NOT EXISTS nyc_taxi_analytics;
USE nyc_taxi_analytics;


-- ------------------------------------------------------------
-- Step 2: Create trips table
-- ------------------------------------------------------------
SELECT '---- Step 2: Creating trips table ----' AS step;

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
PRIMARY KEY (pickup_datetime, dropoff_datetime)
ORDER BY (pickup_datetime, dropoff_datetime);

-- ============================================================
-- TALKING POINT
-- ============================================================
-- Notice the schema design choices:
--   * LowCardinality(String) for neighborhood names — these are
--     repeated values, so ClickHouse stores them as a dictionary
--     (like an enum but dynamic), which cuts storage significantly.
--   * Enum for payment_type — stored as a UInt8 (1 byte) internally.
--   * Nullable(Float64) for lat/lon — some records have no coordinates.
--   * ORDER BY (pickup_datetime, dropoff_datetime) — this becomes our
--     primary key, making time-range queries very fast.
-- ============================================================


-- ------------------------------------------------------------
-- Step 3: Load data from ClickHouse public S3 bucket
-- ------------------------------------------------------------
-- We load files 0, 1, 2 → approximately 8 million rows.
-- The full dataset has 200 files (~3 billion rows total).
-- Tip: increase {0..9} to load more data (each file ~2.7M rows).
-- ------------------------------------------------------------
SELECT '---- Step 3: Loading data from S3 (this takes 3–5 minutes) ----' AS step;

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
SETTINGS
    max_insert_threads = 4;


-- ------------------------------------------------------------
-- Step 4: Verify load and show storage stats
-- ------------------------------------------------------------
SELECT '---- Step 4: Verifying data load ----' AS step;

SELECT formatReadableQuantity(count()), min(pickup_datetime), max(pickup_datetime)
FROM trips;

SELECT
    formatReadableSize(sum(data_compressed_bytes))   AS compressed_size,
    formatReadableSize(sum(data_uncompressed_bytes)) AS uncompressed_size,
    round(sum(data_uncompressed_bytes) / sum(data_compressed_bytes), 1) AS compression_ratio
FROM system.parts
WHERE database = 'nyc_taxi_analytics'
  AND table = 'trips'
  AND active = 1;

-- ============================================================
-- TALKING POINT
-- ============================================================
-- Point out the compression ratio (typically 5–10x for this dataset).
-- ClickHouse compresses each column separately with codecs tuned
-- to the data type — numeric sequences compress especially well.
-- ============================================================

SELECT '[OK] Setup complete. nyc_taxi_analytics.trips is ready.' AS status;
