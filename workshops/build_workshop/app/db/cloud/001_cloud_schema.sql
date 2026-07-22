-- ===========================================================================
-- ClickHouse Cloud schema for the NYC Taxi Ops War Room workshop.
--
-- Adapted from db/init/001_schema.sql (+ the runtime CDC objects that the local
-- stack created in the clickhouse-cdc-init service). Run this against your
-- ClickHouse Cloud service, e.g. from the Cloud SQL console, clickhousectl, or
-- your agent. Every statement is idempotent: re-running it is safe.
--
-- RUN ORDER: this file is safe to run at any time and applies cleanly on a fresh
-- service -- it creates only base tables/views. The CDC materialized view, which
-- must run AFTER the ClickPipe's initial snapshot creates its destination table,
-- lives in db/cloud/003_cdc_mv.sql (kept separate so this schema file never fails
-- with UNKNOWN_TABLE on a service where the pipe does not exist yet). The
-- ClickPipe only needs the nyc_tlc_data database, which the CREATE DATABASE below
-- provides.
--
-- Notes for ClickHouse Cloud:
--   * `ENGINE = MergeTree` is transparently backed by SharedMergeTree on Cloud;
--     you do NOT need to specify Replicated* engines or ON CLUSTER. Keep the
--     plain MergeTree declarations below.
--   * There are no local file() reads here. The local seed (db/init/002_sample_data.sql)
--     uses file('sample/...') from user_files, which is unavailable on Cloud.
--     Load reference/historical data from remote sources instead -- see the
--     optional seeding block at the bottom of this file.
--   * Real-time trips arrive via ClickPipes (Postgres -> ClickHouse Cloud), not
--     via the local Kafka/Debezium path. The CDC materialized view lives in
--     db/cloud/003_cdc_mv.sql (run it after the ClickPipe snapshot exists).
-- ===========================================================================

CREATE DATABASE IF NOT EXISTS nyc_tlc_data;

CREATE TABLE IF NOT EXISTS nyc_tlc_data.taxi_zones
(
  location_id UInt16,
  zone String,
  borough String,
  subregion String
)
ENGINE = MergeTree
ORDER BY (location_id);

CREATE TABLE IF NOT EXISTS nyc_tlc_data.fhv_trips
(
  hvfhs_license_num String,
  company String,
  dispatching_base_num Nullable(String),
  originating_base_num Nullable(String),
  request_datetime Nullable(DateTime('UTC')),
  on_scene_datetime Nullable(DateTime('UTC')),
  pickup_datetime DateTime('UTC'),
  dropoff_datetime DateTime('UTC'),
  pickup_location_id Nullable(UInt16),
  dropoff_location_id Nullable(UInt16),
  pickup_borough Nullable(String),
  dropoff_borough Nullable(String),
  trip_miles Nullable(Float64),
  trip_time Nullable(UInt32),
  base_passenger_fare Nullable(Float64),
  tolls Nullable(Float64),
  black_car_fund Nullable(Float64),
  sales_tax Nullable(Float64),
  congestion_surcharge Nullable(Float64),
  airport_fee Nullable(Float64),
  tips Nullable(Float64),
  driver_pay Nullable(Float64),
  shared_request Nullable(Bool),
  shared_match Nullable(Bool),
  access_a_ride Nullable(Bool),
  wav_request Nullable(Bool),
  wav_match Nullable(Bool),
  legacy_shared_ride Nullable(UInt16),
  filename String
)
ENGINE = MergeTree
ORDER BY (company, pickup_datetime);

CREATE TABLE IF NOT EXISTS nyc_tlc_data.taxi_trips
(
  car_type String,
  vendor_id Nullable(UInt16),
  pickup_datetime DateTime('UTC'),
  dropoff_datetime DateTime('UTC'),
  pickup_location_id Nullable(UInt16),
  dropoff_location_id Nullable(UInt16),
  pickup_borough Nullable(String),
  dropoff_borough Nullable(String),
  passenger_count Nullable(UInt16),
  trip_distance Nullable(Float64),
  rate_code_id Nullable(UInt16),
  store_and_fwd_flag Nullable(Bool),
  payment_type Nullable(UInt16),
  fare_amount Nullable(Float64),
  extra Nullable(Float64),
  mta_tax Nullable(Float64),
  tip_amount Nullable(Float64),
  tolls_amount Nullable(Float64),
  improvement_surcharge Nullable(Float64),
  total_amount Nullable(Float64),
  congestion_surcharge Nullable(Float64),
  airport_fee Nullable(Float64),
  trip_type Nullable(UInt16),
  ehail_fee Nullable(Float64),
  filename String
)
ENGINE = MergeTree
ORDER BY (car_type, pickup_datetime);

CREATE OR REPLACE VIEW nyc_tlc_data.fhv_trips_expanded AS
SELECT
  *,
  trip_time / 60 AS trip_minutes,
  trip_miles / trip_time * 3600 AS mph,
  (
    trip_miles >= 0.2
    AND trip_miles < 100
    AND trip_time >= 60
    AND trip_time < 60 * 60 * 4
    AND mph >= 1
    AND mph < 100
    AND base_passenger_fare >= 2
    AND base_passenger_fare < 2000
    AND driver_pay >= 1
    AND driver_pay < 2000
  ) AS reasonable_time_distance_fare,
  (
    shared_request = false
    AND access_a_ride = false
    AND wav_request = false
  ) AS solo_non_special_request,
  coalesce(tolls, 0) +
    coalesce(black_car_fund, 0) +
    coalesce(sales_tax, 0) +
    coalesce(congestion_surcharge, 0) +
    coalesce(airport_fee, 0) AS extra_charges
FROM nyc_tlc_data.fhv_trips;

CREATE OR REPLACE VIEW nyc_tlc_data.taxi_trips_expanded AS
SELECT
  *,
  (dropoff_datetime - pickup_datetime) / 60 AS trip_minutes,
  trip_distance / (dropoff_datetime - pickup_datetime) * 3600 AS mph,
  (
    trip_distance >= 0.2
    AND trip_distance < 100
    AND trip_minutes >= 1
    AND trip_minutes < 240
    AND mph >= 1
    AND mph < 100
    AND fare_amount >= 2
    AND fare_amount < 2000
    AND total_amount >= 2
    AND total_amount < 2000
  ) AS reasonable_time_distance_fare,
  coalesce(extra, 0) +
    coalesce(mta_tax, 0) +
    coalesce(tolls_amount, 0) +
    coalesce(improvement_surcharge, 0) +
    coalesce(congestion_surcharge, 0) +
    coalesce(airport_fee, 0) +
    coalesce(ehail_fee, 0) AS extra_charges
FROM nyc_tlc_data.taxi_trips;

-- ===========================================================================
-- Real-time CDC via ClickPipes
-- ---------------------------------------------------------------------------
-- In the WORKSHOP, ClickPipes (Postgres -> ClickHouse Cloud) replaces the local
-- Kafka/Debezium path, and a materialized view fans new CDC rows into
-- nyc_tlc_data.taxi_trips so the existing dashboards keep working unchanged.
--
-- That materialized view is NOT defined here. It references the ClickPipe's
-- destination table, which does not exist until the pipe's initial snapshot
-- runs, so inlining it made this schema file fail with UNKNOWN_TABLE on a fresh
-- service. It now lives in db/cloud/003_cdc_mv.sql -- run that file AFTER the
-- ClickPipe exists. It documents where the destination table lands (console
-- wizard -> nyc_tlc_data.realtime_trips; clickhousectl CLI -> default.realtime_trips)
-- and the observed engine (console: ReplacingMergeTree; CLI: plain MergeTree).
-- ===========================================================================

-- ===========================================================================
-- Optional seeding (reference + historical data)
-- ---------------------------------------------------------------------------
-- Moved to a runnable file: db/cloud/002_seed_historical.sql. It loads
-- taxi_zones (TLC zone-lookup CSV) and a one-month yellow-taxi subset (TLC
-- parquet) from public object storage via url(), each guarded by a count()=0
-- check so re-running cannot double-load. Run it AFTER this schema file, e.g.
--   clickhouse client ... --multiquery < db/cloud/002_seed_historical.sql
-- (kept separate so app startup never triggers the network fetch).
-- ===========================================================================
