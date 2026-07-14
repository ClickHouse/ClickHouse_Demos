-- ===========================================================================
-- ClickHouse Cloud schema for the NYC Taxi Ops War Room workshop.
--
-- Adapted from db/init/001_schema.sql (+ the runtime CDC objects that the local
-- stack created in the clickhouse-cdc-init service). Run this against your
-- ClickHouse Cloud service, e.g. from the Cloud SQL console, clickhousectl, or
-- your agent. Every statement is idempotent: re-running it is safe.
--
-- RUN ORDER: the base tables/views are safe to run at any time. The CDC
-- materialized view near the bottom reads from nyc_tlc_data.realtime_trips,
-- which the ClickPipe creates -- so run this file (or at least that MV) AFTER
-- the ClickPipe's initial snapshot exists. The ClickPipe only needs the
-- nyc_tlc_data database, which the CREATE DATABASE below provides.
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
--     via the local Kafka/Debezium path. See the CDC section below.
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
-- In the LOCAL stack, Debezium + Kafka Connect sank Postgres CDC into a landing
-- table (realtime_trips_cdc) and a materialized view fanned those rows into
-- nyc_tlc_data.taxi_trips, which is what every dashboard queries.
--
-- In the WORKSHOP, ClickPipes replaces Kafka/Debezium. When you create a
-- ClickPipe from the shared managed Postgres table `public.realtime_trips` and
-- keep the default destination table name, ClickPipes CREATES the table
-- `nyc_tlc_data.realtime_trips` for you. ClickPipes/PeerDB destinations are
-- always ENGINE = ReplacingMergeTree(_peerdb_version) and carry three extra
-- bookkeeping columns:
--     _peerdb_synced_at  DateTime64(9)
--     _peerdb_is_deleted Int8
--     _peerdb_version    Int64
-- Because of that, ad-hoc queries against nyc_tlc_data.realtime_trips should use
-- FINAL and filter soft-deletes, e.g.
--     SELECT * FROM nyc_tlc_data.realtime_trips FINAL WHERE _peerdb_is_deleted = 0;
--
-- The materialized view below fans new CDC rows into taxi_trips so the existing
-- dashboards keep working unchanged. Notes:
--   * An MV fires per inserted block, so it does NOT need FINAL; we only filter
--     out soft-deletes with WHERE _peerdb_is_deleted = 0. The loadgen is
--     append-only (INSERT only), so updates/deletes are not expected in the
--     workshop and ReplacingMergeTree version churn does not occur.
--   * Column names below assume the workshop-standard ClickPipe: destination
--     database nyc_tlc_data, table name kept as realtime_trips (the playbook
--     tells participants to pick nyc_tlc_data and keep the source table name in
--     the wizard's tables step). Confirm the column TYPES live with
--     `DESCRIBE nyc_tlc_data.realtime_trips` -- the expected PeerDB mapping is
--     timestamptz -> DateTime64(6), int2 -> Int16, int4 -> Int32,
--     int8 -> Int64, double precision -> Float64, text -> String.
--
-- IMPORTANT: the CREATE below references nyc_tlc_data.realtime_trips, so it only
-- succeeds AFTER the ClickPipe's initial snapshot has created that table. Run
-- this whole file once the ClickPipe is live (the ClickPipe itself only needs
-- the nyc_tlc_data database, which the CREATE DATABASE at the top provides).
-- The statement is idempotent (CREATE MATERIALIZED VIEW IF NOT EXISTS).
-- ===========================================================================

CREATE MATERIALIZED VIEW IF NOT EXISTS nyc_tlc_data.realtime_trips_to_taxi_trips_mv
TO nyc_tlc_data.taxi_trips
AS
SELECT
  car_type,
  CAST(vendor_id AS UInt16) AS vendor_id,
  -- ClickPipes delivers Postgres timestamptz as DateTime64; cast to the UTC
  -- DateTime used by taxi_trips.
  CAST(pickup_datetime AS DateTime('UTC')) AS pickup_datetime,
  CAST(dropoff_datetime AS DateTime('UTC')) AS dropoff_datetime,
  CAST(pickup_location_id AS UInt16) AS pickup_location_id,
  CAST(dropoff_location_id AS UInt16) AS dropoff_location_id,
  CAST(passenger_count AS UInt16) AS passenger_count,
  trip_distance AS trip_distance,
  CAST(payment_type AS UInt16) AS payment_type,
  fare_amount AS fare_amount,
  tip_amount AS tip_amount,
  total_amount AS total_amount,
  'realtime_cdc' AS filename
FROM nyc_tlc_data.realtime_trips
WHERE _peerdb_is_deleted = 0;

-- ===========================================================================
-- Optional seeding (reference + historical data)
-- ---------------------------------------------------------------------------
-- Cloud has no access to local user_files, so seed from remote sources with
-- url()/s3(). These statements are commented out because they pull data over
-- the network; run them by hand (in order: zones first, then trips) when you
-- want the Historical dashboard to have real volume.
--
-- 1) taxi_zones gives dashboards human-readable zone/borough names. Without it,
--    zone filters and the borough enrichment below fall back to numeric IDs.
--
-- INSERT INTO nyc_tlc_data.taxi_zones (location_id, zone, borough, subregion)
-- SELECT LocationID, Zone, Borough, service_zone
-- FROM url(
--   'https://d37ci6vzurychx.cloudfront.net/misc/taxi_zone_lookup.csv',
--   'CSVWithNames',
--   'LocationID UInt16, Borough String, Zone String, service_zone String'
-- )
-- WHERE (SELECT count() FROM nyc_tlc_data.taxi_zones) = 0;
--
-- 2) A 1-3 month yellow-taxi subset from the public TLC parquet exports. Column
--    mapping cribbed from nyc_taxi_data/clickhouse/setup_files/load_yellow_trips.sql.
--    Load one month first; widen the brace list for more (e.g. {2022-07,2022-08,2022-09}).
--    ~3M rows per month, so keep the range small for a workshop Cloud service.
--
-- INSERT INTO nyc_tlc_data.taxi_trips (
--   car_type, vendor_id, pickup_datetime, dropoff_datetime, pickup_location_id,
--   dropoff_location_id, pickup_borough, dropoff_borough, passenger_count,
--   trip_distance, rate_code_id, store_and_fwd_flag, payment_type, fare_amount,
--   extra, mta_tax, tip_amount, tolls_amount, improvement_surcharge,
--   total_amount, congestion_surcharge, airport_fee, filename
-- )
-- SELECT
--   'yellow',
--   VendorID,
--   tpep_pickup_datetime,
--   tpep_dropoff_datetime,
--   PULocationID,
--   DOLocationID,
--   multiIf(
--     PULocationID IN (SELECT location_id FROM nyc_tlc_data.taxi_zones WHERE borough = 'Bronx'), 'Bronx',
--     PULocationID IN (SELECT location_id FROM nyc_tlc_data.taxi_zones WHERE borough = 'Brooklyn'), 'Brooklyn',
--     PULocationID IN (SELECT location_id FROM nyc_tlc_data.taxi_zones WHERE borough = 'Manhattan'), 'Manhattan',
--     PULocationID IN (SELECT location_id FROM nyc_tlc_data.taxi_zones WHERE borough = 'Queens'), 'Queens',
--     PULocationID IN (SELECT location_id FROM nyc_tlc_data.taxi_zones WHERE borough = 'Staten Island'), 'Staten Island',
--     PULocationID IN (SELECT location_id FROM nyc_tlc_data.taxi_zones WHERE borough = 'EWR'), 'EWR',
--     null
--   ),
--   multiIf(
--     DOLocationID IN (SELECT location_id FROM nyc_tlc_data.taxi_zones WHERE borough = 'Bronx'), 'Bronx',
--     DOLocationID IN (SELECT location_id FROM nyc_tlc_data.taxi_zones WHERE borough = 'Brooklyn'), 'Brooklyn',
--     DOLocationID IN (SELECT location_id FROM nyc_tlc_data.taxi_zones WHERE borough = 'Manhattan'), 'Manhattan',
--     DOLocationID IN (SELECT location_id FROM nyc_tlc_data.taxi_zones WHERE borough = 'Queens'), 'Queens',
--     DOLocationID IN (SELECT location_id FROM nyc_tlc_data.taxi_zones WHERE borough = 'Staten Island'), 'Staten Island',
--     DOLocationID IN (SELECT location_id FROM nyc_tlc_data.taxi_zones WHERE borough = 'EWR'), 'EWR',
--     null
--   ),
--   passenger_count,
--   trip_distance,
--   RatecodeID,
--   multiIf(store_and_fwd_flag = 'Y', true, store_and_fwd_flag = 'N', false, null),
--   payment_type,
--   fare_amount,
--   extra,
--   mta_tax,
--   tip_amount,
--   tolls_amount,
--   improvement_surcharge,
--   total_amount,
--   congestion_surcharge,
--   airport_fee,
--   'yellow_tripdata_2022-07.parquet'
-- FROM url(
--   'https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_2022-07.parquet',
--   'Parquet'
-- );
-- ===========================================================================
