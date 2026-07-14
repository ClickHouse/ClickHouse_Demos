-- ===========================================================================
-- CDC materialized view for the NYC Taxi Ops War Room workshop.
--
-- Fans new CDC rows (ClickPipes: Postgres -> ClickHouse Cloud) into
-- nyc_tlc_data.taxi_trips, so the existing dashboards keep working unchanged.
--
-- RUN ORDER: run this AFTER db/cloud/001_cloud_schema.sql AND after the
-- ClickPipe's initial snapshot has created its destination table. It is split
-- out of 001 on purpose: 001 must apply cleanly on a fresh service, but this MV
-- references the pipe's destination table, which does not exist until the pipe
-- runs -- inlining it in 001 made that run fail with an UNKNOWN_TABLE error.
--
-- The statement is idempotent (CREATE MATERIALIZED VIEW IF NOT EXISTS).
--
-- ---------------------------------------------------------------------------
-- WHERE THE PIPE'S DESTINATION TABLE LIVES depends on how you created the pipe:
--
--   * Console wizard (ClickPipes UI): you choose the destination database. The
--     workshop tells you to pick `nyc_tlc_data` and keep the source table name,
--     which yields `nyc_tlc_data.realtime_trips`. Use VARIANT A below (default).
--
--   * CLI (clickhousectl cloud clickpipe create ...): the destination table
--     ALWAYS lands in the `default` database -- `--table-mapping` does not accept
--     a database qualifier. Use VARIANT B below instead (comment out A, uncomment B).
--
-- Confirm the actual location and column types first (DESCRIBE-first):
--   SELECT database, name, engine FROM system.tables WHERE name = 'realtime_trips';
--   DESCRIBE <that database>.realtime_trips;
--
-- ENGINE / bookkeeping columns: a ClickPipes/PeerDB destination carries three
-- extra columns -- `_peerdb_synced_at`, `_peerdb_is_deleted`, `_peerdb_version`.
-- The engine has been observed to differ by creation path: the console wizard is
-- documented to use `ReplacingMergeTree(_peerdb_version)`, while a CLI-created
-- pipe was observed live to land a plain (Shared)MergeTree ORDER BY id (the
-- `_peerdb_*` columns and the PeerDB type mapping were present in both). The MV
-- below only filters `WHERE _peerdb_is_deleted = 0` and does NOT use FINAL, so it
-- works with either engine; the loadgen is append-only (INSERT only), so there is
-- no version churn or soft-delete to reconcile. For ad-hoc queries against a
-- ReplacingMergeTree destination you can add FINAL to collapse versions; plain
-- MergeTree does not support (and does not need) FINAL.
--
-- Expected PeerDB type mapping: timestamptz -> DateTime64(6), int2 -> Int16,
-- int4 -> Int32, int8 -> Int64, double precision -> Float64, text -> String.
-- ===========================================================================

-- VARIANT A -- console-wizard pipe (destination database nyc_tlc_data). Default.
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

-- VARIANT B -- CLI-created pipe (destination lands in the `default` database).
-- Comment out VARIANT A above and uncomment this if you created the pipe with
-- clickhousectl. It is identical except for the FROM table.
--
-- CREATE MATERIALIZED VIEW IF NOT EXISTS nyc_tlc_data.realtime_trips_to_taxi_trips_mv
-- TO nyc_tlc_data.taxi_trips
-- AS
-- SELECT
--   car_type,
--   CAST(vendor_id AS UInt16) AS vendor_id,
--   CAST(pickup_datetime AS DateTime('UTC')) AS pickup_datetime,
--   CAST(dropoff_datetime AS DateTime('UTC')) AS dropoff_datetime,
--   CAST(pickup_location_id AS UInt16) AS pickup_location_id,
--   CAST(dropoff_location_id AS UInt16) AS dropoff_location_id,
--   CAST(passenger_count AS UInt16) AS passenger_count,
--   trip_distance AS trip_distance,
--   CAST(payment_type AS UInt16) AS payment_type,
--   fare_amount AS fare_amount,
--   tip_amount AS tip_amount,
--   total_amount AS total_amount,
--   'realtime_cdc' AS filename
-- FROM default.realtime_trips
-- WHERE _peerdb_is_deleted = 0;
