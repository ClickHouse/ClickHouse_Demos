-- ===========================================================================
-- Optional historical seed for the NYC Taxi Ops War Room workshop.
--
-- Runnable companion to db/cloud/001_cloud_schema.sql. 001 creates the schema
-- (tables + views + the CDC materialized view); this file loads reference and
-- historical data so the Historical dashboard has real volume. Run it AFTER 001.
--
-- Cloud has no access to local user_files, so both loads read from public object
-- storage with url(). They pull data over the network, so run this deliberately
-- (not as part of app startup), e.g.:
--
--   clickhouse client --host <your-service>.clickhouse.cloud --port 9440 --secure \
--     --user default --password '<password>' --multiquery < db/cloud/002_seed_historical.sql
--
--   -- or paste both statements into the Cloud SQL console (zones first, then trips)
--
-- Both statements are idempotent: each is guarded by a count()=0 check, so
-- re-running this file cannot double-load. Zones must exist before trips (the
-- borough enrichment below looks them up), and this file keeps them in that order.
--
-- To load more than one month, add another copy of statement 2 with the next
-- month's file name in both the url() and the filename literal (the literal is
-- what the guard keys on), e.g. yellow_tripdata_2022-08.parquet. Each month is
-- ~3M rows, so keep the range small for a workshop Cloud service.
-- ===========================================================================

-- 1) taxi_zones gives dashboards human-readable zone/borough names. Without it,
--    zone filters and the borough enrichment below fall back to numeric IDs.
--    Guard: only load when the table is empty.
INSERT INTO nyc_tlc_data.taxi_zones (location_id, zone, borough, subregion)
SELECT LocationID, Zone, Borough, service_zone
FROM url(
  'https://d37ci6vzurychx.cloudfront.net/misc/taxi_zone_lookup.csv',
  'CSVWithNames',
  'LocationID UInt16, Borough String, Zone String, service_zone String'
)
WHERE (SELECT count() FROM nyc_tlc_data.taxi_zones) = 0;

-- 2) A one-month yellow-taxi subset from the public TLC parquet exports. Column
--    mapping cribbed from nyc_taxi_data/clickhouse/setup_files/load_yellow_trips.sql.
--    Guard: only load when this month's file has not already been ingested (keyed
--    on the filename literal set in the SELECT), so re-runs cannot double-load.
INSERT INTO nyc_tlc_data.taxi_trips (
  car_type, vendor_id, pickup_datetime, dropoff_datetime, pickup_location_id,
  dropoff_location_id, pickup_borough, dropoff_borough, passenger_count,
  trip_distance, rate_code_id, store_and_fwd_flag, payment_type, fare_amount,
  extra, mta_tax, tip_amount, tolls_amount, improvement_surcharge,
  total_amount, congestion_surcharge, airport_fee, filename
)
SELECT
  'yellow',
  VendorID,
  tpep_pickup_datetime,
  tpep_dropoff_datetime,
  PULocationID,
  DOLocationID,
  multiIf(
    PULocationID IN (SELECT location_id FROM nyc_tlc_data.taxi_zones WHERE borough = 'Bronx'), 'Bronx',
    PULocationID IN (SELECT location_id FROM nyc_tlc_data.taxi_zones WHERE borough = 'Brooklyn'), 'Brooklyn',
    PULocationID IN (SELECT location_id FROM nyc_tlc_data.taxi_zones WHERE borough = 'Manhattan'), 'Manhattan',
    PULocationID IN (SELECT location_id FROM nyc_tlc_data.taxi_zones WHERE borough = 'Queens'), 'Queens',
    PULocationID IN (SELECT location_id FROM nyc_tlc_data.taxi_zones WHERE borough = 'Staten Island'), 'Staten Island',
    PULocationID IN (SELECT location_id FROM nyc_tlc_data.taxi_zones WHERE borough = 'EWR'), 'EWR',
    null
  ),
  multiIf(
    DOLocationID IN (SELECT location_id FROM nyc_tlc_data.taxi_zones WHERE borough = 'Bronx'), 'Bronx',
    DOLocationID IN (SELECT location_id FROM nyc_tlc_data.taxi_zones WHERE borough = 'Brooklyn'), 'Brooklyn',
    DOLocationID IN (SELECT location_id FROM nyc_tlc_data.taxi_zones WHERE borough = 'Manhattan'), 'Manhattan',
    DOLocationID IN (SELECT location_id FROM nyc_tlc_data.taxi_zones WHERE borough = 'Queens'), 'Queens',
    DOLocationID IN (SELECT location_id FROM nyc_tlc_data.taxi_zones WHERE borough = 'Staten Island'), 'Staten Island',
    DOLocationID IN (SELECT location_id FROM nyc_tlc_data.taxi_zones WHERE borough = 'EWR'), 'EWR',
    null
  ),
  passenger_count,
  trip_distance,
  RatecodeID,
  multiIf(store_and_fwd_flag = 'Y', true, store_and_fwd_flag = 'N', false, null),
  payment_type,
  fare_amount,
  extra,
  mta_tax,
  tip_amount,
  tolls_amount,
  improvement_surcharge,
  total_amount,
  congestion_surcharge,
  airport_fee,
  'yellow_tripdata_2022-07.parquet'
FROM url(
  'https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_2022-07.parquet',
  'Parquet'
)
WHERE (
  SELECT count() FROM nyc_tlc_data.taxi_trips
  WHERE filename = 'yellow_tripdata_2022-07.parquet'
) = 0;
