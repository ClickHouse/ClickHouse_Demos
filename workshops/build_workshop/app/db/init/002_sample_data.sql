-- Tiny seed so the demo stack boots with data immediately.
-- This seed targets the *full* nyc_tlc_data schema (not the older demo schema).
--
-- IMPORTANT: If you already loaded the full dataset, this script will NO-OP
-- (it only inserts when tables are empty).

INSERT INTO nyc_tlc_data.taxi_zones (location_id, zone, borough, subregion)
SELECT
  zone_id AS location_id,
  zone,
  borough,
  service_zone AS subregion
FROM file(
  'sample/zones.csv',
  'CSVWithNames',
  'zone_id UInt16, borough String, zone String, service_zone String, centroid_lat Float64, centroid_lon Float64'
)
WHERE (SELECT count() FROM nyc_tlc_data.taxi_zones) = 0;

INSERT INTO nyc_tlc_data.taxi_trips
(
  car_type,
  vendor_id,
  pickup_datetime,
  dropoff_datetime,
  pickup_location_id,
  dropoff_location_id,
  passenger_count,
  trip_distance,
  payment_type,
  fare_amount,
  tip_amount,
  total_amount,
  filename
)
SELECT
  'yellow' AS car_type,
  toUInt16(vendor_id) AS vendor_id,
  toDateTime(pickup_datetime, 'UTC') AS pickup_datetime,
  toDateTime(dropoff_datetime, 'UTC') AS dropoff_datetime,
  pickup_zone_id AS pickup_location_id,
  dropoff_zone_id AS dropoff_location_id,
  toUInt16(passenger_count) AS passenger_count,
  toFloat64(trip_distance) AS trip_distance,
  toUInt16(payment_type) AS payment_type,
  toFloat64(fare_amount) AS fare_amount,
  toFloat64(tip_amount) AS tip_amount,
  toFloat64(fare_amount + tip_amount) AS total_amount,
  'sample.csv' AS filename
FROM file(
  'sample/trips.csv',
  'CSVWithNames',
  'pickup_datetime DateTime, dropoff_datetime DateTime, vendor_id UInt8, passenger_count UInt8, trip_distance Float32, pickup_zone_id UInt16, dropoff_zone_id UInt16, payment_type UInt8, fare_amount Float32, tip_amount Float32'
)
WHERE (SELECT count() FROM nyc_tlc_data.taxi_trips) = 0;

