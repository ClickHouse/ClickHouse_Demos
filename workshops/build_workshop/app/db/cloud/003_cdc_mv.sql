-- CDC materialized view for the CLI-created workshop ClickPipe.
-- Run after the pipe creates default.realtime_trips.
CREATE MATERIALIZED VIEW IF NOT EXISTS nyc_tlc_data.realtime_trips_to_taxi_trips_mv
TO nyc_tlc_data.taxi_trips
AS
SELECT
  car_type,
  CAST(vendor_id AS UInt16) AS vendor_id,
  CAST(pickup_datetime AS DateTime('UTC')) AS pickup_datetime,
  CAST(dropoff_datetime AS DateTime('UTC')) AS dropoff_datetime,
  CAST(pickup_location_id AS UInt16) AS pickup_location_id,
  CAST(dropoff_location_id AS UInt16) AS dropoff_location_id,
  CAST(passenger_count AS UInt16) AS passenger_count,
  trip_distance,
  CAST(payment_type AS UInt16) AS payment_type,
  fare_amount,
  tip_amount,
  total_amount,
  'realtime_cdc' AS filename
FROM default.realtime_trips
WHERE _peerdb_is_deleted = 0;
