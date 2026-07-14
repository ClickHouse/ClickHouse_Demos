-- CDC sink target table for Kafka Connect → ClickHouse.
-- Note: docker-entrypoint-initdb.d scripts only run on a fresh ClickHouse volume.
-- We also create this table at runtime via the `clickhouse-cdc-init` service.

CREATE TABLE IF NOT EXISTS nyc_tlc_data.realtime_trips_cdc
(
  id UInt64,
  pickup_datetime DateTime64(3),
  dropoff_datetime DateTime64(3),
  pickup_location_id UInt16,
  dropoff_location_id UInt16,
  passenger_count UInt16,
  trip_distance Float64,
  fare_amount Float64,
  tip_amount Float64,
  total_amount Float64,
  payment_type UInt16,
  vendor_id UInt16,
  car_type LowCardinality(String),
  created_at DateTime64(3),
  __deleted UInt8 DEFAULT 0
)
ENGINE = MergeTree
ORDER BY (pickup_datetime, id);

