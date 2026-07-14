CREATE TABLE IF NOT EXISTS taxi_zones (
  zone_id integer PRIMARY KEY,
  borough text NOT NULL,
  zone text NOT NULL,
  service_zone text NOT NULL,
  centroid_lat double precision NULL,
  centroid_lon double precision NULL
);

CREATE TABLE IF NOT EXISTS taxi_trips (
  pickup_datetime timestamptz NOT NULL,
  dropoff_datetime timestamptz NOT NULL,
  vendor_id smallint NOT NULL,
  passenger_count smallint NOT NULL,
  trip_distance real NOT NULL,
  pickup_zone_id integer NOT NULL,
  dropoff_zone_id integer NOT NULL,
  payment_type smallint NOT NULL,
  fare_amount real NOT NULL,
  tip_amount real NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_taxi_trips_pickup_datetime ON taxi_trips (pickup_datetime);
CREATE INDEX IF NOT EXISTS idx_taxi_trips_pickup_zone ON taxi_trips (pickup_zone_id);
CREATE INDEX IF NOT EXISTS idx_taxi_trips_dropoff_zone ON taxi_trips (dropoff_zone_id);

