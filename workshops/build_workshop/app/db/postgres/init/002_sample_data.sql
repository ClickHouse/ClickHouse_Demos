-- Load the same mini seed used by ClickHouse (mounted at /sample).

COPY taxi_zones(zone_id, borough, zone, service_zone, centroid_lat, centroid_lon)
FROM '/sample/zones.csv'
WITH (FORMAT csv, HEADER true);

COPY taxi_trips(pickup_datetime, dropoff_datetime, vendor_id, passenger_count, trip_distance, pickup_zone_id, dropoff_zone_id, payment_type, fare_amount, tip_amount)
FROM '/sample/trips.csv'
WITH (FORMAT csv, HEADER true);

