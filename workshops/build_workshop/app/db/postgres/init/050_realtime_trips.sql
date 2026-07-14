-- CDC demo table in the default database (POSTGRES_DB=taxi).
-- Debezium will capture changes from this table.
CREATE TABLE IF NOT EXISTS realtime_trips (
  id bigserial PRIMARY KEY,
  pickup_datetime timestamptz NOT NULL,
  dropoff_datetime timestamptz NOT NULL,
  pickup_location_id integer NOT NULL,
  dropoff_location_id integer NOT NULL,
  passenger_count smallint NOT NULL,
  trip_distance double precision NOT NULL,
  fare_amount double precision NOT NULL,
  tip_amount double precision NOT NULL,
  total_amount double precision NOT NULL,
  payment_type smallint NOT NULL,
  vendor_id smallint NOT NULL,
  car_type text NOT NULL DEFAULT 'yellow',
  created_at timestamptz NOT NULL DEFAULT now()
);

-- Required for Debezium + pgoutput logical decoding.
ALTER ROLE taxi WITH REPLICATION;

-- Publication for Debezium (autocreate mode is also enabled; this makes it explicit)
DO $$
BEGIN
  IF NOT EXISTS (SELECT 1 FROM pg_publication WHERE pubname = 'dbz_publication') THEN
    CREATE PUBLICATION dbz_publication FOR TABLE realtime_trips;
  END IF;
END $$;

