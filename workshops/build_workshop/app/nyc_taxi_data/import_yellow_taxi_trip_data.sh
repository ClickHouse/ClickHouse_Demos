#!/usr/bin/env bash
set -euo pipefail
shopt -s nullglob

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "${SCRIPT_DIR}"

# Default to the Docker Compose Postgres exposed on localhost:5432.
export PGHOST="${PGHOST:-127.0.0.1}"
export PGPORT="${PGPORT:-5432}"
export PGUSER="${PGUSER:-taxi}"
export PGPASSWORD="${PGPASSWORD:-taxi}"
export PGDATABASE="${PGDATABASE:-nyc-taxi-data}"

psql_cmd() {
  psql -v ON_ERROR_STOP=1 -h "${PGHOST}" -p "${PGPORT}" -U "${PGUSER}" "$@"
}

year_month_regex="tripdata_([0-9]{4})-([0-9]{2})"

yellow_schema="(vendor_id, tpep_pickup_datetime, tpep_dropoff_datetime, passenger_count, trip_distance, rate_code_id, store_and_fwd_flag, pickup_location_id, dropoff_location_id, payment_type, fare_amount, extra, mta_tax, tip_amount, tolls_amount, improvement_surcharge, total_amount, congestion_surcharge, airport_fee)"

yellow_schema_pre_2011="(vendor_id, tpep_pickup_datetime, tpep_dropoff_datetime, passenger_count, trip_distance, pickup_longitude, pickup_latitude, rate_code_id, store_and_fwd_flag, dropoff_longitude, dropoff_latitude, payment_type, fare_amount, extra, mta_tax, tip_amount, tolls_amount, total_amount)"

ensure_tables() {
  # Yellow loads require:
  # - yellow_tripdata_staging (with id PK used by populate sql)
  # - cab_types (with row 'yellow')
  # - trips (target table)
  psql_cmd -d "${PGDATABASE}" -c "
    CREATE TABLE IF NOT EXISTS cab_types (
      id serial primary key,
      type text
    );
    INSERT INTO cab_types (type)
    SELECT 'yellow' WHERE NOT EXISTS (SELECT 1 FROM cab_types WHERE type='yellow');
    INSERT INTO cab_types (type)
    SELECT 'green' WHERE NOT EXISTS (SELECT 1 FROM cab_types WHERE type='green');

    CREATE TABLE IF NOT EXISTS trips (
      id bigserial primary key,
      cab_type_id integer,
      vendor_id integer,
      pickup_datetime timestamp without time zone,
      dropoff_datetime timestamp without time zone,
      store_and_fwd_flag boolean,
      rate_code_id integer,
      pickup_longitude numeric,
      pickup_latitude numeric,
      dropoff_longitude numeric,
      dropoff_latitude numeric,
      passenger_count integer,
      trip_distance numeric,
      fare_amount numeric,
      extra numeric,
      mta_tax numeric,
      tip_amount numeric,
      tolls_amount numeric,
      ehail_fee numeric,
      improvement_surcharge numeric,
      congestion_surcharge numeric,
      airport_fee numeric,
      total_amount numeric,
      payment_type integer,
      trip_type integer,
      pickup_nyct2010_gid integer,
      dropoff_nyct2010_gid integer,
      pickup_location_id integer,
      dropoff_location_id integer
    );

    CREATE TABLE IF NOT EXISTS yellow_tripdata_staging (
      id bigserial primary key,
      vendor_id text,
      tpep_pickup_datetime timestamp without time zone,
      tpep_dropoff_datetime timestamp without time zone,
      passenger_count integer,
      trip_distance numeric,
      pickup_longitude numeric,
      pickup_latitude numeric,
      rate_code_id text,
      store_and_fwd_flag text,
      dropoff_longitude numeric,
      dropoff_latitude numeric,
      pickup_location_id integer,
      dropoff_location_id integer,
      payment_type text,
      fare_amount numeric,
      extra numeric,
      mta_tax numeric,
      tip_amount numeric,
      tolls_amount numeric,
      improvement_surcharge numeric,
      total_amount numeric,
      congestion_surcharge numeric,
      airport_fee numeric
    );

    -- Upgrade older staging tables if missing newer columns
    ALTER TABLE yellow_tripdata_staging ADD COLUMN IF NOT EXISTS pickup_location_id integer;
    ALTER TABLE yellow_tripdata_staging ADD COLUMN IF NOT EXISTS dropoff_location_id integer;
    ALTER TABLE yellow_tripdata_staging ADD COLUMN IF NOT EXISTS congestion_surcharge numeric;
    ALTER TABLE yellow_tripdata_staging ADD COLUMN IF NOT EXISTS airport_fee numeric;
    ALTER TABLE yellow_tripdata_staging ADD COLUMN IF NOT EXISTS improvement_surcharge numeric;
  " >/dev/null
}

ensure_tables
SKIP_POPULATE="${SKIP_POPULATE:-0}"

for parquet_filename in data/yellow_tripdata*.parquet; do
  [[ $parquet_filename =~ $year_month_regex ]]
  year=${BASH_REMATCH[1]}

  if [ $year -lt 2011 ]; then
    schema=$yellow_schema_pre_2011
  else
    schema=$yellow_schema
  fi

  echo "`date`: converting ${parquet_filename} to csv"
  ./setup_files/convert_parquet_to_csv.R "${parquet_filename}"

  csv_filename=${parquet_filename/.parquet/.csv}
  cat "$csv_filename" | psql_cmd -d "${PGDATABASE}" -c "COPY yellow_tripdata_staging ${schema} FROM stdin CSV HEADER;"
  echo "`date`: finished raw load for ${csv_filename}"

  if [[ "${SKIP_POPULATE}" == "1" ]]; then
    echo "`date`: NOTE SKIP_POPULATE=1; leaving data in yellow_tripdata_staging"
  else
    # The original populate_yellow_trips.sql requires PostGIS + nyct2010 polygons.
    # If PostGIS isn't available, fall back to a no-PostGIS populate that uses location_id
    # when present and leaves nyct2010_gid NULL.
    has_postgis="$(psql_cmd -d "${PGDATABASE}" -tAc "SELECT 1 FROM pg_extension WHERE extname='postgis' LIMIT 1" || true)"
    has_nyct="$(psql_cmd -d "${PGDATABASE}" -tAc "SELECT to_regclass('public.nyct2010') IS NOT NULL" || true)"
    has_map="$(psql_cmd -d "${PGDATABASE}" -tAc "SELECT to_regclass('public.nyct2010_taxi_zones_mapping') IS NOT NULL" || true)"

    if [[ -f setup_files/populate_yellow_trips.sql && "${has_postgis}" == "1" && "${has_nyct}" == "t" && "${has_map}" == "t" ]]; then
      psql_cmd -d "${PGDATABASE}" -f setup_files/populate_yellow_trips.sql
      echo "`date`: loaded trips for ${csv_filename}"
    elif [[ -f setup_files/populate_yellow_trips_no_postgis.sql ]]; then
      if [[ "${year}" -lt 2011 ]]; then
        echo "`date`: NOTE year<2011 and PostGIS mapping unavailable; loading without zone mapping (pickup/dropoff_location_id may be NULL)."
      fi
      psql_cmd -d "${PGDATABASE}" -f setup_files/populate_yellow_trips_no_postgis.sql
      echo "`date`: loaded trips for ${csv_filename} (no-postgis)"
    else
      echo "`date`: NOTE populate SQL missing; leaving data in yellow_tripdata_staging"
    fi
  fi

  rm -f "$csv_filename"
  echo "`date`: deleted ${csv_filename}"
done;
