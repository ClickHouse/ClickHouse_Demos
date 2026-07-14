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

green_schema="(vendor_id, lpep_pickup_datetime, lpep_dropoff_datetime, store_and_fwd_flag, rate_code_id, pickup_location_id, dropoff_location_id, passenger_count, trip_distance, fare_amount, extra, mta_tax, tip_amount, tolls_amount, ehail_fee, improvement_surcharge, total_amount, payment_type, trip_type, congestion_surcharge)"

ensure_tables() {
  # Green loads require:
  # - green_tripdata_staging
  # - cab_types (with row 'green')
  # - trips (target table)
  psql_cmd -d "${PGDATABASE}" -c "
    CREATE TABLE IF NOT EXISTS cab_types (
      id serial primary key,
      type text
    );
    INSERT INTO cab_types (type)
    SELECT 'green' WHERE NOT EXISTS (SELECT 1 FROM cab_types WHERE type='green');
    INSERT INTO cab_types (type)
    SELECT 'yellow' WHERE NOT EXISTS (SELECT 1 FROM cab_types WHERE type='yellow');

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

    CREATE TABLE IF NOT EXISTS green_tripdata_staging (
      id bigserial primary key,
      vendor_id integer,
      lpep_pickup_datetime timestamp without time zone,
      lpep_dropoff_datetime timestamp without time zone,
      store_and_fwd_flag text,
      rate_code_id integer,
      dropoff_location_id integer,
      congestion_surcharge numeric,
      passenger_count integer,
      trip_distance numeric,
      fare_amount numeric,
      extra numeric,
      mta_tax numeric,
      tip_amount numeric,
      tolls_amount numeric,
      ehail_fee numeric,
      improvement_surcharge numeric,
      total_amount numeric,
      payment_type integer,
      trip_type integer,
      pickup_location_id integer
    );
  " >/dev/null
}

ensure_tables

SKIP_POPULATE="${SKIP_POPULATE:-0}"

for parquet_filename in data/green_tripdata*.parquet; do
  echo "`date`: converting ${parquet_filename} to csv"
  ./setup_files/convert_parquet_to_csv.R "${parquet_filename}"

  csv_filename=${parquet_filename/.parquet/.csv}
  cat "$csv_filename" | psql_cmd -d "${PGDATABASE}" -c "COPY green_tripdata_staging ${green_schema} FROM stdin CSV HEADER;"
  echo "`date`: finished raw load for ${csv_filename}"

  if [[ -f setup_files/populate_green_trips.sql && "${SKIP_POPULATE}" != "1" ]]; then
    psql_cmd -d "${PGDATABASE}" -f setup_files/populate_green_trips.sql
    echo "`date`: loaded trips for ${csv_filename}"
  else
    if [[ "${SKIP_POPULATE}" == "1" ]]; then
      echo "`date`: NOTE SKIP_POPULATE=1; leaving data in green_tripdata_staging"
    else
      echo "`date`: NOTE populate SQL missing (setup_files/populate_green_trips.sql); leaving data in green_tripdata_staging"
    fi
  fi

  rm -f "$csv_filename"
  echo "`date`: deleted ${csv_filename}"
done;
