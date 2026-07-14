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

fhv_schema="(dispatching_base_num, pickup_datetime, dropoff_datetime, pickup_location_id, dropoff_location_id, legacy_shared_ride_flag, affiliated_base_num)"

ensure_tables() {
  # Ensure the required staging + target tables exist, without requiring PostGIS.
  #
  # The full nyc_taxi_data schema file includes PostGIS and many other tables; for this
  # importer, we only need:
  #   - fhv_trips_staging (superset schema used by both FHV and FHVHV loaders)
  #   - fhv_trips (target table populated by setup_files/populate_fhv_trips.sql)
  #
  # Creating these tables is idempotent and avoids failing on missing relations.
  psql_cmd -d "${PGDATABASE}" -c "
    CREATE TABLE IF NOT EXISTS fhv_trips_staging (
      dispatching_base_num text,
      pickup_datetime timestamp without time zone,
      dropoff_datetime timestamp without time zone,
      pickup_location_id integer,
      dropoff_location_id integer,
      legacy_shared_ride_flag text,
      affiliated_base_num text
    );

    -- Ensure superset columns exist (older runs may have created a narrower staging table).
    ALTER TABLE fhv_trips_staging ADD COLUMN IF NOT EXISTS hvfhs_license_num text;
    ALTER TABLE fhv_trips_staging ADD COLUMN IF NOT EXISTS originating_base_num text;
    ALTER TABLE fhv_trips_staging ADD COLUMN IF NOT EXISTS request_datetime timestamp without time zone;
    ALTER TABLE fhv_trips_staging ADD COLUMN IF NOT EXISTS on_scene_datetime timestamp without time zone;
    ALTER TABLE fhv_trips_staging ADD COLUMN IF NOT EXISTS trip_miles numeric;
    ALTER TABLE fhv_trips_staging ADD COLUMN IF NOT EXISTS trip_time numeric;
    ALTER TABLE fhv_trips_staging ADD COLUMN IF NOT EXISTS base_passenger_fare numeric;
    ALTER TABLE fhv_trips_staging ADD COLUMN IF NOT EXISTS tolls numeric;
    ALTER TABLE fhv_trips_staging ADD COLUMN IF NOT EXISTS black_car_fund numeric;
    ALTER TABLE fhv_trips_staging ADD COLUMN IF NOT EXISTS sales_tax numeric;
    ALTER TABLE fhv_trips_staging ADD COLUMN IF NOT EXISTS congestion_surcharge numeric;
    ALTER TABLE fhv_trips_staging ADD COLUMN IF NOT EXISTS airport_fee numeric;
    ALTER TABLE fhv_trips_staging ADD COLUMN IF NOT EXISTS tips numeric;
    ALTER TABLE fhv_trips_staging ADD COLUMN IF NOT EXISTS driver_pay numeric;
    ALTER TABLE fhv_trips_staging ADD COLUMN IF NOT EXISTS shared_request_flag text;
    ALTER TABLE fhv_trips_staging ADD COLUMN IF NOT EXISTS shared_match_flag text;
    ALTER TABLE fhv_trips_staging ADD COLUMN IF NOT EXISTS access_a_ride_flag text;
    ALTER TABLE fhv_trips_staging ADD COLUMN IF NOT EXISTS wav_request_flag text;
    ALTER TABLE fhv_trips_staging ADD COLUMN IF NOT EXISTS wav_match_flag text;
    ALTER TABLE fhv_trips_staging ADD COLUMN IF NOT EXISTS shared_ride_flag text;

    CREATE TABLE IF NOT EXISTS fhv_trips (
      id bigserial primary key,
      hvfhs_license_num text,
      dispatching_base_num text,
      originating_base_num text,
      request_datetime timestamp without time zone,
      on_scene_datetime timestamp without time zone,
      pickup_datetime timestamp without time zone,
      dropoff_datetime timestamp without time zone,
      pickup_location_id integer,
      dropoff_location_id integer,
      trip_miles numeric,
      trip_time numeric,
      base_passenger_fare numeric,
      tolls numeric,
      black_car_fund numeric,
      sales_tax numeric,
      congestion_surcharge numeric,
      airport_fee numeric,
      tips numeric,
      driver_pay numeric,
      shared_request boolean,
      shared_match boolean,
      access_a_ride boolean,
      wav_request boolean,
      wav_match boolean,
      legacy_shared_ride integer,
      affiliated_base_num text
    );
  " >/dev/null
}

ensure_tables

SKIP_POPULATE="${SKIP_POPULATE:-0}"

for parquet_filename in data/fhv_tripdata*.parquet; do
  echo "`date`: converting ${parquet_filename} to csv"
  ./setup_files/convert_parquet_to_csv.R "${parquet_filename}"

  csv_filename=${parquet_filename/.parquet/.csv}
  cat "$csv_filename" | psql_cmd -d "${PGDATABASE}" -c "COPY fhv_trips_staging ${fhv_schema} FROM stdin CSV HEADER;"
  echo "`date`: finished raw load for ${csv_filename}"

  if [[ -f setup_files/populate_fhv_trips.sql && "${SKIP_POPULATE}" != "1" ]]; then
    psql_cmd -d "${PGDATABASE}" -f setup_files/populate_fhv_trips.sql
    echo "`date`: loaded trips for ${csv_filename}"
  else
    if [[ "${SKIP_POPULATE}" == "1" ]]; then
      echo "`date`: NOTE SKIP_POPULATE=1; leaving data in fhv_trips_staging"
    elif [[ -f setup_files/populate_fhv_trips.sql ]]; then
      echo "`date`: NOTE skipping populate_fhv_trips.sql; leaving data in fhv_trips_staging"
    else
      echo "`date`: NOTE populate SQL missing (setup_files/populate_fhv_trips.sql); leaving data in fhv_trips_staging"
    fi
  fi

  rm -f "$csv_filename"
  echo "`date`: deleted ${csv_filename}"
done;
