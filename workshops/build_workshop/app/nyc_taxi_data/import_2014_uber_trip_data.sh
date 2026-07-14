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

# load 2014 Uber data into `fhv_trips` table
for filename in data/uber-raw-data*14.csv; do
  echo "`date`: beginning load for $filename"
  cat "$filename" | psql_cmd -d "${PGDATABASE}" -c "SET datestyle = 'ISO, MDY'; COPY uber_trips_2014 (pickup_datetime, pickup_latitude, pickup_longitude, base_code) FROM stdin CSV HEADER;"
  echo "`date`: finished raw load for $filename"
done;

psql_cmd -d "${PGDATABASE}" -f setup_files/populate_2014_uber_trips.sql
