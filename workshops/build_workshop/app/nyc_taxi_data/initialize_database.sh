#!/usr/bin/env bash
set -euo pipefail

# This script was originally written to load a Postgres database using local psql/createdb.
# In this repo, Postgres typically runs via Docker Compose, so we default to connecting to:
#   host: 127.0.0.1  port: 5432  user: taxi  password: taxi
#
# You can override connection settings via environment variables:
#   PGHOST, PGPORT, PGUSER, PGPASSWORD, PGDATABASE

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "${SCRIPT_DIR}/.." && pwd)"

export PGHOST="${PGHOST:-127.0.0.1}"
export PGPORT="${PGPORT:-5432}"
export PGUSER="${PGUSER:-taxi}"
export PGPASSWORD="${PGPASSWORD:-taxi}"
export PGDATABASE="${PGDATABASE:-nyc-taxi-data}"

psql_cmd() {
  psql -v ON_ERROR_STOP=1 -h "${PGHOST}" -p "${PGPORT}" -U "${PGUSER}" "$@"
}

createdb_cmd() {
  createdb -h "${PGHOST}" -p "${PGPORT}" -U "${PGUSER}" "$@"
}

echo "Connecting to Postgres at ${PGHOST}:${PGPORT} as ${PGUSER}…"
psql_cmd -d postgres -c "SELECT 1" >/dev/null

echo "Ensuring database ${PGDATABASE} exists…"
if ! psql_cmd -d postgres -tAc "SELECT 1 FROM pg_database WHERE datname='${PGDATABASE}'" | grep -q 1; then
  createdb_cmd "${PGDATABASE}"
else
  echo "Database ${PGDATABASE} already exists; skipping createdb."
fi

# Schema SQL (fallback to this repo's demo schema if the original file isn't present)
SCHEMA_SQL="${SCRIPT_DIR}/setup_files/create_nyc_taxi_schema.sql"
FALLBACK_SCHEMA_SQL="${REPO_ROOT}/db/postgres/init/001_schema.sql"

if [[ -f "${SCHEMA_SQL}" ]]; then
  echo "Applying schema: ${SCHEMA_SQL}"
  psql_cmd -d "${PGDATABASE}" -f "${SCHEMA_SQL}"
elif [[ -f "${FALLBACK_SCHEMA_SQL}" ]]; then
  echo "Applying fallback schema: ${FALLBACK_SCHEMA_SQL}"
  psql_cmd -d "${PGDATABASE}" -f "${FALLBACK_SCHEMA_SQL}"
else
  echo "WARNING: No schema SQL found; skipping schema load."
fi

# Optional: shapefile loads require PostGIS + shp2pgsql. Skip gracefully if not available.
if command -v shp2pgsql >/dev/null 2>&1; then
  if ! psql_cmd -d "${PGDATABASE}" -tAc "SELECT 1 FROM pg_extension WHERE extname='postgis'" | grep -q 1; then
    echo "NOTE: PostGIS extension not installed in this Postgres instance; skipping shapefile imports."
    echo "      (To enable: use a PostGIS image like postgis/postgis and CREATE EXTENSION postgis;)"
  else
  if [[ -f "${SCRIPT_DIR}/shapefiles/taxi_zones/taxi_zones.shp" ]]; then
    echo "Loading taxi_zones shapefile…"
    # Drop existing tables created by previous runs to keep this script idempotent.
    psql_cmd -d "${PGDATABASE}" -c "DROP TABLE IF EXISTS taxi_zones CASCADE;"
    shp2pgsql -s 2263:4326 -I "${SCRIPT_DIR}/shapefiles/taxi_zones/taxi_zones.shp" | psql_cmd -d "${PGDATABASE}"
    psql_cmd -d "${PGDATABASE}" -c "CREATE INDEX IF NOT EXISTS idx_taxi_zones_locationid ON taxi_zones (locationid);"
    psql_cmd -d "${PGDATABASE}" -c "VACUUM ANALYZE taxi_zones;"
  fi

  if [[ -f "${SCRIPT_DIR}/shapefiles/nyct2010_15b/nyct2010.shp" ]]; then
    echo "Loading nyct2010 shapefile…"
    psql_cmd -d "${PGDATABASE}" -c "DROP TABLE IF EXISTS nyct2010 CASCADE;"
    shp2pgsql -s 2263:4326 -I "${SCRIPT_DIR}/shapefiles/nyct2010_15b/nyct2010.shp" | psql_cmd -d "${PGDATABASE}"
    if [[ -f "${SCRIPT_DIR}/setup_files/add_newark_airport.sql" ]]; then
      psql_cmd -d "${PGDATABASE}" -f "${SCRIPT_DIR}/setup_files/add_newark_airport.sql"
    fi
    psql_cmd -d "${PGDATABASE}" -c "CREATE INDEX IF NOT EXISTS idx_nyct2010_ntacode ON nyct2010 (ntacode);"
    psql_cmd -d "${PGDATABASE}" -c "VACUUM ANALYZE nyct2010;"
  fi
  fi
else
  echo "NOTE: shp2pgsql not found; skipping shapefile imports (requires PostGIS tools)."
fi

if [[ -f "${SCRIPT_DIR}/setup_files/add_tract_to_zone_mapping.sql" ]]; then
  psql_cmd -d "${PGDATABASE}" -f "${SCRIPT_DIR}/setup_files/add_tract_to_zone_mapping.sql"
fi

# Optional CSV loads (only if the target tables exist; keep script non-fatal for this repo).
if [[ -f "${SCRIPT_DIR}/data/fhv_bases.csv" ]]; then
  if [[ "$(psql_cmd -d "${PGDATABASE}" -tAc "SELECT to_regclass('public.fhv_bases') IS NOT NULL")" == "t" ]]; then
    cat "${SCRIPT_DIR}/data/fhv_bases.csv" | psql_cmd -d "${PGDATABASE}" -c "COPY fhv_bases FROM stdin WITH CSV HEADER;"
  else
    echo "NOTE: table fhv_bases not found; skipping fhv_bases.csv import."
  fi
fi

if [[ -f "${SCRIPT_DIR}/data/central_park_weather.csv" ]]; then
  if [[ "$(psql_cmd -d "${PGDATABASE}" -tAc "SELECT to_regclass('public.central_park_weather_observations') IS NOT NULL")" == "t" ]]; then
    weather_schema="station_id, station_name, date, average_wind_speed, precipitation, snowfall, snow_depth, max_temperature, min_temperature"
    cat "${SCRIPT_DIR}/data/central_park_weather.csv" | psql_cmd -d "${PGDATABASE}" -c "COPY central_park_weather_observations (${weather_schema}) FROM stdin WITH CSV HEADER;"
    psql_cmd -d "${PGDATABASE}" -c "UPDATE central_park_weather_observations SET average_wind_speed = NULL WHERE average_wind_speed = -9999;"
  else
    echo "NOTE: table central_park_weather_observations not found; skipping central_park_weather.csv import."
  fi
fi

echo "Done."
