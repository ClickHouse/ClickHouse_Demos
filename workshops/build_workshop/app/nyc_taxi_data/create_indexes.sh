#!/usr/bin/env bash
set -euo pipefail

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

echo "Creating indexes in ${PGDATABASE} on ${PGHOST}:${PGPORT}…"

# Only create if the tables exist (this repo's demo Postgres schema is different).
psql_cmd -d "${PGDATABASE}" -c "\\dt trips" >/dev/null 2>&1 && \
  psql_cmd -d "${PGDATABASE}" -c "CREATE INDEX IF NOT EXISTS idx_trips_pickup_datetime_brin ON trips USING BRIN (pickup_datetime) WITH (pages_per_range = 32);" || true

psql_cmd -d "${PGDATABASE}" -c "\\dt fhv_trips" >/dev/null 2>&1 && \
  psql_cmd -d "${PGDATABASE}" -c "CREATE INDEX IF NOT EXISTS idx_fhv_trips_pickup_datetime_brin ON fhv_trips USING BRIN (pickup_datetime) WITH (pages_per_range = 32);" || true

echo "Done."
