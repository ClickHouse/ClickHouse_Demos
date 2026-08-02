#!/usr/bin/env bash
set -euo pipefail

ROOT=$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)
TMP=$(mktemp -d "${TMPDIR:-/tmp}/workshop-preflight-test.XXXXXX")
APP="${TMP}/app"
BIN="${TMP}/bin"
trap 'rm -rf "${TMP}"' EXIT

mkdir -p "${APP}" "${BIN}"
cp "${ROOT}/app/preflight.sh" "${APP}/preflight.sh"

cat > "${BIN}/docker" <<'EOF'
#!/usr/bin/env bash
case "${1:-}" in
  --version)
    echo "Docker version 27.0.0, build test"
    ;;
  info)
    if [ "${2:-}" = "--format" ]; then
      echo "8589934592"
    fi
    ;;
  compose)
    if [ "${2:-}" = "version" ] && [ "${3:-}" = "--short" ]; then
      echo "2.30.0"
    fi
    ;;
esac
exit 0
EOF

cat > "${BIN}/curl" <<'EOF'
#!/usr/bin/env bash
echo "200"
EOF

cat > "${BIN}/nc" <<'EOF'
#!/usr/bin/env bash
exit 0
EOF

chmod +x "${BIN}/docker" "${BIN}/curl" "${BIN}/nc"

write_env() {
  local pg_host=${1:-}
  local pg_password=${2:-}
  local otlp_token=${3:-}
  cat > "${APP}/.env.workshop" <<EOF
CLICKHOUSE_HOST=test.clickhouse.cloud
CLICKHOUSE_PORT=8443
CLICKHOUSE_USER=default
CLICKHOUSE_PASSWORD=test-password
CLICKHOUSE_DATABASE=nyc_tlc_data
CLICKHOUSE_SECURE=true
PGHOST=${pg_host}
PGPORT=5432
PGDATABASE=postgres
PGUSER=postgres
PGPASSWORD=${pg_password}
PGSSLMODE=require
FRONTEND_HOST_PORT=39080
BACKEND_HOST_PORT=39081
OTEL_GRPC_HOST_PORT=39082
OTEL_HTTP_HOST_PORT=39083
OTLP_AUTH_TOKEN=${otlp_token}
EOF
}

OUTPUT=""
STATUS=0
run_preflight() {
  set +e
  OUTPUT=$(env -i NO_COLOR=1 PATH="${BIN}:${PATH}" HOME="${HOME}" \
    TMPDIR="${TMPDIR:-/tmp}" "${APP}/preflight.sh" "$@" 2>&1)
  STATUS=$?
  set -e
}

assert_status() {
  local expected=$1
  if [ "${STATUS}" -ne "${expected}" ]; then
    printf 'Expected exit %s, got %s. Output:\n%s\n' "${expected}" "${STATUS}" "${OUTPUT}" >&2
    exit 1
  fi
}

assert_contains() {
  local expected=$1
  if ! printf '%s\n' "${OUTPUT}" | grep -Fq -- "${expected}"; then
    printf 'Expected output to contain %s. Output:\n%s\n' "${expected}" "${OUTPUT}" >&2
    exit 1
  fi
}

assert_not_contains() {
  local unexpected=$1
  if printf '%s\n' "${OUTPUT}" | grep -Fq -- "${unexpected}"; then
    printf 'Expected output not to contain %s. Output:\n%s\n' "${unexpected}" "${OUTPUT}" >&2
    exit 1
  fi
}

run_preflight --help
assert_status 0
assert_contains "Usage: ./preflight.sh [--cdc] [--otel] [--all]"

run_preflight --unknown
assert_status 2
assert_contains "Unknown option: --unknown"

# Module 00 ignores Postgres because it is not configured until Module 03.
write_env "postgres" "local-password"
run_preflight
assert_status 0
assert_contains "Overall: READY -- start the base app with:"
assert_not_contains "points to a local database"
assert_not_contains "OTEL_GRPC_HOST_PORT"

# Module 03 requires the complete managed-Postgres tuple.
write_env "" ""
run_preflight --cdc
assert_status 1
assert_contains "PGHOST is not configured for module 03"
assert_contains "PGPASSWORD is not configured for module 03"
assert_contains "rerun ./preflight.sh --cdc"

write_env "postgres" "local-password"
run_preflight --cdc
assert_status 1
assert_contains "points to a local database, which this workshop does not use"

write_env "managed-postgres.example.com" "managed-password"
run_preflight --cdc
assert_status 0
assert_contains "managed Postgres reachable"
assert_contains "Overall: READY -- module 03 Postgres checks passed."

# Exported shell values take precedence over --env-file in Docker Compose, so
# stale values must make preflight fail instead of certifying the file as ready.
set +e
OUTPUT=$(env -i NO_COLOR=1 PATH="${BIN}:${PATH}" HOME="${HOME}" \
  TMPDIR="${TMPDIR:-/tmp}" PGHOST= PGPASSWORD= \
  "${APP}/preflight.sh" --cdc 2>&1)
STATUS=$?
set -e
assert_status 1
assert_contains "PGHOST is exported in your shell and differs from .env.workshop"
assert_contains "PGPASSWORD is exported in your shell and differs from .env.workshop"

# Module 05 requires authenticated ingest and checks collector ports without
# requiring Module 03 credentials.
write_env "" ""
run_preflight --otel
assert_status 1
assert_contains "OTLP_AUTH_TOKEN is missing or still a placeholder for module 05"

write_env "" "" "test-otel-token"
run_preflight --otel
assert_status 0
assert_contains "OTEL_GRPC_HOST_PORT"
assert_contains "OTEL_HTTP_HOST_PORT"
assert_contains "Overall: READY -- module 05 OpenTelemetry checks passed."

set +e
OUTPUT=$(env -i NO_COLOR=1 PATH="${BIN}:${PATH}" HOME="${HOME}" \
  TMPDIR="${TMPDIR:-/tmp}" OTEL_HTTP_HOST_PORT=39999 \
  "${APP}/preflight.sh" --otel 2>&1)
STATUS=$?
set -e
assert_status 1
assert_contains "OTEL_HTTP_HOST_PORT is exported in your shell and differs from .env.workshop"

write_env "managed-postgres.example.com" "managed-password" "test-otel-token"
run_preflight --all
assert_status 0
assert_contains "additional checks: cdc=1 otel=1"
assert_contains "managed Postgres reachable"
assert_contains "OTEL_GRPC_HOST_PORT"

echo "Workshop staged preflight behavior checks passed."
