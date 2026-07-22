#!/usr/bin/env bash
set -euo pipefail

ROOT=$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)
CONTENT="${ROOT}/playbook/content/docs"

fail_if_found() {
  local description=$1
  local pattern=$2
  shift 2
  if rg -n --pcre2 "${pattern}" "$@"; then
    echo "ERROR: ${description}" >&2
    exit 1
  fi
}

fail_if_found \
  "active workshop docs must use the dev/main promotion model" \
  'build-workshop-v1' \
  "${ROOT}/README.md" "${CONTENT}"

fail_if_found \
  "spell the Cloud management CLI as clickhousectl" \
  '(^|[^[:alnum:]_-])chctl([^[:alnum:]_-]|$)' \
  "${ROOT}/README.md" "${CONTENT}"

fail_if_found \
  "learners must not be told to edit SQL comments or execute a hidden SQL file" \
  '(comment|uncomment).*(sql|variant)|(?:run|execute|open).*(?:db/cloud|\.sql file)' \
  "${CONTENT}/learner"

require_fixed() {
  local description=$1
  local literal=$2
  local file=$3
  if ! rg -Fq -- "${literal}" "${file}"; then
    echo "ERROR: ${description}" >&2
    exit 1
  fi
}

require_count() {
  local description=$1
  local expected=$2
  local literal=$3
  local file=$4
  local actual
  actual=$(rg -F -c -- "${literal}" "${file}" || true)
  if [ "${actual}" -ne "${expected}" ]; then
    echo "ERROR: ${description} (expected ${expected}, found ${actual})" >&2
    exit 1
  fi
}

require_fixed \
  "the copied app environment must use the working local Postgres service literal" \
  'PGHOST=postgres' \
  "${ROOT}/app/.env.workshop.example"

for literal in 'PGDATABASE=taxi' 'PGUSER=taxi' 'PGPASSWORD=taxi' 'PGSSLMODE=prefer'; do
  require_fixed \
    "the copied app environment must use the complete local Postgres connection tuple" \
    "${literal}" \
    "${ROOT}/app/.env.workshop.example"
done

require_fixed \
  "the app environment must expose successful query logs to ClickStack" \
  'LOG_LEVEL=DEBUG' \
  "${ROOT}/app/.env.workshop.example"

require_fixed \
  "the observability overlay default must expose successful query logs" \
  'LOG_LEVEL=${LOG_LEVEL:-DEBUG}' \
  "${ROOT}/app/docker-compose.otel.yml"

require_count \
  "initial and reset Ops dashboard intervals must both be one minute" \
  2 \
  'interval: "1m"' \
  "${ROOT}/app/frontend/src/pages/DashboardPage.tsx"

require_count \
  "initial and reset Ops dashboard states must both refresh every five seconds" \
  2 \
  'auto_refresh_s: 5' \
  "${ROOT}/app/frontend/src/pages/DashboardPage.tsx"

# MCP server definitions belong in setup. Later modules should use or link to
# that setup instead of asking learners to configure the same endpoint again.
if rg -n 'mcpServers|"clickhouse"\s*:\s*\{' "${CONTENT}/learner" --glob '!00-setup.mdx'; then
  echo "ERROR: learner MCP server configuration must appear only in 00-setup.mdx" >&2
  exit 1
fi

echo "Workshop documentation policy checks passed."
