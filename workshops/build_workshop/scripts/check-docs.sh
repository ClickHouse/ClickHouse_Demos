#!/usr/bin/env bash
set -euo pipefail

ROOT=$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)
CONTENT="${ROOT}/playbook/content/docs"

fail_if_found() {
  local description=$1
  local pattern=$2
  shift 2
  if grep -RInE -- "${pattern}" "$@"; then
    echo "ERROR: ${description}" >&2
    exit 1
  fi
}

fail_if_found \
  "active workshop docs must not use the retired dev/main promotion commands" \
  'git switch main|git clone[^[:cntrl:]]*--branch main|`dev` to (protected )?`main`|PR it to protected `dev`' \
  "${ROOT}/README.md" "${CONTENT}"

fail_if_found \
  "spell the Cloud management CLI as clickhousectl" \
  '(^|[^[:alnum:]_-])chctl([^[:alnum:]_-]|$)' \
  "${ROOT}/README.md" "${CONTENT}"

fail_if_found \
  "learners must not be told to edit SQL comments or execute a hidden SQL file" \
  '(comment|uncomment).*(sql|variant)|(run|execute|open).*(db/cloud|\.sql file)' \
  "${CONTENT}/learner"

fail_if_found \
  "workshop material must not direct users to local managed-service substitutes" \
  'PGHOST=postgres|localhost:3090|docker-compose\.librechat|local `mcp-clickhouse`|start (a |the )?local (Postgres|ClickHouse|LibreChat|HyperDX)|using (a |the )?local (Postgres|ClickHouse)' \
  "${ROOT}/README.md" "${ROOT}/app/README.md" "${CONTENT}"

fail_if_found \
  "the workshop environment must not expose local database or LibreChat server settings" \
  '^(POSTGRES_|POSTGRES_HOST_PORT|LIBRECHAT_PORT|LIBRECHAT_DEFAULT_|JWT_SECRET|JWT_REFRESH_SECRET|CREDS_KEY|CREDS_IV)=' \
  "${ROOT}/app/.env.workshop.example"

fail_if_found \
  "the workshop Compose file must not define local database or hosted-product services" \
  '^[[:space:]]+(postgres|clickhouse|mongodb|librechat|hyperdx):[[:space:]]*$|image:[[:space:]]*(postgres|clickhouse/clickhouse-server|mongo|ghcr\.io/danny-avila/librechat|hyperdx)/?' \
  "${ROOT}"/app/docker-compose*.yml

if [ -e "${ROOT}/app/librechat/docker-compose.librechat.yml" ]; then
  echo "ERROR: local LibreChat Compose deployment must not exist in the cloud-only workshop" >&2
  exit 1
fi

require_fixed() {
  local description=$1
  local literal=$2
  local file=$3
  if ! grep -Fq -- "${literal}" "${file}"; then
    echo "ERROR: ${description}" >&2
    exit 1
  fi
}

require_fixed \
  "maintainer docs must name the workshop staging branch" \
  'dev-build-workshop-v1' \
  "${ROOT}/README.md"

require_fixed \
  "learner setup must switch to the production workshop branch" \
  'git switch build-workshop-v1' \
  "${CONTENT}/learner/00-setup.mdx"

require_count() {
  local description=$1
  local expected=$2
  local literal=$3
  local file=$4
  local actual
  actual=$(grep -F -c -- "${literal}" "${file}" || true)
  if [ "${actual}" -ne "${expected}" ]; then
    echo "ERROR: ${description} (expected ${expected}, found ${actual})" >&2
    exit 1
  fi
}

for literal in 'PGHOST=' 'PGDATABASE=postgres' 'PGUSER=postgres' 'PGPASSWORD=' 'PGSSLMODE=require'; do
  require_fixed \
    "the copied app environment must use the managed Postgres connection template" \
    "${literal}" \
    "${ROOT}/app/.env.workshop.example"
done

require_fixed \
  "the CDC writer must stay off until managed Postgres is configured" \
  'profiles: ["cdc"]' \
  "${ROOT}/app/docker-compose.workshop.yml"

require_fixed \
  "preflight must reject local Postgres hosts" \
  'points to a local database, which this workshop does not use' \
  "${ROOT}/app/preflight.sh"

require_fixed \
  "preflight must reject local ClickHouse hosts" \
  'points to a local server, which this workshop does not use' \
  "${ROOT}/app/preflight.sh"

require_fixed \
  "backend ClickHouse defaults must use the Cloud TLS port" \
  'clickhouse_port: int = 8443' \
  "${ROOT}/app/backend/app/settings.py"

require_fixed \
  "the trip writer must require a managed Postgres hostname" \
  'POSTGRES = load_postgres_config()' \
  "${ROOT}/app/loadgen/pg_trip_writer.py"

if command -v docker >/dev/null 2>&1; then
  compose_config=$(
    cd "${ROOT}/app" &&
      docker compose --profile cdc --profile container-logs \
        --env-file .env.workshop.example \
        -f docker-compose.workshop.yml -f docker-compose.otel.yml config
  )
  if printf '%s\n' "${compose_config}" | grep -Eiq \
    'image:[[:space:]]*(postgres|clickhouse/clickhouse-server|mongo|ghcr\.io/danny-avila/librechat|hyperdx)|^[[:space:]]{2}(postgres|clickhouse|mongodb|librechat|hyperdx):'; then
    echo "ERROR: rendered workshop Compose config contains a local managed-service substitute" >&2
    exit 1
  fi
fi

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
if grep -RInE --exclude='00-setup.mdx' \
  'mcpServers|"clickhouse"[[:space:]]*:[[:space:]]*\{' "${CONTENT}/learner"; then
  echo "ERROR: learner MCP server configuration must appear only in 00-setup.mdx" >&2
  exit 1
fi

echo "Workshop documentation policy checks passed."
