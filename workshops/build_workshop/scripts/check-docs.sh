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
  "${CONTENT}/learner" "${CONTENT}/polymarket/learner"

fail_if_found \
  "the Polymarket track must not request trading credentials or call order endpoints" \
  'POLYMARKET_(API_KEY|SECRET|PRIVATE_KEY)|/orders?([/?`[:space:]]|$)|wallet[_ -]?private' \
  "${CONTENT}/polymarket" "${ROOT}/polymarket"

if grep -RInE --exclude='06b-ai-sre-librechat.mdx' \
  '06a|06b|LibreChat' "${CONTENT}"; then
  echo "ERROR: active workshop must use Module 06 and exclude archived Module 06b" >&2
  exit 1
fi

fail_if_found \
  "active workshop summaries and diagrams must not include archived Module 06b" \
  '06b|LibreChat' \
  "${ROOT}/README.md" \
  "${ROOT}/app/README.md" \
  "${ROOT}/app/WORKSHOP_CHANGES.md" \
  "${ROOT}/docs/diagrams/gen_diagrams.py" \
  "${ROOT}/docs/diagrams/workshop-module-flow.svg" \
  "${ROOT}/docs/diagrams/workshop-architecture.svg" \
  "${ROOT}/playbook/public/workshop-module-flow.svg" \
  "${ROOT}/playbook/public/workshop-architecture.svg"

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

fail_if_found \
  "module 01 client commands must consume the loaded environment instead of placeholders" \
  '<your-service-hostname>|<password>' \
  "${CONTENT}/learner/01-clickhouse-cloud.mdx"

fail_if_found \
  "setup client commands must consume the loaded environment instead of a hostname placeholder" \
  '<your-service-hostname>' \
  "${CONTENT}/learner/00-setup.mdx"

fail_if_found \
  "learner client commands must use the environment credential without argv exposure or stdin prompts" \
  '--ask-password|--password[[:space:]]+"?\$CLICKHOUSE_PASSWORD"?' \
  "${CONTENT}/learner"

fail_if_found \
  "active workshop docs must use the staged preflight flags" \
  '--require-postgres' \
  "${ROOT}/README.md" "${ROOT}/app" "${CONTENT}"

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

require_fixed \
  "Polymarket learner setup must switch to the production workshop branch" \
  'git switch build-workshop-v1' \
  "${CONTENT}/polymarket/learner/00-setup.mdx"

require_fixed \
  "Polymarket dev rehearsal must use the staging workshop branch" \
  'git switch dev-build-workshop-v1' \
  "${CONTENT}/polymarket/rehearsal.mdx"

require_fixed \
  "the former learner Module 06b page must remain clearly archived" \
  'Module 06b is no longer part of the workshop.' \
  "${CONTENT}/learner/06b-ai-sre-librechat.mdx"

require_fixed \
  "the former instructor Module 06b page must remain clearly archived" \
  'Module 06b is no longer part of the run of show.' \
  "${CONTENT}/instructor/06b-ai-sre-librechat.mdx"

require_fixed \
  "the archived instructor page must link to active Module 07" \
  '[Module 07](/docs/instructor/07-break-and-fix)' \
  "${CONTENT}/instructor/06b-ai-sre-librechat.mdx"

for file in \
  "${ROOT}/app/README.md" \
  "${ROOT}/app/OBSERVABILITY.md" \
  "${CONTENT}/learner/05-clickstack.mdx" \
  "${CONTENT}/instructor/05-clickstack.mdx"; do
  require_fixed \
    "ClickStack overlay startup must rebuild the frontend telemetry bundle" \
    'docker-compose.otel.yml up -d --build' \
    "${file}"
done

require_fixed \
  "the optional container-log overlay must also rebuild the frontend" \
  '--profile container-logs up -d --build' \
  "${ROOT}/app/OBSERVABILITY.md"

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

for literal in \
  'CLICKHOUSE_HOST=' \
  'CLICKHOUSE_PORT=8443' \
  'CLICKHOUSE_DATABASE=polymarket' \
  'POLYMARKET_MODE=live'; do
  require_fixed \
    "the Polymarket environment must expose the documented Cloud/collector contract" \
    "${literal}" \
    "${ROOT}/polymarket/.env.polymarket.example"
done

require_fixed \
  "Polymarket setup must source its environment after editing" \
  'set -a; source ./.env.polymarket; set +a' \
  "${CONTENT}/polymarket/learner/00-setup.mdx"

require_fixed \
  "Polymarket Module 02 must contain the complete copyable database DDL" \
  'CREATE MATERIALIZED VIEW IF NOT EXISTS polymarket.market_midpoints_1m_mv' \
  "${CONTENT}/polymarket/learner/02-model-data.mdx"

require_fixed \
  "Polymarket Module 05 must contain the clean trade-volume query" \
  'FROM polymarket.trades_clean' \
  "${CONTENT}/polymarket/learner/05-investigate-movement.mdx"

python3 "${ROOT}/scripts/check-polymarket-sql.py"

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

  polymarket_services=$(
    cd "${ROOT}/polymarket" &&
      docker compose --env-file .env.polymarket.example config --services
  )
  if [[ "${polymarket_services}" != "collector" ]]; then
    echo "ERROR: Polymarket Compose must define only the stateless collector" >&2
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

require_fixed \
  "setup must explicitly install the stable ClickHouse binary and client" \
  'clickhousectl local install stable' \
  "${CONTENT}/learner/00-setup.mdx"

require_fixed \
  "setup must verify the standalone ClickHouse client" \
  'clickhouse client --version' \
  "${CONTENT}/learner/00-setup.mdx"

require_count \
  "setup must reload the workshop environment after each credential update" \
  2 \
  'set -a; source ./.env.workshop; set +a' \
  "${CONTENT}/learner/00-setup.mdx"

require_count \
  "every module 01 client command must use CLICKHOUSE_HOST from the loaded environment" \
  5 \
  '--host "$CLICKHOUSE_HOST"' \
  "${CONTENT}/learner/01-clickhouse-cloud.mdx"

require_fixed \
  "module 01 must document the ClickHouse environment credential path" \
  'client reads `CLICKHOUSE_PASSWORD` from the' \
  "${CONTENT}/learner/01-clickhouse-cloud.mdx"

require_fixed \
  "module 03 must reload managed Postgres credentials before Compose" \
  'set -a; source ./.env.workshop; set +a' \
  "${CONTENT}/learner/03-realtime-cdc.mdx"

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

require_service_set() {
  local description=$1
  local expected=$2
  shift 2
  local actual
  actual=$(docker compose "$@" config --services | sort | tr '\n' ' ')
  if [ "${actual}" != "${expected}" ]; then
    echo "ERROR: ${description} (expected '${expected}', found '${actual}')" >&2
    exit 1
  fi
}

if command -v docker >/dev/null 2>&1; then
  require_service_set \
    "module 00 must start only the base app" \
    'backend frontend ' \
    -f "${ROOT}/app/docker-compose.workshop.yml"

  require_service_set \
    "module 03 cdc profile must add only the trip writer" \
    'backend frontend pg-trip-writer ' \
    -f "${ROOT}/app/docker-compose.workshop.yml" --profile cdc
fi

require_fixed \
  "preflight must expose the module 03 stage check" \
  '  --cdc   also check module 03 Postgres settings and connectivity' \
  "${ROOT}/app/preflight.sh"

require_fixed \
  "preflight must expose the module 05 stage check" \
  '  --otel  also check module 05 OpenTelemetry collector ports' \
  "${ROOT}/app/preflight.sh"

require_fixed \
  "preflight must require authenticated OTLP ingest for module 05" \
  'OTLP_AUTH_TOKEN is missing or still a placeholder for module 05' \
  "${ROOT}/app/preflight.sh"

for literal in \
  '127.0.0.1:${OTEL_GRPC_HOST_PORT:-4317}:4317' \
  '127.0.0.1:${OTEL_HTTP_HOST_PORT:-4318}:4318'; do
  require_fixed \
    "OTLP host receivers must bind to loopback" \
    "${literal}" \
    "${ROOT}/app/docker-compose.otel.yml"
done

# MCP server definitions belong in setup. Later modules should use or link to
# that setup instead of asking learners to configure the same endpoint again.
if grep -RInE --exclude='00-setup.mdx' \
  'mcpServers|"clickhouse"[[:space:]]*:[[:space:]]*\{' "${CONTENT}/learner"; then
  echo "ERROR: learner MCP server configuration must appear only in 00-setup.mdx" >&2
  exit 1
fi

echo "Workshop documentation policy checks passed."
