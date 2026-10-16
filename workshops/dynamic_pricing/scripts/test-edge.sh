#!/bin/bash
set -euo pipefail
lab_root=$(cd "$(dirname "$0")/.." && pwd)
cd "$lab_root"
(
  cd app/frontend
  test -d node_modules || npm ci --no-audit --no-fund
  ./node_modules/.bin/playwright install chromium
)
project="pricing-edge-$$"
compose=(docker compose -p "$project" --env-file app/.env.workshop.example --profile developer -f app/docker-compose.workshop.yml -f app/docker-compose.developer.yml)
trap '"${compose[@]}" down --volumes >/dev/null 2>&1' EXIT
export FRONTEND_HOST_PORT=$(.venv/bin/python -c 'import socket; s=socket.socket(); s.bind(("127.0.0.1",0)); print(s.getsockname()[1]); s.close()')
export PRICING_ALLOWED_ORIGIN="http://localhost:$FRONTEND_HOST_PORT" PRICING_OWNER_ID=learner-1 PRICING_SCENARIO_ID=grocery PRICING_CLICKHOUSE_DATABASE=pricing PRICING_MODEL_PATH=/opt/pricing/pricing/artifacts/development/model.json
"${compose[@]}" up --build --detach --wait --wait-timeout 120
pg=$("${compose[@]}" port postgres 5432);ch=$("${compose[@]}" port clickhouse 8123)
export PRICING_TEST_POSTGRES_DSN="postgresql://pricing_test:test-only-password@127.0.0.1:${pg##*:}/postgres"
export PRICING_TEST_CLICKHOUSE_PORT="${ch##*:}"
PYTHONPATH="$PWD:$PWD/tests:$PWD/app/backend" .venv/bin/python tests/prepare_retail_edge.py
export PYTHONPATH="$PWD:$PWD/tests:$PWD/app/backend"
node app/frontend/e2e/retail-real-smoke.mjs
