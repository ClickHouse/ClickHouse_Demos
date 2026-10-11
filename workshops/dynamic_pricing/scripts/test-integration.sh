#!/usr/bin/env bash
set -euo pipefail
lab_root=$(cd "$(dirname "$0")/.." && pwd)
cd "$lab_root"
python_bin="$lab_root/.venv/bin/python"
test -x "$python_bin" || { echo "Install the lab test environment first." >&2; exit 1; }
project="pricing-integration-$$"
compose=(docker compose --project-name "$project" -f docker-compose.local.yml)
cleanup() { "${compose[@]}" down --volumes --remove-orphans >/dev/null 2>&1; }
trap cleanup EXIT
"${compose[@]}" up --detach --wait --wait-timeout 60
pg_address=$("${compose[@]}" port postgres 5432)
ch_address=$("${compose[@]}" port clickhouse 8123)
export PRICING_TEST_POSTGRES_DSN="postgresql://pricing_test:test-only-password@127.0.0.1:${pg_address##*:}/postgres"
export PRICING_TEST_CLICKHOUSE_PORT="${ch_address##*:}"
mkdir -p .run
report="$lab_root/.run/integration-$$.xml"
if [ "$#" -eq 0 ]; then set -- tests; fi
"$python_bin" -m pytest "$@" -m integration --strict-markers --junitxml="$report" -q
"$python_bin" - "$report" <<'PY'
import sys
import xml.etree.ElementTree as ET
root = ET.parse(sys.argv[1]).getroot()
suites = list(root.iter('testsuite'))
assert sum(int(s.get('tests', 0)) for s in suites) > 0, 'No integration tests executed.'
assert sum(int(s.get('skipped', 0)) for s in suites) == 0, 'Required integration tests skipped.'
PY
