#!/usr/bin/env bash
set -euo pipefail
lab_root=$(cd "$(dirname "$0")/.." && pwd)
cd "$lab_root"
python_bin="$lab_root/.venv/bin/python"
test -x "$python_bin" || { echo 'Install the isolated Python 3.12 lab venv first.' >&2; exit 1; }
"$python_bin" -c 'import sys; assert sys.version_info[:2] == (3,12), "Use Python 3.12."'
node -e 'if (process.versions.node.split(".")[0] !== "22") throw Error("Use Node 22.")'
docker info >/dev/null
mkdir -p .run
"$python_bin" -m pytest tests -m 'not integration and not cloud' --strict-markers --junitxml=.run/units.xml -q
"$python_bin" - .run/units.xml <<'PY'
import sys,xml.etree.ElementTree as ET
suites=list(ET.parse(sys.argv[1]).getroot().iter('testsuite'))
assert sum(int(s.get('tests',0)) for s in suites)>0,'No unit tests ran.'
assert sum(int(s.get('skipped',0)) for s in suites)==0,'Required unit tests skipped.'
PY
bash scripts/test-integration.sh
# Real container builds and source/receipt smoke path, explicitly local replay.
bash scripts/test-edge.sh
bash scripts/test-observability.sh
(
  cd app/frontend
  test -d node_modules || npm ci --no-audit --no-fund
  ./node_modules/.bin/playwright install chromium
  npm run build
  npm run test:e2e
)
docker compose --env-file app/.env.workshop.example -f app/docker-compose.workshop.yml config --quiet
echo 'Pricing kit: local SQL, model, container edge, OTLP, and frontend passed. Cloud rehearsal is separate.'
