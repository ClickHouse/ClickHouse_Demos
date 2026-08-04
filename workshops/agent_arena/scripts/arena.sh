#!/usr/bin/env bash
# AgentArena lifecycle orchestrator.
#
#   scripts/arena.sh up               seed ClickHouse business tables + views
#   scripts/arena.sh down             drop the arena ClickHouse database + read-only user
#   scripts/arena.sh serve            (re)start the dashboard API + web UI
#   scripts/arena.sh serve --api-only (re)start ONLY the backend dashboard API — fastest
#   scripts/arena.sh stop             stop just the local servers
#   scripts/arena.sh status           show what's running
#
# Requires: .venv (deps installed), .env (CLICKHOUSE_*, LANGFUSE_*, ARENA_RO_PASSWORD).
set -euo pipefail

ROOT="$(cd "$(dirname "$0")/.." && pwd)"
cd "$ROOT"
# Put the repo root on sys.path so file-path scripts (scripts/*.py, schema/*.py)
# can `import arena` / `agents` / `eval`, not just the `-m` / `-c` invocations.
export PYTHONPATH="$ROOT${PYTHONPATH:+:$PYTHONPATH}"
PY="$ROOT/.venv/bin/python"
RUN_DIR="$ROOT/.run"

log() { printf '\n\033[1;33m▶ %s\033[0m\n' "$*"; }
load_env() { set -a; . ./.env; set +a; }

# Start the dashboard JSON API (:$API_PORT, default 8000) + the React web UI (:5174) in the
# background; logs in .run/. Idempotent (kills prior instances first).
start_servers() {
  local api_only="${1:-}" ok_api=0 ok_web=0 i
  # Backend port is configurable: API_PORT=9000 scripts/arena.sh serve
  local api_port="${API_PORT:-8000}"
  mkdir -p "$RUN_DIR"
  log "Starting dashboard API (:$api_port)$([ "$api_only" = "--api-only" ] || echo ' + web UI (:5174)')"
  pkill -f "uvicorn dashboard.app" 2>/dev/null || true
  ( cd "$ROOT" && PYTHONPATH="$ROOT" nohup "$PY" -m uvicorn dashboard.app:app \
      --port "$api_port" --log-level warning >"$RUN_DIR/dashboard-api.log" 2>&1 & )
  # -fs => only a real 2xx counts (a foreign app 404ing on the port is NOT "up").
  for i in $(seq 1 30); do curl --max-time 2 -fs -o /dev/null "localhost:$api_port/healthz" && { ok_api=1; break; }; sleep 1; done
  echo "  dashboard API : http://localhost:$api_port  [$([ $ok_api = 1 ] && echo ready || echo 'NOT up')]  (.run/dashboard-api.log)"
  [ $ok_api = 1 ] || echo "  ⚠ dashboard API didn't come up — is port $api_port already in use? check .run/dashboard-api.log"

  if [ "$api_only" = "--api-only" ]; then return; fi

  pkill -f "vite" 2>/dev/null || true
  [ -d "$ROOT/web/node_modules" ] || ( cd "$ROOT/web" && npm install )
  # point the web UI at whatever port the API bound to
  ( cd "$ROOT/web" && VITE_API_BASE="http://localhost:$api_port" nohup npm run dev -- --port 5174 >"$RUN_DIR/web.log" 2>&1 & )
  for i in $(seq 1 45); do curl -fs -o /dev/null localhost:5174 && { ok_web=1; break; }; sleep 1; done
  echo "  web UI        : http://localhost:5174  [$([ $ok_web = 1 ] && echo ready || echo 'NOT up')]  (.run/web.log)"
}

stop_servers() {
  pkill -f "uvicorn dashboard.app" 2>/dev/null || true
  pkill -f "uvicorn serving.api" 2>/dev/null || true
  pkill -f "vite" 2>/dev/null || true
}

require_venv() { [ -x "$PY" ] || { echo "missing .venv — python3.11 -m venv .venv && .venv/bin/pip install -r requirements.txt"; exit 1; }; }

up() {
  require_venv; load_env
  log "ClickHouse: business database + read-only agent user"
  $PY scripts/setup_clickhouse.py
  log "Seeding ClickHouse directly + views + schema context"
  $PY -m datagen.generator --seed "${SEED:-42}"
  $PY schema/gen_schema_context.py
  start_servers
  log "UP complete (business dataset in ClickHouse; benchmark results in Langfuse)."
  echo "  Open: http://localhost:5174  (run the harness to populate the Leaderboard)"
}

down() {
  require_venv; load_env
  log "Stopping local servers (web UI + dashboard/serving API)"
  stop_servers

  log "Dropping ClickHouse arena database + read-only user"
  $PY -c "
from arena.config import load_config; from agents.chclient import make_admin_client
c = load_config()
a = make_admin_client(c.clickhouse, database='default')
a.command(f'DROP DATABASE IF EXISTS {c.clickhouse.database}')
a.command(f'DROP USER IF EXISTS {c.clickhouse.ro_user}')
print(f'dropped {c.clickhouse.database} and {c.clickhouse.ro_user}')
"
  log "DOWN complete."
}

status() {
  require_venv; load_env
  log "Web / API servers"
  curl --max-time 2 -fs -o /dev/null "localhost:${API_PORT:-8000}/healthz" && echo "  dashboard API :${API_PORT:-8000}  up" || echo "  dashboard API :${API_PORT:-8000}  down"
  curl -fs -o /dev/null localhost:5174 && echo "  web UI        :5174  up" || echo "  web UI        :5174  down"
  log "ClickHouse view counts"
  $PY -c "
from arena.config import load_config; from agents.chclient import make_admin_client
a=make_admin_client(load_config().clickhouse)
for v in ['v_customers','v_products','v_orders','v_order_items','v_events']:
    try: print(f'  {v:16} {a.query(f\"SELECT count() FROM {v}\").result_rows[0][0]}')
    except Exception as e: print(f'  {v:16} (n/a)')
"
}

case "${1:-}" in
  up)     up ;;
  down)   down ;;
  serve)  require_venv; load_env; start_servers "${2:-}" ;;
  stop)   stop_servers; echo "local servers stopped" ;;
  status) status ;;
  *) echo "usage: scripts/arena.sh {up | down | serve [--api-only] | stop | status}"; exit 1 ;;
esac
