#!/usr/bin/env bash
#
# preflight.sh -- ClickHouse BUILD workshop readiness check.
#
# Run this from the app directory before the relevant `docker compose ... up`:
#
#   ./preflight.sh          # module 00: base app
#   ./preflight.sh --cdc    # module 03: Postgres CDC
#   ./preflight.sh --otel   # module 05: OpenTelemetry overlay
#
# It verifies, and prints PASS / WARN / FAIL for, everything the live bring-up of
# the stack actually tripped over: the Docker CLI + daemon, that the daemon can
# really start a container (catches a wedged engine), host-port collisions on the
# EFFECTIVE ports for the requested stage, the required .env.workshop values,
# and network reachability of ClickHouse Cloud. Managed Postgres and collector
# checks are opt-in because those services are not configured until modules 03
# and 05.
#
# Every failure prints a one-line fix hint. The script exits non-zero if any check
# FAILs (warnings do not affect the exit code), so it composes into scripts and CI.
# It needs no root, is bash 3.2 compatible (macOS default), and prints plain ASCII.

# Intentionally NOT `set -e`: each check runs to completion so you see every
# problem in one pass, not just the first.
set -u

SCRIPT_DIR=$(cd "$(dirname "$0")" 2>/dev/null && pwd)
[ -n "$SCRIPT_DIR" ] || SCRIPT_DIR=$PWD
ENV_FILE="$SCRIPT_DIR/.env.workshop"
EXAMPLE_FILE="$SCRIPT_DIR/.env.workshop.example"
PREFLIGHT_CTR="ch-workshop-preflight"

CHECK_CDC=0
CHECK_OTEL=0
RERUN_CMD="./preflight.sh"

usage() {
  cat <<'EOF'
Usage: ./preflight.sh [--cdc] [--otel] [--all]

With no flags, check only the module 00 base app.
  --cdc   also check module 03 Postgres settings and connectivity
  --otel  also check module 05 OpenTelemetry collector ports
  --all   run every stage-specific check
EOF
}

while [ "$#" -gt 0 ]; do
  case "$1" in
    --cdc) CHECK_CDC=1 ;;
    --otel) CHECK_OTEL=1 ;;
    --all) CHECK_CDC=1; CHECK_OTEL=1 ;;
    -h|--help) usage; exit 0 ;;
    *) printf 'Unknown option: %s\n\n' "$1" >&2; usage >&2; exit 2 ;;
  esac
  shift
done

[ "$CHECK_CDC" -eq 1 ] && RERUN_CMD="$RERUN_CMD --cdc"
[ "$CHECK_OTEL" -eq 1 ] && RERUN_CMD="$RERUN_CMD --otel"

# --- Output helpers --------------------------------------------------------
if [ -t 1 ] && [ -z "${NO_COLOR:-}" ]; then
  C_PASS=$'\033[32m'; C_WARN=$'\033[33m'; C_FAIL=$'\033[31m'; C_DIM=$'\033[2m'; C_RST=$'\033[0m'
else
  C_PASS=''; C_WARN=''; C_FAIL=''; C_DIM=''; C_RST=''
fi

PASS_COUNT=0
WARN_COUNT=0
FAIL_COUNT=0
WARN_LINES=()
FAIL_LINES=()

pass() {
  PASS_COUNT=$(( PASS_COUNT + 1 ))
  printf '%s[PASS]%s %s\n' "$C_PASS" "$C_RST" "$1"
}

warn() {
  WARN_COUNT=$(( WARN_COUNT + 1 ))
  WARN_LINES+=("$1")
  printf '%s[WARN]%s %s\n' "$C_WARN" "$C_RST" "$1"
  if [ -n "${2:-}" ]; then
    printf '       %shint:%s %s\n' "$C_DIM" "$C_RST" "$2"
  fi
  return 0
}

fail() {
  FAIL_COUNT=$(( FAIL_COUNT + 1 ))
  FAIL_LINES+=("$1")
  printf '%s[FAIL]%s %s\n' "$C_FAIL" "$C_RST" "$1"
  if [ -n "${2:-}" ]; then
    printf '       %shint:%s %s\n' "$C_DIM" "$C_RST" "$2"
  fi
  return 0
}

section() {
  printf '\n%s== %s ==%s\n' "$C_DIM" "$1" "$C_RST"
}

# --- Portable helpers ------------------------------------------------------

# run_with_timeout SECONDS CMD... -- returns 124 if CMD overran SECONDS.
# Uses GNU/coreutils timeout when present; otherwise a bash-3.2 polling fallback
# (macOS ships no `timeout`).
run_with_timeout() {
  local secs="$1"; shift
  if command -v timeout >/dev/null 2>&1; then
    timeout "$secs" "$@"
    return $?
  fi
  if command -v gtimeout >/dev/null 2>&1; then
    gtimeout "$secs" "$@"
    return $?
  fi
  "$@" &
  local pid=$!
  local waited=0
  while kill -0 "$pid" 2>/dev/null; do
    if [ "$waited" -ge "$secs" ]; then
      kill -TERM "$pid" 2>/dev/null
      sleep 1
      kill -KILL "$pid" 2>/dev/null
      wait "$pid" 2>/dev/null
      return 124
    fi
    sleep 1
    waited=$(( waited + 1 ))
  done
  wait "$pid"
  return $?
}

# port_in_use PORT -- return 0 if something is listening on localhost:PORT.
# A successful TCP connect proves a listener regardless of which user owns it
# (more reliable across macOS/Linux than a non-root lsof).
port_in_use() {
  run_with_timeout 3 bash -c "exec 3<>/dev/tcp/127.0.0.1/$1" >/dev/null 2>&1
}

# suggest_port PORT -- a free port to override with, following the +20000 remap
# convention (base port + 20000, e.g. 8080 -> 28080).
suggest_port() {
  local base=$(( $1 + 20000 ))
  local p="$base"
  local i=0
  while [ "$i" -lt 100 ]; do
    if ! port_in_use "$p"; then
      echo "$p"
      return 0
    fi
    p=$(( p + 1 ))
    i=$(( i + 1 ))
  done
  echo "$base"
}

# docker_publisher_of PORT -- name of the running container publishing that host
# port, or empty. Used to tell "occupied by this workshop stack" from a real clash.
docker_publisher_of() {
  [ "$DAEMON_OK" -eq 1 ] || { echo ""; return 0; }
  run_with_timeout 5 docker ps --format '{{.Names}} {{.Ports}}' 2>/dev/null \
    | awk -v pat=":$1->" 'index($0, pat) { print $1; exit }'
}

# tcp_reachable HOST PORT [SECONDS] -- return 0 if a TCP connect succeeds.
# Prefer nc: bash /dev/tcp probes to REMOTE hosts have been observed getting
# killed (exit 137) on some macOS setups while nc connects fine; /dev/tcp stays
# as the fallback for systems without nc.
tcp_reachable() {
  if command -v nc >/dev/null 2>&1; then
    nc -z -w "${3:-6}" "$1" "$2" >/dev/null 2>&1
  else
    run_with_timeout "${3:-6}" bash -c "exec 3<>/dev/tcp/$1/$2" >/dev/null 2>&1
  fi
}

# env_get KEY -- value of KEY in .env.workshop, LAST occurrence wins (matching
# docker compose semantics for duplicate keys), quotes and trailing CR stripped.
env_get() {
  [ "$HAVE_ENV" -eq 1 ] || { echo ""; return 0; }
  local line
  line=$(grep -E "^[[:space:]]*$1=" "$ENV_FILE" 2>/dev/null | tail -n 1)
  [ -n "$line" ] || { echo ""; return 0; }
  local val="${line#*=}"
  val="${val%$'\r'}"
  case "$val" in
    \"*\") val="${val#\"}"; val="${val%\"}" ;;
    \'*\') val="${val#\'}"; val="${val%\'}" ;;
  esac
  # Trim trailing whitespace.
  val="${val%"${val##*[![:space:]]}"}"
  echo "$val"
}

# Return success when a required value is blank or still contains the
# template's angle-bracket placeholder.
is_unset_or_placeholder() {
  case "$1" in
    ''|\<*\>) return 0 ;;
    *) return 1 ;;
  esac
}

# resolve_port VAR DEFAULT -- effective host port (env value, else default).
resolve_port() {
  local v
  v=$(env_get "$1")
  if [ -n "$v" ]; then echo "$v"; else echo "$2"; fi
}

# check_port VAR PORT SEVERITY NOTE -- probe one host port.
# SEVERITY is "core" (a real conflict FAILs) or "optional" (WARN only, since the
# port is bound solely when you opt into the otel overlay).
check_port() {
  local var="$1" port="$2" sev="$3" note="$4"
  if port_in_use "$port"; then
    local owner
    owner=$(docker_publisher_of "$port")
    case "$owner" in
      nyc-taxi-workshop-*)
        pass "port $port ($var) is in use by this workshop stack ($owner) -- fine, compose reuses it"
        ;;
      *)
        local sug who
        sug=$(suggest_port "$port")
        if [ -n "$owner" ]; then who="container '$owner'"; else who="another process"; fi
        if [ "$sev" = "core" ]; then
          fail "port $port ($var) is already in use by $who" \
               "set $var=$sug in .env.workshop, then re-run preflight (avoids the 'port is already allocated' bind error)"
        else
          warn "port $port ($var) is in use by $who$note" \
               "set $var=$sug in .env.workshop if you plan to use it"
        fi
        ;;
    esac
  else
    pass "port $port ($var) is free$note"
  fi
}

# check_shell_override VAR -- fail if an exported shell value would silently win
# over .env.workshop. This includes exported empty values: Compose resolves ${VAR}
# from the shell before --env-file, so a stale blank PGHOST can override a newly
# completed file and send the trip writer the wrong connection tuple.
check_shell_override() {
  local var="$1" shell_v file_v
  if env | grep -q "^${var}="; then
    shell_v=$(printenv "$var")
    file_v=$(env_get "$var")
    if [ "$shell_v" != "$file_v" ]; then
      fail "$var is exported in your shell and differs from .env.workshop" \
           "re-run: set -a; source ./.env.workshop; set +a  (Compose gives the shell value precedence over --env-file)"
    fi
  fi
  return 0
}

# ===========================================================================
printf '%sClickHouse BUILD workshop -- preflight%s\n' "$C_DIM" "$C_RST"
printf '%sapp dir: %s%s\n' "$C_DIM" "$SCRIPT_DIR" "$C_RST"
if [ "$CHECK_CDC" -eq 1 ] || [ "$CHECK_OTEL" -eq 1 ]; then
  printf '%sadditional checks: cdc=%s otel=%s%s\n' \
    "$C_DIM" "$CHECK_CDC" "$CHECK_OTEL" "$C_RST"
fi

# --- Docker CLI + daemon ---------------------------------------------------
section "Docker engine"
DOCKER_OK=0
DAEMON_OK=0
if command -v docker >/dev/null 2>&1; then
  pass "docker CLI found ($(docker --version 2>/dev/null | head -1))"
  DOCKER_OK=1
else
  fail "docker CLI not found on PATH" \
       "install Docker Desktop / OrbStack / Colima and make sure 'docker' is on your PATH"
fi

if [ "$DOCKER_OK" -eq 1 ]; then
  if run_with_timeout 15 docker info >/dev/null 2>&1; then
    pass "docker daemon is responding"
    DAEMON_OK=1
  else
    fail "docker daemon is not responding (docker info timed out or errored)" \
         "start / restart Docker Desktop / OrbStack / Colima and wait until it reports Running"
  fi
fi

if [ "$DOCKER_OK" -eq 1 ] && run_with_timeout 10 docker compose version >/dev/null 2>&1; then
  pass "docker compose v2 available ($(docker compose version --short 2>/dev/null))"
else
  fail "docker compose v2 (the 'docker compose' subcommand) is not available" \
       "install Compose v2 (bundled with Docker Desktop / OrbStack); the legacy 'docker-compose' v1 is not enough"
fi

# Wedge detector: a daemon can answer `docker info` yet be unable to actually
# start containers (they hang in 'Created') -- the OrbStack symptom from the live
# bring-up. Prove it can run a tiny container, bounded by a timeout.
run_start_test() {
  run_with_timeout 5 docker rm -f "$PREFLIGHT_CTR" >/dev/null 2>&1
  run_with_timeout "$1" docker run --rm --name "$PREFLIGHT_CTR" "$TEST_IMAGE" true >/dev/null 2>&1
  local rc=$?
  run_with_timeout 5 docker rm -f "$PREFLIGHT_CTR" >/dev/null 2>&1
  return $rc
}

TEST_IMAGE="alpine:3"
if [ "$DAEMON_OK" -eq 1 ]; then
  IMAGE_OK=1
  # Pull the test image first (if absent) so we time container START, not a pull.
  if ! docker image inspect "$TEST_IMAGE" >/dev/null 2>&1; then
    if ! run_with_timeout 90 docker pull "$TEST_IMAGE" >/dev/null 2>&1; then
      IMAGE_OK=0
      warn "could not pull the test image ($TEST_IMAGE) to check container start" \
           "check network access to Docker Hub, then re-run preflight"
    fi
  fi
  if [ "$IMAGE_OK" -eq 1 ]; then
    run_start_test 30
    START_RC=$?
    if [ "$START_RC" -eq 0 ]; then
      pass "docker can start a container (ran $TEST_IMAGE 'true')"
    elif [ "$START_RC" -eq 124 ]; then
      # A cold container runtime can be genuinely slow on its first start (observed
      # ~60s, then seconds thereafter), which is not a wedge. Retry once with a
      # longer ceiling; a truly stuck daemon times out on both attempts.
      run_start_test 60
      START_RC2=$?
      if [ "$START_RC2" -eq 0 ]; then
        pass "docker can start a container (ran $TEST_IMAGE 'true'; the runtime was slow to warm up)"
      elif [ "$START_RC2" -eq 124 ]; then
        fail "docker could not start a container within 30s then 60s -- the daemon looks wedged" \
             "restart Docker Desktop / OrbStack / Colima (containers stuck in 'Created' are the classic symptom), then re-run"
      else
        fail "docker failed to run a test container (exit $START_RC2)" \
             "restart your Docker engine, then re-run preflight"
      fi
    else
      fail "docker failed to run a test container (exit $START_RC)" \
           "restart your Docker engine, then re-run preflight"
    fi
  fi
else
  warn "skipping the container-start test because the daemon is not responding" \
       "start your Docker engine, then re-run preflight"
fi

# --- Versions / resource advisories ---------------------------------------
section "Versions and resources"
if command -v git >/dev/null 2>&1; then
  pass "git found ($(git --version 2>/dev/null))"
else
  warn "git not found on PATH" "install git (you have already cloned the repo, so this only matters for pulling updates)"
fi

if [ "$DAEMON_OK" -eq 1 ]; then
  MEM_BYTES=$(run_with_timeout 10 docker info --format '{{.MemTotal}}' 2>/dev/null)
  case "$MEM_BYTES" in
    ''|*[!0-9]*)
      warn "could not read Docker's memory allocation" \
           "ensure Docker has >= 6 GB allocated (Docker Desktop: Settings > Resources)"
      ;;
    *)
      MEM_GIB=$(( MEM_BYTES / 1073741824 ))
      if [ "$MEM_BYTES" -ge 6000000000 ]; then
        pass "Docker memory allocation ~${MEM_GIB} GB (>= 6 GB)"
      else
        warn "Docker memory allocation ~${MEM_GIB} GB is below the recommended 6 GB" \
             "raise it in Docker Desktop: Settings > Resources > Memory; the stack may not come up healthy with less"
      fi
      ;;
  esac
fi

AVAIL_KB=$(df -Pk "$SCRIPT_DIR" 2>/dev/null | awk 'NR==2 { print $4 }')
case "$AVAIL_KB" in
  ''|*[!0-9]*)
    warn "could not determine free disk space" "ensure roughly 10 GB is free for Docker images and volumes"
    ;;
  *)
    AVAIL_GIB=$(( AVAIL_KB / 1048576 ))
    if [ "$AVAIL_KB" -ge 10485760 ]; then
      pass "free disk ~${AVAIL_GIB} GB (>= 10 GB)"
    else
      warn "free disk ~${AVAIL_GIB} GB is below the recommended ~10 GB" \
           "free up space; the images and volumes for this stack need several GB"
    fi
    ;;
esac

# --- .env.workshop ---------------------------------------------------------
section ".env.workshop configuration"
HAVE_ENV=0
[ -f "$ENV_FILE" ] && HAVE_ENV=1

CH_HOST=""
CH_PORT="8443"
CH_PW=""
PGHOST_VAL=""
PGPORT_VAL="5432"
PG_CONFIG_OK=1

if [ "$HAVE_ENV" -eq 1 ]; then
  pass ".env.workshop found"
  CH_HOST=$(env_get CLICKHOUSE_HOST)
  CH_PORT=$(env_get CLICKHOUSE_PORT); [ -n "$CH_PORT" ] || CH_PORT="8443"
  CH_PW=$(env_get CLICKHOUSE_PASSWORD)

  case "$CH_HOST" in
    localhost|127.0.0.1|0.0.0.0|::1|host.docker.internal|clickhouse)
      fail "CLICKHOUSE_HOST=$CH_HOST points to a local server, which this workshop does not use" \
           "paste the bare ClickHouse Cloud hostname from the service Connect dialog"
      ;;
    "")
      fail "CLICKHOUSE_HOST is empty" \
           "paste your service host (bare hostname, no https:// and no port) from the Cloud Connect modal into .env.workshop"
      ;;
    *)
      pass "CLICKHOUSE_HOST is set ($CH_HOST)"
      ;;
  esac
  if [ -n "$CH_PW" ]; then
    pass "CLICKHOUSE_PASSWORD is set"
  else
    fail "CLICKHOUSE_PASSWORD is empty" \
         "paste the default-user password from the Cloud Connect modal into .env.workshop"
  fi
  if [ "$(env_get CLICKHOUSE_SECURE)" = "true" ]; then
    pass "CLICKHOUSE_SECURE=true (TLS required for ClickHouse Cloud)"
  else
    fail "CLICKHOUSE_SECURE must be true" \
         "set CLICKHOUSE_SECURE=true; local or plaintext ClickHouse endpoints are not supported"
  fi

  if [ -n "$(env_get OPENAI_API_KEY)" ]; then
    pass "OPENAI_API_KEY is set"
  else
    warn "OPENAI_API_KEY is empty (needed for module 08)" \
         "add it before module 08 for the AI chat"
  fi

  LF_PUB=$(env_get LANGFUSE_PUBLIC_KEY)
  LF_SEC=$(env_get LANGFUSE_SECRET_KEY)
  if [ -n "$LF_PUB" ] && [ -n "$LF_SEC" ]; then
    pass "LANGFUSE_PUBLIC_KEY and LANGFUSE_SECRET_KEY are set"
  else
    warn "LANGFUSE keys not fully set (needed only for module 08: chat tracing)" \
         "add LANGFUSE_PUBLIC_KEY/SECRET_KEY before module 08 if you want traces; chat works untraced without them"
  fi

  if [ "$CHECK_OTEL" -eq 1 ]; then
    OTLP_TOKEN=$(env_get OTLP_AUTH_TOKEN)
    case "$OTLP_TOKEN" in
      ""|\<*\>|change-me*|changeme*|replace-*)
        fail "OTLP_AUTH_TOKEN is missing or still a placeholder for module 05" \
             "generate a random token, save it in .env.workshop, source the file again, then rerun with --otel"
        ;;
      *)
        pass "OTLP_AUTH_TOKEN is configured for module 05"
        ;;
    esac
  fi

  if [ "$CHECK_CDC" -eq 1 ]; then
    PGHOST_VAL=$(env_get PGHOST)
    PGPORT_VAL=$(env_get PGPORT); [ -n "$PGPORT_VAL" ] || PGPORT_VAL="5432"
    PGSSLMODE_VAL=$(env_get PGSSLMODE)

    case "$PGHOST_VAL" in
      postgres|localhost|127.0.0.1|0.0.0.0|::1|host.docker.internal)
        fail "PGHOST=$PGHOST_VAL points to a local database, which this workshop does not use" \
             "replace PGHOST with the hostname returned by 'clickhousectl cloud postgres create' in Module 03"
        PG_CONFIG_OK=0
        ;;
      ""|\<*\>)
        fail "PGHOST is not configured for module 03" \
             "create managed Postgres first, paste its hostname into .env.workshop, then rerun with --cdc"
        PG_CONFIG_OK=0
        ;;
      *)
        pass "PGHOST=$PGHOST_VAL (using managed Postgres)"
        ;;
    esac

    for pg_var in PGUSER PGDATABASE PGPASSWORD; do
      pg_val=$(env_get "$pg_var")
      if is_unset_or_placeholder "$pg_val"; then
        fail "$pg_var is not configured for module 03" \
             "fill $pg_var in .env.workshop, then rerun with --cdc"
        PG_CONFIG_OK=0
      else
        pass "$pg_var is set"
      fi
    done

    if [ "$PGSSLMODE_VAL" = "require" ]; then
      pass "PGSSLMODE=require (TLS required for managed Postgres)"
    else
      fail "PGSSLMODE must be require for managed Postgres (current: ${PGSSLMODE_VAL:-empty})" \
           "set PGSSLMODE=require in .env.workshop"
      PG_CONFIG_OK=0
    fi
  fi
else
  fail ".env.workshop not found in $SCRIPT_DIR" \
       "run: cp .env.workshop.example .env.workshop  then fill in CLICKHOUSE_HOST and CLICKHOUSE_PASSWORD"
  if [ -f "$EXAMPLE_FILE" ]; then
    warn "port checks below use the built-in defaults for the selected stage" \
         "create .env.workshop so preflight checks your real, effective ports"
  fi
  [ "$CHECK_CDC" -eq 1 ] && PG_CONFIG_OK=0
fi

# --- Shell environment vs .env.workshop ------------------------------------
section "Shell environment vs .env.workshop"
_fails_before=$FAIL_COUNT
for shell_var in \
  CLICKHOUSE_HOST CLICKHOUSE_PORT CLICKHOUSE_USER CLICKHOUSE_PASSWORD \
  CLICKHOUSE_DATABASE CLICKHOUSE_SECURE CLICKHOUSE_CONNECT_TIMEOUT \
  API_CORS_ORIGINS QUERY_TIMEOUT_SECONDS MAX_ROWS_TO_READ MAX_BYTES_TO_READ \
  OPENAI_API_KEY LLM_MODEL LLM_BASE_URL LANGFUSE_PUBLIC_KEY \
  LANGFUSE_SECRET_KEY LANGFUSE_BASE_URL BACKEND_HOST_PORT FRONTEND_HOST_PORT; do
  check_shell_override "$shell_var"
done
if [ "$CHECK_CDC" -eq 1 ]; then
  for shell_var in \
    PGHOST PGPORT PGDATABASE PGUSER PGPASSWORD PGSSLMODE PG_PUBLICATION \
    RATE_PER_SEC BATCH_SIZE; do
    check_shell_override "$shell_var"
  done
fi
if [ "$CHECK_OTEL" -eq 1 ]; then
  for shell_var in \
    OTLP_AUTH_TOKEN CLICKSTACK_DATABASE OTEL_SERVICE_NAME \
    OTEL_FRONTEND_SERVICE_NAME OTEL_GRPC_HOST_PORT OTEL_HTTP_HOST_PORT LOG_LEVEL; do
    check_shell_override "$shell_var"
  done
fi
if [ "$FAIL_COUNT" -eq "$_fails_before" ]; then
  pass "no exported shell variable is shadowing .env.workshop for this stage"
fi

# --- Host ports ------------------------------------------------------------
section "Host port availability (effective ports)"
FRONTEND_PORT=$(resolve_port FRONTEND_HOST_PORT 8080)
BACKEND_PORT=$(resolve_port BACKEND_HOST_PORT 8000)

check_port FRONTEND_HOST_PORT "$FRONTEND_PORT" core ""
check_port BACKEND_HOST_PORT "$BACKEND_PORT" core ""

if [ "$CHECK_OTEL" -eq 1 ]; then
  OTEL_GRPC_PORT=$(resolve_port OTEL_GRPC_HOST_PORT 4317)
  OTEL_HTTP_PORT=$(resolve_port OTEL_HTTP_HOST_PORT 4318)
  check_port OTEL_GRPC_HOST_PORT "$OTEL_GRPC_PORT" core " (OpenTelemetry overlay, module 05)"
  check_port OTEL_HTTP_HOST_PORT "$OTEL_HTTP_PORT" core " (OpenTelemetry overlay, module 05)"
fi

# --- Connectivity ----------------------------------------------------------
section "Connectivity"
if [ -n "$CH_HOST" ]; then
  if command -v curl >/dev/null 2>&1; then
    HTTP_CODE=$(run_with_timeout 20 curl -sS -o /dev/null -w '%{http_code}' "https://$CH_HOST:$CH_PORT/ping" 2>/dev/null)
    CRC=$?
    if [ "$CRC" -eq 0 ]; then
      pass "ClickHouse Cloud reachable over TLS (https://$CH_HOST:$CH_PORT/ping -> HTTP $HTTP_CODE)"
    else
      case "$CRC" in
        6)        CHINT="DNS could not resolve $CH_HOST -- check the hostname (bare host, no https://)" ;;
        7)        CHINT="connection refused -- re-check CLICKHOUSE_HOST and CLICKHOUSE_PORT" ;;
        28|124)   CHINT="timed out -- check wifi / VPN / firewall; a Cloud IP-access-list may be blocking your IP" ;;
        35|51|60) CHINT="TLS handshake failed -- confirm port $CH_PORT and CLICKHOUSE_SECURE=true" ;;
        *)        CHINT="could not reach it -- check network / wifi / VPN / Cloud IP allowlist" ;;
      esac
      fail "ClickHouse Cloud not reachable at https://$CH_HOST:$CH_PORT (curl exit $CRC)" "$CHINT"
    fi
  else
    warn "curl not found; skipping the ClickHouse reachability check" \
         "install curl to verify Cloud connectivity"
  fi
else
  warn "skipping the ClickHouse reachability check (CLICKHOUSE_HOST is not set)" \
       "set CLICKHOUSE_HOST in .env.workshop first"
fi

if [ "$CHECK_CDC" -eq 1 ] && [ "$PG_CONFIG_OK" -eq 1 ]; then
  if tcp_reachable "$PGHOST_VAL" "$PGPORT_VAL" 8; then
    pass "managed Postgres reachable ($PGHOST_VAL:$PGPORT_VAL, TCP)"
  else
    fail "managed Postgres not reachable at $PGHOST_VAL:$PGPORT_VAL" \
         "wait for provisioning, then check wifi / VPN / firewall, the hostname, and its IP allowlist before rerunning with --cdc"
  fi
fi

# --- Summary ---------------------------------------------------------------
printf '\n%s==================================================%s\n' "$C_DIM" "$C_RST"
printf 'Preflight summary: %d passed, %d warning(s), %d failure(s)\n' "$PASS_COUNT" "$WARN_COUNT" "$FAIL_COUNT"

if [ "$FAIL_COUNT" -gt 0 ]; then
  printf '\n%sFailures to fix before the requested docker compose up:%s\n' "$C_FAIL" "$C_RST"
  for line in "${FAIL_LINES[@]}"; do
    printf '  - %s\n' "$line"
  done
fi
if [ "$WARN_COUNT" -gt 0 ]; then
  printf '\n%sWarnings (safe to proceed, review before the relevant module):%s\n' "$C_WARN" "$C_RST"
  for line in "${WARN_LINES[@]}"; do
    printf '  - %s\n' "$line"
  done
fi

printf '\n'
if [ "$FAIL_COUNT" -gt 0 ]; then
  printf '%sOverall: NOT READY -- fix the failures above, then rerun %s%s\n' "$C_FAIL" "$RERUN_CMD" "$C_RST"
  printf '%s==================================================%s\n' "$C_DIM" "$C_RST"
  exit 1
fi
if [ "$CHECK_CDC" -eq 1 ]; then
  printf '%sOverall: READY -- module 03 Postgres checks passed.%s\n' "$C_PASS" "$C_RST"
  printf '  docker compose --env-file .env.workshop -f docker-compose.workshop.yml --profile cdc up -d pg-trip-writer\n'
elif [ "$CHECK_OTEL" -eq 1 ]; then
  printf '%sOverall: READY -- module 05 OpenTelemetry checks passed.%s\n' "$C_PASS" "$C_RST"
else
  printf '%sOverall: READY -- start the base app with:%s\n' "$C_PASS" "$C_RST"
  printf '  docker compose --env-file .env.workshop -f docker-compose.workshop.yml up -d\n'
fi
printf '%s==================================================%s\n' "$C_DIM" "$C_RST"
exit 0
