#!/usr/bin/env bash
#
# preflight.sh -- ClickHouse BUILD workshop readiness check.
#
# Run this from the app directory BEFORE `docker compose ... up`:
#
#   ./preflight.sh
#
# It verifies, and prints PASS / WARN / FAIL for, everything the live bring-up of
# the stack actually tripped over: the Docker CLI + daemon, that the daemon can
# really start a container (catches a wedged engine), host-port collisions on the
# EFFECTIVE ports from .env.workshop, the required .env.workshop values, and
# network reachability of ClickHouse Cloud (and the shared Postgres, if used).
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

# Capture the calling shell's values for the vars docker compose interpolates,
# BEFORE anything else runs, so the shell-vs-.env.workshop override check can tell
# whether an exported shell value would silently win over the file (see that
# section below). Unset -> empty, which the check treats as "not overriding".
SHELL_OPENAI_API_KEY="${OPENAI_API_KEY:-}"
SHELL_LANGFUSE_PUBLIC_KEY="${LANGFUSE_PUBLIC_KEY:-}"
SHELL_LANGFUSE_SECRET_KEY="${LANGFUSE_SECRET_KEY:-}"
SHELL_CLICKHOUSE_PASSWORD="${CLICKHOUSE_PASSWORD:-}"

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

# check_shell_override VAR SHELL_VALUE -- warn if an exported shell value would
# silently win over .env.workshop. docker compose resolves ${VAR} from the shell
# environment BEFORE the --env-file, so a value exported in the calling shell
# overrides a blank/different line in .env.workshop -- e.g. an exported
# OPENAI_API_KEY makes the app do real LLM calls even though the file looks empty.
check_shell_override() {
  local var="$1" shell_v="$2" file_v
  [ -n "$shell_v" ] || return 0
  file_v=$(env_get "$var")
  if [ "$shell_v" != "$file_v" ]; then
    warn "$var is set in your shell and differs from .env.workshop" \
         "docker compose uses the shell value (it overrides .env.workshop via interpolation), so the file is ignored for $var -- run 'unset $var' or set the same value in .env.workshop"
  fi
  return 0
}

# ===========================================================================
printf '%sClickHouse BUILD workshop -- preflight%s\n' "$C_DIM" "$C_RST"
printf '%sapp dir: %s%s\n' "$C_DIM" "$SCRIPT_DIR" "$C_RST"

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
PGHOST_VAL="postgres"

if [ "$HAVE_ENV" -eq 1 ]; then
  pass ".env.workshop found"
  CH_HOST=$(env_get CLICKHOUSE_HOST)
  CH_PORT=$(env_get CLICKHOUSE_PORT); [ -n "$CH_PORT" ] || CH_PORT="8443"
  CH_PW=$(env_get CLICKHOUSE_PASSWORD)

  if [ -n "$CH_HOST" ]; then
    pass "CLICKHOUSE_HOST is set ($CH_HOST)"
  else
    fail "CLICKHOUSE_HOST is empty" \
         "paste your service host (bare hostname, no https:// and no port) from the Cloud Connect modal into .env.workshop"
  fi
  if [ -n "$CH_PW" ]; then
    pass "CLICKHOUSE_PASSWORD is set"
  else
    fail "CLICKHOUSE_PASSWORD is empty" \
         "paste the default-user password from the Cloud Connect modal into .env.workshop"
  fi

  if [ -n "$(env_get OPENAI_API_KEY)" ]; then
    pass "OPENAI_API_KEY is set"
  else
    warn "OPENAI_API_KEY is empty (needed for optional module 06b and module 08)" \
         "add it before 06b if using LibreChat, or before module 08 for the AI chat"
  fi

  LF_PUB=$(env_get LANGFUSE_PUBLIC_KEY)
  LF_SEC=$(env_get LANGFUSE_SECRET_KEY)
  if [ -n "$LF_PUB" ] && [ -n "$LF_SEC" ]; then
    pass "LANGFUSE_PUBLIC_KEY and LANGFUSE_SECRET_KEY are set"
  else
    warn "LANGFUSE keys not fully set (needed only for module 08: chat tracing)" \
         "add LANGFUSE_PUBLIC_KEY/SECRET_KEY before module 08 if you want traces; chat works untraced without them"
  fi

  PGHOST_VAL=$(env_get PGHOST); [ -n "$PGHOST_VAL" ] || PGHOST_VAL="postgres"
  if [ "$PGHOST_VAL" = "postgres" ]; then
    pass "PGHOST=postgres (using the local fallback Postgres container)"
  else
    pass "PGHOST=$PGHOST_VAL (using a shared managed Postgres)"
  fi
else
  fail ".env.workshop not found in $SCRIPT_DIR" \
       "run: cp .env.workshop.example .env.workshop  then fill in CLICKHOUSE_HOST and CLICKHOUSE_PASSWORD"
  if [ -f "$EXAMPLE_FILE" ]; then
    warn "port checks below use the example defaults (8080/8000/5432/4317/4318)" \
         "create .env.workshop so preflight checks your real, effective ports"
  fi
fi

# --- Shell environment vs .env.workshop ------------------------------------
section "Shell environment vs .env.workshop"
_warns_before=$WARN_COUNT
check_shell_override OPENAI_API_KEY "$SHELL_OPENAI_API_KEY"
check_shell_override LANGFUSE_PUBLIC_KEY "$SHELL_LANGFUSE_PUBLIC_KEY"
check_shell_override LANGFUSE_SECRET_KEY "$SHELL_LANGFUSE_SECRET_KEY"
check_shell_override CLICKHOUSE_PASSWORD "$SHELL_CLICKHOUSE_PASSWORD"
if [ "$WARN_COUNT" -eq "$_warns_before" ]; then
  pass "no shell variable is shadowing .env.workshop (OPENAI_API_KEY, LANGFUSE_*, CLICKHOUSE_PASSWORD)"
fi

# --- Host ports ------------------------------------------------------------
section "Host port availability (effective ports)"
FRONTEND_PORT=$(resolve_port FRONTEND_HOST_PORT 8080)
BACKEND_PORT=$(resolve_port BACKEND_HOST_PORT 8000)
POSTGRES_PORT=$(resolve_port POSTGRES_HOST_PORT 5432)
OTEL_GRPC_PORT=$(resolve_port OTEL_GRPC_HOST_PORT 4317)
OTEL_HTTP_PORT=$(resolve_port OTEL_HTTP_HOST_PORT 4318)

check_port FRONTEND_HOST_PORT "$FRONTEND_PORT" core ""
check_port BACKEND_HOST_PORT "$BACKEND_PORT" core ""
check_port POSTGRES_HOST_PORT "$POSTGRES_PORT" core ""
check_port OTEL_GRPC_HOST_PORT "$OTEL_GRPC_PORT" optional " (only with the otel overlay)"
check_port OTEL_HTTP_HOST_PORT "$OTEL_HTTP_PORT" optional " (only with the otel overlay)"

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

if [ "$PGHOST_VAL" != "postgres" ] && [ -n "$PGHOST_VAL" ]; then
  PGPORT_VAL=$(env_get PGPORT); [ -n "$PGPORT_VAL" ] || PGPORT_VAL="5432"
  if tcp_reachable "$PGHOST_VAL" "$PGPORT_VAL" 8; then
    pass "shared Postgres reachable ($PGHOST_VAL:$PGPORT_VAL, TCP)"
  else
    # WARN, not FAIL: the app dashboards run without the loadgen; the shared
    # Postgres only feeds the live CDC path (module 03).
    warn "shared Postgres not reachable at $PGHOST_VAL:$PGPORT_VAL (only needed for the live CDC path, module 03)" \
         "check wifi / VPN / firewall and that the shared Postgres endpoint and its IP allowlist are correct"
  fi
else
  pass "using the local fallback Postgres (no external Postgres reachability check needed)"
fi

# --- Summary ---------------------------------------------------------------
printf '\n%s==================================================%s\n' "$C_DIM" "$C_RST"
printf 'Preflight summary: %d passed, %d warning(s), %d failure(s)\n' "$PASS_COUNT" "$WARN_COUNT" "$FAIL_COUNT"

if [ "$FAIL_COUNT" -gt 0 ]; then
  printf '\n%sFailures to fix before `docker compose ... up`:%s\n' "$C_FAIL" "$C_RST"
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
  printf '%sOverall: NOT READY -- fix the failures above, then re-run ./preflight.sh%s\n' "$C_FAIL" "$C_RST"
  printf '%s==================================================%s\n' "$C_DIM" "$C_RST"
  exit 1
fi
printf '%sOverall: READY -- start the stack with:%s\n' "$C_PASS" "$C_RST"
printf '  docker compose --env-file .env.workshop -f docker-compose.workshop.yml up -d\n'
printf '%s==================================================%s\n' "$C_DIM" "$C_RST"
exit 0
