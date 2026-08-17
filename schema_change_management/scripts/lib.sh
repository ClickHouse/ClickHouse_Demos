#!/usr/bin/env bash
# Shared helpers. Sourced by the other scripts, not run directly.

set -euo pipefail

REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"

# Load .env if present, exporting everything in it.
load_env() {
  if [[ -f "${REPO_ROOT}/.env" ]]; then
    set -a
    # shellcheck disable=SC1091
    source "${REPO_ROOT}/.env"
    set +a
  else
    echo "ERROR: ${REPO_ROOT}/.env not found. Run: cp .env.example .env" >&2
    exit 1
  fi
}

# Resolve which target we are talking to: cloud (default) or local.
# Sets TARGET_HTTP, TARGET_AUTH_ARGS, TARGET_DB, TARGET_LABEL.
resolve_target() {
  local target="${1:-cloud}"
  case "$target" in
    cloud)
      : "${CH_CLOUD_HTTP:?CH_CLOUD_HTTP not set}"
      : "${CH_CLOUD_USER:?CH_CLOUD_USER not set}"
      : "${CH_CLOUD_PASSWORD:?CH_CLOUD_PASSWORD not set}"
      TARGET_HTTP="${CH_CLOUD_HTTP}"
      TARGET_USER="${CH_CLOUD_USER}"
      TARGET_PASS="${CH_CLOUD_PASSWORD}"
      TARGET_DB="${CH_CLOUD_DB:-adtech}"
      TARGET_LABEL="ClickHouse Cloud (${CH_CLOUD_HOST:-unknown host})"
      ;;
    local)
      TARGET_HTTP="${CH_LOCAL_HTTP:-http://localhost:8123}"
      TARGET_USER="default"
      TARGET_PASS="localpass"
      TARGET_DB="adtech"
      TARGET_LABEL="local ClickHouse OSS (docker compose)"
      ;;
    *)
      echo "ERROR: unknown target '${target}'. Use 'cloud' or 'local'." >&2
      exit 1
      ;;
  esac
}

# Run a query over HTTP. Avoids needing clickhouse-client installed.
#   ch_query "SELECT 1"
ch_query() {
  local sql="$1"
  curl --silent --show-error --fail-with-body \
    --user "${TARGET_USER}:${TARGET_PASS}" \
    --data-binary "${sql}" \
    "${TARGET_HTTP}/?database=${TARGET_DB}"
}

# Run a query with NO database scope, so it lands in `default`.
#
# Needed for anything that runs before the adtech database exists: server
# version, "does this database exist", etc. Scoping those to a database that has
# not been created yet makes ClickHouse return UNKNOWN_DATABASE, which reads like
# a connectivity failure and sends you debugging the wrong thing.
ch_query_nodb() {
  local sql="$1"
  curl --silent --show-error --fail-with-body \
    --user "${TARGET_USER}:${TARGET_PASS}" \
    --data-binary "${sql}" \
    "${TARGET_HTTP}/"
}

# Retry wrapper for a query whose failure would be reported as a hard FAIL.
# ClickHouse Cloud services idle, and the first query or two after a wake can fail
# transiently. Observed live: `SELECT count() FROM system.parts` failed once on a
# cold service and preflight reported a missing grant that was present.
ch_query_nodb_retry() {
  local sql="$1" i
  for i in 1 2 3; do
    if ch_query_nodb "$sql" 2>/dev/null; then return 0; fi
    sleep 2
  done
  return 1
}

# Same, but tolerate failure and print the body. Used where a query is expected
# to fail as part of the demo.
ch_query_soft() {
  local sql="$1"
  curl --silent --show-error \
    --user "${TARGET_USER}:${TARGET_PASS}" \
    --data-binary "${sql}" \
    "${TARGET_HTTP}/?database=${TARGET_DB}" || true
}

# Run a multi-statement .sql file, one statement per request.
# ClickHouse's HTTP interface takes one statement at a time.
ch_file() {
  local file="$1"
  local stmt=""
  while IFS= read -r line; do
    # strip full-line comments
    [[ "$line" =~ ^[[:space:]]*-- ]] && continue
    stmt+="$line"$'\n'
    if [[ "$line" =~ \;[[:space:]]*$ ]]; then
      stmt="${stmt%%;*}"
      if [[ -n "${stmt//[[:space:]]/}" ]]; then
        echo "  -> ${stmt//$'\n'/ }" | cut -c1-110
        ch_query "$stmt"
      fi
      stmt=""
    fi
  done < "$file"
}

# Rewrite the database path segment of a ClickHouse URL.
#
#   atlas_url_with_db "clickhouse://u:p@h:9440/adtech?secure=true" default
#   -> clickhouse://u:p@h:9440/default?secure=true
#
# Needed because the database name in a native-protocol URL is sent in the client
# handshake, not per query. Connecting with a database that does not exist is
# refused outright with UNKNOWN_DATABASE (code 81), which looks like a TLS or port
# problem and is not one. Probing against `default` — which always exists on
# ClickHouse Cloud — separates "cannot connect" from "database not created yet".
#
# docker:// URLs are returned unchanged: there the path is <image>/<version>/<db>
# and Atlas creates that database itself.
# The `(.*@)?` is greedy on purpose: it consumes up to the LAST '@', which is how
# RFC 3986 separates userinfo from host. That matters because ClickHouse Cloud
# passwords are pasted in raw and can contain '/', '?' and '@'. A non-greedy or
# character-class match splits such a URL at the wrong place and hands Atlas a
# different host.
atlas_url_with_db() {
  local url="${1:-}" db="${2:-default}"
  [[ -z "$url" ]] && return 0
  case "$url" in docker://*) printf '%s' "$url"; return 0 ;; esac
  printf '%s' "$url" | sed -E "s#^([a-zA-Z0-9+.-]+://(.*@)?[^/?#]*)(/[^?#]*)?#\1/${db}#"
}

# The database named in a ClickHouse URL, empty if there is none or if the URL is
# a docker:// one (where the path is not a database path).
atlas_url_db() {
  case "${1:-}" in docker://*) printf ''; return 0 ;; esac
  [[ -z "${1:-}" ]] && return 0
  printf '%s' "$1" | sed -E 's#^[a-zA-Z0-9+.-]+://(.*@)?[^/?#]*/?([^?#]*).*#\2#'
}

# Password-redacted copy of a URL, safe to print on a shared screen. Greedy for
# the same reason as above: a password containing '@' must be redacted whole.
redact_url() {
  printf '%s' "${1:-}" | sed -E 's#://([^:/@]+):.*@#://\1:***@#'
}

hr() { printf '%s\n' "-------------------------------------------------------------------------"; }

say() { printf '\n\033[1m%s\033[0m\n' "$*"; }
