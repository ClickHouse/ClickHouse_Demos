#!/usr/bin/env bash
set -euo pipefail

ROOT=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
ENV_FILE="${ROOT}/.env.polymarket"

fail() {
  echo "FAIL: $*" >&2
  exit 1
}

command -v docker >/dev/null 2>&1 || fail "Docker is not installed"
docker compose version >/dev/null 2>&1 || fail "Docker Compose v2 is not available"
command -v clickhouse >/dev/null 2>&1 || fail "ClickHouse client is not installed"

[[ -f "${ENV_FILE}" ]] || fail "copy .env.polymarket.example to .env.polymarket first"
set -a
# shellcheck disable=SC1090
source "${ENV_FILE}"
set +a

[[ -n "${CLICKHOUSE_HOST:-}" ]] || fail "CLICKHOUSE_HOST is empty"
[[ -n "${CLICKHOUSE_PASSWORD:-}" ]] || fail "CLICKHOUSE_PASSWORD is empty"
if [[ "${CLICKHOUSE_HOST}" =~ ^(localhost|127\.0\.0\.1|::1)$ ]]; then
  fail "CLICKHOUSE_HOST points to a local server; this track uses ClickHouse Cloud"
fi

clickhouse client \
  --host "${CLICKHOUSE_HOST}" \
  --port "${CLICKHOUSE_PORT:-8443}" \
  --user "${CLICKHOUSE_USER:-default}" \
  --password "${CLICKHOUSE_PASSWORD}" \
  --secure \
  --query "SELECT 1" >/dev/null

public_ok=true
market_payload=$(
  curl --fail --silent --show-error --max-time 15 \
    'https://gamma-api.polymarket.com/markets?active=true&closed=false&limit=20'
) || public_ok=false

condition_id=""
token_id=""
if [[ "${public_ok}" == "true" ]]; then
  if ! read -r condition_id token_id < <(
      printf '%s' "${market_payload}" | python3 -c '
import json, sys
for market in json.load(sys.stdin):
    if market.get("acceptingOrders"):
        tokens = json.loads(market["clobTokenIds"])
        if tokens:
            print(market["conditionId"], tokens[0])
            break
'
    ); then
    public_ok=false
  fi
  [[ -n "${condition_id}" && -n "${token_id}" ]] || public_ok=false
fi

if [[ "${public_ok}" == "true" ]]; then
  curl --fail --silent --show-error --max-time 15 --get \
    'https://data-api.polymarket.com/trades' \
    --data-urlencode "market=${condition_id}" \
    --data-urlencode 'limit=1' >/dev/null || public_ok=false
  curl --fail --silent --show-error --max-time 15 --get \
    'https://clob.polymarket.com/book' \
    --data-urlencode "token_id=${token_id}" >/dev/null || public_ok=false
fi

if [[ "${public_ok}" != "true" ]]; then
  if [[ "${POLYMARKET_MODE:-live}" == "fixture" ]]; then
    echo "WARN: Polymarket public data is unreachable; fixture mode will continue"
  else
    fail "a Polymarket public API is unreachable; set POLYMARKET_MODE=fixture and rerun"
  fi
fi

if [[ "${public_ok}" == "true" ]]; then
  echo "READY: Docker, ClickHouse Cloud, Gamma, Data API, and CLOB REST are reachable"
else
  echo "READY: Docker and ClickHouse Cloud are reachable; fixture mode will supply data"
fi
