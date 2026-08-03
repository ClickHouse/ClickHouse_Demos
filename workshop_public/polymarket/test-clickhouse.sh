#!/usr/bin/env bash
set -euo pipefail

ROOT=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
NAME="polymarket-workshop-test-${RANDOM}"

cleanup() {
  docker rm -f "${NAME}" >/dev/null 2>&1 || true
}
trap cleanup EXIT

docker run -d \
  --name "${NAME}" \
  -e CLICKHOUSE_PASSWORD=workshop \
  -p 127.0.0.1::8123 \
  clickhouse/clickhouse-server:26.3 >/dev/null
for _ in $(seq 1 60); do
  if docker exec "${NAME}" clickhouse client --password workshop --query "SELECT 1" >/dev/null 2>&1; then
    break
  fi
  sleep 1
done
docker exec "${NAME}" clickhouse client --password workshop --query "SELECT 1" >/dev/null
docker exec -i "${NAME}" clickhouse client --password workshop --multiquery < "${ROOT}/db/schema.sql"

docker exec -i "${NAME}" clickhouse client --password workshop --multiquery <<'SQL'
INSERT INTO polymarket.markets VALUES
(
  1,
  '0x1111111111111111111111111111111111111111111111111111111111111111',
  1001,
  'Yes',
  'Will the fixture move?',
  'fixture-move',
  true,
  false,
  1000,
  now64(3)
);

INSERT INTO polymarket.price_ticks VALUES
(
  'aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa',
  '0x1111111111111111111111111111111111111111111111111111111111111111',
  1001,
  now64(3) - INTERVAL 1 MINUTE,
  now64(3),
  'best_bid_ask',
  'FIXTURE',
  0,
  0,
  'UNKNOWN',
  0.48,
  0.52,
  0.50,
  'fixture-1',
  '{}'
),
(
  'bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb',
  '0x1111111111111111111111111111111111111111111111111111111111111111',
  1001,
  now64(3),
  now64(3),
  'best_bid_ask',
  'FIXTURE',
  0,
  0,
  'UNKNOWN',
  0.53,
  0.57,
  0.55,
  'fixture-2',
  '{}'
);

INSERT INTO polymarket.trades VALUES
(
  'cccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccc',
  '0x1111111111111111111111111111111111111111111111111111111111111111',
  1001,
  toDateTime64('2026-07-29 00:00:00', 3, 'UTC'),
  now64(3),
  '0x1111111111111111111111111111111111111111',
  'BUY',
  0.55,
  10,
  'Yes',
  '0x2222222222222222222222222222222222222222222222222222222222222222',
  'Will the fixture move?'
),
(
  'cccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccc',
  '0x1111111111111111111111111111111111111111111111111111111111111111',
  1001,
  toDateTime64('2026-07-29 00:00:00', 3, 'UTC'),
  now64(3) + INTERVAL 1 SECOND,
  '0x1111111111111111111111111111111111111111',
  'BUY',
  0.56,
  10,
  'Yes',
  '0x2222222222222222222222222222222222222222222222222222222222222222',
  'Will the fixture move?'
);
SQL

for _ in $(seq 1 30); do
  rows=$(docker exec "${NAME}" clickhouse client --password workshop --query \
    "SELECT count() FROM polymarket.market_midpoints_1m")
  [[ "${rows}" -gt 0 ]] && break
  sleep 1
done

docker exec "${NAME}" clickhouse client --password workshop --query \
  "SELECT throwIf(count() = 0, 'midpoint MV is empty') FROM polymarket.market_midpoints_1m"
docker exec "${NAME}" clickhouse client --password workshop --query \
  "SELECT throwIf(count() != 1, 'trade dedupe view failed') FROM polymarket.trades_clean"
docker exec "${NAME}" clickhouse client --password workshop --query \
  "SELECT throwIf(any(price) != 0.56, 'trade dedupe kept the older row') FROM polymarket.trades_clean"
docker exec "${NAME}" clickhouse client --password workshop --multiquery < "${ROOT}/db/queries.sql" >/dev/null

HOST_PORT=$(docker port "${NAME}" 8123/tcp | sed 's/.*://')
(
  cd "${ROOT}/collector"
  env \
    POLYMARKET_CLICKHOUSE_INTEGRATION=1 \
    CLICKHOUSE_HOST=127.0.0.1 \
    CLICKHOUSE_PORT="${HOST_PORT}" \
    CLICKHOUSE_PASSWORD=workshop \
    "${PYTHON_BIN:-python3}" -m pytest tests/test_storage_integration.py -q
)

echo "Polymarket ClickHouse schema and reference queries passed"
