#!/bin/sh
set -eu

CONNECT_URL="${CONNECT_URL:-http://connect:8083}"
PG_HOST="${PG_HOST:-postgres}"
PG_PORT="${PG_PORT:-5432}"
PG_DB="${PG_DB:-taxi}"
PG_USER="${PG_USER:-taxi}"
PG_PASSWORD="${PG_PASSWORD:-taxi}"

CH_HOST="${CH_HOST:-clickhouse}"
CH_PORT="${CH_PORT:-8123}"
CH_DB="${CH_DB:-nyc_tlc_data}"
CH_USER="${CH_USER:-default}"
CH_PASSWORD="${CH_PASSWORD:-}"

TOPIC_PREFIX="${TOPIC_PREFIX:-nyc}"
SOURCE_CONNECTOR_NAME="${SOURCE_CONNECTOR_NAME:-pg-source}"
SINK_CONNECTOR_NAME="${SINK_CONNECTOR_NAME:-ch-sink}"

TOPIC="${TOPIC_PREFIX}.public.realtime_trips"

echo "Waiting for Kafka Connect at ${CONNECT_URL}..."
i=0
until curl -fsS "${CONNECT_URL}/connectors" >/dev/null 2>&1; do
  i=$((i + 1))
  if [ "$i" -ge 60 ]; then
    echo "Kafka Connect did not become ready in time."
    exit 1
  fi
  sleep 2
done

delete_if_exists() {
  name="$1"
  if curl -fsS "${CONNECT_URL}/connectors/${name}" >/dev/null 2>&1; then
    echo "Deleting existing connector ${name}..."
    curl -fsS -X DELETE "${CONNECT_URL}/connectors/${name}" >/dev/null
    sleep 2
  fi
}

delete_if_exists "${SOURCE_CONNECTOR_NAME}"
delete_if_exists "${SINK_CONNECTOR_NAME}"

echo "Registering Debezium PostgreSQL source connector (${SOURCE_CONNECTOR_NAME})..."
curl -fsS -X PUT \
  -H 'Content-Type: application/json' \
  "${CONNECT_URL}/connectors/${SOURCE_CONNECTOR_NAME}/config" \
  -d "{
    \"connector.class\": \"io.debezium.connector.postgresql.PostgresConnector\",
    \"database.hostname\": \"${PG_HOST}\",
    \"database.port\": \"${PG_PORT}\",
    \"database.user\": \"${PG_USER}\",
    \"database.password\": \"${PG_PASSWORD}\",
    \"database.dbname\": \"${PG_DB}\",
    \"plugin.name\": \"pgoutput\",

    \"topic.prefix\": \"${TOPIC_PREFIX}\",
    \"schema.include.list\": \"public\",
    \"table.include.list\": \"public.realtime_trips\",

    \"slot.name\": \"dbz_slot\",
    \"publication.autocreate.mode\": \"filtered\",
    \"publication.name\": \"dbz_publication\",

    \"tombstones.on.delete\": \"false\",
    \"heartbeat.interval.ms\": \"10000\"
  }" >/dev/null

echo "Registering ClickHouse sink connector (${SINK_CONNECTOR_NAME})..."
curl -fsS -X PUT \
  -H 'Content-Type: application/json' \
  "${CONNECT_URL}/connectors/${SINK_CONNECTOR_NAME}/config" \
  -d "{
    \"connector.class\": \"com.clickhouse.kafka.connect.ClickHouseSinkConnector\",
    \"tasks.max\": \"1\",
    \"topics\": \"${TOPIC}\",

    \"hostname\": \"${CH_HOST}\",
    \"port\": \"${CH_PORT}\",
    \"database\": \"${CH_DB}\",
    \"username\": \"${CH_USER}\",
    \"password\": \"${CH_PASSWORD}\",

    \"topic2TableMap\": \"${TOPIC}=realtime_trips_cdc\",
    \"bypassSchemaValidation\": \"true\",

    \"transforms\": \"unwrap,cast\",
    \"transforms.unwrap.type\": \"io.debezium.transforms.ExtractNewRecordState\",
    \"transforms.unwrap.drop.tombstones\": \"true\",
    \"transforms.unwrap.delete.handling.mode\": \"drop\",

    \"transforms.cast.type\": \"org.apache.kafka.connect.transforms.Cast\$Value\",
    \"transforms.cast.spec\": \"passenger_count:int32,payment_type:int32,vendor_id:int32\",

    \"errors.tolerance\": \"all\",
    \"errors.log.enable\": \"true\",
    \"errors.log.include.messages\": \"true\"
  }" >/dev/null

echo "Connectors registered."

