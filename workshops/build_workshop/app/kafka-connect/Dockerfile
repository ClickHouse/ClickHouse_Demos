FROM confluentinc/cp-kafka-connect:8.0.0

# Install connectors (Debezium Postgres source + ClickHouse sink)
# References:
# - Confluent "KRaft combined mode" compose example (Kafka + Connect runtime): https://raw.githubusercontent.com/confluentinc/cp-all-in-one/8.0.0-post/cp-all-in-one-community/docker-compose.yml
# - Debezium Postgres connector docs: https://debezium.io/documentation/reference/stable/connectors/postgresql.html
# - ClickHouse Kafka Connect sink docs: https://clickhouse.com/docs/integrations/kafka/clickhouse-kafka-connect-sink
#
# NOTE: Connector coordinates can change over time. If build fails, update the versions below
# to match the latest Confluent Hub entries for:
# - debezium/debezium-connector-postgresql
# - clickhouse/clickhouse-kafka-connect

ARG DEBEZIUM_PG_CONNECTOR=debezium/debezium-connector-postgresql:3.1.2
ARG CLICKHOUSE_SINK_CONNECTOR=clickhouse/clickhouse-kafka-connect:v1.3.5

USER root
RUN confluent-hub install --no-prompt ${DEBEZIUM_PG_CONNECTOR} \
  && confluent-hub install --no-prompt ${CLICKHOUSE_SINK_CONNECTOR}
USER appuser

FROM confluentinc/cp-kafka-connect:8.0.0

# Install connectors (versions pinned for reproducibility)
# - Debezium PostgreSQL source connector
# - ClickHouse Kafka Connect sink connector
RUN confluent-hub install --no-prompt debezium/debezium-connector-postgresql:3.1.2 && \
    confluent-hub install --no-prompt clickhouse/clickhouse-kafka-connect:v1.3.5

