-- ============================================================
-- Part 3: ClickHouse Schema for Elasticsearch Migration Lab
-- ============================================================
-- All DDL is idempotent (CREATE TABLE/DICTIONARY/VIEW IF NOT EXISTS).
-- Run this file in order from top to bottom.
--
-- Prerequisite: dictionaries.sql must be run BEFORE this file.
-- The geoip_country and geoip_city dictionaries must exist because
-- otel_logs_v2 has MATERIALIZED columns that reference them.
--
-- The text index type requires enable_full_text_index = 1 on CH < 26.2.
-- On CH 26.2+ the setting is not needed (text index is GA).
SET enable_full_text_index = 1;

CREATE DATABASE IF NOT EXISTS otel;
USE otel;
--
-- Ingest architecture:
--   OTel Collector
--       └──► otel_logs (Null engine — ingestion target, stores nothing)
--                 └──► otel_logs_mv (Materialized View)
--                           └──► otel_logs_v2 (MergeTree — where data lives)
--
-- Each materialized column below is annotated with the ES ingest
-- pipeline processor it replaces.
-- ============================================================


-- ============================================================
-- 1. INGESTION TARGET: Null table
-- ============================================================
-- The Null engine accepts inserts but discards the data immediately.
-- The attached materialized view (otel_logs_mv) fires on every insert
-- and writes transformed rows to otel_logs_v2.
-- This avoids storing raw data twice (once in a staging table and
-- once in the optimized target table).
-- ============================================================
CREATE TABLE IF NOT EXISTS otel_logs
(
    `Timestamp`           DateTime64(9) CODEC(Delta(8), ZSTD(1)),
    `TraceId`             String CODEC(ZSTD(1)),
    `SpanId`              String CODEC(ZSTD(1)),
    `TraceFlags`          UInt32 CODEC(ZSTD(1)),
    `SeverityText`        LowCardinality(String) CODEC(ZSTD(1)),
    `SeverityNumber`      Int32 CODEC(ZSTD(1)),
    `ServiceName`         LowCardinality(String) CODEC(ZSTD(1)),
    `Body`                String CODEC(ZSTD(1)),
    `ResourceSchemaUrl`   String CODEC(ZSTD(1)),
    `ResourceAttributes`  Map(LowCardinality(String), String) CODEC(ZSTD(1)),
    `ScopeSchemaUrl`      String CODEC(ZSTD(1)),
    `ScopeName`           String CODEC(ZSTD(1)),
    `ScopeVersion`        String CODEC(ZSTD(1)),
    `ScopeAttributes`     Map(LowCardinality(String), String) CODEC(ZSTD(1)),
    `LogAttributes`       Map(LowCardinality(String), String) CODEC(ZSTD(1))
)
ENGINE = Null;


-- ============================================================
-- 2. TARGET TABLE: Optimized MergeTree with materialized columns
-- ============================================================
-- Schema decisions explained:
--   ORDER BY (ServiceName, SeverityText, TimestampTime)
--     → Primary query: "errors for payment-service in last hour"
--       Filter order: ServiceName (equality) → SeverityText (equality) → time (range)
--       Range column last = maximum granule skipping
--   PARTITION BY TimestampDate
--     → Daily partitions let TTL drop entire partitions cheaply (no row-by-row scan)
--   TTL TimestampDate + INTERVAL 30 DAY DELETE
--     → Directly replaces the ES ILM delete phase; ttl_only_drop_parts=1 drops
--       whole partitions instead of individual rows
--
-- Materialized columns replace every ES ingest pipeline processor:
--   StatusCode, DerivedSeverity  → "script" processor (severity from HTTP status)
--   GeoCountry, GeoCity          → "geoip" processor (IP → country/city via dictionary)
--   BrowserFamily, OSFamily, IsBot → "user_agent" processor (UA string parsing)
--   RequestType, RequestPath, etc. → "grok"/"dissect" (field extraction)
-- ============================================================
CREATE TABLE IF NOT EXISTS otel_logs_v2
(
    `Timestamp`       DateTime64(9) CODEC(Delta(8), ZSTD(1)),
    `TimestampDate`   Date          DEFAULT toDate(Timestamp),
    `TimestampTime`   DateTime      DEFAULT toDateTime(Timestamp),
    -- IngestTime replaces ES default-enrichment pipeline (event.ingested)
    `IngestTime`      DateTime      DEFAULT now(),

    `TraceId`         String CODEC(ZSTD(1)),
    `SpanId`          String CODEC(ZSTD(1)),
    `SeverityText`    LowCardinality(String) CODEC(ZSTD(1)),
    `SeverityNumber`  Int32 CODEC(ZSTD(1)),
    `ServiceName`     LowCardinality(String) CODEC(ZSTD(1)),
    `Body`            String CODEC(ZSTD(1)),
    `ResourceAttributes` Map(LowCardinality(String), String) CODEC(ZSTD(1)),
    `LogAttributes`   Map(LowCardinality(String), String) CODEC(ZSTD(1)),

    -- ── Replaces ES "script" processor (severity derivation from HTTP status) ──
    `StatusCode`      UInt16 MATERIALIZED toUInt16OrZero(LogAttributes['status']),
    `DerivedSeverity` LowCardinality(String) MATERIALIZED multiIf(
        toUInt16OrZero(LogAttributes['status']) >= 500, 'critical',
        toUInt16OrZero(LogAttributes['status']) >= 400, 'error',
        toUInt16OrZero(LogAttributes['status']) >= 300, 'warning',
        lower(LogAttributes['level']) != '', lower(LogAttributes['level']),
        'info'
    ),

    -- ── Replaces ES "geoip" processor ──
    -- GeoCountry / GeoCity use the geoip_country / geoip_city ip_trie
    -- dictionaries defined in dictionaries.sql. The dictionary maps CIDR
    -- ranges to country/city via a longest-prefix match on the IPv4 address.
    `RemoteAddr`  String MATERIALIZED LogAttributes['remote_addr'],
    `GeoCountry`  String MATERIALIZED dictGetOrDefault('otel.geoip_country', 'country',
                      toIPv4OrDefault(LogAttributes['remote_addr']), ''),
    `GeoCity`     String MATERIALIZED dictGetOrDefault('otel.geoip_city', 'city',
                      toIPv4OrDefault(LogAttributes['remote_addr']), ''),

    -- ── Replaces ES "user_agent" processor ──
    `UserAgent`     String MATERIALIZED LogAttributes['user_agent'],
    `BrowserFamily` LowCardinality(String) MATERIALIZED
        regexpExtract(LogAttributes['user_agent'],
            '(Chrome|Firefox|Safari|Edge|Opera|MSIE|Trident)[/\\s]', 1),
    `OSFamily`      LowCardinality(String) MATERIALIZED multiIf(
        position(LogAttributes['user_agent'], 'Windows') > 0, 'Windows',
        position(LogAttributes['user_agent'], 'Mac OS')  > 0, 'macOS',
        position(LogAttributes['user_agent'], 'Linux')   > 0, 'Linux',
        position(LogAttributes['user_agent'], 'Android') > 0, 'Android',
        position(LogAttributes['user_agent'], 'iPhone')  > 0
            OR position(LogAttributes['user_agent'], 'iPad') > 0, 'iOS',
        'Other'
    ),
    -- 1 = bot, 0 = human; replaces ES "set" processor + painless bot-detection script
    `IsBot`  UInt8 MATERIALIZED multiIf(
        position(lower(LogAttributes['user_agent']), 'bot')     > 0, 1,
        position(lower(LogAttributes['user_agent']), 'crawler') > 0, 1,
        position(lower(LogAttributes['user_agent']), 'spider')  > 0, 1,
        0
    ),

    -- ── Replaces ES "grok"/"dissect" field extraction ──
    `RequestType` LowCardinality(String) MATERIALIZED LogAttributes['request_type'],
    `RequestPath` String MATERIALIZED LogAttributes['request_path'],
    -- path() strips query string: "/api/orders?id=1" → "/api/orders"
    `RequestPage` String MATERIALIZED path(LogAttributes['request_path']),
    `LogLevel`    LowCardinality(String) MATERIALIZED LogAttributes['level'],
    `HostName`    LowCardinality(String) MATERIALIZED ResourceAttributes['host.name'],

    -- ── Skip indices ──
    -- TraceId: bloom filter for point lookups (trace drill-down)
    INDEX idx_trace_id    TraceId                    TYPE bloom_filter(0.001) GRANULARITY 1,
    -- Body: full-text token search using the text index.
    -- sparseGrams tokenizer works on CH 25.12+ (enable_full_text_index = 1 required pre-26.2).
    -- On CH 26.2+, text index is GA and the tokenizer syntax becomes optional.
    INDEX idx_body        Body                       TYPE text(tokenizer='sparseGrams') GRANULARITY 8,
    -- LogAttributes keys/values: bloom filter for Map key/value lookups
    INDEX idx_log_attr_k  mapKeys(LogAttributes)     TYPE bloom_filter(0.01) GRANULARITY 1,
    INDEX idx_log_attr_v  mapValues(LogAttributes)   TYPE bloom_filter(0.01) GRANULARITY 1
)
ENGINE = MergeTree
PARTITION BY TimestampDate
ORDER BY (ServiceName, SeverityText, TimestampTime)
-- TTL replaces the entire ES ILM policy (hot→warm→cold→delete).
-- ClickHouse Cloud stores everything on object storage with automatic caching;
-- hot/warm/cold node tiers are irrelevant. Only deletion is needed.
TTL TimestampDate + INTERVAL 30 DAY DELETE
SETTINGS
    ttl_only_drop_parts = 1,  -- drop whole partitions, not row-by-row
    index_granularity   = 8192;


-- ============================================================
-- 3. MATERIALIZED VIEW: Route Null → otel_logs_v2
-- ============================================================
-- This MV fires on every INSERT into otel_logs (the Null table).
-- The SELECT aliases must match column names in otel_logs_v2 exactly —
-- a MV is essentially INSERT INTO otel_logs_v2 SELECT ... FROM otel_logs.
--
-- The SeverityText override: the OTel Collector may leave SeverityText
-- empty for web access logs (no log-level field in HTTP logs). The MV
-- derives severity from the HTTP status code for those rows.
-- ============================================================
CREATE MATERIALIZED VIEW IF NOT EXISTS otel_logs_mv TO otel_logs_v2
AS SELECT
    Timestamp,
    TraceId,
    SpanId,
    multiIf(
        SeverityText != '',                                   SeverityText,
        toUInt16OrZero(LogAttributes['status']) >= 500,       'CRITICAL',
        toUInt16OrZero(LogAttributes['status']) >= 400,       'ERROR',
        toUInt16OrZero(LogAttributes['status']) >= 300,       'WARNING',
        'INFO'
    ) AS SeverityText,
    SeverityNumber,
    ServiceName,
    Body,
    ResourceAttributes,
    LogAttributes
FROM otel_logs;


-- ============================================================
-- 4. TRACES TABLE: Standard OTel trace schema
-- ============================================================
CREATE TABLE IF NOT EXISTS otel_traces
(
    `Timestamp`       DateTime64(9) CODEC(Delta(8), ZSTD(1)),
    `TraceId`         String CODEC(ZSTD(1)),
    `SpanId`          String CODEC(ZSTD(1)),
    `ParentSpanId`    String CODEC(ZSTD(1)),
    `TraceState`      String CODEC(ZSTD(1)),
    `SpanName`        LowCardinality(String) CODEC(ZSTD(1)),
    `SpanKind`        LowCardinality(String) CODEC(ZSTD(1)),
    `ServiceName`     LowCardinality(String) CODEC(ZSTD(1)),
    `ResourceAttributes` Map(LowCardinality(String), String) CODEC(ZSTD(1)),
    `ScopeName`       String CODEC(ZSTD(1)),
    `ScopeVersion`    String CODEC(ZSTD(1)),
    `SpanAttributes`  Map(LowCardinality(String), String) CODEC(ZSTD(1)),
    `Duration`        UInt64 CODEC(ZSTD(1)),
    `StatusCode`      LowCardinality(String) CODEC(ZSTD(1)),
    `StatusMessage`   String CODEC(ZSTD(1)),
    `Events.Timestamp`  Array(DateTime64(9)) CODEC(ZSTD(1)),
    `Events.Name`       Array(LowCardinality(String)) CODEC(ZSTD(1)),
    `Events.Attributes` Array(Map(LowCardinality(String), String)) CODEC(ZSTD(1)),
    `Links.TraceId`     Array(String) CODEC(ZSTD(1)),
    `Links.SpanId`      Array(String) CODEC(ZSTD(1)),
    `Links.TraceState`  Array(String) CODEC(ZSTD(1)),
    `Links.Attributes`  Array(Map(LowCardinality(String), String)) CODEC(ZSTD(1)),

    INDEX idx_trace_id      TraceId                       TYPE bloom_filter(0.001) GRANULARITY 1,
    INDEX idx_res_attr_k    mapKeys(ResourceAttributes)   TYPE bloom_filter(0.01)  GRANULARITY 1,
    INDEX idx_res_attr_v    mapValues(ResourceAttributes) TYPE bloom_filter(0.01)  GRANULARITY 1,
    INDEX idx_span_attr_k   mapKeys(SpanAttributes)       TYPE bloom_filter(0.01)  GRANULARITY 1,
    INDEX idx_span_attr_v   mapValues(SpanAttributes)     TYPE bloom_filter(0.01)  GRANULARITY 1,
    INDEX idx_duration      Duration                      TYPE minmax              GRANULARITY 1
)
ENGINE = MergeTree
PARTITION BY toDate(Timestamp)
ORDER BY (ServiceName, SpanName, toDateTime(Timestamp))
TTL toDate(Timestamp) + INTERVAL 30 DAY DELETE
SETTINGS ttl_only_drop_parts = 1;


-- ============================================================
-- 5. METRICS TABLES: Standard OTel metrics schema (5 tables)
-- ============================================================
-- The OTel Collector ClickHouse exporter writes to separate tables
-- per metric type. This replaces the APM server as the metrics backend.
--
-- Source: https://clickhouse.com/docs/use-cases/observability/clickstack/ingesting-data/schemas#metrics
--
-- All tables share the same ORDER BY and 30-day TTL as otel_traces.
-- ============================================================

CREATE TABLE IF NOT EXISTS otel_metrics_gauge
(
    `ResourceAttributes`            Map(LowCardinality(String), String) CODEC(ZSTD(1)),
    `ResourceSchemaUrl`             String CODEC(ZSTD(1)),
    `ScopeName`                     String CODEC(ZSTD(1)),
    `ScopeVersion`                  String CODEC(ZSTD(1)),
    `ScopeAttributes`               Map(LowCardinality(String), String) CODEC(ZSTD(1)),
    `ScopeDroppedAttrCount`         UInt32 CODEC(ZSTD(1)),
    `ScopeSchemaUrl`                String CODEC(ZSTD(1)),
    `ServiceName`                   LowCardinality(String) CODEC(ZSTD(1)),
    `MetricName`                    String CODEC(ZSTD(1)),
    `MetricDescription`             String CODEC(ZSTD(1)),
    `MetricUnit`                    String CODEC(ZSTD(1)),
    `Attributes`                    Map(LowCardinality(String), String) CODEC(ZSTD(1)),
    `StartTimeUnix`                 DateTime64(9) CODEC(Delta(8), ZSTD(1)),
    `TimeUnix`                      DateTime64(9) CODEC(Delta(8), ZSTD(1)),
    `Value`                         Float64 CODEC(ZSTD(1)),
    `Flags`                         UInt32 CODEC(ZSTD(1)),
    `Exemplars.FilteredAttributes`  Array(Map(LowCardinality(String), String)) CODEC(ZSTD(1)),
    `Exemplars.TimeUnix`            Array(DateTime64(9)) CODEC(ZSTD(1)),
    `Exemplars.Value`               Array(Float64) CODEC(ZSTD(1)),
    `Exemplars.SpanId`              Array(String) CODEC(ZSTD(1)),
    `Exemplars.TraceId`             Array(String) CODEC(ZSTD(1)),
    INDEX idx_res_attr_key   mapKeys(ResourceAttributes)   TYPE bloom_filter(0.01) GRANULARITY 1,
    INDEX idx_res_attr_value mapValues(ResourceAttributes) TYPE bloom_filter(0.01) GRANULARITY 1,
    INDEX idx_scope_attr_key mapKeys(ScopeAttributes)      TYPE bloom_filter(0.01) GRANULARITY 1,
    INDEX idx_scope_attr_val mapValues(ScopeAttributes)    TYPE bloom_filter(0.01) GRANULARITY 1,
    INDEX idx_attr_key       mapKeys(Attributes)           TYPE bloom_filter(0.01) GRANULARITY 1,
    INDEX idx_attr_value     mapValues(Attributes)         TYPE bloom_filter(0.01) GRANULARITY 1
)
ENGINE = MergeTree
PARTITION BY toDate(TimeUnix)
ORDER BY (ServiceName, MetricName, Attributes, toUnixTimestamp64Nano(TimeUnix))
TTL toDate(TimeUnix) + INTERVAL 30 DAY DELETE
SETTINGS ttl_only_drop_parts = 1;

CREATE TABLE IF NOT EXISTS otel_metrics_sum
(
    `ResourceAttributes`            Map(LowCardinality(String), String) CODEC(ZSTD(1)),
    `ResourceSchemaUrl`             String CODEC(ZSTD(1)),
    `ScopeName`                     String CODEC(ZSTD(1)),
    `ScopeVersion`                  String CODEC(ZSTD(1)),
    `ScopeAttributes`               Map(LowCardinality(String), String) CODEC(ZSTD(1)),
    `ScopeDroppedAttrCount`         UInt32 CODEC(ZSTD(1)),
    `ScopeSchemaUrl`                String CODEC(ZSTD(1)),
    `ServiceName`                   LowCardinality(String) CODEC(ZSTD(1)),
    `MetricName`                    String CODEC(ZSTD(1)),
    `MetricDescription`             String CODEC(ZSTD(1)),
    `MetricUnit`                    String CODEC(ZSTD(1)),
    `Attributes`                    Map(LowCardinality(String), String) CODEC(ZSTD(1)),
    `StartTimeUnix`                 DateTime64(9) CODEC(Delta(8), ZSTD(1)),
    `TimeUnix`                      DateTime64(9) CODEC(Delta(8), ZSTD(1)),
    `Value`                         Float64 CODEC(ZSTD(1)),
    `Flags`                         UInt32 CODEC(ZSTD(1)),
    `Exemplars.FilteredAttributes`  Array(Map(LowCardinality(String), String)) CODEC(ZSTD(1)),
    `Exemplars.TimeUnix`            Array(DateTime64(9)) CODEC(ZSTD(1)),
    `Exemplars.Value`               Array(Float64) CODEC(ZSTD(1)),
    `Exemplars.SpanId`              Array(String) CODEC(ZSTD(1)),
    `Exemplars.TraceId`             Array(String) CODEC(ZSTD(1)),
    `AggregationTemporality`        Int32 CODEC(ZSTD(1)),
    `IsMonotonic`                   Bool CODEC(Delta(1), ZSTD(1)),
    INDEX idx_res_attr_key   mapKeys(ResourceAttributes)   TYPE bloom_filter(0.01) GRANULARITY 1,
    INDEX idx_res_attr_value mapValues(ResourceAttributes) TYPE bloom_filter(0.01) GRANULARITY 1,
    INDEX idx_scope_attr_key mapKeys(ScopeAttributes)      TYPE bloom_filter(0.01) GRANULARITY 1,
    INDEX idx_scope_attr_val mapValues(ScopeAttributes)    TYPE bloom_filter(0.01) GRANULARITY 1,
    INDEX idx_attr_key       mapKeys(Attributes)           TYPE bloom_filter(0.01) GRANULARITY 1,
    INDEX idx_attr_value     mapValues(Attributes)         TYPE bloom_filter(0.01) GRANULARITY 1
)
ENGINE = MergeTree
PARTITION BY toDate(TimeUnix)
ORDER BY (ServiceName, MetricName, Attributes, toUnixTimestamp64Nano(TimeUnix))
TTL toDate(TimeUnix) + INTERVAL 30 DAY DELETE
SETTINGS ttl_only_drop_parts = 1;

CREATE TABLE IF NOT EXISTS otel_metrics_histogram
(
    `ResourceAttributes`            Map(LowCardinality(String), String) CODEC(ZSTD(1)),
    `ResourceSchemaUrl`             String CODEC(ZSTD(1)),
    `ScopeName`                     String CODEC(ZSTD(1)),
    `ScopeVersion`                  String CODEC(ZSTD(1)),
    `ScopeAttributes`               Map(LowCardinality(String), String) CODEC(ZSTD(1)),
    `ScopeDroppedAttrCount`         UInt32 CODEC(ZSTD(1)),
    `ScopeSchemaUrl`                String CODEC(ZSTD(1)),
    `ServiceName`                   LowCardinality(String) CODEC(ZSTD(1)),
    `MetricName`                    String CODEC(ZSTD(1)),
    `MetricDescription`             String CODEC(ZSTD(1)),
    `MetricUnit`                    String CODEC(ZSTD(1)),
    `Attributes`                    Map(LowCardinality(String), String) CODEC(ZSTD(1)),
    `StartTimeUnix`                 DateTime64(9) CODEC(Delta(8), ZSTD(1)),
    `TimeUnix`                      DateTime64(9) CODEC(Delta(8), ZSTD(1)),
    `Count`                         UInt64 CODEC(Delta(8), ZSTD(1)),
    `Sum`                           Float64 CODEC(ZSTD(1)),
    `BucketCounts`                  Array(UInt64) CODEC(ZSTD(1)),
    `ExplicitBounds`                Array(Float64) CODEC(ZSTD(1)),
    `Exemplars.FilteredAttributes`  Array(Map(LowCardinality(String), String)) CODEC(ZSTD(1)),
    `Exemplars.TimeUnix`            Array(DateTime64(9)) CODEC(ZSTD(1)),
    `Exemplars.Value`               Array(Float64) CODEC(ZSTD(1)),
    `Exemplars.SpanId`              Array(String) CODEC(ZSTD(1)),
    `Exemplars.TraceId`             Array(String) CODEC(ZSTD(1)),
    `Flags`                         UInt32 CODEC(ZSTD(1)),
    `Min`                           Float64 CODEC(ZSTD(1)),
    `Max`                           Float64 CODEC(ZSTD(1)),
    `AggregationTemporality`        Int32 CODEC(ZSTD(1)),
    INDEX idx_res_attr_key   mapKeys(ResourceAttributes)   TYPE bloom_filter(0.01) GRANULARITY 1,
    INDEX idx_res_attr_value mapValues(ResourceAttributes) TYPE bloom_filter(0.01) GRANULARITY 1,
    INDEX idx_scope_attr_key mapKeys(ScopeAttributes)      TYPE bloom_filter(0.01) GRANULARITY 1,
    INDEX idx_scope_attr_val mapValues(ScopeAttributes)    TYPE bloom_filter(0.01) GRANULARITY 1,
    INDEX idx_attr_key       mapKeys(Attributes)           TYPE bloom_filter(0.01) GRANULARITY 1,
    INDEX idx_attr_value     mapValues(Attributes)         TYPE bloom_filter(0.01) GRANULARITY 1
)
ENGINE = MergeTree
PARTITION BY toDate(TimeUnix)
ORDER BY (ServiceName, MetricName, Attributes, toUnixTimestamp64Nano(TimeUnix))
TTL toDate(TimeUnix) + INTERVAL 30 DAY DELETE
SETTINGS ttl_only_drop_parts = 1;

CREATE TABLE IF NOT EXISTS otel_metrics_exponentialhistogram
(
    `ResourceAttributes`            Map(LowCardinality(String), String) CODEC(ZSTD(1)),
    `ResourceSchemaUrl`             String CODEC(ZSTD(1)),
    `ScopeName`                     String CODEC(ZSTD(1)),
    `ScopeVersion`                  String CODEC(ZSTD(1)),
    `ScopeAttributes`               Map(LowCardinality(String), String) CODEC(ZSTD(1)),
    `ScopeDroppedAttrCount`         UInt32 CODEC(ZSTD(1)),
    `ScopeSchemaUrl`                String CODEC(ZSTD(1)),
    `ServiceName`                   LowCardinality(String) CODEC(ZSTD(1)),
    `MetricName`                    String CODEC(ZSTD(1)),
    `MetricDescription`             String CODEC(ZSTD(1)),
    `MetricUnit`                    String CODEC(ZSTD(1)),
    `Attributes`                    Map(LowCardinality(String), String) CODEC(ZSTD(1)),
    `StartTimeUnix`                 DateTime64(9) CODEC(Delta(8), ZSTD(1)),
    `TimeUnix`                      DateTime64(9) CODEC(Delta(8), ZSTD(1)),
    `Count`                         UInt64 CODEC(Delta(8), ZSTD(1)),
    `Sum`                           Float64 CODEC(ZSTD(1)),
    `Scale`                         Int32 CODEC(ZSTD(1)),
    `ZeroCount`                     UInt64 CODEC(ZSTD(1)),
    `PositiveOffset`                Int32 CODEC(ZSTD(1)),
    `PositiveBucketCounts`          Array(UInt64) CODEC(ZSTD(1)),
    `NegativeOffset`                Int32 CODEC(ZSTD(1)),
    `NegativeBucketCounts`          Array(UInt64) CODEC(ZSTD(1)),
    `Exemplars.FilteredAttributes`  Array(Map(LowCardinality(String), String)) CODEC(ZSTD(1)),
    `Exemplars.TimeUnix`            Array(DateTime64(9)) CODEC(ZSTD(1)),
    `Exemplars.Value`               Array(Float64) CODEC(ZSTD(1)),
    `Exemplars.SpanId`              Array(String) CODEC(ZSTD(1)),
    `Exemplars.TraceId`             Array(String) CODEC(ZSTD(1)),
    `Flags`                         UInt32 CODEC(ZSTD(1)),
    `Min`                           Float64 CODEC(ZSTD(1)),
    `Max`                           Float64 CODEC(ZSTD(1)),
    `AggregationTemporality`        Int32 CODEC(ZSTD(1)),
    INDEX idx_res_attr_key   mapKeys(ResourceAttributes)   TYPE bloom_filter(0.01) GRANULARITY 1,
    INDEX idx_res_attr_value mapValues(ResourceAttributes) TYPE bloom_filter(0.01) GRANULARITY 1,
    INDEX idx_scope_attr_key mapKeys(ScopeAttributes)      TYPE bloom_filter(0.01) GRANULARITY 1,
    INDEX idx_scope_attr_val mapValues(ScopeAttributes)    TYPE bloom_filter(0.01) GRANULARITY 1,
    INDEX idx_attr_key       mapKeys(Attributes)           TYPE bloom_filter(0.01) GRANULARITY 1,
    INDEX idx_attr_value     mapValues(Attributes)         TYPE bloom_filter(0.01) GRANULARITY 1
)
ENGINE = MergeTree
PARTITION BY toDate(TimeUnix)
ORDER BY (ServiceName, MetricName, Attributes, toUnixTimestamp64Nano(TimeUnix))
TTL toDate(TimeUnix) + INTERVAL 30 DAY DELETE
SETTINGS ttl_only_drop_parts = 1;

CREATE TABLE IF NOT EXISTS otel_metrics_summary
(
    `ResourceAttributes`              Map(LowCardinality(String), String) CODEC(ZSTD(1)),
    `ResourceSchemaUrl`               String CODEC(ZSTD(1)),
    `ScopeName`                       String CODEC(ZSTD(1)),
    `ScopeVersion`                    String CODEC(ZSTD(1)),
    `ScopeAttributes`                 Map(LowCardinality(String), String) CODEC(ZSTD(1)),
    `ScopeDroppedAttrCount`           UInt32 CODEC(ZSTD(1)),
    `ScopeSchemaUrl`                  String CODEC(ZSTD(1)),
    `ServiceName`                     LowCardinality(String) CODEC(ZSTD(1)),
    `MetricName`                      String CODEC(ZSTD(1)),
    `MetricDescription`               String CODEC(ZSTD(1)),
    `MetricUnit`                      String CODEC(ZSTD(1)),
    `Attributes`                      Map(LowCardinality(String), String) CODEC(ZSTD(1)),
    `StartTimeUnix`                   DateTime64(9) CODEC(Delta(8), ZSTD(1)),
    `TimeUnix`                        DateTime64(9) CODEC(Delta(8), ZSTD(1)),
    `Count`                           UInt64 CODEC(Delta(8), ZSTD(1)),
    `Sum`                             Float64 CODEC(ZSTD(1)),
    `ValueAtQuantiles.Quantile`       Array(Float64) CODEC(ZSTD(1)),
    `ValueAtQuantiles.Value`          Array(Float64) CODEC(ZSTD(1)),
    `Flags`                           UInt32 CODEC(ZSTD(1)),
    INDEX idx_res_attr_key   mapKeys(ResourceAttributes)   TYPE bloom_filter(0.01) GRANULARITY 1,
    INDEX idx_res_attr_value mapValues(ResourceAttributes) TYPE bloom_filter(0.01) GRANULARITY 1,
    INDEX idx_scope_attr_key mapKeys(ScopeAttributes)      TYPE bloom_filter(0.01) GRANULARITY 1,
    INDEX idx_scope_attr_val mapValues(ScopeAttributes)    TYPE bloom_filter(0.01) GRANULARITY 1,
    INDEX idx_attr_key       mapKeys(Attributes)           TYPE bloom_filter(0.01) GRANULARITY 1,
    INDEX idx_attr_value     mapValues(Attributes)         TYPE bloom_filter(0.01) GRANULARITY 1
)
ENGINE = MergeTree
PARTITION BY toDate(TimeUnix)
ORDER BY (ServiceName, MetricName, Attributes, toUnixTimestamp64Nano(TimeUnix))
TTL toDate(TimeUnix) + INTERVAL 30 DAY DELETE
SETTINGS ttl_only_drop_parts = 1;
