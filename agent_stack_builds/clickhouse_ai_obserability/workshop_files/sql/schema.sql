-- OpenTelemetry v2 Schema for ClickHouse
-- This schema matches the otel_v2 tables available on sql.clickhouse.com
-- Optimized for observability analytics and AI-powered querying

-- ============================================================================
-- 1. TRACES TABLE
-- ============================================================================
-- Stores distributed trace spans following OpenTelemetry specification
-- Each span represents a unit of work or operation in a distributed system

CREATE TABLE IF NOT EXISTS otel_traces
(
    -- Trace Identifiers
    TraceId String CODEC(ZSTD(1)),                    -- Unique trace identifier (16 bytes, hex encoded)
    SpanId String CODEC(ZSTD(1)),                     -- Unique span identifier (8 bytes, hex encoded)
    ParentSpanId String CODEC(ZSTD(1)),              -- Parent span ID for building trace trees

    -- Span Metadata
    TraceState String CODEC(ZSTD(1)),                -- W3C TraceState for vendor-specific data
    SpanName LowCardinality(String),                  -- Operation name (e.g., "GET /api/users")
    SpanKind LowCardinality(String),                  -- SERVER, CLIENT, PRODUCER, CONSUMER, INTERNAL
    ServiceName LowCardinality(String),               -- Service generating this span

    -- Resource Attributes
    ResourceAttributes Map(LowCardinality(String), String) CODEC(ZSTD(1)),

    -- Span Attributes (business logic metadata)
    SpanAttributes Map(LowCardinality(String), String) CODEC(ZSTD(1)),

    -- Timing Information
    Duration Int64,                                   -- Span duration in nanoseconds
    Timestamp DateTime64(9) CODEC(Delta, ZSTD(1)),   -- Span start time (nanosecond precision)

    -- Status
    StatusCode LowCardinality(String),               -- OK, ERROR, UNSET
    StatusMessage String CODEC(ZSTD(1)),             -- Error message if status is ERROR

    -- Events (structured logs within span)
    Events Nested(
        Timestamp DateTime64(9),
        Name LowCardinality(String),
        Attributes Map(LowCardinality(String), String)
    ) CODEC(ZSTD(1)),

    -- Links (references to other spans)
    Links Nested(
        TraceId String,
        SpanId String,
        TraceState String,
        Attributes Map(LowCardinality(String), String)
    ) CODEC(ZSTD(1)),

    -- Indexing
    INDEX idx_trace_id TraceId TYPE bloom_filter(0.001) GRANULARITY 1,
    INDEX idx_duration Duration TYPE minmax GRANULARITY 1,
    INDEX idx_service_name ServiceName TYPE set(100) GRANULARITY 1
)
ENGINE = MergeTree
PARTITION BY toDate(Timestamp)
ORDER BY (ServiceName, SpanName, Timestamp)
TTL toDateTime(Timestamp) + INTERVAL 30 DAY
SETTINGS index_granularity = 8192;

-- ============================================================================
-- 2. METRICS TABLE
-- ============================================================================
-- Stores time-series metrics data (gauges, counters, histograms)

CREATE TABLE IF NOT EXISTS otel_metrics
(
    -- Metric Identifiers
    MetricName LowCardinality(String),               -- Metric name (e.g., "http.server.duration")

    -- Resource Context
    ServiceName LowCardinality(String),              -- Service emitting the metric
    ResourceAttributes Map(LowCardinality(String), String) CODEC(ZSTD(1)),

    -- Metric Attributes (dimensions)
    Attributes Map(LowCardinality(String), String) CODEC(ZSTD(1)),

    -- Timing
    Timestamp DateTime64(9) CODEC(Delta, ZSTD(1)),   -- Metric timestamp

    -- Metric Values (only one field populated based on type)
    Value Float64,                                    -- For Gauge and Sum metrics

    -- Histogram data
    HistogramCount Float64,                           -- Number of observations
    HistogramSum Float64,                             -- Sum of all observations
    HistogramBucketCounts Array(Float64) CODEC(ZSTD(1)),  -- Counts per bucket
    HistogramExplicitBounds Array(Float64) CODEC(ZSTD(1)), -- Bucket boundaries

    -- Summary data (for percentiles)
    SummaryCount Float64,
    SummarySum Float64,
    SummaryQuantileValues Array(Float64) CODEC(ZSTD(1)),
    SummaryQuantiles Array(Float64) CODEC(ZSTD(1)),

    -- Metadata
    MetricType LowCardinality(String),               -- GAUGE, SUM, HISTOGRAM, SUMMARY
    AggregationTemporality LowCardinality(String),   -- DELTA, CUMULATIVE
    IsMonotonic UInt8,                                -- For SUM metrics

    -- Indexing
    INDEX idx_metric_name MetricName TYPE set(1000) GRANULARITY 1,
    INDEX idx_service_name ServiceName TYPE set(100) GRANULARITY 1
)
ENGINE = MergeTree
PARTITION BY toDate(Timestamp)
ORDER BY (ServiceName, MetricName, Timestamp)
TTL toDateTime(Timestamp) + INTERVAL 90 DAY
SETTINGS index_granularity = 8192;

-- ============================================================================
-- 3. LOGS TABLE
-- ============================================================================
-- Stores structured application logs

CREATE TABLE IF NOT EXISTS otel_logs
(
    -- Log Record Identifiers
    TraceId String CODEC(ZSTD(1)),                   -- Associated trace ID (if any)
    SpanId String CODEC(ZSTD(1)),                    -- Associated span ID (if any)

    -- Timing
    Timestamp DateTime64(9) CODEC(Delta, ZSTD(1)),   -- Log timestamp
    ObservedTimestamp DateTime64(9) CODEC(Delta, ZSTD(1)), -- When log was observed

    -- Log Content
    Body String CODEC(ZSTD(1)),                      -- Log message body
    SeverityText LowCardinality(String),             -- DEBUG, INFO, WARN, ERROR, FATAL
    SeverityNumber Int32,                             -- Numeric severity (1-24)

    -- Resource Context
    ServiceName LowCardinality(String),
    ResourceAttributes Map(LowCardinality(String), String) CODEC(ZSTD(1)),

    -- Log Attributes
    LogAttributes Map(LowCardinality(String), String) CODEC(ZSTD(1)),

    -- Indexing
    INDEX idx_trace_id TraceId TYPE bloom_filter(0.001) GRANULARITY 1,
    INDEX idx_severity SeverityText TYPE set(10) GRANULARITY 1,
    INDEX idx_service_name ServiceName TYPE set(100) GRANULARITY 1,
    INDEX idx_body Body TYPE tokenbf_v1(32768, 3, 0) GRANULARITY 1
)
ENGINE = MergeTree
PARTITION BY toDate(Timestamp)
ORDER BY (ServiceName, SeverityText, Timestamp)
TTL toDateTime(Timestamp) + INTERVAL 30 DAY
SETTINGS index_granularity = 8192;

-- ============================================================================
-- 4. SERVICES TABLE
-- ============================================================================
-- Service catalog with metadata and SLA definitions

CREATE TABLE IF NOT EXISTS otel_services
(
    -- Service Identity
    ServiceName LowCardinality(String),
    ServiceVersion String,
    ServiceNamespace LowCardinality(String),         -- e.g., "production", "staging"

    -- Service Metadata
    Description String,
    Owner String,
    Team String,
    Repository String,

    -- SLA Definitions
    SLA_P50_ms Int32,                                -- 50th percentile latency target
    SLA_P95_ms Int32,                                -- 95th percentile latency target
    SLA_P99_ms Int32,                                -- 99th percentile latency target
    SLA_ErrorRate Float32,                           -- Maximum acceptable error rate (0-1)
    SLA_Availability Float32,                        -- Target availability (0-1)

    -- Resource Information
    ResourceAttributes Map(LowCardinality(String), String) CODEC(ZSTD(1)),

    -- Timestamps
    FirstSeen DateTime DEFAULT now(),
    LastSeen DateTime DEFAULT now(),

    -- Status
    Status LowCardinality(String) DEFAULT 'active'  -- active, deprecated, sunset
)
ENGINE = ReplacingMergeTree(LastSeen)
ORDER BY (ServiceNamespace, ServiceName, ServiceVersion)
SETTINGS index_granularity = 8192;

-- ============================================================================
-- SAMPLE QUERIES FOR AI AGENTS
-- ============================================================================

-- Example 1: Find slow traces (p99 latency)
-- SELECT
--     ServiceName,
--     SpanName,
--     quantile(0.99)(Duration / 1000000) as p99_latency_ms
-- FROM otel_traces
-- WHERE Timestamp >= now() - INTERVAL 1 HOUR
-- GROUP BY ServiceName, SpanName
-- ORDER BY p99_latency_ms DESC
-- LIMIT 10;

-- Example 2: Error rate by service
-- SELECT
--     ServiceName,
--     countIf(StatusCode = 'ERROR') / count() as error_rate
-- FROM otel_traces
-- WHERE Timestamp >= now() - INTERVAL 1 HOUR
-- GROUP BY ServiceName
-- HAVING error_rate > 0
-- ORDER BY error_rate DESC;

-- Example 3: Service dependency graph
-- SELECT DISTINCT
--     parent.ServiceName as from_service,
--     child.ServiceName as to_service,
--     count() as call_count
-- FROM otel_traces as parent
-- INNER JOIN otel_traces as child ON parent.TraceId = child.TraceId AND parent.SpanId = child.ParentSpanId
-- WHERE parent.Timestamp >= now() - INTERVAL 1 HOUR
-- GROUP BY from_service, to_service
-- ORDER BY call_count DESC;

-- Example 4: Request throughput by service
-- SELECT
--     ServiceName,
--     toStartOfMinute(Timestamp) as minute,
--     count() as requests_per_minute
-- FROM otel_traces
-- WHERE Timestamp >= now() - INTERVAL 1 HOUR
--   AND SpanKind = 'SERVER'
-- GROUP BY ServiceName, minute
-- ORDER BY ServiceName, minute;

-- Example 5: SLA compliance check
-- SELECT
--     t.ServiceName,
--     quantile(0.95)(t.Duration / 1000000) as p95_actual_ms,
--     s.SLA_P95_ms as p95_target_ms,
--     if(p95_actual_ms > s.SLA_P95_ms, 'VIOLATION', 'COMPLIANT') as status
-- FROM otel_traces t
-- LEFT JOIN otel_services s ON t.ServiceName = s.ServiceName
-- WHERE t.Timestamp >= now() - INTERVAL 1 HOUR
--   AND t.SpanKind = 'SERVER'
-- GROUP BY t.ServiceName, s.SLA_P95_ms;
