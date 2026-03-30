-- Materialized Views for OpenTelemetry Data
-- Pre-aggregated views for common observability queries
-- These dramatically improve query performance for dashboards and alerting

-- ============================================================================
-- 1. SERVICE METRICS SUMMARY (1-minute aggregations)
-- ============================================================================
-- Real-time service health metrics aggregated per minute

CREATE MATERIALIZED VIEW IF NOT EXISTS otel_service_metrics_1m
ENGINE = SummingMergeTree()
PARTITION BY toDate(timestamp_minute)
ORDER BY (ServiceName, SpanName, timestamp_minute)
TTL toDateTime(timestamp_minute) + INTERVAL 7 DAY
POPULATE
AS SELECT
    ServiceName,
    SpanName,
    toStartOfMinute(Timestamp) as timestamp_minute,

    -- Request Counts
    count() as request_count,
    countIf(StatusCode = 'ERROR') as error_count,
    countIf(StatusCode = 'OK') as success_count,

    -- Latency Statistics (in milliseconds)
    avg(Duration) / 1000000 as avg_duration_ms,
    quantile(0.50)(Duration) / 1000000 as p50_duration_ms,
    quantile(0.95)(Duration) / 1000000 as p95_duration_ms,
    quantile(0.99)(Duration) / 1000000 as p99_duration_ms,
    min(Duration) / 1000000 as min_duration_ms,
    max(Duration) / 1000000 as max_duration_ms

FROM otel_traces
WHERE SpanKind = 'SERVER'  -- Only aggregate server spans (entry points)
GROUP BY ServiceName, SpanName, timestamp_minute;

-- Query Example: Get last hour of service metrics
-- SELECT
--     ServiceName,
--     sum(request_count) as total_requests,
--     sum(error_count) / sum(request_count) as error_rate,
--     avg(p95_duration_ms) as avg_p95_latency
-- FROM otel_service_metrics_1m
-- WHERE timestamp_minute >= now() - INTERVAL 1 HOUR
-- GROUP BY ServiceName
-- ORDER BY total_requests DESC;

-- ============================================================================
-- 2. ERROR LOGS SUMMARY
-- ============================================================================
-- Aggregated error logs with counts for quick error analysis

CREATE MATERIALIZED VIEW IF NOT EXISTS otel_error_logs_summary
ENGINE = AggregatingMergeTree()
PARTITION BY toDate(timestamp_hour)
ORDER BY (ServiceName, SeverityText, error_signature, timestamp_hour)
TTL toDateTime(timestamp_hour) + INTERVAL 7 DAY
POPULATE
AS SELECT
    ServiceName,
    SeverityText,
    toStartOfHour(Timestamp) as timestamp_hour,

    -- Error signature (simplified body for grouping)
    substring(Body, 1, 200) as error_signature,

    -- Aggregations
    count() as error_count,
    uniq(TraceId) as affected_traces,
    any(Body) as sample_message,
    min(Timestamp) as first_seen,
    max(Timestamp) as last_seen

FROM otel_logs
WHERE SeverityText IN ('ERROR', 'FATAL', 'CRITICAL')
GROUP BY ServiceName, SeverityText, timestamp_hour, error_signature;

-- Query Example: Find most common errors in last 24 hours
-- SELECT
--     ServiceName,
--     error_signature,
--     sum(error_count) as total_errors,
--     any(sample_message) as example
-- FROM otel_error_logs_summary
-- WHERE timestamp_hour >= now() - INTERVAL 24 HOUR
-- GROUP BY ServiceName, error_signature
-- ORDER BY total_errors DESC
-- LIMIT 20;

-- ============================================================================
-- 3. TRACE RELATIONSHIPS (Service Dependency Map)
-- ============================================================================
-- Pre-computed service-to-service call patterns

CREATE MATERIALIZED VIEW IF NOT EXISTS otel_service_dependencies
ENGINE = SummingMergeTree()
PARTITION BY toDate(timestamp_hour)
ORDER BY (parent_service, child_service, timestamp_hour)
TTL toDateTime(timestamp_hour) + INTERVAL 7 DAY
AS SELECT
    parent.ServiceName as parent_service,
    child.ServiceName as child_service,
    toStartOfHour(parent.Timestamp) as timestamp_hour,

    -- Call Statistics
    count() as call_count,
    countIf(child.StatusCode = 'ERROR') as error_count,

    -- Latency
    avg(child.Duration) / 1000000 as avg_duration_ms,
    quantile(0.95)(child.Duration) / 1000000 as p95_duration_ms

FROM otel_traces as parent
INNER JOIN otel_traces as child
    ON parent.TraceId = child.TraceId
    AND parent.SpanId = child.ParentSpanId
WHERE parent.ServiceName != child.ServiceName  -- Exclude self-calls
GROUP BY parent_service, child_service, timestamp_hour;

-- Query Example: Build service dependency graph for last hour
-- SELECT
--     parent_service,
--     child_service,
--     sum(call_count) as total_calls,
--     sum(error_count) / sum(call_count) as error_rate,
--     avg(p95_duration_ms) as avg_p95_latency
-- FROM otel_service_dependencies
-- WHERE timestamp_hour >= now() - INTERVAL 1 HOUR
-- GROUP BY parent_service, child_service
-- ORDER BY total_calls DESC;

-- ============================================================================
-- 4. HTTP ENDPOINT METRICS
-- ============================================================================
-- Performance metrics for HTTP endpoints (extracted from span attributes)

CREATE MATERIALIZED VIEW IF NOT EXISTS otel_http_endpoint_metrics
ENGINE = SummingMergeTree()
PARTITION BY toDate(timestamp_minute)
ORDER BY (ServiceName, http_method, http_route, timestamp_minute)
TTL toDateTime(timestamp_minute) + INTERVAL 7 DAY
AS SELECT
    ServiceName,
    toStartOfMinute(Timestamp) as timestamp_minute,

    -- HTTP-specific attributes
    SpanAttributes['http.method'] as http_method,
    SpanAttributes['http.route'] as http_route,
    SpanAttributes['http.status_code'] as http_status_code,

    -- Request Counts
    count() as request_count,
    countIf(toInt32OrNull(SpanAttributes['http.status_code']) >= 500) as error_5xx_count,
    countIf(toInt32OrNull(SpanAttributes['http.status_code']) >= 400 AND toInt32OrNull(SpanAttributes['http.status_code']) < 500) as error_4xx_count,

    -- Latency
    avg(Duration) / 1000000 as avg_duration_ms,
    quantile(0.95)(Duration) / 1000000 as p95_duration_ms,
    quantile(0.99)(Duration) / 1000000 as p99_duration_ms

FROM otel_traces
WHERE SpanKind = 'SERVER'
  AND SpanAttributes['http.method'] != ''
GROUP BY ServiceName, timestamp_minute, http_method, http_route, http_status_code;

-- Query Example: Find slowest HTTP endpoints
-- SELECT
--     ServiceName,
--     http_method,
--     http_route,
--     sum(request_count) as total_requests,
--     avg(p95_duration_ms) as avg_p95_latency
-- FROM otel_http_endpoint_metrics
-- WHERE timestamp_minute >= now() - INTERVAL 1 HOUR
-- GROUP BY ServiceName, http_method, http_route
-- ORDER BY avg_p95_latency DESC
-- LIMIT 20;

-- ============================================================================
-- 5. DATABASE QUERY PERFORMANCE
-- ============================================================================
-- Track database query performance (for services that log DB spans)

CREATE MATERIALIZED VIEW IF NOT EXISTS otel_database_metrics
ENGINE = SummingMergeTree()
PARTITION BY toDate(timestamp_minute)
ORDER BY (ServiceName, db_system, db_operation, timestamp_minute)
TTL toDateTime(timestamp_minute) + INTERVAL 7 DAY
AS SELECT
    ServiceName,
    toStartOfMinute(Timestamp) as timestamp_minute,

    -- Database attributes
    SpanAttributes['db.system'] as db_system,
    SpanAttributes['db.operation'] as db_operation,
    SpanAttributes['db.name'] as db_name,

    -- Query Statistics
    count() as query_count,
    countIf(StatusCode = 'ERROR') as error_count,

    -- Performance
    avg(Duration) / 1000000 as avg_duration_ms,
    quantile(0.95)(Duration) / 1000000 as p95_duration_ms,
    max(Duration) / 1000000 as max_duration_ms

FROM otel_traces
WHERE SpanKind = 'CLIENT'
  AND SpanAttributes['db.system'] != ''
GROUP BY ServiceName, timestamp_minute, db_system, db_operation, db_name;

-- Query Example: Find slow database queries
-- SELECT
--     ServiceName,
--     db_system,
--     db_operation,
--     sum(query_count) as total_queries,
--     avg(p95_duration_ms) as avg_p95_latency
-- FROM otel_database_metrics
-- WHERE timestamp_minute >= now() - INTERVAL 1 HOUR
-- GROUP BY ServiceName, db_system, db_operation
-- HAVING avg_p95_latency > 100  -- slower than 100ms
-- ORDER BY avg_p95_latency DESC;

-- ============================================================================
-- 6. HOURLY SERVICE HEALTH REPORT
-- ============================================================================
-- Comprehensive hourly rollup for historical analysis

CREATE MATERIALIZED VIEW IF NOT EXISTS otel_service_health_hourly
ENGINE = AggregatingMergeTree()
PARTITION BY toDate(timestamp_hour)
ORDER BY (ServiceName, timestamp_hour)
TTL toDateTime(timestamp_hour) + INTERVAL 90 DAY
POPULATE
AS SELECT
    ServiceName,
    toStartOfHour(Timestamp) as timestamp_hour,

    -- Volume
    count() as total_requests,
    uniq(TraceId) as unique_traces,

    -- Errors
    countIf(StatusCode = 'ERROR') as error_count,
    countIf(StatusCode = 'ERROR') / count() as error_rate,

    -- Latency Percentiles (milliseconds)
    quantile(0.50)(Duration) / 1000000 as p50_duration_ms,
    quantile(0.75)(Duration) / 1000000 as p75_duration_ms,
    quantile(0.90)(Duration) / 1000000 as p90_duration_ms,
    quantile(0.95)(Duration) / 1000000 as p95_duration_ms,
    quantile(0.99)(Duration) / 1000000 as p99_duration_ms,
    quantile(0.999)(Duration) / 1000000 as p999_duration_ms,

    -- Duration Stats
    avg(Duration) / 1000000 as avg_duration_ms,
    min(Duration) / 1000000 as min_duration_ms,
    max(Duration) / 1000000 as max_duration_ms

FROM otel_traces
WHERE SpanKind = 'SERVER'
GROUP BY ServiceName, timestamp_hour;

-- Query Example: Weekly trend analysis
-- SELECT
--     ServiceName,
--     timestamp_hour,
--     total_requests,
--     error_rate,
--     p95_duration_ms
-- FROM otel_service_health_hourly
-- WHERE timestamp_hour >= now() - INTERVAL 7 DAY
-- ORDER BY ServiceName, timestamp_hour;

-- ============================================================================
-- UTILITY VIEWS
-- ============================================================================

-- Current Active Services (services seen in last 5 minutes)
CREATE MATERIALIZED VIEW IF NOT EXISTS otel_active_services
ENGINE = ReplacingMergeTree(last_seen)
ORDER BY ServiceName
POPULATE
AS SELECT
    ServiceName,
    max(Timestamp) as last_seen,
    count() as recent_requests
FROM otel_traces
WHERE Timestamp >= now() - INTERVAL 5 MINUTE
GROUP BY ServiceName;

-- Query Example: List currently active services
-- SELECT * FROM otel_active_services
-- WHERE last_seen >= now() - INTERVAL 5 MINUTE
-- ORDER BY recent_requests DESC;
