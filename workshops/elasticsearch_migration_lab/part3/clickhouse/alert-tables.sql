-- ============================================================
-- Part 3: Alert Pre-computation Tables and Aggregation Summary
-- ============================================================
-- Two approaches to replace ES Kibana alerting:
--   Option A (alert-tables.sql): Pre-compute alert state via MVs.
--     An external poller checks the alert table rather than
--     re-aggregating raw data on every check. The MV shifts the
--     expensive aggregation to INSERT time.
--   Option B: HyperDX Alerts on saved searches / charts — see README Step 8.
--
-- Additionally: logs_summary_1min demonstrates AggregatingMergeTree
-- as a replacement for ES transforms (incremental rollups).
-- ============================================================

CREATE DATABASE IF NOT EXISTS otel;
USE otel;


-- ============================================================
-- A. Alert pre-computation: High error rate
-- ============================================================
-- Equivalent to Kibana alerting rule: "fire if 5xx rate > 5% over 5min"

CREATE TABLE IF NOT EXISTS alert_error_rate
(
    `minute`       DateTime,
    `error_count`  UInt64,
    `total_count`  UInt64,
    -- error_rate is a MATERIALIZED column so it's always correct without an extra computation
    `error_rate`   Float64 MATERIALIZED if(total_count > 0, error_count / total_count, 0)
)
ENGINE = MergeTree
PARTITION BY toDate(minute)
ORDER BY minute
TTL toDate(minute) + INTERVAL 7 DAY DELETE;

-- MV: continuously populates alert_error_rate from incoming logs
-- Only counts web access logs (those with a request_type attribute)
CREATE MATERIALIZED VIEW IF NOT EXISTS alert_error_rate_mv TO alert_error_rate
AS SELECT
    toStartOfMinute(Timestamp) AS minute,
    countIf(toUInt16OrZero(LogAttributes['status']) >= 500) AS error_count,
    count()                                                  AS total_count
FROM otel_logs
WHERE LogAttributes['request_type'] != ''
GROUP BY minute;

-- ── Alerting poll query ──────────────────────────────────────────────
-- Run this on a schedule (e.g. every 1 minute via cron or any external poller):
--
--   SELECT minute, error_rate
--   FROM alert_error_rate
--   WHERE minute >= now() - INTERVAL 5 MINUTE
--     AND error_rate > 0.05
--   ORDER BY minute DESC;
--
-- If this query returns any rows, the alert fires.
-- Because alert_error_rate is tiny (~1 row/minute), this poll is a
-- trivial scan — orders of magnitude cheaper than re-aggregating millions
-- of raw log rows on every check interval (which is what ES does by default).


-- ============================================================
-- B. 1-minute log summary (replaces ES transforms)
-- ============================================================
-- AggregatingMergeTree stores PARTIAL aggregation states (not final values).
-- New rows are merged incrementally without re-scanning historical data —
-- fundamentally different from ES transforms that re-aggregate periodically.
--
-- Query pattern: use -Merge combinators to read final values.

CREATE TABLE IF NOT EXISTS logs_summary_1min
(
    `minute`          DateTime,
    `ServiceName`     LowCardinality(String),
    `SeverityText`    LowCardinality(String),
    `count`           AggregateFunction(count),
    `avg_run_time`    AggregateFunction(avg,          Float64),
    `p99_run_time`    AggregateFunction(quantile(0.99), Float64),
    `uniq_remote_addr` AggregateFunction(uniq,        String)
)
ENGINE = AggregatingMergeTree
PARTITION BY toDate(minute)
ORDER BY (ServiceName, SeverityText, minute)
TTL toDate(minute) + INTERVAL 90 DAY DELETE;

-- MV: incrementally populates logs_summary_1min from the Null ingestion table
CREATE MATERIALIZED VIEW IF NOT EXISTS logs_summary_1min_mv TO logs_summary_1min
AS SELECT
    toStartOfMinute(Timestamp) AS minute,
    ServiceName,
    SeverityText,
    countState()                                                   AS count,
    avgState(toFloat64OrZero(LogAttributes['run_time']))           AS avg_run_time,
    quantileState(0.99)(toFloat64OrZero(LogAttributes['run_time'])) AS p99_run_time,
    uniqState(LogAttributes['remote_addr'])                        AS uniq_remote_addr
FROM otel_logs
GROUP BY minute, ServiceName, SeverityText;

-- ── Example query against the summary table ──────────────────────────
-- Note the -Merge combinator suffix on every aggregate function:
--
--   SELECT
--       minute,
--       ServiceName,
--       SeverityText,
--       countMerge(count)                       AS total_events,
--       avgMerge(avg_run_time)                  AS avg_run_time_ms,
--       quantileMerge(0.99)(p99_run_time)       AS p99_run_time_ms,
--       uniqMerge(uniq_remote_addr)             AS unique_ips
--   FROM logs_summary_1min
--   WHERE minute >= now() - INTERVAL 1 HOUR
--   GROUP BY minute, ServiceName, SeverityText
--   ORDER BY minute DESC;
