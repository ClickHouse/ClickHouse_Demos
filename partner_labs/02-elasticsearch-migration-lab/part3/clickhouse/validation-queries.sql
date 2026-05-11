-- ============================================================
-- Part 3: Validation Queries
-- ============================================================
-- Run these after Step 4 (Parallel Run) to confirm the migration
-- is working correctly. These queries are also called by
-- scripts/validate_migration.sh and scripts/validate_enrichment.sh.
-- ============================================================

USE otel;


-- ── 1. Row counts per service ─────────────────────────────────────────
SELECT
    ServiceName,
    count()                                    AS total_rows,
    min(TimestampTime)                         AS oldest_record,
    max(TimestampTime)                         AS newest_record,
    formatReadableTimeDelta(
        dateDiff('second', min(TimestampTime), max(TimestampTime))
    )                                          AS time_span
FROM otel_logs_v2
GROUP BY ServiceName
ORDER BY total_rows DESC;


-- ── 2. Partition sizes (storage overview) ────────────────────────────
SELECT
    partition,
    sum(rows)                                  AS total_rows,
    formatReadableSize(sum(bytes_on_disk))     AS disk_size,
    min(min_time)                              AS oldest_data,
    max(max_time)                              AS newest_data
FROM system.parts
WHERE table = 'otel_logs_v2' AND active
GROUP BY partition
ORDER BY partition;


-- ── 3. Enrichment coverage: GeoIP ───────────────────────────────────
-- > 90% of web access rows should have a non-empty GeoCountry
SELECT
    count()                                    AS total_web_rows,
    countIf(GeoCountry != '')                  AS rows_with_geo,
    round(countIf(GeoCountry != '') /
          count() * 100, 2)                    AS geo_coverage_pct
FROM otel_logs_v2
WHERE RequestType != '';  -- web access logs only


-- ── 4. Enrichment coverage: Browser family ───────────────────────────
SELECT
    count()                                    AS total_web_rows,
    countIf(BrowserFamily != '')               AS rows_with_browser,
    round(countIf(BrowserFamily != '') /
          count() * 100, 2)                    AS browser_coverage_pct
FROM otel_logs_v2
WHERE RequestType != '';


-- ── 5. Enrichment spot-check: sample enriched rows ──────────────────
SELECT
    RemoteAddr,
    GeoCountry,
    GeoCity,
    BrowserFamily,
    OSFamily,
    IsBot,
    DerivedSeverity,
    StatusCode
FROM otel_logs_v2
WHERE RemoteAddr != ''
  AND GeoCountry != ''
LIMIT 10
FORMAT Pretty;


-- ── 6. Query parity: Top 10 request paths ───────────────────────────
-- Compare against Elasticsearch aggregation output.
-- Expected: top paths match within ~5% count difference.
SELECT
    RequestPage  AS path,
    count()      AS requests
FROM otel_logs_v2
WHERE RequestType != ''
GROUP BY path
ORDER BY requests DESC
LIMIT 10;


-- ── 7. Query parity: 5xx error rate by minute (last hour) ───────────
SELECT
    toStartOfMinute(TimestampTime)             AS minute,
    countIf(StatusCode >= 500)                 AS errors_5xx,
    count()                                    AS total,
    round(errors_5xx / total * 100, 2)         AS error_rate_pct
FROM otel_logs_v2
WHERE TimestampTime >= now() - INTERVAL 1 HOUR
  AND RequestType != ''
GROUP BY minute
ORDER BY minute;


-- ── 8. Trace count (from OTel APM) ──────────────────────────────────
SELECT
    ServiceName,
    count()                                    AS span_count,
    uniq(TraceId)                              AS unique_traces,
    avg(Duration)                              AS avg_duration_ns,
    quantile(0.95)(Duration)                   AS p95_duration_ns
FROM otel_traces
GROUP BY ServiceName
ORDER BY span_count DESC;


-- ── 9. TTL configuration check ───────────────────────────────────────
SELECT
    name,
    engine,
    partition_key,
    sorting_key,
    ttl_expression
FROM system.tables
WHERE name IN ('otel_logs_v2', 'otel_traces')
  AND database = currentDatabase();


-- ── 10. Skip index status ─────────────────────────────────────────────
SELECT
    table,
    name,
    type,
    granularity
FROM system.data_skipping_indices
WHERE table IN ('otel_logs_v2', 'otel_traces')
  AND database = currentDatabase()
ORDER BY table, name;


-- ── 11. Dictionary load status ────────────────────────────────────────
SELECT
    name,
    status,
    element_count,
    formatReadableSize(bytes_allocated)        AS memory_used,
    last_successful_update_time
FROM system.dictionaries
WHERE name IN ('geoip_country', 'geoip_city')
ORDER BY name;


-- ── 12. Alert table sanity check ─────────────────────────────────────
-- Verify the alert pre-computation MV is writing rows
SELECT
    minute,
    error_count,
    total_count,
    round(error_count / total_count * 100, 2)  AS error_rate_pct
FROM alert_error_rate
ORDER BY minute DESC
LIMIT 10;
