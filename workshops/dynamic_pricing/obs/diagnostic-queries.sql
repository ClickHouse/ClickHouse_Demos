-- Run with read-only credentials on the ClickStack database.
-- Supply the owner and generation parameters; use real wall-clock time here.
SELECT Timestamp, TraceId, SpanId, SpanName, Duration / 1000000 AS duration_ms,
       SpanAttributes['pricing.snapshot_id'] AS snapshot_id,
       SpanAttributes['pricing.decision_id'] AS decision_id,
       SpanAttributes['pricing.error_type'] AS error_type
FROM otel.otel_traces
WHERE Timestamp >= now() - INTERVAL 30 MINUTE
  AND SpanAttributes['pricing.owner_id'] = {owner:String}
  AND SpanAttributes['pricing.generation'] = {generation:String}
ORDER BY Timestamp DESC LIMIT 100;

SELECT SpanName, count() AS samples, quantile(0.5)(Duration) / 1000000 AS median_ms
FROM otel.otel_traces
WHERE Timestamp >= now() - INTERVAL 30 MINUTE
  AND SpanAttributes['pricing.owner_id'] = {owner:String}
GROUP BY SpanName ORDER BY samples DESC LIMIT 20;

-- Run this query on the pricing service. Application spans do not expose
-- ClickPipes internal spans or prove that the latest source values converged.
SELECT snapshot_id, captured_at,
       JSONExtractString(snapshot_json, 'source_versions', 'inventory') AS inventory_revision,
       JSONExtractFloat(snapshot_json, 'source_ages_seconds', 'inventory') AS inventory_age_seconds
FROM pricing.input_snapshots FINAL
WHERE sandbox_id = {sandbox:String} AND generation = {generation:UInt32}
ORDER BY captured_at DESC LIMIT 20;
