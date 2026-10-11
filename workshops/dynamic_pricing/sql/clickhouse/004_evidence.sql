-- Stable IDs make identical outbox retries logically idempotent under FINAL.
CREATE TABLE IF NOT EXISTS __DATABASE__.input_snapshots (
  snapshot_id UUID, sandbox_id String, generation UInt32, sku UInt32, store_id UInt16,
  scenario_cutoff DateTime64(3, 'UTC'), captured_at DateTime64(3, 'UTC'), stock_units UInt32,
  current_price_minor Int64, snapshot_json String, evidence_json String, version UInt64
) ENGINE = ReplacingMergeTree(version) ORDER BY (sandbox_id, generation, snapshot_id);
CREATE TABLE IF NOT EXISTS __DATABASE__.evidence_events (
  event_id UUID, sandbox_id String, generation UInt32, event_kind LowCardinality(String),
  occurred_at DateTime64(3, 'UTC'), payload String, version UInt64
) ENGINE = ReplacingMergeTree(version) ORDER BY (sandbox_id, generation, event_id);
CREATE OR REPLACE VIEW __DATABASE__.decision_events AS
SELECT * FROM __DATABASE__.evidence_events FINAL WHERE event_kind IN ('policy', 'approval', 'rejection');
CREATE OR REPLACE VIEW __DATABASE__.application_events AS
SELECT * FROM __DATABASE__.evidence_events FINAL WHERE event_kind IN ('application_attempt', 'receipt');
CREATE OR REPLACE VIEW __DATABASE__.quarantine_events AS
SELECT * FROM __DATABASE__.evidence_events FINAL WHERE event_kind = 'quarantine';
