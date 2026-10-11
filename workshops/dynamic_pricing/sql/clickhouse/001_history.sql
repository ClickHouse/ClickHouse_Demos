-- Derived ordering from bounded sandbox/store/SKU/time queries. Start without partitions.
-- Per schema-types-native-types and schema-pk-prioritize-filters.
CREATE TABLE __DATABASE__.__RAW__ (
  sandbox_id String,
  batch_id UUID,
  dataset_version LowCardinality(String),
  event_id UUID,
  original_event_id UUID,
  sku UInt32,
  store_id UInt16,
  channel LowCardinality(String),
  event_type LowCardinality(String),
  quantity Int32,
  offered_price_minor Int64,
  receipts_minor Int64,
  sold_cost_minor Int64,
  event_time DateTime64(3, 'UTC'),
  calibration Bool,
  currency LowCardinality(String)
) ENGINE = MergeTree
ORDER BY (sandbox_id, store_id, sku, event_time, event_id);
