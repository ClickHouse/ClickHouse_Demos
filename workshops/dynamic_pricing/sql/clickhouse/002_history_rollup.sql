-- Only immutable, isolated history inserts feed this target. Never attach to CDC versions.
-- Read with GROUP BY/sum; background merging is not a correctness prerequisite.
CREATE TABLE __DATABASE__.__ROLLUP__ (
  sandbox_id String, store_id UInt16, sku UInt32, hour DateTime('UTC'),
  net_units Int64, receipts_minor Int64, sold_cost_minor Int64, row_count UInt64
) ENGINE = SummingMergeTree
ORDER BY (sandbox_id, store_id, sku, hour);
CREATE MATERIALIZED VIEW __DATABASE__.__MV__ TO __DATABASE__.__ROLLUP__ AS
SELECT sandbox_id, store_id, sku, toStartOfHour(event_time) AS hour,
       sum(quantity) AS net_units, sum(receipts_minor) AS receipts_minor,
       sum(sold_cost_minor) AS sold_cost_minor, count() AS row_count
FROM __DATABASE__.__RAW__
GROUP BY sandbox_id, store_id, sku, hour;
