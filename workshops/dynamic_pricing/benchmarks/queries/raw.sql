-- Replace __DATABASE__ and __RAW__ with the verified immutable import tables.
-- Use the same store, SKU, and complete-hour cutoff as rollup.sql.
SELECT count() AS canonical_rows, sum(quantity) AS net_units,
       sum(receipts_minor) AS receipts_minor, sum(sold_cost_minor) AS sold_cost_minor
FROM __DATABASE__.__RAW__
WHERE store_id={store:Int32} AND sku={sku:Int32}
  AND event_time<{cutoff:DateTime64(3,'UTC')}
SETTINGS use_query_cache=0;
