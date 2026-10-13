-- Read all unmerged aggregate rows; GROUP BY/sum does not depend on merging.
SELECT sum(row_count) AS canonical_rows, sum(net_units) AS net_units,
       sum(receipts_minor) AS receipts_minor, sum(sold_cost_minor) AS sold_cost_minor
FROM __DATABASE__.__ROLLUP__
WHERE store_id={store:Int32} AND sku={sku:Int32}
  AND hour<{cutoff:DateTime64(3,'UTC')}
SETTINGS use_query_cache=0;
