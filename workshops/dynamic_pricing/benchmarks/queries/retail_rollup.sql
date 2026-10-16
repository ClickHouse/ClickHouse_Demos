SELECT district_id,sku,sumMerge(units_state) AS units,sumMerge(receipts_state) AS receipts_minor,
 uniqExactMerge(transactions_state) AS transactions
FROM __DATABASE__.retail_district_minute
WHERE district_id={district:UInt16} AND minute<{cutoff:DateTime64(3,'UTC')}
 AND branch='applied' AND (source='history' OR (source='live' AND run_id={run:UUID}))
GROUP BY district_id,sku ORDER BY district_id,sku
