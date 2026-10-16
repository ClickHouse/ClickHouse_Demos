SELECT district_id,sku,sum(quantity) AS units,sum(receipts_minor) AS receipts_minor,
 uniqExact(transaction_id) AS transactions
FROM __DATABASE__.retail_sales
WHERE district_id={district:UInt16} AND event_time<{cutoff:DateTime64(3,'UTC')}
 AND branch='applied' AND (source='history' OR (source='live' AND run_id={run:UUID}))
GROUP BY district_id,sku ORDER BY district_id,sku
