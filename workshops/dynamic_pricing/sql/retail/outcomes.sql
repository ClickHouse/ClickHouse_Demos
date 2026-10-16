WITH receipt AS (
 SELECT data FROM __DATABASE__.retail_receipts_cdc FINAL
 WHERE run_id={run:UUID} AND receipt_id={receipt:UUID} AND _peerdb_is_deleted=0
),
parameters AS (
 SELECT JSONExtractInt(data,'inputs','stock_units') initial_stock,
 JSONExtractInt(data,'inputs','unit_cost_minor') unit_cost,
 JSONExtractInt(data,'inputs','selling_cost_minor') selling_cost,
 JSONExtractInt(data,'inputs','disposal_cost_minor') disposal_cost,
 JSONExtractInt(data,'inputs','current_price_minor') original_price,
 JSONExtractInt(data,'receipt','price_minor') applied_price,
 JSONExtractInt(data,'inputs','subject','store_id') store,
 JSONExtractInt(data,'inputs','subject','sku') product,
 parseDateTime64BestEffort(JSONExtractString(data,'inputs','expires_at')) expires_at,
 parseDateTime64BestEffort(JSONExtractString(data,'started_at')) started_at FROM receipt
),
clock AS (
 SELECT max(logical_end) through FROM __DATABASE__.retail_batches_cdc FINAL
 WHERE run_id={run:UUID} AND status='acknowledged' AND _peerdb_is_deleted=0
 AND ((event_count=0 AND batch_id NOT IN (SELECT batch_id FROM __DATABASE__.retail_sales WHERE run_id={run:UUID} AND source='live')) OR (batch_id,event_count) IN (SELECT batch_id,count() FROM __DATABASE__.retail_sales
 WHERE run_id={run:UUID} AND source='live' GROUP BY batch_id HAVING count()=uniqExact(event_id)))
),
observed AS (
 SELECT toStartOfMinute(event_time) minute,branch,sum(quantity) minute_units,sum(receipts_minor) minute_receipts
 FROM __DATABASE__.retail_sales WHERE district_id=intDiv((SELECT store FROM parameters)-1,250)+1
 AND store_id=(SELECT store FROM parameters) AND sku=(SELECT product FROM parameters)
 AND run_id={run:UUID} AND receipt_id={receipt:UUID} AND source='live'
 AND event_time>=(SELECT started_at FROM parameters) AND event_time<(SELECT through FROM clock)
 GROUP BY minute,branch
),
grid AS (
 SELECT p.*,c.through,arrayJoin(['applied','comparison']) branch,
 toStartOfMinute(started_at)+toIntervalMinute(arrayJoin(range(toUInt32(greatest(0,least(480,dateDiff('minute',started_at,least(expires_at,through)))))+1))) minute
 FROM parameters p CROSS JOIN clock c WHERE through>=started_at
),
cumulative AS (
 SELECT g.*,
 sum(o.minute_units) OVER (PARTITION BY g.branch ORDER BY g.minute ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) units,
 sum(o.minute_receipts) OVER (PARTITION BY g.branch ORDER BY g.minute ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) receipts_minor
 FROM grid g LEFT JOIN observed o ON g.minute=o.minute AND g.branch=o.branch
)
SELECT minute AS minute,branch AS branch,if(branch='applied',applied_price,original_price) price_minor,
 units,receipts_minor,greatest(0,initial_stock-units) remaining_stock,
 minute>=expires_at expired,if(expired,remaining_stock,0) waste_units,
 receipts_minor-units*selling_cost-initial_stock*unit_cost-waste_units*disposal_cost contribution_minor
FROM cumulative ORDER BY minute,branch LIMIT 1000
