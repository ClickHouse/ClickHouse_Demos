CREATE OR REPLACE VIEW __DATABASE__.retail_analysis_context AS
SELECT run_id,toStartOfMinute(max(logical_end)) AS cutoff,
 (SELECT min(model_version) FROM __DATABASE__.retail_model_parameters) AS model_version,
 dateDiff('second',max(acknowledged_at),now64(3)) AS delivery_age_seconds
FROM __DATABASE__.retail_batches_cdc FINAL
WHERE status='acknowledged' AND _peerdb_is_deleted=0
AND run_id IN (SELECT run_id FROM __DATABASE__.retail_runs_cdc FINAL WHERE _peerdb_is_deleted=0 ORDER BY created_at DESC LIMIT 1)
AND ((event_count=0 AND (run_id,batch_id) NOT IN (SELECT run_id,batch_id FROM __DATABASE__.retail_sales WHERE source='live')) OR (run_id,batch_id,event_count) IN (
 SELECT run_id,batch_id,count() FROM __DATABASE__.retail_sales
 WHERE source='live' GROUP BY run_id,batch_id
 HAVING count()=uniqExact(event_id)
)) GROUP BY run_id HAVING count()=max(sequence);
