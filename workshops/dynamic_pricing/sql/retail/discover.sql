WITH
raw_batches AS (
 SELECT batch_id,count() n,uniqExact(event_id) distinct_n FROM __DATABASE__.retail_sales
 WHERE run_id={run:UUID} AND source='live'
 AND event_time>=toDateTime((SELECT max(toDate(hour)) FROM __DATABASE__.retail_availability)+INTERVAL 1 DAY,'UTC') GROUP BY batch_id
),
completed AS (
 SELECT b.sequence,b.logical_end,b.acknowledged_at FROM __DATABASE__.retail_batches_cdc AS b FINAL
 LEFT JOIN raw_batches r ON b.batch_id=r.batch_id
 WHERE b.run_id={run:UUID} AND b.status='acknowledged' AND b._peerdb_is_deleted=0
 AND b.logical_end<={cutoff:DateTime64(3,'UTC')} AND coalesce(r.n,0)=b.event_count AND coalesce(r.distinct_n,0)=b.event_count
),
boundary AS (
 SELECT if(count()=max(sequence),max(sequence),toInt64(0)) seq,max(logical_end) through,
 max(acknowledged_at) acknowledged FROM completed
),
inventory AS (
 SELECT store_id,sku,max(version) inventory_version,
 argMax(tuple(stock_units,expires_at,effective_sequence,_peerdb_is_deleted),version) value
 FROM __DATABASE__.retail_inventory_history_cdc FINAL
 WHERE run_id={run:UUID} AND effective_sequence<=(SELECT seq FROM boundary)
 AND store_id BETWEEN ({district:UInt16}-1)*250+1 AND {district:UInt16}*250 GROUP BY store_id,sku
),
prices AS (
 SELECT store_id,sku,max(version) price_version,argMax(tuple(price_minor,_peerdb_is_deleted),version) value
 FROM __DATABASE__.retail_price_history_cdc FINAL
 WHERE run_id={run:UUID} AND effective_sequence<=(SELECT seq FROM boundary) GROUP BY store_id,sku
),
policies AS (
 SELECT store_id,sku,max(version) rule_version,
 argMax(tuple(permitted_prices_minor,unit_cost_minor,selling_cost_minor,disposal_cost_minor,_peerdb_is_deleted),version) value
 FROM __DATABASE__.retail_policy_history_cdc FINAL
 WHERE run_id={run:UUID} AND effective_sequence<=(SELECT seq FROM boundary) GROUP BY store_id,sku
),
exposure AS (
 SELECT toHour(hour) slot,sum(exposure_hours) hours FROM __DATABASE__.retail_availability
 WHERE open AND ample_stock AND hour<{cutoff:DateTime64(3,'UTC')}
 AND toDayOfWeek(hour)=toDayOfWeek({cutoff:DateTime64(3,'UTC')}) GROUP BY slot
),
history AS (
 SELECT s.store_id AS store_id,s.sku AS sku,toHour(s.minute) slot,sumMerge(s.sold_units_state) gross_units
 FROM __DATABASE__.retail_sales_minute s
 INNER JOIN __DATABASE__.retail_availability a ON toStartOfHour(s.minute)=a.hour
 WHERE s.district_id={district:UInt16} AND s.source='history' AND s.branch='applied'
 AND a.open AND a.ample_stock AND s.minute<{cutoff:DateTime64(3,'UTC')}
 AND toDayOfWeek(s.minute)=toDayOfWeek({cutoff:DateTime64(3,'UTC')}) GROUP BY s.store_id,s.sku,slot
),
models AS (
 SELECT * FROM __DATABASE__.retail_model_parameters
 WHERE model_version=(SELECT min(model_version) FROM __DATABASE__.retail_model_parameters)
),
subjects AS (
 SELECT i.store_id AS store_id,i.sku AS sku,i.inventory_version AS inventory_version,i.value.1 stock_units,i.value.2 expires_at,
 i.value.3 inventory_sequence,i.value.4 inventory_deleted,
 p.price_version AS price_version,p.value.1 current_price_minor,p.value.2 price_deleted,
 c.rule_version AS rule_version,c.value.1 permitted_prices_minor,c.value.2 unit_cost_minor,
 c.value.3 selling_cost_minor,c.value.4 disposal_cost_minor,c.value.5 policy_deleted,
 d.reference_price_minor AS reference_price_minor,m.model_version AS model_version,m.elasticity AS elasticity,m.min_price_ratio AS min_price_ratio,m.max_price_ratio AS max_price_ratio
 FROM inventory i LEFT JOIN prices p USING(store_id,sku) LEFT JOIN policies c USING(store_id,sku)
 LEFT JOIN __DATABASE__.retail_products d ON i.sku=d.sku
 LEFT JOIN __DATABASE__.retail_stores stores ON i.store_id=stores.store_id
 LEFT JOIN models m ON i.sku=m.sku AND stores.store_group=m.store_group
),
slots AS (
 SELECT s.*,arrayJoin(range(2,10)) slot,
 toDateTime(toDate({cutoff:DateTime64(3,'UTC')}),'UTC')+toIntervalHour(slot) slot_start
 FROM subjects s
),
horizons AS (
 SELECT s.store_id AS store_id,s.sku AS sku,
 sum(if(e.hours>0,h.gross_units/e.hours,0)*greatest(0,dateDiff('second',
 greatest(s.slot_start,{cutoff:DateTime64(3,'UTC')}),least(s.slot_start+INTERVAL 1 HOUR,s.expires_at)))/3600.) reference_remaining,
 sum(if(e.hours>0,h.gross_units/e.hours,0)*greatest(0,dateDiff('second',
 greatest(s.slot_start,{cutoff:DateTime64(3,'UTC')}-INTERVAL 15 MINUTE),
 least(s.slot_start+INTERVAL 1 HOUR,{cutoff:DateTime64(3,'UTC')})))/3600.) reference_recent,
 sum(e.hours) exposure_hours,
 countIf(coalesce(e.hours,0)<=0 AND (
   (s.slot_start<s.expires_at AND s.slot_start+INTERVAL 1 HOUR>{cutoff:DateTime64(3,'UTC')} AND s.expires_at>{cutoff:DateTime64(3,'UTC')})
   OR (s.slot_start<{cutoff:DateTime64(3,'UTC')} AND s.slot_start+INTERVAL 1 HOUR>{cutoff:DateTime64(3,'UTC')}-INTERVAL 15 MINUTE))) missing_required_slots
 FROM slots s LEFT JOIN history h ON s.store_id=h.store_id AND s.sku=h.sku AND s.slot=h.slot
 LEFT JOIN exposure e ON s.slot=e.slot GROUP BY s.store_id,s.sku
),
operational_sales AS (
 SELECT store_id,sku,max(sequence) last_sales_sequence,maxIf(price_version,quantity>0) last_price_version
 FROM __DATABASE__.retail_sales
 WHERE district_id={district:UInt16} AND run_id={run:UUID} AND source='live' AND branch='applied'
 AND event_time<{cutoff:DateTime64(3,'UTC')} AND sequence<=(SELECT seq FROM boundary)
 GROUP BY store_id,sku
),
recent AS (
 SELECT s.store_id AS store_id,s.sku AS sku,max(s.sequence) last_sales_sequence,
 sumIf(s.quantity/pow(toFloat64(s.price_minor)/greatest(p.reference_price_minor,1),m.elasticity),s.quantity>0) reference_units
 FROM __DATABASE__.retail_sales s LEFT JOIN __DATABASE__.retail_products p ON s.sku=p.sku
 LEFT JOIN __DATABASE__.retail_stores stores ON s.store_id=stores.store_id
 LEFT JOIN models m ON s.sku=m.sku AND stores.store_group=m.store_group
 WHERE s.district_id={district:UInt16} AND s.run_id={run:UUID} AND s.source='live' AND s.branch='applied'
 AND s.event_time>={cutoff:DateTime64(3,'UTC')}-INTERVAL 15 MINUTE AND s.event_time<{cutoff:DateTime64(3,'UTC')}
 AND s.sequence<=(SELECT seq FROM boundary) GROUP BY s.store_id,s.sku
),
stockouts AS (
 SELECT store_id,sku,countIf(stock_units=0 AND _peerdb_is_deleted=0) n
 FROM __DATABASE__.retail_inventory_history_cdc FINAL WHERE run_id={run:UUID}
 AND effective_sequence IN (SELECT sequence FROM completed WHERE logical_end>{cutoff:DateTime64(3,'UTC')}-INTERVAL 15 MINUTE)
 GROUP BY store_id,sku
),
scenario AS (
 SELECT model_version,arrayMap(t->t.2,values) demand_multipliers,arrayMap(t->t.3,values) scenario_weights
 FROM (SELECT model_version,arraySort(t->t.1,groupArray(tuple(scenario,multiplier,weight))) values
 FROM __DATABASE__.retail_demand_scenarios GROUP BY model_version)
)
SELECT s.store_id AS store_id,
 s.sku AS sku,
 s.inventory_version AS inventory_version,
 s.stock_units AS stock_units,
 s.expires_at AS expires_at,
 s.inventory_sequence AS inventory_sequence,
 s.price_version AS price_version,
 s.current_price_minor AS current_price_minor,
 s.rule_version AS rule_version,
 s.unit_cost_minor AS unit_cost_minor,
 s.selling_cost_minor AS selling_cost_minor,
 s.disposal_cost_minor AS disposal_cost_minor,
 s.reference_price_minor AS reference_price_minor,
 s.model_version AS model_version,
 s.elasticity AS elasticity,
 s.min_price_ratio AS min_price_ratio,
 s.max_price_ratio AS max_price_ratio,
 arrayFilter(p -> p<=s.current_price_minor,s.permitted_prices_minor) AS permitted_prices_minor,b.seq sales_sequence, {cutoff:DateTime64(3,'UTC')} cutoff,
 greatest(0.5,least(1.5,if(h.reference_recent>0,r.reference_units/h.reference_recent,1))) recent_demand_factor,
 h.reference_remaining*recent_demand_factor*pow(toFloat64(greatest(s.current_price_minor,1))/greatest(s.reference_price_minor,1),s.elasticity) base_demand_units,
 greatest(0,s.stock_units-base_demand_units) excess_units,
 scenarios.demand_multipliers AS demand_multipliers,scenarios.scenario_weights AS scenario_weights,
 dateDiff('second',b.acknowledged,now64(3)) delivery_age_seconds,
 multiIf((SELECT count() FROM __DATABASE__.retail_runs_cdc FINAL WHERE run_id={run:UUID} AND status IN ('running','stopped') AND _peerdb_is_deleted=0)!=1
 OR {cutoff:DateTime64(3,'UTC')}<toDateTime((SELECT max(toDate(hour)) FROM __DATABASE__.retail_availability)+INTERVAL 1 DAY,'UTC')+INTERVAL 2 HOUR+INTERVAL 15 MINUTE
 OR dateDiff('second',b.acknowledged,now64(3))>120 OR b.seq=0 OR b.through!={cutoff:DateTime64(3,'UTC')} OR s.inventory_sequence<o.last_sales_sequence OR s.price_version<o.last_price_version
 OR s.inventory_deleted OR s.price_deleted OR s.policy_deleted,'waiting_for_cdc',
 s.stock_units=0,'no_stock',s.expires_at<={cutoff:DateTime64(3,'UTC')},'expired',
 z.n>0,'stockout',h.exposure_hours=0 OR h.missing_required_slots>0,'missing_history',
 s.model_version='' OR s.elasticity>0 OR NOT isFinite(s.elasticity)
 OR NOT has(s.permitted_prices_minor,s.current_price_minor)
 OR abs(arraySum(scenarios.scenario_weights)-1)>0.000001 OR length(scenarios.scenario_weights)=0 OR s.price_version=0 OR s.rule_version=0
 OR NOT arrayAll(p -> p/s.reference_price_minor>=s.min_price_ratio AND p/s.reference_price_minor<=s.max_price_ratio,s.permitted_prices_minor),'unsupported_model','ready') eligibility
FROM subjects s CROSS JOIN boundary b LEFT JOIN horizons h USING(store_id,sku)
LEFT JOIN operational_sales o USING(store_id,sku) LEFT JOIN recent r USING(store_id,sku) LEFT JOIN stockouts z USING(store_id,sku)
LEFT JOIN scenario scenarios ON s.model_version=scenarios.model_version
