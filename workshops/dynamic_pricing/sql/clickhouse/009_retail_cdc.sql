CREATE OR REPLACE VIEW __DATABASE__.retail_inventory_current AS
SELECT run_id, store_id, sku, latest.1 AS stock_units, latest.2 AS expires_at,
 latest_version AS inventory_version, latest.3 AS effective_sequence
FROM (
 SELECT run_id,store_id,sku,max(version) latest_version,
 argMax(tuple(stock_units,expires_at,effective_sequence,_peerdb_is_deleted),version) latest
 FROM __DATABASE__.retail_inventory_history_cdc FINAL GROUP BY run_id,store_id,sku
) WHERE latest.4=0;
CREATE OR REPLACE VIEW __DATABASE__.retail_price_current AS
SELECT run_id,store_id,sku,latest.1 AS current_price_minor,latest_version AS price_version,
 latest.2 AS effective_sequence
FROM (
 SELECT run_id,store_id,sku,max(version) latest_version,
 argMax(tuple(price_minor,effective_sequence,_peerdb_is_deleted),version) latest
 FROM __DATABASE__.retail_price_history_cdc FINAL GROUP BY run_id,store_id,sku
) WHERE latest.3=0;
CREATE OR REPLACE VIEW __DATABASE__.retail_policy_current AS
SELECT run_id,store_id,sku,latest_version AS rule_version,latest.1 AS permitted_prices_minor,
 latest.2 AS unit_cost_minor,latest.3 AS selling_cost_minor,latest.4 AS disposal_cost_minor
FROM (
 SELECT run_id,store_id,sku,max(version) latest_version,
 argMax(tuple(permitted_prices_minor,unit_cost_minor,selling_cost_minor,disposal_cost_minor,_peerdb_is_deleted),version) latest
 FROM __DATABASE__.retail_policy_history_cdc FINAL GROUP BY run_id,store_id,sku
) WHERE latest.5=0;
