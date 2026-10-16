CREATE ROLE IF NOT EXISTS __ROLE__;
GRANT SELECT ON __DATABASE__.retail_analysis_context TO __ROLE__;
GRANT SELECT ON __DATABASE__.retail_opportunities TO __ROLE__;
GRANT SELECT ON __DATABASE__.retail_price_comparison TO __ROLE__;
GRANT SELECT ON __DATABASE__.retail_outcome_series TO __ROLE__;
GRANT SELECT ON __DATABASE__.retail_sales TO __ROLE__;
GRANT SELECT ON __DATABASE__.retail_sales_minute TO __ROLE__;
GRANT SELECT ON __DATABASE__.retail_district_minute TO __ROLE__;
GRANT SELECT ON __DATABASE__.retail_products TO __ROLE__;
GRANT SELECT ON __DATABASE__.retail_stores TO __ROLE__;
GRANT SELECT ON __DATABASE__.retail_availability TO __ROLE__;
GRANT SELECT ON __DATABASE__.retail_model_parameters TO __ROLE__;
GRANT SELECT ON __DATABASE__.retail_demand_scenarios TO __ROLE__;
GRANT SELECT ON __DATABASE__.retail_inventory_current TO __ROLE__;
GRANT SELECT ON __DATABASE__.retail_price_current TO __ROLE__;
GRANT SELECT ON __DATABASE__.retail_policy_current TO __ROLE__;
GRANT SELECT ON __DATABASE__.retail_runs_cdc TO __ROLE__;
GRANT SELECT ON __DATABASE__.retail_batches_cdc TO __ROLE__;
GRANT SELECT ON __DATABASE__.retail_inventory_history_cdc TO __ROLE__;
GRANT SELECT ON __DATABASE__.retail_price_history_cdc TO __ROLE__;
GRANT SELECT ON __DATABASE__.retail_policy_history_cdc TO __ROLE__;
GRANT SELECT ON __DATABASE__.retail_receipts_cdc TO __ROLE__;
CREATE SETTINGS PROFILE IF NOT EXISTS __PROFILE__ SETTINGS
 readonly=1 CONST,
 max_execution_time=30 CONST,
 max_rows_to_read=200000000 CONST,
 max_result_rows=10000 CONST,
 max_memory_usage=2000000000 CONST,
 read_overflow_mode='throw' CONST,
 result_overflow_mode='throw' CONST
 TO __ROLE__;
