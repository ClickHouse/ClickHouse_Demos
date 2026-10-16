CREATE TABLE IF NOT EXISTS __DATABASE__.retail_stores (
 store_id UInt16, store_name String, district_id UInt16, district_name String, store_group UInt8, timezone LowCardinality(String)
) ENGINE = MergeTree ORDER BY store_id;
CREATE TABLE IF NOT EXISTS __DATABASE__.retail_products (
 sku UInt32, product_name String, category LowCardinality(String), reference_price_minor Int64, unit_cost_minor Int64,
 selling_cost_minor Int64, disposal_cost_minor Int64, permitted_prices_minor Array(Int64), currency LowCardinality(String)
) ENGINE = MergeTree ORDER BY sku;
CREATE TABLE IF NOT EXISTS __DATABASE__.retail_availability (
 hour DateTime64(3, 'UTC'), open Bool, exposure_hours Float64, ample_stock Bool, calendar_factor Float64
) ENGINE = MergeTree ORDER BY hour;
CREATE TABLE IF NOT EXISTS __DATABASE__.retail_model_parameters (
 model_version String, sku UInt32, store_group UInt8, elasticity Float64,
 reference_rate Float64, min_price_ratio Float64, max_price_ratio Float64
) ENGINE = MergeTree ORDER BY (model_version, sku, store_group);
CREATE TABLE IF NOT EXISTS __DATABASE__.retail_demand_scenarios (
 model_version String, scenario LowCardinality(String), multiplier Float64, weight Float64
) ENGINE = MergeTree ORDER BY (model_version, scenario);
