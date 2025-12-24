-- ================================================
-- FastMart Demo: Cleanup Script
-- ================================================
-- Purpose: Tear down all demo objects
-- WARNING: This will DELETE ALL demo data!
-- Use this to reset the demo or clean up after presentation
-- ================================================

-- Confirm database before proceeding
SELECT
    'WARNING: This script will DROP the entire fastmart_demo database!' AS warning,
    'All tables, views, dictionaries, and data will be permanently deleted.' AS notice,
    'Press Ctrl+C now to cancel, or wait 2 seconds to proceed...' AS action;

SELECT sleep(2);

-- ================================================
-- Drop Database (simplest approach)
-- ================================================

SELECT 'Dropping fastmart_demo database...' AS status;

DROP DATABASE IF EXISTS fastmart_demo;

SELECT 'Database dropped successfully!' AS status;

-- ================================================
-- Verify cleanup
-- ================================================

SELECT
    count() AS remaining_tables
FROM system.tables
WHERE database = 'fastmart_demo';

-- Expected: 0

SELECT
    'Cleanup complete!' AS status,
    'All demo objects removed.' AS result,
    'Run sql/setup/00_config.sql to start fresh' AS next_step;

-- ================================================
-- ALTERNATIVE: Granular Cleanup (if you prefer)
-- ================================================
-- Uncomment the sections below if you want to drop objects individually
-- instead of dropping the entire database

/*
USE fastmart_demo;

-- ================================================
-- 1. Drop Materialized Views First (dependencies)
-- ================================================

SELECT '1. Dropping materialized views...' AS status;

DROP VIEW IF EXISTS orders_bronze_to_silver_mv;
DROP VIEW IF EXISTS clicks_bronze_to_silver_mv;
DROP VIEW IF EXISTS inventory_bronze_to_silver_mv;
DROP VIEW IF EXISTS orders_enrichment_mv;
DROP VIEW IF EXISTS orders_high_value_mv;
DROP VIEW IF EXISTS sales_by_minute_mv;
DROP VIEW IF EXISTS sales_by_minute_brand_mv;
DROP VIEW IF EXISTS sales_by_minute_tier_mv;
DROP VIEW IF EXISTS sales_by_hour_mv;
DROP VIEW IF EXISTS sales_by_hour_brand_mv;
DROP VIEW IF EXISTS sales_by_day_mv;
DROP VIEW IF EXISTS anomaly_high_value_mv;
DROP VIEW IF EXISTS customer_velocity_mv;
DROP VIEW IF EXISTS anomaly_velocity_mv;
DROP VIEW IF EXISTS anomaly_unusual_time_mv;
DROP VIEW IF EXISTS anomaly_trends_mv;
DROP VIEW IF EXISTS orders_with_defaults_mv;
DROP VIEW IF EXISTS events_orders_mv;
DROP VIEW IF EXISTS events_clicks_mv;
DROP VIEW IF EXISTS events_inventory_mv;

SELECT 'Materialized views dropped.' AS status;

-- ================================================
-- 2. Drop Dictionaries
-- ================================================

SELECT '2. Dropping dictionaries...' AS status;

DROP DICTIONARY IF EXISTS products_dict;
DROP DICTIONARY IF EXISTS customers_dict;
DROP DICTIONARY IF EXISTS suppliers_dict;
DROP DICTIONARY IF EXISTS categories_hierarchy_dict;

SELECT 'Dictionaries dropped.' AS status;

-- ================================================
-- 3. Drop Gold Layer Tables
-- ================================================

SELECT '3. Dropping Gold layer tables...' AS status;

DROP TABLE IF EXISTS sales_by_minute;
DROP TABLE IF EXISTS sales_by_minute_brand;
DROP TABLE IF EXISTS sales_by_minute_tier;
DROP TABLE IF EXISTS sales_by_hour;
DROP TABLE IF EXISTS sales_by_hour_brand;
DROP TABLE IF EXISTS sales_by_day;
DROP TABLE IF EXISTS order_anomalies;
DROP TABLE IF EXISTS customer_order_velocity;
DROP TABLE IF EXISTS anomaly_trends;

SELECT 'Gold layer tables dropped.' AS status;

-- ================================================
-- 4. Drop Silver Layer Tables
-- ================================================

SELECT '4. Dropping Silver layer tables...' AS status;

DROP TABLE IF EXISTS orders_silver;
DROP TABLE IF EXISTS clicks_silver;
DROP TABLE IF EXISTS inventory_silver;
DROP TABLE IF EXISTS orders_enriched;
DROP TABLE IF EXISTS orders_high_value;
DROP TABLE IF EXISTS orders_with_defaults;

SELECT 'Silver layer tables dropped.' AS status;

-- ================================================
-- 5. Drop Bronze Layer Tables
-- ================================================

SELECT '5. Dropping Bronze layer tables...' AS status;

DROP TABLE IF EXISTS events_raw;
DROP TABLE IF EXISTS events_ingestion;
DROP TABLE IF EXISTS events_orders_raw;
DROP TABLE IF EXISTS events_clicks_raw;
DROP TABLE IF EXISTS events_inventory_raw;

SELECT 'Bronze layer tables dropped.' AS status;

-- ================================================
-- 6. Drop Dimension Tables
-- ================================================

SELECT '6. Dropping dimension tables...' AS status;

DROP TABLE IF EXISTS products;
DROP TABLE IF EXISTS customers;
DROP TABLE IF EXISTS suppliers;
DROP TABLE IF EXISTS categories_hierarchy;

SELECT 'Dimension tables dropped.' AS status;

-- ================================================
-- 7. Verify Cleanup
-- ================================================

SELECT '7. Verifying cleanup...' AS status;

SELECT
    table,
    engine
FROM system.tables
WHERE database = 'fastmart_demo'
  AND table NOT LIKE '.%'
ORDER BY table;

-- Expected: Empty result

SELECT 'Granular cleanup complete!' AS status;
*/

-- ================================================
-- PARTIAL CLEANUP OPTIONS
-- ================================================

-- Option 1: Keep schema, drop data only
/*
TRUNCATE TABLE events_raw;
TRUNCATE TABLE orders_silver;
TRUNCATE TABLE orders_enriched;
TRUNCATE TABLE sales_by_minute;
TRUNCATE TABLE sales_by_hour;
TRUNCATE TABLE sales_by_day;
TRUNCATE TABLE order_anomalies;
SELECT 'Data truncated, schema preserved.' AS status;
*/

-- Option 2: Drop only aggregates (keep raw data)
/*
DROP TABLE IF EXISTS sales_by_minute;
DROP TABLE IF EXISTS sales_by_hour;
DROP TABLE IF EXISTS sales_by_day;
SELECT 'Aggregates dropped, raw data preserved.' AS status;
*/

-- Option 3: Drop only test data
/*
DELETE FROM events_raw WHERE payload LIKE '%test%';
DELETE FROM orders_silver WHERE order_id LIKE '%test%';
SELECT 'Test data removed.' AS status;
*/
