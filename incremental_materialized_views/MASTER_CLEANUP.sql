-- ================================================
-- MASTER CLEANUP: Drops ALL Demo Databases
-- ================================================
-- WARNING: This script permanently deletes ALL demo data!
-- Use this to reset everything for a fresh start.
--
-- Databases dropped:
--   - mv_demo_basic       (Example 1: Basic MVs)
--   - mv_demo_summing     (Example 2: SummingMergeTree)
--   - mv_demo_aggregating (Example 3: AggregatingMergeTree)
--   - mv_demo_dictionaries (Example 4: Dictionaries)
--   - fastmart_demo       (Example 5: Medallion Architecture)
--
-- DROP DATABASE removes ALL objects within each database:
--   - Tables (MergeTree, SummingMergeTree, AggregatingMergeTree)
--   - Materialized Views
--   - Views
--   - Dictionaries
-- ================================================

SELECT '================================================';
SELECT 'MASTER CLEANUP: Dropping ALL demo databases';
SELECT '================================================';

-- Show what exists BEFORE cleanup
SELECT '-- Databases to drop:' AS info;
SELECT name AS database_name, engine
FROM system.databases
WHERE name IN ('mv_demo_basic', 'mv_demo_summing', 'mv_demo_aggregating', 'mv_demo_dictionaries', 'fastmart_demo');

SELECT '-- Tables to drop:' AS info;
SELECT database, name AS table_name, engine
FROM system.tables
WHERE database IN ('mv_demo_basic', 'mv_demo_summing', 'mv_demo_aggregating', 'mv_demo_dictionaries', 'fastmart_demo')
ORDER BY database, name;

SELECT '-- Dictionaries to drop:' AS info;
SELECT database, name AS dictionary_name
FROM system.dictionaries
WHERE database IN ('mv_demo_basic', 'mv_demo_summing', 'mv_demo_aggregating', 'mv_demo_dictionaries', 'fastmart_demo')
ORDER BY database, name;

SELECT '================================================';
SELECT 'Executing DROP DATABASE commands...';
SELECT '================================================';

-- Drop all demo databases (this removes ALL objects within each)
DROP DATABASE IF EXISTS mv_demo_basic;
DROP DATABASE IF EXISTS mv_demo_summing;
DROP DATABASE IF EXISTS mv_demo_aggregating;
DROP DATABASE IF EXISTS mv_demo_dictionaries;
DROP DATABASE IF EXISTS fastmart_demo;

-- Verify cleanup
SELECT '================================================';
SELECT 'Verification: Checking for remaining objects...';
SELECT '================================================';

SELECT '-- Remaining databases (should be empty):' AS info;
SELECT name AS remaining_database
FROM system.databases
WHERE name IN ('mv_demo_basic', 'mv_demo_summing', 'mv_demo_aggregating', 'mv_demo_dictionaries', 'fastmart_demo');

SELECT '-- Remaining tables (should be empty):' AS info;
SELECT database, name AS remaining_table
FROM system.tables
WHERE database IN ('mv_demo_basic', 'mv_demo_summing', 'mv_demo_aggregating', 'mv_demo_dictionaries', 'fastmart_demo');

SELECT '-- Remaining dictionaries (should be empty):' AS info;
SELECT database, name AS remaining_dictionary
FROM system.dictionaries
WHERE database IN ('mv_demo_basic', 'mv_demo_summing', 'mv_demo_aggregating', 'mv_demo_dictionaries', 'fastmart_demo');

SELECT '================================================';
SELECT 'CLEANUP COMPLETE - All demo databases dropped';
SELECT '================================================';
