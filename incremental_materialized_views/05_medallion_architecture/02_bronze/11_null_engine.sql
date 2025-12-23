-- ================================================
-- FastMart Demo: Null Engine Pattern (OPTIONAL)
-- ================================================
-- Purpose: Best practice for feeding multiple MVs from single source
-- Pattern: Ingest to Null table → Multiple MVs → Different target tables
-- Benefit: Prevents duplicate storage and processing
-- ================================================

USE fastmart_demo;

-- ================================================
-- BEST PRACTICE: Null Engine as Ingestion Point
-- ================================================
-- When multiple materialized views need to process the same raw data,
-- use a Null table as the ingestion point instead of MergeTree.
--
-- WHY?
-- 1. Data is written once, processed by multiple MVs
-- 2. No duplicate storage of raw data
-- 3. Atomic processing across all MVs
-- 4. Better performance for high-throughput scenarios

-- ================================================
-- Alternative Bronze Layer using Null Engine
-- ================================================

DROP TABLE IF EXISTS events_ingestion;

CREATE TABLE events_ingestion (
    event_id UUID DEFAULT generateUUIDv4(),
    event_time DateTime64(3) DEFAULT now64(3),
    event_type LowCardinality(String),
    source_system LowCardinality(String) DEFAULT 'web',
    payload String,
    ingestion_time DateTime64(3) DEFAULT now64(3)
)
ENGINE = Null()  -- Data is NOT stored, only passed to MVs
COMMENT 'Null engine: Ingestion point for multiple materialized views';

-- ================================================
-- Create separate raw tables per event type
-- ================================================
-- Instead of one large events_raw, split by event type
-- This improves query performance and allows different TTLs

DROP TABLE IF EXISTS events_orders_raw;
CREATE TABLE events_orders_raw (
    event_id UUID,
    event_time DateTime64(3),
    source_system LowCardinality(String),
    payload String,
    ingestion_time DateTime64(3)
)
ENGINE = MergeTree()
PARTITION BY toYYYYMMDD(event_time)
ORDER BY (event_time, event_id)
TTL event_time + INTERVAL 7 DAY
COMMENT 'Raw order events only';

DROP TABLE IF EXISTS events_clicks_raw;
CREATE TABLE events_clicks_raw (
    event_id UUID,
    event_time DateTime64(3),
    source_system LowCardinality(String),
    payload String,
    ingestion_time DateTime64(3)
)
ENGINE = MergeTree()
PARTITION BY toYYYYMMDD(event_time)
ORDER BY (event_time, event_id)
TTL event_time + INTERVAL 3 DAY  -- Shorter retention for clicks
COMMENT 'Raw click events only';

DROP TABLE IF EXISTS events_inventory_raw;
CREATE TABLE events_inventory_raw (
    event_id UUID,
    event_time DateTime64(3),
    source_system LowCardinality(String),
    payload String,
    ingestion_time DateTime64(3)
)
ENGINE = MergeTree()
PARTITION BY toYYYYMMDD(event_time)
ORDER BY (event_time, event_id)
TTL event_time + INTERVAL 14 DAY  -- Longer retention for inventory
COMMENT 'Raw inventory events only';

-- ================================================
-- Materialized Views to route events by type
-- ================================================

DROP VIEW IF EXISTS events_orders_mv;
CREATE MATERIALIZED VIEW events_orders_mv TO events_orders_raw AS
SELECT
    event_id,
    event_time,
    source_system,
    payload,
    ingestion_time
FROM events_ingestion
WHERE event_type = 'order';

DROP VIEW IF EXISTS events_clicks_mv;
CREATE MATERIALIZED VIEW events_clicks_mv TO events_clicks_raw AS
SELECT
    event_id,
    event_time,
    source_system,
    payload,
    ingestion_time
FROM events_ingestion
WHERE event_type = 'click';

DROP VIEW IF EXISTS events_inventory_mv;
CREATE MATERIALIZED VIEW events_inventory_mv TO events_inventory_raw AS
SELECT
    event_id,
    event_time,
    source_system,
    payload,
    ingestion_time
FROM events_ingestion
WHERE event_type = 'inventory_update';

-- ================================================
-- Test the Null Engine Pattern
-- ================================================

-- Insert events to the Null table
INSERT INTO events_ingestion (event_type, source_system, payload) VALUES
    ('order', 'web', '{"order_id": "null-test-001", "customer_id": 1001, "product_id": 1, "quantity": 1, "price": 29.99}'),
    ('click', 'web', '{"session_id": "null-sess-001", "customer_id": 1001, "page": "/products/mouse"}'),
    ('inventory_update', 'batch', '{"product_id": 1, "warehouse_id": 101, "quantity_change": -1}');

-- Verify routing worked
SELECT '--- Null Engine Pattern Test ---' AS section;

SELECT
    'Orders routed' AS event_type,
    count() AS count
FROM events_orders_raw
WHERE payload LIKE '%null-test%'
UNION ALL
SELECT
    'Clicks routed' AS event_type,
    count() AS count
FROM events_clicks_raw
WHERE payload LIKE '%null-sess%'
UNION ALL
SELECT
    'Inventory routed' AS event_type,
    count() AS count
FROM events_inventory_raw
WHERE payload LIKE '%null-test%';

-- ================================================
-- COMPARISON: Storage Efficiency
-- ================================================

SELECT '--- Storage Comparison ---' AS section;

-- With MergeTree (duplicate storage)
SELECT
    'MergeTree approach' AS approach,
    'All events stored in one table' AS storage_pattern,
    'Higher storage cost' AS cost;

-- With Null engine (no duplication)
SELECT
    'Null engine approach' AS approach,
    'Events routed to separate tables' AS storage_pattern,
    'Lower storage cost, better performance' AS cost;

-- ================================================
-- DEMO TALKING POINTS
-- ================================================
-- 1. Null engine prevents duplicate storage
-- 2. Single ingestion point for multiple MVs
-- 3. Different TTLs per event type (cost optimization)
-- 4. Better query performance (smaller, focused tables)
-- 5. Atomic processing across all MVs

-- ================================================
-- When to use Null Engine Pattern?
-- ================================================
-- USE when:
-- - Multiple MVs need the same source data
-- - Different event types need different retention
-- - High ingestion rate (millions/second)
-- - Cost optimization is critical
--
-- DON'T USE when:
-- - You need to query raw events directly
-- - Only one or two MVs needed
-- - Low data volume
-- - Simple use case

-- ================================================
-- NEXT STEPS
-- ================================================
SELECT
    'Null engine pattern demonstrated' AS status,
    'This is OPTIONAL - use events_raw for simpler demos' AS note,
    'Next: Create Silver layer transformations' AS next_step;
