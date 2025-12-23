-- Step 5: Materialized Views (Bronze -> Silver)
-- Auto-transform JSON to typed columns on INSERT
-- NOTE: Do NOT insert data here. Enrichment MV and dictionaries must be created first.

USE fastmart_demo;

-- MV: Orders Bronze -> Silver (parse JSON, validate, calculate total)
DROP VIEW IF EXISTS orders_bronze_to_silver_mv;
CREATE MATERIALIZED VIEW orders_bronze_to_silver_mv
TO orders_silver
AS
SELECT
    JSONExtractString(payload, 'order_id')::UUID AS order_id,
    JSONExtract(payload, 'customer_id', 'UInt64') AS customer_id,
    JSONExtract(payload, 'product_id', 'UInt64') AS product_id,
    JSONExtract(payload, 'quantity', 'UInt32') AS quantity,
    JSONExtract(payload, 'price', 'Decimal64(2)') AS price,
    JSONExtract(payload, 'quantity', 'UInt32') * JSONExtract(payload, 'price', 'Decimal64(2)') AS total_amount,
    JSONExtractString(payload, 'payment_method') AS payment_method,
    event_time AS order_time,
    source_system,
    ingestion_time
FROM events_raw
WHERE event_type = 'order'
  AND JSONHas(payload, 'order_id')
  AND JSONHas(payload, 'customer_id')
  AND JSONHas(payload, 'product_id')
  AND JSONExtract(payload, 'quantity', 'UInt32') > 0
  AND JSONExtract(payload, 'price', 'Decimal64(2)') > 0;

-- MV: Clicks Bronze -> Silver
DROP VIEW IF EXISTS clicks_bronze_to_silver_mv;
CREATE MATERIALIZED VIEW clicks_bronze_to_silver_mv
TO clicks_silver
AS
SELECT
    event_id AS click_id,
    JSONExtractString(payload, 'session_id') AS session_id,
    JSONExtract(payload, 'customer_id', 'UInt64') AS customer_id,
    JSONExtractString(payload, 'page') AS page,
    JSONExtractString(payload, 'action') AS action,
    JSONExtract(payload, 'product_id', 'UInt64') AS product_id,
    JSONExtract(payload, 'duration_seconds', 'UInt32') AS duration_seconds,
    event_time AS click_time,
    source_system,
    ingestion_time
FROM events_raw
WHERE event_type = 'click'
  AND JSONHas(payload, 'session_id')
  AND JSONHas(payload, 'page');

-- MV: Inventory Bronze -> Silver
DROP VIEW IF EXISTS inventory_bronze_to_silver_mv;
CREATE MATERIALIZED VIEW inventory_bronze_to_silver_mv
TO inventory_silver
AS
SELECT
    event_id AS update_id,
    JSONExtract(payload, 'product_id', 'UInt64') AS product_id,
    JSONExtract(payload, 'warehouse_id', 'UInt32') AS warehouse_id,
    JSONExtract(payload, 'quantity_change', 'Int32') AS quantity_change,
    JSONExtract(payload, 'new_stock_level', 'UInt32') AS new_stock_level,
    JSONExtractString(payload, 'reason') AS reason,
    event_time AS update_time,
    source_system,
    ingestion_time
FROM events_raw
WHERE event_type = 'inventory_update'
  AND JSONHas(payload, 'product_id')
  AND JSONHas(payload, 'warehouse_id');

SELECT '[OK] MVs created: Bronze -> Silver (orders, clicks, inventory)' AS step;
