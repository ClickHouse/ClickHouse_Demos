-- ============================================================================
-- CLICKHOUSE DEMO: Incremental Materialized Views & Medallion Architecture
-- ============================================================================
-- Environment: ClickHouse Cloud
-- ============================================================================
--
-- TABLE OF CONTENTS:
-- ==================
--   SECTION 0: SETUP (Run first to create tables and sample data)
--   -----------------------------------------------------------------
--   SECTION 1: Incremental Materialized Views
--   SECTION 2: The AVG Problem + AggregatingMergeTree
--   SECTION 3: Dictionaries
--   SECTION 4: Medallion Architecture Finale
--   -----------------------------------------------------------------
--   SECTION 5: CLEANUP (Run when done)
--
-- HOW TO USE:
-- ===========
--   1. Run SECTION 0 (Setup) first - creates all tables and sample data
--   2. Work through SECTIONs 1-4, executing queries step by step
--   3. Run SECTION 5 (Cleanup) when you're done to drop the demo database
--
-- ============================================================================




-- ****************************************************************************
-- ****************************************************************************
-- **                                                                        **
-- **  SECTION 0: SETUP                                                      **
-- **  ================                                                      **
-- **  Run this FIRST (takes ~30 seconds)                                    **
-- **  Creates all tables, dictionaries, and sample data                     **
-- **                                                                        **
-- ****************************************************************************
-- ****************************************************************************

-- Drop any existing demo database and start fresh
DROP DATABASE IF EXISTS webinar_demo;
CREATE DATABASE webinar_demo;
USE webinar_demo;

SELECT '=== SETUP: Creating dimension tables ===' AS status;

-- -----------------------------------------------
-- Products dimension table (10 sample products)
-- This will be loaded into a dictionary later
-- -----------------------------------------------
CREATE TABLE dim_products (
    product_id UInt32,
    product_name String,
    category String,
    brand String,
    price Decimal64(2),
    cost Decimal64(2)
) ENGINE = MergeTree()
ORDER BY product_id;

INSERT INTO dim_products VALUES
    (1, 'Wireless Mouse', 'Electronics', 'TechBrand', 29.99, 12.00),
    (2, 'USB-C Cable', 'Electronics', 'TechBrand', 12.99, 4.00),
    (3, 'Coffee Mug', 'Home', 'HomeBrand', 9.99, 3.50),
    (4, 'Notebook Pack', 'Office', 'PaperCo', 14.99, 5.00),
    (5, 'Desk Lamp', 'Home', 'LightPro', 39.99, 18.00),
    (6, 'Keyboard', 'Electronics', 'TechBrand', 79.99, 35.00),
    (7, 'Monitor Stand', 'Office', 'ErgoDesk', 49.99, 22.00),
    (8, 'Plant Pot', 'Home', 'GreenLife', 19.99, 8.00),
    (9, 'Webcam HD', 'Electronics', 'TechBrand', 59.99, 25.00),
    (10, 'Desk Organizer', 'Office', 'PaperCo', 24.99, 10.00);

-- -----------------------------------------------
-- Customers dimension table (10 sample customers)
-- This will be loaded into a dictionary later
-- -----------------------------------------------
CREATE TABLE dim_customers (
    customer_id UInt32,
    customer_name String,
    tier String,
    country String
) ENGINE = MergeTree()
ORDER BY customer_id;

INSERT INTO dim_customers VALUES
    (1001, 'Alice Johnson', 'Gold', 'USA'),
    (1002, 'Bob Smith', 'Silver', 'USA'),
    (1003, 'Carol White', 'Platinum', 'USA'),
    (1004, 'David Brown', 'Bronze', 'Canada'),
    (1005, 'Emma Davis', 'Gold', 'UK'),
    (1006, 'Frank Miller', 'Silver', 'Germany'),
    (1007, 'Grace Lee', 'Platinum', 'Japan'),
    (1008, 'Henry Wilson', 'Bronze', 'Australia'),
    (1009, 'Ivy Chen', 'Gold', 'Singapore'),
    (1010, 'Jack Taylor', 'Silver', 'USA');

SELECT '=== SETUP: Verifying dimension data ===' AS status;
SELECT count() AS products FROM dim_products;
SELECT count() AS customers FROM dim_customers;

SELECT '=== SETUP: Creating dictionaries ===' AS status;

-- -----------------------------------------------
-- Products dictionary
-- Loads product data into memory as a hash table
-- Enables O(1) lookups instead of JOINs
-- Note: Using QUERY source for ClickHouse Cloud compatibility
-- -----------------------------------------------
CREATE DICTIONARY products_dict (
    product_id UInt32,
    product_name String,
    category String,
    brand String,
    price Decimal64(2),
    cost Decimal64(2)
)
PRIMARY KEY product_id
SOURCE(CLICKHOUSE(QUERY 'SELECT product_id, product_name, category, brand, price, cost FROM webinar_demo.dim_products'))
LAYOUT(HASHED())                 -- Store as hash table for O(1) lookups
LIFETIME(MIN 300 MAX 600);       -- Refresh every 5-10 minutes

-- -----------------------------------------------
-- Customers dictionary
-- Same pattern as products - in-memory hash table
-- -----------------------------------------------
CREATE DICTIONARY customers_dict (
    customer_id UInt32,
    customer_name String,
    tier String,
    country String
)
PRIMARY KEY customer_id
SOURCE(CLICKHOUSE(QUERY 'SELECT customer_id, customer_name, tier, country FROM webinar_demo.dim_customers'))
LAYOUT(HASHED())
LIFETIME(MIN 300 MAX 600);

-- Force load dictionaries into memory immediately
SYSTEM RELOAD DICTIONARY webinar_demo.products_dict;
SYSTEM RELOAD DICTIONARY webinar_demo.customers_dict;

SELECT '=== SETUP: Creating tables for SECTION 1 (MVs demo) ===' AS status;

-- -----------------------------------------------
-- Tables for SECTION 1: Materialized Views Demo
-- raw_logs: source table where data arrives
-- log_summary: target table that aggregates by log type
-- Uses SummingMergeTree for automatic aggregation!
-- -----------------------------------------------
CREATE TABLE raw_logs (
    event_time DateTime DEFAULT now(),
    log_message String
) ENGINE = MergeTree()
ORDER BY event_time;

-- Target table uses SummingMergeTree to auto-sum event_count
-- This shows MV doing real TRANSFORMATION, not just copying!
CREATE TABLE log_summary (
    log_type String,
    event_count UInt64
) ENGINE = SummingMergeTree()
ORDER BY log_type;

SELECT '=== SETUP: Creating tables for SECTION 2 (AVG problem demo) ===' AS status;

-- -----------------------------------------------
-- Tables for SECTION 2: The AVG Problem Demo
-- tt_avg_latency_wrong: stores pre-computed averages (BAD!)
-- tt_avg_latency: stores aggregate state (GOOD!)
-- -----------------------------------------------

-- The WRONG way: storing pre-computed averages in a regular table
-- This leads to incorrect results when averaging across batches
CREATE TABLE tt_avg_latency_wrong (
    endpoint String,
    avg_latency Float64,      -- Pre-computed average (loses count info!)
    batch_id UInt32
) ENGINE = MergeTree()
ORDER BY endpoint;

-- The RIGHT way: AggregatingMergeTree stores aggregate STATE
-- avgState stores (sum, count) so merging works correctly
CREATE TABLE tt_avg_latency (
    endpoint String,
    avg_latency AggregateFunction(avg, Float64),    -- Stores (sum, count)
    count_requests AggregateFunction(count, UInt64), -- For demo visibility
    sum_latency AggregateFunction(sum, Float64)      -- For demo visibility
) ENGINE = AggregatingMergeTree()
ORDER BY endpoint;

SELECT '=== SETUP: Creating tables for SECTION 3 (Dictionaries demo) ===' AS status;

-- -----------------------------------------------
-- Tables for SECTION 3: Dictionaries Demo
-- orders_fact: fact table with foreign keys
-- We'll compare JOIN vs dictGet approaches
-- -----------------------------------------------
CREATE TABLE orders_fact (
    order_id UUID DEFAULT generateUUIDv4(),
    order_time DateTime DEFAULT now(),
    customer_id UInt32,        -- FK to dim_customers
    product_id UInt32,         -- FK to dim_products
    quantity UInt32
) ENGINE = MergeTree()
ORDER BY order_time;

-- Pre-populate with 1000 sample orders for the demo
INSERT INTO orders_fact (customer_id, product_id, quantity)
SELECT
    1001 + (number % 10) AS customer_id,   -- Random customer 1001-1010
    1 + (number % 10) AS product_id,       -- Random product 1-10
    1 + (number % 5) AS quantity           -- Random quantity 1-5
FROM numbers(1000);

SELECT '=== SETUP: Creating tables for SECTION 4 (Medallion demo) ===' AS status;

-- -----------------------------------------------
-- BRONZE layer: Raw JSON events (landing zone)
-- Events arrive as JSON payloads, minimal processing
-- -----------------------------------------------
CREATE TABLE bronze_events (
    event_id UUID DEFAULT generateUUIDv4(),
    event_time DateTime DEFAULT now(),
    event_type LowCardinality(String),
    source_system LowCardinality(String) DEFAULT 'web',
    payload String                         -- Raw JSON
) ENGINE = MergeTree()
PARTITION BY toYYYYMMDD(event_time)
ORDER BY (event_time, event_type);

-- -----------------------------------------------
-- SILVER layer: Parsed orders (cleaned data)
-- JSON is parsed, typed, validated
-- -----------------------------------------------
CREATE TABLE silver_orders (
    order_id UUID,
    customer_id UInt32,
    product_id UInt32,
    quantity UInt32,
    price Decimal64(2),
    total_amount Decimal64(2),
    order_time DateTime,
    source_system String
) ENGINE = MergeTree()
ORDER BY (order_time, order_id);

-- -----------------------------------------------
-- SILVER layer: Enriched orders (with dimension data)
-- Uses dictGet to add customer/product details
-- -----------------------------------------------
CREATE TABLE silver_orders_enriched (
    order_id UUID,
    customer_id UInt32,
    customer_name String,        -- From customers_dict
    customer_tier String,        -- From customers_dict
    product_id UInt32,
    product_name String,         -- From products_dict
    category String,             -- From products_dict
    brand String,                -- From products_dict
    quantity UInt32,
    unit_price Decimal64(2),
    total_amount Decimal64(2),
    profit_margin Decimal64(2),  -- Calculated: total - (qty * cost)
    order_time DateTime
) ENGINE = MergeTree()
ORDER BY (order_time, order_id);

-- -----------------------------------------------
-- GOLD layer: Daily aggregates (business metrics)
-- Pre-aggregated using AggregatingMergeTree
-- -----------------------------------------------
CREATE TABLE gold_sales_day (
    day Date,
    category String,
    total_orders AggregateFunction(count, UUID),
    total_revenue AggregateFunction(sum, Decimal64(2)),
    total_profit AggregateFunction(sum, Decimal64(2)),
    unique_customers AggregateFunction(uniq, UInt32)
) ENGINE = AggregatingMergeTree()
ORDER BY (day, category);

SELECT '=== SETUP: Creating Materialized Views for Medallion pipeline ===' AS status;

-- -----------------------------------------------
-- MV 1: Bronze -> Silver (parse JSON)
-- Triggered on INSERT to bronze_events
-- Extracts fields from JSON payload
-- -----------------------------------------------
CREATE MATERIALIZED VIEW bronze_to_silver_mv
TO silver_orders
AS
SELECT
    -- Extract and cast fields from JSON
    JSONExtractString(payload, 'order_id')::UUID AS order_id,
    JSONExtract(payload, 'customer_id', 'UInt32') AS customer_id,
    JSONExtract(payload, 'product_id', 'UInt32') AS product_id,
    JSONExtract(payload, 'quantity', 'UInt32') AS quantity,
    JSONExtract(payload, 'price', 'Decimal64(2)') AS price,
    JSONExtract(payload, 'quantity', 'UInt32') * JSONExtract(payload, 'price', 'Decimal64(2)') AS total_amount,
    event_time AS order_time,
    source_system
FROM bronze_events
WHERE event_type = 'order'
  AND JSONHas(payload, 'order_id');   -- Only valid orders

-- -----------------------------------------------
-- MV 2: Silver -> Silver Enriched (add dimension data)
-- Triggered on INSERT to silver_orders
-- Uses dictGet for O(1) lookups (no JOINs!)
-- -----------------------------------------------
CREATE MATERIALIZED VIEW silver_enrichment_mv
TO silver_orders_enriched
AS
SELECT
    order_id,
    customer_id,
    -- Enrich with customer data via dictionary lookup
    dictGet('webinar_demo.customers_dict', 'customer_name', customer_id) AS customer_name,
    dictGet('webinar_demo.customers_dict', 'tier', customer_id) AS customer_tier,
    product_id,
    -- Enrich with product data via dictionary lookup
    dictGet('webinar_demo.products_dict', 'product_name', product_id) AS product_name,
    dictGet('webinar_demo.products_dict', 'category', product_id) AS category,
    dictGet('webinar_demo.products_dict', 'brand', product_id) AS brand,
    quantity,
    price AS unit_price,
    total_amount,
    -- Calculate profit: total - (quantity * unit cost from dictionary)
    total_amount - (quantity * dictGet('webinar_demo.products_dict', 'cost', product_id)) AS profit_margin,
    order_time
FROM silver_orders;

-- -----------------------------------------------
-- MV 3: Silver Enriched -> Gold (aggregate by day)
-- Triggered on INSERT to silver_orders_enriched
-- Uses State functions for correct aggregation
-- -----------------------------------------------
CREATE MATERIALIZED VIEW silver_to_gold_day_mv
TO gold_sales_day
AS
SELECT
    toDate(order_time) AS day,
    category,
    -- Use State functions (not regular aggregates!)
    countState(order_id) AS total_orders,       -- Stores count state
    sumState(total_amount) AS total_revenue,    -- Stores sum state
    sumState(profit_margin) AS total_profit,    -- Stores sum state
    uniqState(customer_id) AS unique_customers  -- Stores HyperLogLog state
FROM silver_orders_enriched
GROUP BY day, category;

SELECT '=== SETUP: Generating seed data (50,000 orders) ===' AS status;

-- -----------------------------------------------
-- Generate 50,000 sample orders for the demo
-- Data flows: bronze -> silver -> silver_enriched -> gold
-- All MVs trigger automatically!
-- -----------------------------------------------
INSERT INTO bronze_events (event_time, event_type, source_system, payload)
SELECT
    now() - INTERVAL (rand() % 86400) SECOND AS event_time,  -- Random time in last 24h
    'order' AS event_type,
    arrayElement(['web', 'mobile', 'api'], 1 + rand() % 3) AS source_system,
    -- Build JSON payload
    concat(
        '{"order_id":"', toString(generateUUIDv4()),
        '","customer_id":', toString(1001 + rand() % 10),
        ',"product_id":', toString(1 + rand() % 10),
        ',"quantity":', toString(1 + rand() % 5),
        ',"price":', toString(arrayElement([9.99, 12.99, 14.99, 19.99, 24.99, 29.99, 39.99, 49.99, 59.99, 79.99], 1 + rand() % 10)),
        '}'
    ) AS payload
FROM numbers(50000);

SELECT '=== SETUP COMPLETE ===' AS status;

-- Verify everything was created correctly
SELECT 'Dimension tables:' AS check,
    (SELECT count() FROM dim_products) AS products,
    (SELECT count() FROM dim_customers) AS customers;

SELECT 'Dictionaries:' AS check;
SELECT name, status, element_count
FROM system.dictionaries
WHERE database = 'webinar_demo';

SELECT 'Medallion pipeline:' AS check,
    (SELECT count() FROM bronze_events) AS bronze,
    (SELECT count() FROM silver_orders) AS silver,
    (SELECT count() FROM silver_orders_enriched) AS silver_enriched,
    (SELECT count() FROM gold_sales_day) AS gold_day;

-- ****************************************************************************
-- **  END OF SETUP - Demo starts below                                      **
-- ****************************************************************************




-- ****************************************************************************
-- ****************************************************************************
-- **                                                                        **
-- **  SECTION 1: INCREMENTAL MATERIALIZED VIEWS                             **
-- **  ==========================================                             **
-- **                                                                        **
-- **  KEY MESSAGE: MVs are INSERT triggers, not cached queries!             **
-- **  Data flows AND transforms automatically with zero orchestration.      **
-- **                                                                        **
-- ****************************************************************************
-- ****************************************************************************

USE webinar_demo;

-- ---------------------------------------------------------------------------
-- BUSINESS CONTEXT: Your app generates thousands of events per second.
-- You need real-time counts by event type. Traditional approach: batch ETL.
-- ClickHouse approach: Incremental Materialized Views!
-- ---------------------------------------------------------------------------

SELECT '>>> SECTION 1: Incremental Materialized Views <<<' AS demo;


-- ---------------------------------------------------------------------------
-- STEP 1.1: Show the two tables we'll use
-- ---------------------------------------------------------------------------
-- raw_logs: where events land (source)
-- log_summary: aggregated counts by type (target) - uses SummingMergeTree!

SELECT 'raw_logs (source) - raw events land here' AS table_1;
SELECT 'log_summary (target) - aggregated counts by type' AS table_2;

-- Verify both tables are empty
SELECT count() AS raw_logs_count FROM raw_logs;
SELECT count() AS log_summary_count FROM log_summary;


-- ---------------------------------------------------------------------------
-- STEP 1.2: Create a Materialized View that TRANSFORMS data
-- ---------------------------------------------------------------------------
-- This MV extracts the first word (log type) and counts events
-- Not just copying - it's doing real aggregation!

DROP VIEW IF EXISTS logs_mv;

CREATE MATERIALIZED VIEW logs_mv
TO log_summary                           -- Target: SummingMergeTree table
AS
SELECT
    -- Extract first word as log type (e.g., "User", "Error", "API")
    splitByChar(' ', log_message)[1] AS log_type,
    1 AS event_count                     -- Each event counts as 1
FROM raw_logs;

SELECT 'MV created! Extracts log type and counts events automatically' AS status;


-- ---------------------------------------------------------------------------
-- STEP 1.3: Insert events into raw_logs
-- ---------------------------------------------------------------------------
-- Watch the MV transform these into aggregated counts!

INSERT INTO raw_logs (log_message) VALUES
    ('User login successful'),
    ('Error connection timeout'),
    ('User clicked checkout'),
    ('Error payment failed'),
    ('API request received'),
    ('User logged out');

SELECT 'Inserted 6 events into raw_logs' AS action;


-- ---------------------------------------------------------------------------
-- STEP 1.4: Check log_summary - AGGREGATED automatically!
-- ---------------------------------------------------------------------------
-- WOW moment: 6 events -> 3 aggregated rows!
-- MV extracted types AND counted them!

SELECT '>>> Data transformed and aggregated automatically! <<<' AS wow_moment;

SELECT log_type, sum(event_count) AS total_events
FROM log_summary
GROUP BY log_type
ORDER BY total_events DESC;

-- Result should show: User: 3, Error: 2, API: 1
-- We inserted 6 rows, got 3 aggregated rows - real transformation!

-- TALKING POINT:
-- "The MV is an INSERT TRIGGER that transforms data!
-- We inserted 6 raw events, MV extracted types and counted them.
-- No scheduler, no batch job. Real-time aggregation!"


-- ---------------------------------------------------------------------------
-- STEP 1.5: Insert more events - watch counts update automatically
-- ---------------------------------------------------------------------------

INSERT INTO raw_logs (log_message) VALUES
    ('Error database connection lost'),
    ('Error timeout exceeded');

SELECT 'Inserted 2 more Error events...' AS action;

SELECT log_type, sum(event_count) AS total_events
FROM log_summary
GROUP BY log_type
ORDER BY total_events DESC;

-- Result: Error count increased from 2 to 4 - automatically!




-- ****************************************************************************
-- ****************************************************************************
-- **                                                                        **
-- **  SECTION 2: THE AVG PROBLEM + AGGREGATINGMERGETREE                     **
-- **  =================================================                     **
-- **                                                                        **
-- **  KEY MESSAGE: avg(avg) is WRONG!                                       **
-- **  State/Merge functions preserve the math for correct results.          **
-- **                                                                        **
-- **  This is the MOST IMPORTANT demo - the key teaching moment!            **
-- **                                                                        **
-- ****************************************************************************
-- ****************************************************************************

USE webinar_demo;

-- ---------------------------------------------------------------------------
-- BUSINESS CONTEXT: Your analytics team needs accurate metrics.
-- Data arrives in batches (streaming, micro-batches, different sources).
-- If you pre-compute averages per batch, you'll get WRONG results!
-- ---------------------------------------------------------------------------

SELECT '>>> SECTION 2: The AVG Problem <<<' AS demo;


-- ---------------------------------------------------------------------------
-- STEP 2.1: Set up with SIMPLE numbers (easy mental math!)
-- ---------------------------------------------------------------------------
-- 4 API latency measurements that anyone can calculate

SELECT '--- ALL 4 latency measurements (in ms) ---' AS step;

SELECT toFloat64(arrayJoin([10, 10, 10, 50])) AS latency_ms;

-- Calculate the TRUE average: (10+10+10+50)/4 = 80/4 = 20ms
SELECT '--- TRUE AVERAGE (verify the math yourself!) ---' AS step;

SELECT
    round(avg(latency), 2) AS true_average_ms,
    count() AS total_requests,
    sum(latency) AS total_sum
FROM (SELECT toFloat64(arrayJoin([10, 10, 10, 50])) AS latency);

-- Result: 20ms from 4 requests (sum=80)
-- REMEMBER: 20ms is the CORRECT answer!


-- ---------------------------------------------------------------------------
-- STEP 2.2: Data arrives in separate batches
-- ---------------------------------------------------------------------------
-- This is realistic: different time windows, different sources, micro-batches

SELECT '--- But data arrives in BATCHES... ---' AS step;

SELECT 'Batch 1: 3 requests [10, 10, 10] -> avg = 10ms' AS batch1;
SELECT 'Batch 2: 1 request [50] -> avg = 50ms' AS batch2;

-- Store pre-computed batch averages (common but WRONG approach!)
INSERT INTO tt_avg_latency_wrong (endpoint, avg_latency, batch_id)
VALUES ('/api/users', 10.0, 1);

INSERT INTO tt_avg_latency_wrong (endpoint, avg_latency, batch_id)
VALUES ('/api/users', 50.0, 2);


-- ---------------------------------------------------------------------------
-- STEP 2.3: THE PROBLEM - avg(avg) gives WRONG answer!
-- ---------------------------------------------------------------------------
-- If we average the batch averages: (10 + 50) / 2 = 30ms ... WRONG!

SELECT '>>> THE PROBLEM: avg(avg) = 30ms ... but true avg is 20ms! <<<' AS problem;

SELECT
    endpoint,
    avg(avg_latency) AS wrong_average,
    'WRONG!' AS status
FROM tt_avg_latency_wrong
GROUP BY endpoint;

-- The dramatic comparison:
SELECT
    '20ms' AS true_average,
    '30ms' AS wrong_average,
    '50% OFF!' AS error_magnitude;

-- Why wrong? Batch 2 had 1 request but got EQUAL weight to Batch 1's 3 requests!
-- This is the "avg of avg" problem - a classic mistake in analytics!


-- ---------------------------------------------------------------------------
-- STEP 2.4: THE SOLUTION - AggregatingMergeTree with State functions
-- ---------------------------------------------------------------------------
-- avgState stores (sum, count) so batches combine correctly!

SELECT '>>> THE SOLUTION: AggregatingMergeTree <<<' AS solution;

-- Insert Batch 1 using avgState (stores sum=30, count=3)
INSERT INTO tt_avg_latency (endpoint, avg_latency, count_requests, sum_latency)
SELECT
    '/api/users' AS endpoint,
    avgState(latency) AS avg_latency,          -- Stores (sum=30, count=3)
    countState(latency) AS count_requests,
    sumState(latency) AS sum_latency
FROM (
    SELECT toFloat64(arrayJoin([10, 10, 10])) AS latency
);

-- Insert Batch 2 using avgState (stores sum=50, count=1)
INSERT INTO tt_avg_latency (endpoint, avg_latency, count_requests, sum_latency)
SELECT
    '/api/users' AS endpoint,
    avgState(latency) AS avg_latency,          -- Stores (sum=50, count=1)
    countState(latency) AS count_requests,
    sumState(latency) AS sum_latency
FROM (
    SELECT toFloat64(50) AS latency
);


-- ---------------------------------------------------------------------------
-- STEP 2.5: Query with Merge functions - CORRECT result!
-- ---------------------------------------------------------------------------

SELECT '>>> CORRECT: avgMerge = 20ms (matches true average!) <<<' AS wow_moment;

SELECT
    endpoint,
    round(avgMerge(avg_latency), 2) AS correct_average,
    countMerge(count_requests) AS total_requests,
    sumMerge(sum_latency) AS total_sum
FROM tt_avg_latency
GROUP BY endpoint;

-- Result: 20ms from 4 requests (sum=80) - CORRECT!
-- avgMerge combines: (30+50)/(3+1) = 80/4 = 20ms


-- ---------------------------------------------------------------------------
-- STEP 2.6: Final comparison - the punchline!
-- ---------------------------------------------------------------------------

SELECT '>>> FINAL COMPARISON <<<' AS section;

SELECT
    'True average' AS method,
    20.00 AS result_ms,
    '(10+10+10+50)/4 = 20' AS math
UNION ALL
SELECT
    'avg(avg) WRONG' AS method,
    30.00 AS result_ms,
    '(10+50)/2 = 30 -- 50% ERROR!' AS math
UNION ALL
SELECT
    'avgMerge CORRECT' AS method,
    20.00 AS result_ms,
    '(30+50)/(3+1) = 20' AS math;

-- TALKING POINT:
-- "avgState stores (sum, count), not just the average.
-- avgMerge combines them correctly: (30+50)/(3+1) = 20ms
-- This is why AggregatingMergeTree is essential for accurate analytics!"




-- ****************************************************************************
-- ****************************************************************************
-- **                                                                        **
-- **  SECTION 3: DICTIONARIES                                               **
-- **  ========================                                               **
-- **                                                                        **
-- **  KEY MESSAGE: O(1) lookups instead of expensive JOINs!                 **
-- **  Dictionaries are in-memory hash tables for blazing fast enrichment.   **
-- **                                                                        **
-- ****************************************************************************
-- ****************************************************************************

USE webinar_demo;

-- ---------------------------------------------------------------------------
-- BUSINESS CONTEXT: Every query joins customer and product tables.
-- With millions of orders, JOINs scan millions of dimension rows.
-- Dictionaries give you O(1) lookups - same speed at ANY scale!
-- ---------------------------------------------------------------------------

SELECT '>>> SECTION 3: Dictionaries <<<' AS demo;


-- ---------------------------------------------------------------------------
-- STEP 3.1: Show dictionaries are loaded in memory
-- ---------------------------------------------------------------------------
-- Dictionaries are pre-loaded hash tables sitting in RAM

SELECT 'Dictionaries loaded in memory:' AS status;

SELECT
    name,
    status,
    element_count AS rows_loaded,
    formatReadableSize(bytes_allocated) AS memory_used
FROM system.dictionaries
WHERE database = 'webinar_demo'
ORDER BY name;


-- ---------------------------------------------------------------------------
-- STEP 3.2: Traditional JOIN approach
-- ---------------------------------------------------------------------------
-- This is how you'd normally get customer/product names

SELECT 'Traditional JOIN approach:' AS approach;

SELECT
    o.order_id,
    c.customer_name,                    -- From dim_customers via JOIN
    c.tier AS customer_tier,            -- From dim_customers via JOIN
    p.product_name,                     -- From dim_products via JOIN
    p.category,                         -- From dim_products via JOIN
    o.quantity,
    o.quantity * p.price AS total       -- Calculate total
FROM orders_fact o
JOIN dim_customers c ON o.customer_id = c.customer_id   -- JOIN #1
JOIN dim_products p ON o.product_id = p.product_id      -- JOIN #2
LIMIT 5;


-- ---------------------------------------------------------------------------
-- STEP 3.3: dictGet approach - O(1) lookups!
-- ---------------------------------------------------------------------------
-- Same result, but using hash table lookups instead of JOINs

SELECT '>>> dictGet approach - O(1) lookups! <<<' AS wow_moment;

SELECT
    order_id,
    -- O(1) lookup from customers_dict
    dictGet('webinar_demo.customers_dict', 'customer_name', customer_id) AS customer_name,
    dictGet('webinar_demo.customers_dict', 'tier', customer_id) AS customer_tier,
    -- O(1) lookup from products_dict
    dictGet('webinar_demo.products_dict', 'product_name', product_id) AS product_name,
    dictGet('webinar_demo.products_dict', 'category', product_id) AS category,
    quantity,
    quantity * dictGet('webinar_demo.products_dict', 'price', product_id) AS total
FROM orders_fact
LIMIT 5;


-- ---------------------------------------------------------------------------
-- STEP 3.4: Scale comparison - THIS is why dictGet wins!
-- ---------------------------------------------------------------------------
-- The real power shows at scale. Let's visualize the difference.

SELECT '>>> The SCALE advantage <<<' AS key_insight;

SELECT
    'With 1,000 orders' AS data_scale,
    'JOIN scans 1,000 rows per dimension' AS join_work,
    'dictGet: 1,000 hash lookups (instant)' AS dict_work
UNION ALL
SELECT
    'With 1,000,000 orders' AS data_scale,
    'JOIN scans 1,000,000 rows per dimension' AS join_work,
    'dictGet: 1,000,000 hash lookups (still instant!)' AS dict_work
UNION ALL
SELECT
    'With 1,000,000,000 orders' AS data_scale,
    'JOIN: minutes to hours' AS join_work,
    'dictGet: milliseconds (O(1) = constant time!)' AS dict_work;

-- Quick complexity comparison
SELECT
    'JOIN' AS approach,
    'O(n) - linear growth' AS complexity,
    'Gets slower as data grows' AS behavior
UNION ALL
SELECT
    'dictGet' AS approach,
    'O(1) - constant time' AS complexity,
    'Same speed at ANY scale!' AS behavior;

-- TALKING POINT:
-- "At 1,000 orders, both approaches feel fast.
-- At 1 million orders, JOINs start to hurt.
-- At 1 billion orders? dictGet is still instant!
-- This is why dictionaries are essential for real-time analytics."




-- ****************************************************************************
-- ****************************************************************************
-- **                                                                        **
-- **  SECTION 4: MEDALLION ARCHITECTURE FINALE                              **
-- **  ========================================                               **
-- **                                                                        **
-- **  KEY MESSAGE: All concepts combined in one engine!                     **
-- **  - Incremental MVs for automatic data flow                             **
-- **  - Dictionaries for O(1) enrichment                                    **
-- **  - AggregatingMergeTree for correct aggregations                       **
-- **  - No Spark, no Airflow, no external tools!                            **
-- **                                                                        **
-- ****************************************************************************
-- ****************************************************************************

USE webinar_demo;

-- ---------------------------------------------------------------------------
-- BUSINESS CONTEXT: Your CEO wants real-time dashboards.
-- You have 50,000 orders. Scanning them all takes too long.
-- Watch how ClickHouse compresses this into just 3 rows!
-- ---------------------------------------------------------------------------

SELECT '>>> SECTION 4: Medallion Architecture Finale <<<' AS demo;

SELECT 'We have 50,000 orders. CEO wants instant dashboards. Watch this...' AS setup;


-- ---------------------------------------------------------------------------
-- STEP 4.1: THE PUNCHLINE FIRST - Data compression at each layer
-- ---------------------------------------------------------------------------
-- This is the WOW moment! Lead with the result.

SELECT '>>> Data volume at each layer <<<' AS insight;

-- Build anticipation: Bronze...
SELECT 'BRONZE (raw events):' AS layer, count() AS rows FROM bronze_events;

-- Silver...
SELECT 'SILVER (parsed + enriched):' AS layer, count() AS rows FROM silver_orders_enriched;

-- And Gold... (pause for effect)
SELECT 'GOLD (daily aggregates):' AS layer, count() AS rows FROM gold_sales_day;

-- The dramatic reveal
SELECT
    '50,000 events' AS started_with,
    (SELECT count() FROM gold_sales_day) AS compressed_to,
    'aggregate rows!' AS result;

-- CEO gets instant dashboards because queries hit 3 rows, not 50,000!


-- ---------------------------------------------------------------------------
-- STEP 4.2: How did we get here? The Medallion Pipeline
-- ---------------------------------------------------------------------------

SELECT 'The Medallion Pipeline (all automatic, zero orchestration):' AS architecture;

SELECT
    'BRONZE' AS layer,
    'Raw JSON events' AS data_type,
    'Landing zone' AS purpose
UNION ALL
SELECT
    'SILVER' AS layer,
    'Parsed + enriched (dictGet!)' AS data_type,
    'Clean, typed, enriched' AS purpose
UNION ALL
SELECT
    'GOLD' AS layer,
    'Pre-aggregated (AggregatingMT)' AS data_type,
    'Instant business metrics' AS purpose;


-- ---------------------------------------------------------------------------
-- STEP 4.3: Peek at Silver - enrichment with dictGet
-- ---------------------------------------------------------------------------
-- Notice: customer_name, product_name came from dictionaries!
-- Added automatically via MV, not at query time!

SELECT 'Silver layer sample (enriched via dictGet in MV):' AS layer;

SELECT
    customer_name,       -- From customers_dict
    customer_tier,       -- From customers_dict
    product_name,        -- From products_dict
    category,            -- From products_dict
    quantity,
    round(total_amount, 2) AS total
FROM silver_orders_enriched
ORDER BY order_time DESC
LIMIT 3;


-- ---------------------------------------------------------------------------
-- STEP 4.4: Query Gold - instant results from 3 rows!
-- ---------------------------------------------------------------------------
-- Uses Merge functions (remember Section 2? avgMerge, sumMerge...)

SELECT '>>> Gold layer query - instant! <<<' AS wow_moment;

SELECT
    category,
    countMerge(total_orders) AS orders,
    round(sumMerge(total_revenue), 2) AS revenue,
    round(sumMerge(total_profit), 2) AS profit,
    uniqMerge(unique_customers) AS unique_customers
FROM gold_sales_day
GROUP BY category
ORDER BY revenue DESC;

-- This query scanned 3 rows. A query on Bronze would scan 50,000!


-- ---------------------------------------------------------------------------
-- STEP 4.5: Everything ties together
-- ---------------------------------------------------------------------------

SELECT '>>> All 3 concepts working together <<<' AS summary;

SELECT '1. Incremental MVs' AS feature, 'Data flows automatically (Section 1)' AS callback
UNION ALL
SELECT '2. AggregatingMergeTree' AS feature, 'Correct aggregations with State/Merge (Section 2)' AS callback
UNION ALL
SELECT '3. Dictionaries' AS feature, 'O(1) enrichment with dictGet (Section 3)' AS callback
UNION ALL
SELECT '4. Medallion' AS feature, 'All combined: Bronze -> Silver -> Gold' AS callback;


-- ---------------------------------------------------------------------------
-- STEP 4.6: LIVE FINALE - Insert new order, watch it flow!
-- ---------------------------------------------------------------------------
-- This is the grand finale! Real-time data flowing through the entire pipeline.

SELECT '>>> LIVE FINALE: Insert new order, watch it cascade! <<<' AS live_demo;

-- Insert a single order into Bronze
INSERT INTO bronze_events (event_type, source_system, payload) VALUES
    ('order', 'live_demo', '{"order_id":"550e8400-e29b-41d4-a716-446655440099","customer_id":1003,"product_id":6,"quantity":2,"price":79.99}');

SELECT 'Just inserted: Carol bought 2 Keyboards...' AS action;

-- Show it arrived in Silver - already enriched with dictGet!
SELECT
    'It flowed through the ENTIRE pipeline automatically:' AS result;

SELECT
    customer_name,
    customer_tier,
    product_name,
    category,
    quantity,
    round(total_amount, 2) AS total
FROM silver_orders_enriched
WHERE order_id = '550e8400-e29b-41d4-a716-446655440099'::UUID;

-- The punchline
SELECT
    'Bronze -> Silver (with dictGet) -> Gold (with AggregatingMT)' AS pipeline,
    'All automatic. All in milliseconds. All in ClickHouse.' AS result;


-- ---------------------------------------------------------------------------
-- STEP 4.7: THE CLOSING LINE
-- ---------------------------------------------------------------------------

SELECT '>>> FINAL MESSAGE <<<' AS finale;

SELECT 'No Spark. No Airflow. No Kafka Connect. No external tools.' AS what_we_didnt_need;

SELECT 'Just ClickHouse.' AS what_we_used;

-- ---------------------------------------------------------------------------
-- END OF DEMO
-- ---------------------------------------------------------------------------




-- ****************************************************************************
-- ****************************************************************************
-- **                                                                        **
-- **  SECTION 5: CLEANUP                                                    **
-- **  =================                                                     **
-- **  Run when done to remove all demo objects                              **
-- **                                                                        **
-- ****************************************************************************
-- ****************************************************************************

-- Uncomment to run:
-- DROP DATABASE IF EXISTS webinar_demo;
-- SELECT 'Database webinar_demo dropped' AS status;


-- ============================================================================
-- END OF DEMO
-- ============================================================================
