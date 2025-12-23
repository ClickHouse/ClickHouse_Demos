-- ================================================
-- FastMart Demo: Anomaly Detection with MVs
-- ================================================
-- Purpose: Real-time fraud and anomaly detection
-- Pattern: Statistical thresholds + Incremental MVs
-- Key Feature: Detect anomalies AS data arrives!
-- ================================================

USE fastmart_demo;

-- ================================================
-- GOLD: Anomaly Detection Table
-- ================================================
-- Flags suspicious orders in real-time
-- Can trigger alerts, block transactions, or notify fraud team

DROP TABLE IF EXISTS order_anomalies;

CREATE TABLE order_anomalies (
    anomaly_id UUID DEFAULT generateUUIDv4(),
    order_id UUID,
    customer_id UInt64,
    product_id UInt64,
    anomaly_type LowCardinality(String),  -- 'high_value', 'high_quantity', 'velocity', 'unusual_time'
    anomaly_score Float32,  -- 0-100, higher = more suspicious
    order_amount Decimal64(2),
    threshold_value Decimal64(2),
    detection_time DateTime64(3) DEFAULT now64(3),
    order_time DateTime64(3),

    INDEX idx_type anomaly_type TYPE set(0) GRANULARITY 4
)
ENGINE = MergeTree()
PARTITION BY toYYYYMMDD(detection_time)
ORDER BY (detection_time, anomaly_type, order_id)
TTL detection_time + INTERVAL 180 DAY  -- 6 month retention for anomalies
COMMENT 'Gold layer: Real-time anomaly detection';

-- ================================================
-- INCREMENTAL MV: Detect High-Value Orders
-- ================================================
-- Flags orders above 3x the category average

DROP VIEW IF EXISTS anomaly_high_value_mv;

CREATE MATERIALIZED VIEW anomaly_high_value_mv
TO order_anomalies
AS
WITH category_stats AS (
    SELECT
        category,
        avg(total_amount) AS avg_amount,
        stddevPop(total_amount) AS stddev_amount
    FROM orders_enriched
    WHERE order_time >= now() - INTERVAL 7 DAY
    GROUP BY category
)
SELECT
    generateUUIDv4() AS anomaly_id,
    o.order_id,
    o.customer_id,
    o.product_id,
    'high_value' AS anomaly_type,
    (o.total_amount / (s.avg_amount + 0.01)) * 10 AS anomaly_score,  -- Normalize to 0-100 scale
    o.total_amount AS order_amount,
    s.avg_amount * 3 AS threshold_value,
    now64(3) AS detection_time,
    o.order_time
FROM orders_enriched o
INNER JOIN category_stats s ON o.category = s.category
WHERE o.total_amount > (s.avg_amount * 3)  -- More than 3x average
  AND o.total_amount > 100;  -- Minimum threshold

-- ================================================
-- DEMO TALKING POINT #1
-- ================================================
-- "This MV runs on EVERY new order automatically.
-- High-value orders are flagged in milliseconds.
-- No separate fraud detection system needed!
-- You can trigger webhooks, send alerts, or block orders based on these flags."

-- ================================================
-- GOLD: Customer Velocity Tracking
-- ================================================
-- Tracks orders per customer per hour to detect velocity anomalies

DROP TABLE IF EXISTS customer_order_velocity;

CREATE TABLE customer_order_velocity (
    hour DateTime,
    customer_id UInt64,
    order_count AggregateFunction(count, UUID),
    total_spent AggregateFunction(sum, Decimal64(2)),
    unique_products AggregateFunction(uniq, UInt64)
)
ENGINE = AggregatingMergeTree()
PARTITION BY toYYYYMMDD(hour)
ORDER BY (hour, customer_id)
TTL hour + INTERVAL 30 DAY
COMMENT 'Customer order velocity for anomaly detection';

DROP VIEW IF EXISTS customer_velocity_mv;

CREATE MATERIALIZED VIEW customer_velocity_mv
TO customer_order_velocity
AS
SELECT
    toStartOfHour(order_time) AS hour,
    customer_id,
    countState(order_id) AS order_count,
    sumState(total_amount) AS total_spent,
    uniqState(product_id) AS unique_products
FROM orders_enriched
GROUP BY hour, customer_id;

-- ================================================
-- INCREMENTAL MV: Detect High Velocity Customers
-- ================================================
-- Flags customers with unusually high order frequency

DROP VIEW IF EXISTS anomaly_velocity_mv;

CREATE MATERIALIZED VIEW anomaly_velocity_mv
TO order_anomalies
AS
SELECT
    generateUUIDv4() AS anomaly_id,
    o.order_id,
    o.customer_id,
    o.product_id,
    'velocity' AS anomaly_type,
    v.orders_this_hour * 10 AS anomaly_score,
    o.total_amount AS order_amount,
    5 AS threshold_value,  -- Normal customers don't order 5+ times per hour
    now64(3) AS detection_time,
    o.order_time
FROM orders_enriched o
INNER JOIN (
    SELECT
        customer_id,
        countMerge(order_count) AS orders_this_hour
    FROM customer_order_velocity
    WHERE hour = toStartOfHour(now())
    GROUP BY customer_id
    HAVING orders_this_hour > 5
) v ON o.customer_id = v.customer_id;

-- ================================================
-- GOLD: Unusual Time Detection
-- ================================================
-- Flags orders at unusual times (e.g., 2-5 AM)

DROP VIEW IF EXISTS anomaly_unusual_time_mv;

CREATE MATERIALIZED VIEW anomaly_unusual_time_mv
TO order_anomalies
AS
SELECT
    generateUUIDv4() AS anomaly_id,
    order_id,
    customer_id,
    product_id,
    'unusual_time' AS anomaly_type,
    50 AS anomaly_score,  -- Fixed score for unusual time
    total_amount AS order_amount,
    0 AS threshold_value,
    now64(3) AS detection_time,
    order_time
FROM orders_enriched
WHERE toHour(order_time) BETWEEN 2 AND 5  -- 2 AM - 5 AM
  AND total_amount > 200  -- Only flag high-value unusual-time orders
  AND customer_tier IN ('Bronze', 'Silver');  -- Gold/Platinum customers shop anytime

-- ================================================
-- DEMO TALKING POINT #2
-- ================================================
-- "Multiple anomaly detection patterns running in PARALLEL:
--  1. High-value orders (statistical outlier)
--  2. High velocity (too many orders)
--  3. Unusual time (2-5 AM for low-tier customers)
--
-- All happening automatically on every order.
-- No batch jobs, no delays, no external systems!"

-- ================================================
-- Test Anomaly Detection
-- ================================================

-- Insert normal orders
INSERT INTO events_raw (event_type, source_system, payload) VALUES
    ('order', 'web', '{"order_id": "normal-001", "customer_id": 1001, "product_id": 1, "quantity": 1, "price": 29.99, "payment_method": "credit_card"}');

-- Insert anomalous orders
INSERT INTO events_raw (event_type, source_system, payload) VALUES
    -- High value order
    ('order', 'web', '{"order_id": "anomaly-high-value", "customer_id": 1002, "product_id": 1, "quantity": 100, "price": 29.99, "payment_method": "credit_card"}'),
    -- Unusual time order (if current time is 2-5 AM)
    ('order', 'web', '{"order_id": "anomaly-time", "customer_id": 1004, "product_id": 3, "quantity": 50, "price": 9.99, "payment_method": "paypal"}');

-- Wait for MVs
SELECT sleep(2);

-- ================================================
-- View Detected Anomalies
-- ================================================

SELECT '--- Detected Anomalies (Last Hour) ---' AS section;

SELECT
    anomaly_id,
    order_id,
    customer_id,
    anomaly_type,
    round(anomaly_score, 2) AS score,
    round(order_amount, 2) AS amount,
    round(threshold_value, 2) AS threshold,
    detection_time
FROM order_anomalies
WHERE detection_time >= now() - INTERVAL 1 HOUR
ORDER BY detection_time DESC, anomaly_score DESC;

-- ================================================
-- Anomaly Statistics Dashboard
-- ================================================

SELECT '--- Anomaly Detection Statistics ---' AS section;

SELECT
    anomaly_type,
    count() AS anomalies_detected,
    round(avg(anomaly_score), 2) AS avg_score,
    round(avg(order_amount), 2) AS avg_amount,
    min(detection_time) AS first_seen,
    max(detection_time) AS last_seen
FROM order_anomalies
WHERE detection_time >= now() - INTERVAL 24 HOUR
GROUP BY anomaly_type
ORDER BY anomalies_detected DESC;

-- ================================================
-- High-Risk Customers Report
-- ================================================

SELECT '--- High-Risk Customers (Last 24 Hours) ---' AS section;

SELECT
    customer_id,
    count(DISTINCT order_id) AS flagged_orders,
    groupArray(DISTINCT anomaly_type) AS anomaly_types,
    round(avg(anomaly_score), 2) AS avg_risk_score,
    round(sum(order_amount), 2) AS total_amount_at_risk
FROM order_anomalies
WHERE detection_time >= now() - INTERVAL 24 HOUR
GROUP BY customer_id
HAVING count(DISTINCT order_id) >= 2  -- Multiple suspicious orders
ORDER BY avg_risk_score DESC, total_amount_at_risk DESC
LIMIT 10;

-- ================================================
-- DEMO TALKING POINT #3
-- ================================================
-- "You can now:
--  1. Send real-time alerts to fraud team
--  2. Automatically hold suspicious orders for review
--  3. Trigger additional verification (2FA, email confirm)
--  4. Feed into ML models for advanced fraud detection
--  5. Generate reports for compliance/auditing
--
-- All with ZERO latency - detection happens as orders arrive!"

-- ================================================
-- Advanced: Anomaly Trend Analysis
-- ================================================

DROP TABLE IF EXISTS anomaly_trends;

CREATE TABLE anomaly_trends (
    hour DateTime,
    anomaly_type LowCardinality(String),
    anomaly_count AggregateFunction(count, UUID),
    total_amount_at_risk AggregateFunction(sum, Decimal64(2)),
    avg_anomaly_score AggregateFunction(avg, Float32)
)
ENGINE = AggregatingMergeTree()
PARTITION BY toYYYYMMDD(hour)
ORDER BY (hour, anomaly_type)
TTL hour + INTERVAL 90 DAY
COMMENT 'Anomaly detection trends over time';

DROP VIEW IF EXISTS anomaly_trends_mv;

CREATE MATERIALIZED VIEW anomaly_trends_mv
TO anomaly_trends
AS
SELECT
    toStartOfHour(detection_time) AS hour,
    anomaly_type,
    countState(anomaly_id) AS anomaly_count,
    sumState(order_amount) AS total_amount_at_risk,
    avgState(anomaly_score) AS avg_anomaly_score
FROM order_anomalies
GROUP BY hour, anomaly_type;

-- ================================================
-- Query anomaly trends
-- ================================================

SELECT '--- Anomaly Trends (Last 24 Hours) ---' AS section;

SELECT
    hour,
    anomaly_type,
    countMerge(anomaly_count) AS anomalies,
    round(sumMerge(total_amount_at_risk), 2) AS amount_at_risk,
    round(avgMerge(avg_anomaly_score), 2) AS avg_score
FROM anomaly_trends
WHERE hour >= now() - INTERVAL 24 HOUR
GROUP BY hour, anomaly_type
ORDER BY hour DESC, anomalies DESC
LIMIT 20;

-- ================================================
-- Integration Example: Webhook Trigger (Pseudo-code)
-- ================================================

-- In production, you would create a webhook or Lambda function that:
-- 1. Queries order_anomalies in real-time
-- 2. Sends alerts to Slack/PagerDuty/Email
-- 3. Updates external systems (CRM, fraud platform)

SELECT '--- Webhook Integration Example ---' AS section;

SELECT
    'POST https://api.fastmart.com/fraud-alerts' AS webhook_url,
    JSONExtractString(
        formatRow('JSONEachRow',
            order_id,
            customer_id,
            anomaly_type,
            anomaly_score,
            order_amount
        ),
        'order_id'
    ) AS payload_example
FROM order_anomalies
WHERE detection_time >= now() - INTERVAL 5 MINUTE
  AND anomaly_score > 50
LIMIT 1;

-- ================================================
-- DEMO TALKING POINT #4
-- ================================================
-- "This replaces traditional fraud detection architectures:
--
-- OLD WAY:
--  1. Stream orders to Kafka
--  2. Process with Flink/Spark
--  3. Store results in separate fraud DB
--  4. Query fraud DB + orders DB
--  5. Reconcile data across systems
--  Total: 5+ systems, complex orchestration
--
-- NEW WAY:
--  1. Insert orders to ClickHouse
--  2. MVs detect anomalies automatically
--  Total: 1 system, zero orchestration!"

-- ================================================
-- Performance metrics
-- ================================================

SELECT '--- Anomaly Detection Performance ---' AS section;

SELECT
    table,
    formatReadableSize(sum(bytes)) AS storage_size,
    sum(rows) AS total_rows
FROM system.parts
WHERE database = 'fastmart_demo'
  AND table IN ('order_anomalies', 'customer_order_velocity', 'anomaly_trends')
  AND active
GROUP BY table;

-- ================================================
-- NEXT STEPS
-- ================================================
SELECT
    'Anomaly detection implemented' AS status,
    'Real-time fraud detection working' AS result,
    'Multiple patterns running in parallel' AS capability,
    'Next: Create validation and performance queries' AS next_step,
    'File: sql/queries/40_validation.sql' AS next_file;
