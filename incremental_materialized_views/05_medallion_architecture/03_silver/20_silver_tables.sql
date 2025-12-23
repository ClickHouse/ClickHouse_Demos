-- Step 4: Silver Layer Tables
-- Parsed, validated tables with strong typing (30 day TTL)

USE fastmart_demo;

-- Parsed orders
DROP TABLE IF EXISTS orders_silver;
CREATE TABLE orders_silver (
    order_id UUID,
    customer_id UInt64,
    product_id UInt64,
    quantity UInt32,
    price Decimal64(2),
    total_amount Decimal64(2),
    payment_method LowCardinality(String),
    order_time DateTime64(3),
    source_system LowCardinality(String),
    ingestion_time DateTime64(3),
    INDEX idx_customer customer_id TYPE minmax GRANULARITY 4,
    INDEX idx_product product_id TYPE minmax GRANULARITY 4
)
ENGINE = MergeTree()
PARTITION BY toYYYYMMDD(order_time)
ORDER BY (order_time, customer_id, order_id)
TTL order_time + INTERVAL 30 DAY;

-- Parsed clicks
DROP TABLE IF EXISTS clicks_silver;
CREATE TABLE clicks_silver (
    click_id UUID,
    session_id String,
    customer_id UInt64,
    page String,
    action LowCardinality(String),
    product_id UInt64,
    duration_seconds UInt32,
    click_time DateTime64(3),
    source_system LowCardinality(String),
    ingestion_time DateTime64(3),
    INDEX idx_session session_id TYPE bloom_filter(0.01) GRANULARITY 4,
    INDEX idx_customer customer_id TYPE minmax GRANULARITY 4
)
ENGINE = MergeTree()
PARTITION BY toYYYYMMDD(click_time)
ORDER BY (click_time, customer_id, click_id)
TTL click_time + INTERVAL 30 DAY;

-- Parsed inventory
DROP TABLE IF EXISTS inventory_silver;
CREATE TABLE inventory_silver (
    update_id UUID,
    product_id UInt64,
    warehouse_id UInt32,
    quantity_change Int32,
    new_stock_level UInt32,
    reason LowCardinality(String),
    update_time DateTime64(3),
    source_system LowCardinality(String),
    ingestion_time DateTime64(3),
    INDEX idx_product product_id TYPE minmax GRANULARITY 4,
    INDEX idx_warehouse warehouse_id TYPE set(0) GRANULARITY 4
)
ENGINE = MergeTree()
PARTITION BY toYYYYMMDD(update_time)
ORDER BY (update_time, product_id, update_id)
TTL update_time + INTERVAL 30 DAY;

-- Enriched orders (populated by MV with dictGet)
DROP TABLE IF EXISTS orders_enriched;
CREATE TABLE orders_enriched (
    order_id UUID,
    customer_id UInt64,
    customer_name String,
    customer_tier LowCardinality(String),
    product_id UInt64,
    product_name String,
    category LowCardinality(String),
    brand LowCardinality(String),
    quantity UInt32,
    unit_price Decimal64(2),
    total_amount Decimal64(2),
    profit_margin Decimal64(2),
    payment_method LowCardinality(String),
    order_time DateTime64(3),
    source_system LowCardinality(String),
    INDEX idx_category category TYPE set(0) GRANULARITY 4,
    INDEX idx_brand brand TYPE set(0) GRANULARITY 4,
    INDEX idx_tier customer_tier TYPE set(0) GRANULARITY 4
)
ENGINE = MergeTree()
PARTITION BY toYYYYMMDD(order_time)
ORDER BY (order_time, category, customer_id, order_id)
TTL order_time + INTERVAL 30 DAY;

SELECT '[OK] Silver tables created' AS status;
