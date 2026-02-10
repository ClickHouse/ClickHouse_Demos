-- Create telco database
CREATE DATABASE IF NOT EXISTS telco;

-- Customers table
CREATE TABLE IF NOT EXISTS telco.customers (
    customer_id String,
    email String,
    phone_number String,
    first_name String,
    last_name String,
    age UInt8,
    gender String,
    address String,
    city String,
    state String,
    zip_code String,
    signup_date Date,
    plan_type String,
    device_type String,
    segment String,
    monthly_spend Decimal(10, 2),
    lifetime_value Decimal(10, 2),
    churn_probability Decimal(5, 3),
    is_churned Bool,
    created_at DateTime
) ENGINE = MergeTree()
ORDER BY (customer_id, signup_date)
PARTITION BY toYYYYMM(signup_date);

-- Call Detail Records (CDRs) table
CREATE TABLE IF NOT EXISTS telco.call_detail_records (
    cdr_id String,
    customer_id String,
    timestamp DateTime,
    event_type String,
    duration_seconds UInt32,
    data_mb Decimal(10, 2),
    base_station_id String,
    cost Decimal(10, 2),
    created_at DateTime
) ENGINE = MergeTree()
ORDER BY (customer_id, timestamp)
PARTITION BY toYYYYMM(timestamp);

-- Network Events table
CREATE TABLE IF NOT EXISTS telco.network_events (
    event_id String,
    timestamp DateTime,
    event_type String,
    base_station_id String,
    region String,
    technology String,
    bandwidth_mbps Decimal(10, 2),
    latency_ms Decimal(10, 2),
    packet_loss_pct Decimal(5, 3),
    severity String,
    is_anomaly Bool,
    created_at DateTime
) ENGINE = MergeTree()
ORDER BY (timestamp, base_station_id)
PARTITION BY toYYYYMM(timestamp);

-- Marketing Campaigns table
CREATE TABLE IF NOT EXISTS telco.marketing_campaigns (
    campaign_id String,
    campaign_name String,
    campaign_type String,
    start_date Date,
    end_date Date,
    target_segment String,
    channel String,
    budget Decimal(12, 2),
    impressions UInt32,
    clicks UInt32,
    conversions UInt32,
    revenue_generated Decimal(12, 2),
    created_at DateTime
) ENGINE = MergeTree()
ORDER BY (campaign_id, start_date)
PARTITION BY toYYYYMM(start_date);

-- Create materialized views for common analytics queries

-- Customer usage summary view
CREATE MATERIALIZED VIEW IF NOT EXISTS telco.customer_usage_summary
ENGINE = SummingMergeTree()
ORDER BY (customer_id, date)
AS SELECT
    customer_id,
    toDate(timestamp) as date,
    countIf(event_type = 'voice_call') as total_calls,
    sumIf(duration_seconds, event_type = 'voice_call') as total_call_duration,
    countIf(event_type = 'data_session') as total_data_sessions,
    sumIf(data_mb, event_type = 'data_session') as total_data_mb,
    sum(cost) as total_cost
FROM telco.call_detail_records
GROUP BY customer_id, date;

-- Network health summary view
CREATE MATERIALIZED VIEW IF NOT EXISTS telco.network_health_summary
ENGINE = AggregatingMergeTree()
ORDER BY (base_station_id, hour)
AS SELECT
    base_station_id,
    toStartOfHour(timestamp) as hour,
    region,
    technology,
    avgState(bandwidth_mbps) as avg_bandwidth,
    avgState(latency_ms) as avg_latency,
    avgState(packet_loss_pct) as avg_packet_loss,
    countIf(is_anomaly = true) as anomaly_count,
    count() as total_events
FROM telco.network_events
GROUP BY base_station_id, hour, region, technology;

-- Campaign performance view
CREATE MATERIALIZED VIEW IF NOT EXISTS telco.campaign_performance
ENGINE = SummingMergeTree()
ORDER BY (campaign_id, target_segment)
AS SELECT
    campaign_id,
    campaign_name,
    campaign_type,
    target_segment,
    channel,
    budget,
    impressions,
    clicks,
    conversions,
    revenue_generated,
    revenue_generated - budget as roi
FROM telco.marketing_campaigns;
