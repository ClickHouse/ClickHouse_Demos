-- Step 3: Bronze Layer
-- Raw events table with JSON payloads (7 day TTL)
-- NOTE: Do NOT insert data here. MVs must be created first.

USE fastmart_demo;

DROP TABLE IF EXISTS events_raw;

CREATE TABLE events_raw (
    event_id UUID DEFAULT generateUUIDv4(),
    event_time DateTime64(3) DEFAULT now64(3),
    event_type LowCardinality(String),
    source_system LowCardinality(String) DEFAULT 'web',
    payload String,
    ingestion_time DateTime64(3) DEFAULT now64(3),

    INDEX idx_event_type event_type TYPE set(0) GRANULARITY 4,
    INDEX idx_source source_system TYPE set(0) GRANULARITY 4
)
ENGINE = MergeTree()
PARTITION BY toYYYYMMDD(event_time)
ORDER BY (event_time, event_type, event_id)
TTL event_time + INTERVAL 7 DAY
SETTINGS index_granularity = 8192;

SELECT '[OK] Bronze: events_raw created (empty - insert after MVs are ready)' AS step;
