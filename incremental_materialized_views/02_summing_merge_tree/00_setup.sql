-- ================================================
-- Example 2: SummingMergeTree with Incremental MVs
-- ================================================
-- Scenario: Website metrics dashboard
-- Key Concept: SummingMergeTree auto-sums numeric columns on merge
-- ================================================

-- Create database
CREATE DATABASE IF NOT EXISTS mv_demo_summing;
USE mv_demo_summing;

-- ================================================
-- Source Table: Raw website events
-- ================================================
DROP TABLE IF EXISTS events_raw;

CREATE TABLE events_raw (
    event_id UUID DEFAULT generateUUIDv4(),
    event_type String,              -- 'pageview', 'click', 'purchase'
    page_url String,
    user_id UInt64,
    revenue Decimal64(2) DEFAULT 0,
    event_time DateTime DEFAULT now()
)
ENGINE = MergeTree()
ORDER BY (event_time, event_id);

SELECT '[OK] Source table created: events_raw' AS status;
