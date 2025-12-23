-- ================================================
-- Example 1: Basic Incremental Materialized Views
-- ================================================
-- Scenario: Simple page view tracking
-- Key Concept: MVs are INSERT triggers, not cached queries
-- ================================================

-- Create database
CREATE DATABASE IF NOT EXISTS mv_demo_basic;
USE mv_demo_basic;

-- ================================================
-- Source Table: Raw page views
-- ================================================
DROP TABLE IF EXISTS page_views_raw;

CREATE TABLE page_views_raw (
    view_id UUID DEFAULT generateUUIDv4(),
    page_url String,
    user_id UInt64,
    view_time DateTime DEFAULT now()
)
ENGINE = MergeTree()
ORDER BY (view_time, view_id);

-- ================================================
-- Target Table: Aggregated page view counts
-- ================================================
DROP TABLE IF EXISTS page_views_count;

CREATE TABLE page_views_count (
    page_url String,
    view_count UInt64,
    last_updated DateTime DEFAULT now()
)
ENGINE = MergeTree()
ORDER BY page_url;

SELECT '[OK] Tables created: page_views_raw, page_views_count' AS status;
