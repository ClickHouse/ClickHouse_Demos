-- ================================================
-- Example 2: Create SummingMergeTree Table
-- ================================================
-- KEY CONCEPT: SummingMergeTree automatically sums numeric columns
-- when rows with the same ORDER BY key are merged during background compaction
-- ================================================

USE mv_demo_summing;

-- ================================================
-- Target Table: Hourly metrics with SummingMergeTree
-- ================================================
DROP TABLE IF EXISTS hourly_metrics;

CREATE TABLE hourly_metrics (
    hour DateTime,
    page_url String,

    -- These columns will be AUTOMATICALLY SUMMED during merge:
    pageviews UInt64,
    clicks UInt64,
    purchases UInt64,
    revenue Decimal64(2)
)
ENGINE = SummingMergeTree()
ORDER BY (hour, page_url);

-- ================================================
-- TALKING POINT
-- ================================================
-- "SummingMergeTree is like MergeTree, but with a superpower:
-- When rows have the same ORDER BY key (hour, page_url),
-- ClickHouse automatically SUMS the numeric columns during merge.
-- No manual aggregation needed!"

SELECT '[OK] SummingMergeTree table created: hourly_metrics' AS status;
SELECT 'Numeric columns (pageviews, clicks, purchases, revenue) will auto-sum on merge' AS feature;
