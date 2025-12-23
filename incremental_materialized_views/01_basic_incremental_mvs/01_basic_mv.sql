-- ================================================
-- Example 1: Create the Materialized View
-- ================================================
-- KEY CONCEPT: A Materialized View in ClickHouse is an INSERT trigger
-- It runs automatically on EVERY INSERT to the source table
-- No cron jobs, no batch processing, no external orchestration needed
-- ================================================

USE mv_demo_basic;

-- ================================================
-- The Materialized View
-- ================================================
DROP VIEW IF EXISTS page_views_mv;

CREATE MATERIALIZED VIEW page_views_mv
TO page_views_count
AS
SELECT
    page_url,
    count() AS view_count,
    now() AS last_updated
FROM page_views_raw
GROUP BY page_url;

-- ================================================
-- TALKING POINT
-- ================================================
-- "This MV will trigger on EVERY INSERT to page_views_raw.
-- The SELECT query runs automatically, and results go to page_views_count.
-- Zero orchestration - data flows in real-time!"

SELECT '[OK] Materialized View created: page_views_mv' AS status;
SELECT 'MV triggers on INSERT to page_views_raw -> writes to page_views_count' AS flow;
