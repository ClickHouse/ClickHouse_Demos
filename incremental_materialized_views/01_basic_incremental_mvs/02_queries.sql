-- ================================================
-- Example 1: Demo Queries
-- ================================================
-- Run these step-by-step to demonstrate MVs in action
-- ================================================

USE mv_demo_basic;

-- ================================================
-- Step 1: Check initial state (should be empty)
-- ================================================
SELECT '--- Step 1: Initial state (empty) ---' AS step;
SELECT page_url, view_count FROM page_views_count;

-- ================================================
-- Step 2: Insert some page views
-- ================================================
SELECT '--- Step 2: Inserting 5 page views ---' AS step;

INSERT INTO page_views_raw (page_url, user_id) VALUES
    ('/home', 1001),
    ('/products', 1002),
    ('/home', 1003),
    ('/checkout', 1001),
    ('/home', 1004);

-- ================================================
-- Step 3: Check the target table - AUTOMATICALLY updated!
-- ================================================
SELECT '--- Step 3: After INSERT (automatic!) ---' AS step;
SELECT page_url, view_count FROM page_views_count ORDER BY view_count DESC;

-- ================================================
-- TALKING POINT
-- ================================================
-- "Notice we didn't run any ETL job or cron script.
-- The MV triggered automatically on INSERT.
-- Results are already in page_views_count!"

-- ================================================
-- Step 4: Insert more data
-- ================================================
SELECT '--- Step 4: Inserting 3 more page views ---' AS step;

INSERT INTO page_views_raw (page_url, user_id) VALUES
    ('/home', 1005),
    ('/products', 1006),
    ('/products', 1007);

-- ================================================
-- Step 5: Observe incremental updates
-- ================================================
SELECT '--- Step 5: After second INSERT ---' AS step;
SELECT page_url, view_count FROM page_views_count ORDER BY view_count DESC;

-- ================================================
-- KEY INSIGHT
-- ================================================
-- "Notice the counts are ADDITIVE - each INSERT batch creates new rows.
-- The MV processes each INSERT separately.
-- To get totals, we need to SUM:"

SELECT '--- Aggregated totals (manual SUM) ---' AS step;
SELECT
    page_url,
    sum(view_count) AS total_views
FROM page_views_count
GROUP BY page_url
ORDER BY total_views DESC;

-- ================================================
-- TRANSITION TO EXAMPLE 2
-- ================================================
-- "This works, but we have to manually aggregate.
-- What if ClickHouse could automatically merge these rows?
-- That's where SummingMergeTree comes in..."

SELECT '[OK] Demo complete. Next: Example 2 - SummingMergeTree' AS next_step;
