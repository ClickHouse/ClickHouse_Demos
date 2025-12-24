-- ================================================
-- Example 2: Demo Queries
-- ================================================
-- Demonstrates SummingMergeTree auto-aggregation
-- ================================================

USE mv_demo_summing;

-- ================================================
-- Step 1: Insert first batch of events
-- ================================================
SELECT '--- Step 1: Inserting first batch ---' AS step;

INSERT INTO events_raw (event_type, page_url, user_id, revenue) VALUES
    ('pageview', '/home', 1001, 0),
    ('pageview', '/products', 1002, 0),
    ('click', '/home', 1001, 0),
    ('purchase', '/checkout', 1001, 99.99);

-- ================================================
-- Step 2: Check raw metrics (may show multiple rows per key)
-- ================================================
SELECT '--- Step 2: Raw hourly_metrics (before merge) ---' AS step;
SELECT hour, page_url, pageviews, clicks, purchases, revenue
FROM hourly_metrics
ORDER BY page_url;

-- ================================================
-- Step 3: Use FINAL to see merged results
-- ================================================
SELECT '--- Step 3: With FINAL (merged view) ---' AS step;
SELECT hour, page_url, pageviews, clicks, purchases, revenue
FROM hourly_metrics FINAL
ORDER BY page_url;

-- ================================================
-- TALKING POINT
-- ================================================
-- "FINAL forces ClickHouse to merge rows with the same key on-the-fly.
-- Without FINAL, you might see multiple rows that will eventually be merged."

-- ================================================
-- Step 4: Insert more events for same hour
-- ================================================
SELECT '--- Step 4: Inserting second batch (same hour) ---' AS step;

INSERT INTO events_raw (event_type, page_url, user_id, revenue) VALUES
    ('pageview', '/home', 1003, 0),
    ('pageview', '/home', 1004, 0),
    ('click', '/products', 1002, 0),
    ('purchase', '/checkout', 1002, 149.99);

-- ================================================
-- Step 5: See automatic summing with FINAL
-- ================================================
SELECT '--- Step 5: After second INSERT (with FINAL) ---' AS step;
SELECT hour, page_url, pageviews, clicks, purchases, revenue
FROM hourly_metrics FINAL
ORDER BY page_url;

-- ================================================
-- TALKING POINT
-- ================================================
-- "Notice /home now has 3 pageviews and /checkout has 249.98 revenue.
-- SummingMergeTree automatically combined the rows!"

-- ================================================
-- Step 6: Force merge to see final result
-- ================================================
SELECT '--- Step 6: After OPTIMIZE (fully merged) ---' AS step;
OPTIMIZE TABLE hourly_metrics FINAL;

SELECT hour, page_url, pageviews, clicks, purchases, revenue
FROM hourly_metrics FINAL
ORDER BY page_url;

-- ================================================
-- Step 7: Best practice - explicit aggregation
-- ================================================
SELECT '--- Best Practice: Explicit GROUP BY (production queries) ---' AS step;
SELECT
    hour,
    page_url,
    sum(pageviews) AS total_pageviews,
    sum(clicks) AS total_clicks,
    sum(purchases) AS total_purchases,
    sum(revenue) AS total_revenue
FROM hourly_metrics
GROUP BY hour, page_url
ORDER BY page_url;

-- ================================================
-- LIMITATION REVEALED
-- ================================================
SELECT '--- Limitation: What about COUNT DISTINCT? ---' AS step;

-- Let's try to count unique users (same user_id 1001 multiple times)
INSERT INTO events_raw (event_type, page_url, user_id, revenue) VALUES
    ('pageview', '/home', 1001, 0),
    ('pageview', '/home', 1001, 0);

SELECT
    'SummingMergeTree limitation' AS issue,
    'Cannot do COUNT DISTINCT or AVG' AS problem,
    'Only simple SUM operations work' AS constraint;

-- ================================================
-- TRANSITION TO EXAMPLE 3
-- ================================================
-- "SummingMergeTree is great for counters and sums,
-- but what about COUNT DISTINCT users or AVG order value?
-- For that, we need AggregatingMergeTree..."

SELECT '[OK] Demo complete. Next: Example 3 - AggregatingMergeTree' AS next_step;
