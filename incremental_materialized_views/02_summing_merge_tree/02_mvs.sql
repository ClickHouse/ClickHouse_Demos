-- ================================================
-- Example 2: Create MV to Populate SummingMergeTree
-- ================================================

USE mv_demo_summing;

-- ================================================
-- MV: Transform events into hourly metrics
-- ================================================
DROP VIEW IF EXISTS hourly_metrics_mv;

CREATE MATERIALIZED VIEW hourly_metrics_mv
TO hourly_metrics
AS
SELECT
    toStartOfHour(event_time) AS hour,
    page_url,
    countIf(event_type = 'pageview') AS pageviews,
    countIf(event_type = 'click') AS clicks,
    countIf(event_type = 'purchase') AS purchases,
    sumIf(revenue, event_type = 'purchase') AS revenue
FROM events_raw
GROUP BY hour, page_url;

-- ================================================
-- TALKING POINT
-- ================================================
-- "This MV aggregates events by hour and page.
-- It writes to SummingMergeTree, which will further combine
-- rows with the same (hour, page_url) key over time."

SELECT '[OK] MV created: hourly_metrics_mv -> hourly_metrics' AS status;
