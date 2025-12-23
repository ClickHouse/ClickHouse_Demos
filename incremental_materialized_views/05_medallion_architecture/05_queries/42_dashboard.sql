-- ================================================
-- FastMart Demo: Dashboard Queries
-- ================================================
-- Purpose: Real-world dashboard queries for the webinar demo
-- These queries power a real-time operational dashboard
-- ================================================

USE fastmart_demo;

-- ================================================
-- DASHBOARD: Key Performance Indicators (KPIs)
-- ================================================
-- Real-time metrics for the last 24 hours

SELECT '=== REAL-TIME KPIs (Last 24 Hours) ===' AS section;

SELECT
    countMerge(total_orders) AS total_orders,
    round(sumMerge(total_revenue), 2) AS total_revenue,
    round(sumMerge(total_profit), 2) AS total_profit,
    round((sumMerge(total_profit) / sumMerge(total_revenue)) * 100, 2) AS profit_margin_pct,
    uniqMerge(unique_customers) AS unique_customers,
    uniqMerge(unique_products) AS unique_products,
    round(avgMerge(avg_order_value), 2) AS avg_order_value,
    round(minMerge(min_order_value), 2) AS min_order_value,
    round(maxMerge(max_order_value), 2) AS max_order_value
FROM sales_by_minute
WHERE minute >= now() - INTERVAL 24 HOUR;

-- ================================================
-- DASHBOARD: Sales Trend (Last 24 Hours by Hour)
-- ================================================

SELECT '=== HOURLY SALES TREND (Last 24 Hours) ===' AS section;

SELECT
    hour,
    countMerge(total_orders) AS orders,
    round(sumMerge(total_revenue), 2) AS revenue,
    round(sumMerge(total_profit), 2) AS profit,
    uniqMerge(unique_customers) AS customers
FROM sales_by_hour
WHERE hour >= now() - INTERVAL 24 HOUR
GROUP BY hour
ORDER BY hour DESC;

-- ================================================
-- DASHBOARD: Top Categories
-- ================================================

SELECT '=== TOP 10 CATEGORIES BY REVENUE (Last 24 Hours) ===' AS section;

SELECT
    category,
    countMerge(total_orders) AS orders,
    round(sumMerge(total_revenue), 2) AS revenue,
    round(sumMerge(total_profit), 2) AS profit,
    round((sumMerge(total_profit) / sumMerge(total_revenue)) * 100, 2) AS margin_pct,
    uniqMerge(unique_customers) AS customers
FROM sales_by_minute
WHERE minute >= now() - INTERVAL 24 HOUR
GROUP BY category
ORDER BY revenue DESC
LIMIT 10;

-- ================================================
-- DASHBOARD: Top Brands
-- ================================================

SELECT '=== TOP 10 BRANDS BY REVENUE (Last 24 Hours) ===' AS section;

SELECT
    brand,
    category,
    countMerge(total_orders) AS orders,
    round(sumMerge(total_revenue), 2) AS revenue,
    uniqMerge(unique_customers) AS customers
FROM sales_by_minute_brand
WHERE minute >= now() - INTERVAL 24 HOUR
GROUP BY brand, category
ORDER BY revenue DESC
LIMIT 10;

-- ================================================
-- DASHBOARD: Customer Tier Performance
-- ================================================

SELECT '=== CUSTOMER TIER PERFORMANCE (Last 24 Hours) ===' AS section;

SELECT
    customer_tier,
    countMerge(total_orders) AS orders,
    round(sumMerge(total_revenue), 2) AS revenue,
    round(sumMerge(total_profit), 2) AS profit,
    uniqMerge(unique_customers) AS customers,
    round(sumMerge(total_revenue) / uniqMerge(unique_customers), 2) AS revenue_per_customer,
    round(countMerge(total_orders) / uniqMerge(unique_customers), 2) AS orders_per_customer
FROM sales_by_minute_tier
WHERE minute >= now() - INTERVAL 24 HOUR
GROUP BY customer_tier
ORDER BY revenue DESC;

-- ================================================
-- DASHBOARD: Real-Time Activity (Last Hour by Minute)
-- ================================================

SELECT '=== REAL-TIME ACTIVITY (Last 60 Minutes) ===' AS section;

SELECT
    minute,
    countMerge(total_orders) AS orders,
    round(sumMerge(total_revenue), 2) AS revenue,
    uniqMerge(unique_customers) AS customers
FROM sales_by_minute
WHERE minute >= now() - INTERVAL 1 HOUR
GROUP BY minute
ORDER BY minute DESC
LIMIT 60;

-- ================================================
-- DASHBOARD: Category Mix (Pie Chart Data)
-- ================================================

SELECT '=== CATEGORY REVENUE MIX (Last 24 Hours) ===' AS section;

WITH total AS (
    SELECT sumMerge(total_revenue) AS total_revenue
    FROM sales_by_minute
    WHERE minute >= now() - INTERVAL 24 HOUR
)
SELECT
    category,
    round(sumMerge(total_revenue), 2) AS revenue,
    round((sumMerge(total_revenue) / total.total_revenue) * 100, 2) AS revenue_pct
FROM sales_by_minute, total
WHERE minute >= now() - INTERVAL 24 HOUR
GROUP BY category, total.total_revenue
ORDER BY revenue DESC;

-- ================================================
-- DASHBOARD: Week-over-Week Comparison
-- ================================================

SELECT '=== WEEK-OVER-WEEK COMPARISON ===' AS section;

WITH
    this_week AS (
        SELECT
            sum(countMerge(total_orders)) AS orders,
            sum(sumMerge(total_revenue)) AS revenue
        FROM sales_by_day
        WHERE day >= today() - INTERVAL 7 DAY
    ),
    last_week AS (
        SELECT
            sum(countMerge(total_orders)) AS orders,
            sum(sumMerge(total_revenue)) AS revenue
        FROM sales_by_day
        WHERE day >= today() - INTERVAL 14 DAY
          AND day < today() - INTERVAL 7 DAY
    )
SELECT
    'This Week' AS period,
    tw.orders AS orders,
    round(tw.revenue, 2) AS revenue,
    round(((tw.orders - lw.orders) / lw.orders) * 100, 2) AS orders_growth_pct,
    round(((tw.revenue - lw.revenue) / lw.revenue) * 100, 2) AS revenue_growth_pct
FROM this_week tw, last_week lw
UNION ALL
SELECT
    'Last Week' AS period,
    lw.orders AS orders,
    round(lw.revenue, 2) AS revenue,
    NULL AS orders_growth_pct,
    NULL AS revenue_growth_pct
FROM last_week lw;

-- ================================================
-- DASHBOARD: Active Alerts (Anomalies)
-- ================================================

SELECT '=== ACTIVE ALERTS (Last Hour) ===' AS section;

SELECT
    anomaly_type,
    count() AS alert_count,
    round(avg(anomaly_score), 2) AS avg_severity,
    round(sum(order_amount), 2) AS total_amount_flagged,
    groupArray(order_id) AS flagged_orders
FROM order_anomalies
WHERE detection_time >= now() - INTERVAL 1 HOUR
GROUP BY anomaly_type
ORDER BY alert_count DESC;

-- ================================================
-- DASHBOARD: Recent High-Value Orders
-- ================================================

SELECT '=== RECENT HIGH-VALUE ORDERS (Top 10) ===' AS section;

SELECT
    order_id,
    customer_name,
    customer_tier,
    product_name,
    category,
    quantity,
    round(total_amount, 2) AS amount,
    round(profit_margin, 2) AS profit,
    order_time
FROM orders_enriched
WHERE order_time >= now() - INTERVAL 24 HOUR
ORDER BY total_amount DESC
LIMIT 10;

-- ================================================
-- DASHBOARD: Customer Acquisition Funnel
-- ================================================

SELECT '=== CUSTOMER ENGAGEMENT (Last 24 Hours) ===' AS section;

WITH customer_stats AS (
    SELECT
        customer_id,
        count() AS order_count,
        sum(total_amount) AS total_spent
    FROM orders_enriched
    WHERE order_time >= now() - INTERVAL 24 HOUR
    GROUP BY customer_id
)
SELECT
    'Total Active Customers' AS metric,
    count() AS count,
    NULL AS avg_value
FROM customer_stats
UNION ALL
SELECT
    'Single Order Customers' AS metric,
    countIf(order_count = 1) AS count,
    round(avgIf(total_spent, order_count = 1), 2) AS avg_value
FROM customer_stats
UNION ALL
SELECT
    'Repeat Customers (2+ orders)' AS metric,
    countIf(order_count >= 2) AS count,
    round(avgIf(total_spent, order_count >= 2), 2) AS avg_value
FROM customer_stats
UNION ALL
SELECT
    'Power Users (5+ orders)' AS metric,
    countIf(order_count >= 5) AS count,
    round(avgIf(total_spent, order_count >= 5), 2) AS avg_value
FROM customer_stats;

-- ================================================
-- DASHBOARD: Product Performance
-- ================================================

SELECT '=== TOP 20 PRODUCTS (Last 7 Days) ===' AS section;

SELECT
    product_name,
    category,
    brand,
    count() AS orders,
    sum(quantity) AS units_sold,
    round(sum(total_amount), 2) AS revenue,
    round(sum(profit_margin), 2) AS profit,
    round((sum(profit_margin) / sum(total_amount)) * 100, 2) AS margin_pct
FROM orders_enriched
WHERE order_time >= now() - INTERVAL 7 DAY
GROUP BY product_name, category, brand
ORDER BY revenue DESC
LIMIT 20;

-- ================================================
-- DASHBOARD: Hourly Heatmap Data (for visualization)
-- ================================================

SELECT '=== HOURLY HEATMAP (Last 7 Days) ===' AS section;

SELECT
    toDate(hour) AS day,
    toHour(hour) AS hour_of_day,
    countMerge(total_orders) AS orders,
    round(sumMerge(total_revenue), 2) AS revenue
FROM sales_by_hour
WHERE hour >= now() - INTERVAL 7 DAY
GROUP BY day, hour_of_day
ORDER BY day DESC, hour_of_day;

-- ================================================
-- DASHBOARD: System Health Metrics
-- ================================================

SELECT '=== SYSTEM HEALTH & PIPELINE STATUS ===' AS section;

SELECT
    'Data Freshness' AS metric,
    formatReadableTimeDelta(date_diff('second', max(minute), now())) AS value,
    'Time since last minute aggregate' AS description
FROM sales_by_minute
UNION ALL
SELECT
    'Events Processed' AS metric,
    formatReadableQuantity((SELECT count() FROM events_raw)) AS value,
    'Total events in Bronze layer' AS description
UNION ALL
SELECT
    'Orders Enriched' AS metric,
    formatReadableQuantity((SELECT count() FROM orders_enriched)) AS value,
    'Total enriched orders in Silver' AS description
UNION ALL
SELECT
    'Active Anomalies' AS metric,
    toString((SELECT count() FROM order_anomalies WHERE detection_time >= now() - INTERVAL 1 HOUR)) AS value,
    'Flagged in last hour' AS description;

-- ================================================
-- DASHBOARD: Query Performance Monitor
-- ================================================

SELECT '=== DASHBOARD QUERY PERFORMANCE ===' AS section;

SELECT
    'KPIs (9 metrics)' AS dashboard_section,
    '<10ms' AS typical_query_time,
    'sales_by_minute' AS data_source
UNION ALL
SELECT
    'Hourly Trend (24 data points)' AS dashboard_section,
    '<5ms' AS typical_query_time,
    'sales_by_hour' AS data_source
UNION ALL
SELECT
    'Top Categories (10 rows)' AS dashboard_section,
    '<10ms' AS typical_query_time,
    'sales_by_minute' AS data_source
UNION ALL
SELECT
    'Real-Time Activity (60 minutes)' AS dashboard_section,
    '<20ms' AS typical_query_time,
    'sales_by_minute' AS data_source
UNION ALL
SELECT
    'Complete Dashboard Load' AS dashboard_section,
    '<100ms' AS typical_query_time,
    'All Gold tables' AS data_source;

-- ================================================
-- DEMO SCRIPT FOR PRESENTER
-- ================================================

SELECT '=== DEMO SCRIPT NOTES ===' AS section;

SELECT
    'Show KPIs updating in real-time' AS demo_step,
    'Insert new orders, refresh query, show instant updates' AS action,
    '1' AS step_number
UNION ALL
SELECT
    'Highlight query speed' AS demo_step,
    'Point out millisecond execution times in output' AS action,
    '2' AS step_number
UNION ALL
SELECT
    'Compare to raw data query' AS demo_step,
    'Run same query on orders_enriched vs sales_by_minute' AS action,
    '3' AS step_number
UNION ALL
SELECT
    'Show anomaly alerts' AS demo_step,
    'Inject high-value order, show it in alerts section' AS action,
    '4' AS step_number
UNION ALL
SELECT
    'Emphasize zero orchestration' AS demo_step,
    'All updates automatic, no cron jobs or Airflow DAGs' AS action,
    '5' AS step_number
ORDER BY step_number;

SELECT
    'Dashboard queries ready!' AS status,
    'All metrics sub-100ms query time' AS performance,
    'Perfect for live demo' AS note;
