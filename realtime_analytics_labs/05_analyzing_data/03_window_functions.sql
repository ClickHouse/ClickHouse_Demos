-- ============================================================
-- Section 05: Demo 3 — Window Functions
-- ============================================================
-- Scenario : Rank neighborhoods by revenue, compute running
--            totals, compare period-over-period performance
-- Concepts : RANK, ROW_NUMBER, DENSE_RANK, SUM OVER,
--            LAG, LEAD, PARTITION BY, ROWS BETWEEN
-- ============================================================

USE nyc_taxi_analytics;


-- ============================================================
-- 3.1  RANK — top neighborhoods by total revenue
-- ============================================================
SELECT '==== 3.1  Rank neighborhoods by revenue ====' AS step;

SELECT
    pickup_ntaname,
    total_revenue,
    trips,
    RANK() OVER (ORDER BY total_revenue DESC)   AS revenue_rank
FROM (
    SELECT
        pickup_ntaname,
        count()                         AS trips,
        round(sum(total_amount), 0)     AS total_revenue
    FROM trips
    WHERE pickup_ntaname != ''
    GROUP BY pickup_ntaname
)
ORDER BY revenue_rank
LIMIT 20;

-- ============================================================
-- TALKING POINT
-- ============================================================
-- Window functions are applied AFTER aggregation completes.
-- The RANK() assigns rank 1 to the highest revenue zone,
-- with gaps for ties (DENSE_RANK() removes the gaps).
-- ROW_NUMBER() always assigns consecutive numbers regardless of ties.
-- ============================================================


-- ============================================================
-- 3.2  PARTITION BY — rank within a category
-- ============================================================
SELECT '==== 3.2  Rank by payment type PARTITION BY ====' AS step;

-- Rank neighborhoods separately for each payment type.
-- Window function aliases cannot be used in WHERE (window functions
-- are evaluated after WHERE), so we wrap in a subquery and filter
-- on rank_within_payment_type in the outer query.
SELECT
    payment_type,
    pickup_ntaname,
    trips,
    avg_fare,
    rank_within_payment_type
FROM (
    SELECT
        payment_type,
        pickup_ntaname,
        trips,
        avg_fare,
        RANK() OVER (
            PARTITION BY payment_type
            ORDER BY trips DESC
        ) AS rank_within_payment_type
    FROM (
        SELECT
            payment_type,
            pickup_ntaname,
            count()                     AS trips,
            round(avg(fare_amount), 2)  AS avg_fare
        FROM trips
        WHERE pickup_ntaname != ''
          AND fare_amount > 0
        GROUP BY payment_type, pickup_ntaname
    )
)
WHERE rank_within_payment_type <= 5
ORDER BY payment_type, rank_within_payment_type;

-- ============================================================
-- TALKING POINT
-- ============================================================
-- SQL evaluation order: WHERE → GROUP BY → window functions → HAVING → ORDER BY.
-- Because window functions run AFTER WHERE, you cannot filter on a
-- window function alias in the same query level — ClickHouse raises
-- error 184 (ILLEGAL_AGGREGATION) if you try.
-- The fix is always the same: wrap the window function in a subquery
-- (or CTE), then filter on the alias in the outer WHERE.
-- PARTITION BY splits the window into independent groups — here,
-- one ranking per payment_type. Each partition gets its own
-- rank sequence starting from 1.
-- ============================================================


-- ============================================================
-- 3.3  Running totals with SUM OVER
-- ============================================================
SELECT '==== 3.3  Running total revenue by month ====' AS step;

SELECT
    month,
    monthly_revenue,
    SUM(monthly_revenue) OVER (
        ORDER BY month
        ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
    ) AS cumulative_revenue,
    round(monthly_revenue / SUM(monthly_revenue) OVER () * 100, 1) AS pct_of_total
FROM (
    SELECT
        toStartOfMonth(pickup_datetime)     AS month,
        round(sum(total_amount), 0)         AS monthly_revenue
    FROM trips
    GROUP BY month
)
ORDER BY month;

-- ============================================================
-- TALKING POINT
-- ============================================================
-- ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
--   → running total from first row to current
-- ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
--   → SUM OVER () → grand total (used for percentages)
-- These are standard SQL window frames.
-- ============================================================


-- ============================================================
-- 3.4  LAG and LEAD — period-over-period comparison
-- ============================================================
SELECT '==== 3.4  Month-over-month revenue change (LAG) ====' AS step;

SELECT
    month,
    monthly_revenue,
    LAG(monthly_revenue, 1) OVER (ORDER BY month)   AS prev_month_revenue,
    round(
        (monthly_revenue - LAG(monthly_revenue, 1) OVER (ORDER BY month))
        / LAG(monthly_revenue, 1) OVER (ORDER BY month) * 100,
        1
    )   AS mom_change_pct
FROM (
    SELECT
        toStartOfMonth(pickup_datetime)     AS month,
        round(sum(total_amount), 0)         AS monthly_revenue
    FROM trips
    GROUP BY month
)
ORDER BY month;

-- ============================================================
-- TALKING POINT
-- ============================================================
-- LAG(col, N) returns the value N rows BEFORE the current row.
-- LEAD(col, N) returns the value N rows AFTER the current row.
-- This makes period-over-period analysis simple without self-joins.
-- The MoM % formula: (current - previous) / previous * 100
-- ============================================================


-- ============================================================
-- 3.5  ROW_NUMBER — deduplication and top-N per group
-- ============================================================
SELECT '==== 3.5  Top-3 dropoff zones per pickup zone (ROW_NUMBER) ====' AS step;

SELECT
    pickup_ntaname,
    dropoff_ntaname,
    trips,
    row_num
FROM (
    SELECT
        pickup_ntaname,
        dropoff_ntaname,
        count()     AS trips,
        ROW_NUMBER() OVER (
            PARTITION BY pickup_ntaname
            ORDER BY count() DESC
        ) AS row_num
    FROM trips
    WHERE pickup_ntaname != ''
      AND dropoff_ntaname != ''
    GROUP BY pickup_ntaname, dropoff_ntaname
)
WHERE row_num <= 3
ORDER BY pickup_ntaname, row_num
LIMIT 30;

-- ============================================================
-- TALKING POINT
-- ============================================================
-- ROW_NUMBER() + PARTITION BY + filtering WHERE row_num <= N
-- is a classic "top N per group" pattern. It avoids correlated
-- subqueries and is typically much faster.
-- ============================================================

SELECT '[OK] Demo 3 complete: Window Functions.' AS status;
