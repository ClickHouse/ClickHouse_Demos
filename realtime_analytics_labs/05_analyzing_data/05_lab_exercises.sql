-- ============================================================
-- Section 05: Lab Exercises — Analyzing Data
-- ============================================================
-- Complete each exercise independently using what you learned
-- in Demos 1–4. Run your query and verify the result.
-- Answer keys are in the commented block at the bottom.
-- ============================================================

USE nyc_taxi_analytics;

SELECT '============================================================' AS info;
SELECT 'SECTION 05 LAB EXERCISES — Write your queries below each prompt.' AS info;
SELECT '============================================================' AS info;


-- ============================================================
-- Exercise 1 ★☆☆
-- ============================================================
-- Count the number of trips for each hour of the day (0–23).
-- Order the results by hour ascending.
-- Expected columns: hour_of_day, trips
-- ============================================================

-- YOUR QUERY HERE:




-- ============================================================
-- Exercise 2 ★☆☆
-- ============================================================
-- Find the top 10 pickup neighborhoods by total revenue.
-- Include: pickup_ntaname, total_trips, total_revenue, avg_fare
-- Exclude rows where pickup_ntaname is empty.
-- Round monetary values to 2 decimal places.
-- ============================================================

-- YOUR QUERY HERE:




-- ============================================================
-- Exercise 3 ★★☆
-- ============================================================
-- Calculate the average tip percentage for each payment type.
-- Tip percentage = (tip_amount / fare_amount) * 100
-- Only include credit card trips (payment_type = 'CRE')
-- and rows where fare_amount > 0.
-- Group by pickup neighborhood AND payment type.
-- Order by avg_tip_pct DESC. Show top 20 rows.
-- Expected columns: pickup_ntaname, trips, avg_tip_pct
-- ============================================================

-- YOUR QUERY HERE:




-- ============================================================
-- Exercise 4 ★★☆
-- ============================================================
-- Using a window function, rank ALL pickup neighborhoods
-- by average trip distance (highest first).
-- Only include neighborhoods with more than 1000 trips.
-- Expected columns: rank, pickup_ntaname, trips, avg_distance_miles
-- ============================================================

-- YOUR QUERY HERE:




-- ============================================================
-- Exercise 5 ★★★
-- ============================================================
-- Show the monthly revenue trend with month-over-month % change.
-- Columns: month (YYYY-MM format), monthly_revenue,
--          prev_month_revenue, mom_change_pct
-- Use LAG() for the previous month value.
-- Format the month as 'YYYY-MM' using formatDateTime.
-- Round monetary values to 0 decimal places.
-- ============================================================

-- YOUR QUERY HERE:




-- ============================================================
-- ============================================================
-- ANSWER KEY — Review after attempting exercises independently
-- ============================================================
-- ============================================================

/*

-- Exercise 1 Answer
SELECT
    toHour(pickup_datetime)     AS hour_of_day,
    count()                     AS trips
FROM trips
GROUP BY hour_of_day
ORDER BY hour_of_day;


-- Exercise 2 Answer
SELECT
    pickup_ntaname,
    count()                         AS total_trips,
    round(sum(total_amount), 2)     AS total_revenue,
    round(avg(fare_amount), 2)      AS avg_fare
FROM trips
WHERE pickup_ntaname != ''
GROUP BY pickup_ntaname
ORDER BY total_revenue DESC
LIMIT 10;


-- Exercise 3 Answer
SELECT
    pickup_ntaname,
    count()                                                 AS trips,
    round(avg(tip_amount / nullIf(fare_amount, 0)) * 100, 2) AS avg_tip_pct
FROM trips
WHERE payment_type = 'CRE'
  AND fare_amount > 0
  AND pickup_ntaname != ''
GROUP BY pickup_ntaname
ORDER BY avg_tip_pct DESC
LIMIT 20;


-- Exercise 4 Answer
SELECT
    RANK() OVER (ORDER BY avg_distance_miles DESC)  AS rank,
    pickup_ntaname,
    trips,
    avg_distance_miles
FROM (
    SELECT
        pickup_ntaname,
        count()                         AS trips,
        round(avg(trip_distance), 2)    AS avg_distance_miles
    FROM trips
    WHERE pickup_ntaname != ''
    GROUP BY pickup_ntaname
    HAVING trips > 1000
)
ORDER BY rank;


-- Exercise 5 Answer
SELECT
    formatDateTime(month, '%Y-%m')                              AS month,
    monthly_revenue,
    LAG(monthly_revenue, 1) OVER (ORDER BY month)              AS prev_month_revenue,
    round(
        (monthly_revenue - LAG(monthly_revenue, 1) OVER (ORDER BY month))
        / LAG(monthly_revenue, 1) OVER (ORDER BY month) * 100,
        1
    )                                                           AS mom_change_pct
FROM (
    SELECT
        toStartOfMonth(pickup_datetime)     AS month,
        round(sum(total_amount), 0)         AS monthly_revenue
    FROM trips
    GROUP BY month
)
ORDER BY month;

*/
