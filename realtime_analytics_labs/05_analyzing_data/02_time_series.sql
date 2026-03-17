-- ============================================================
-- Section 05: Demo 2 — Time Series Analysis
-- ============================================================
-- Scenario : Analyse NYC taxi demand patterns over time
-- Concepts : toStartOfHour, toStartOfDay, toStartOfMonth,
--            toYear, toDayOfWeek, formatDateTime, dateDiff,
--            time-range filtering with the primary key
-- ============================================================

USE nyc_taxi_analytics;


-- ============================================================
-- 2.1  Date truncation functions
-- ============================================================
SELECT '==== 2.1  Date truncation functions ====' AS step;

-- Show how ClickHouse date functions work on a sample row
SELECT
    pickup_datetime,
    toStartOfHour(pickup_datetime)   AS hour_bucket,
    toStartOfDay(pickup_datetime)    AS day_bucket,
    toStartOfMonth(pickup_datetime)  AS month_bucket,
    toYear(pickup_datetime)          AS year,
    toDayOfWeek(pickup_datetime)     AS day_of_week,   -- 1=Mon … 7=Sun
    toHour(pickup_datetime)          AS hour_of_day
FROM trips
LIMIT 5;

-- ============================================================
-- TALKING POINT
-- ============================================================
-- ClickHouse has over 100 date/time functions. The key pattern is:
--   toStartOf*(datetime) → truncates to that time boundary
--   toX(datetime)        → extracts the X component as a number
-- All of these operate on DateTime/DateTime64 columns. For
-- timezone-aware analysis use toStartOfHour(dt, 'America/New_York').
-- ============================================================


-- ============================================================
-- 2.2  Trips per hour of day (demand pattern)
-- ============================================================
SELECT '==== 2.2  Trips per hour of day ====' AS step;

SELECT
    toHour(pickup_datetime)         AS hour_of_day,
    count()                         AS trips,
    bar(count(), 0, 800000, 40)     AS chart     -- ASCII bar chart!
FROM trips
GROUP BY hour_of_day
ORDER BY hour_of_day;

-- ============================================================
-- TALKING POINT
-- ============================================================
-- bar() is a ClickHouse function that renders an ASCII bar chart
-- directly in the terminal — great for quick visual inspection.
-- Note the bimodal distribution: morning rush (8–9am) and evening
-- rush (6–7pm). This matches intuition — and ClickHouse found it
-- by scanning 8M rows in under a second.
-- ============================================================


-- ============================================================
-- 2.3  Daily ride volume trend
-- ============================================================
SELECT '==== 2.3  Daily ride volume ====' AS step;

SELECT
    toStartOfDay(pickup_datetime)   AS day,
    count()                         AS trips,
    round(avg(fare_amount), 2)      AS avg_fare
FROM trips
GROUP BY day
ORDER BY day
LIMIT 30;


-- ============================================================
-- 2.4  Monthly revenue trend
-- ============================================================
SELECT '==== 2.4  Monthly revenue trend ====' AS step;

SELECT
    toStartOfMonth(pickup_datetime)     AS month,
    formatDateTime(month, '%Y-%m')      AS month_label,
    count()                             AS trips,
    round(sum(total_amount), 0)         AS total_revenue,
    round(avg(total_amount), 2)         AS avg_revenue_per_trip
FROM trips
GROUP BY month
ORDER BY month;


-- ============================================================
-- 2.5  Trip duration with dateDiff
-- ============================================================
SELECT '==== 2.5  Trip duration analysis ====' AS step;

SELECT
    count()                                             AS trips,
    round(avg(dateDiff('minute', pickup_datetime, dropoff_datetime)), 1)  AS avg_duration_min,
    round(avg(trip_distance), 2)                        AS avg_distance_miles,
    round(avg(fare_amount), 2)                          AS avg_fare
FROM trips
WHERE dropoff_datetime > pickup_datetime
  AND dateDiff('minute', pickup_datetime, dropoff_datetime) BETWEEN 1 AND 120;

-- Distribution of trip durations in 5-minute buckets
SELECT
    intDiv(dateDiff('minute', pickup_datetime, dropoff_datetime), 5) * 5 AS duration_bucket_min,
    count()                 AS trips,
    bar(count(), 0, 1200000, 40) AS chart
FROM trips
WHERE dropoff_datetime > pickup_datetime
  AND dateDiff('minute', pickup_datetime, dropoff_datetime) BETWEEN 1 AND 60
GROUP BY duration_bucket_min
ORDER BY duration_bucket_min;

-- ============================================================
-- TALKING POINT
-- ============================================================
-- dateDiff('minute', start, end) computes a precise integer
-- difference between two DateTime values in the given unit.
-- Other units: 'second', 'hour', 'day', 'month', 'year'.
-- intDiv(x, 5) * 5 is a fast integer-based bucketing trick —
-- equivalent to FLOOR(x / 5) * 5 but avoids floating point.
-- ============================================================


-- ============================================================
-- 2.6  Time-range filter — primary key in action
-- ============================================================
SELECT '==== 2.6  Primary key range scan ====' AS step;

-- Query a specific 24-hour window
SELECT
    toStartOfHour(pickup_datetime)  AS hour,
    count()                         AS trips
FROM trips
WHERE pickup_datetime >= '2013-07-04 00:00:00'
  AND pickup_datetime <  '2013-07-05 00:00:00'
GROUP BY hour
ORDER BY hour;

-- ============================================================
-- TALKING POINT
-- ============================================================
-- Because pickup_datetime is the first column in our ORDER BY,
-- ClickHouse uses the sparse primary index to skip directly to the
-- relevant data granules. Check with EXPLAIN indexes=1:
--
--   EXPLAIN indexes=1
--   SELECT count() FROM trips
--   WHERE pickup_datetime >= '2013-07-04'
--     AND pickup_datetime <  '2013-07-05';
--
-- You will see "Used key conditions" and a granule count showing
-- how many of the ~8M rows were actually read.
-- ============================================================

SELECT '[OK] Demo 2 complete: Time Series Analysis.' AS status;
