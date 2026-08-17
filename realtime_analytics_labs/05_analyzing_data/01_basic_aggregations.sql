-- ============================================================
-- Section 05: Demo 1 — Basic Aggregations
-- ============================================================
-- Scenario : Answer common business questions about NYC taxi ops
-- Concepts : COUNT, SUM, AVG, MIN, MAX, uniq, quantile,
--            GROUP BY, HAVING, ORDER BY, topK
-- ============================================================

USE nyc_taxi_analytics;


-- ============================================================
-- 1.1  Row count and quick sanity check
-- ============================================================
SELECT '==== 1.1  Row count ====' AS step;

SELECT count() AS total_trips
FROM trips;

-- ============================================================
-- TALKING POINT
-- ============================================================
-- ClickHouse counts 8M rows in milliseconds because it reads only
-- the primary index meta-data, not the actual data blocks.
-- Try: SELECT count() FROM trips WHERE pickup_datetime >= '2013-01-01'
-- and notice it's equally fast — the index skips irrelevant granules.
-- ============================================================


-- ============================================================
-- 1.2  Standard aggregate functions
-- ============================================================
SELECT '==== 1.2  Standard aggregates ====' AS step;

SELECT
    count()                             AS total_trips,
    round(avg(fare_amount), 2)          AS avg_fare,
    round(avg(trip_distance), 2)        AS avg_distance_miles,
    round(avg(tip_amount), 2)           AS avg_tip,
    sum(total_amount)                   AS total_revenue,
    min(pickup_datetime)                AS first_trip,
    max(pickup_datetime)                AS last_trip
FROM trips
WHERE fare_amount > 0
  AND trip_distance > 0;


-- ============================================================
-- 1.3  GROUP BY — rides by payment type
-- ============================================================
SELECT '==== 1.3  Rides by payment type ====' AS step;

SELECT
    payment_type,
    count()                         AS trips,
    round(avg(fare_amount), 2)      AS avg_fare,
    round(avg(tip_amount), 2)       AS avg_tip,
    round(sum(total_amount), 2)     AS total_revenue
FROM trips
WHERE fare_amount > 0
GROUP BY payment_type
ORDER BY trips DESC;

-- ============================================================
-- TALKING POINT
-- ============================================================
-- Enum columns are stored as integers internally. The GROUP BY runs
-- on 1-byte values, which maximises CPU cache utilisation.
-- Credit card (CRE) payers tip significantly more than cash (CSH) —
-- this is a business insight derived in milliseconds from 8M rows.
-- ============================================================


-- ============================================================
-- 1.4  GROUP BY — rides by passenger count
-- ============================================================
SELECT '==== 1.4  Rides by passenger count ====' AS step;

SELECT
    passenger_count,
    count()                         AS trips,
    round(avg(fare_amount), 2)      AS avg_fare,
    round(avg(trip_distance), 2)    AS avg_distance
FROM trips
WHERE passenger_count > 0
  AND passenger_count <= 6
GROUP BY passenger_count
ORDER BY passenger_count;


-- ============================================================
-- 1.5  HAVING — filter aggregated results
-- ============================================================
SELECT '==== 1.5  Busy pickup neighborhoods (HAVING) ====' AS step;

SELECT
    pickup_ntaname,
    count()                         AS trips,
    round(avg(fare_amount), 2)      AS avg_fare,
    round(sum(total_amount), 0)     AS total_revenue
FROM trips
WHERE pickup_ntaname != ''
GROUP BY pickup_ntaname
HAVING trips > 100000
ORDER BY trips DESC
LIMIT 15;

-- ============================================================
-- TALKING POINT
-- ============================================================
-- HAVING filters rows AFTER aggregation — it works on the result
-- of the GROUP BY, not the raw data. Use WHERE for pre-agg filters
-- (much faster) and HAVING for post-agg conditions.
-- LowCardinality(String) columns maintain a dictionary, so the
-- GROUP BY works on small integer IDs under the hood.
-- ============================================================


-- ============================================================
-- 1.6  ClickHouse-specific aggregates: topK and quantile
-- ============================================================
SELECT '==== 1.6  ClickHouse-specific aggregate functions ====' AS step;

-- topK: approximate top-N without a full sort
SELECT topK(10)(pickup_ntaname) AS top_10_pickup_neighborhoods
FROM trips
WHERE pickup_ntaname != '';

-- quantile / quantiles: percentile calculations
SELECT
    quantile(0.50)(fare_amount)  AS median_fare,
    quantile(0.75)(fare_amount)  AS p75_fare,
    quantile(0.95)(fare_amount)  AS p95_fare,
    quantile(0.99)(fare_amount)  AS p99_fare
FROM trips
WHERE fare_amount > 0;

-- ============================================================
-- TALKING POINT
-- ============================================================
-- topK uses a probabilistic sketch (Space-Saving algorithm) —
-- it's approximate but very fast on large datasets.
-- quantile() uses a t-digest sketch. For exact percentiles you
-- can use quantileExact() — it's slower but precise.
-- ============================================================


-- ============================================================
-- 1.7  uniq — approximate distinct count
-- ============================================================
SELECT '==== 1.7  Unique counts (uniq) ====' AS step;

SELECT
    uniq(pickup_ntaname)    AS distinct_pickup_zones,
    uniq(dropoff_ntaname)   AS distinct_dropoff_zones,
    uniqExact(passenger_count) AS distinct_passenger_counts
FROM trips;

-- ============================================================
-- TALKING POINT
-- ============================================================
-- uniq() uses HyperLogLog — it is approximate but extremely fast
-- and uses very little memory (works on billions of rows).
-- uniqExact() is exact but requires more memory.
-- For counting distinct users / sessions at scale, uniq() is
-- the right tool in ClickHouse.
-- ============================================================

SELECT '[OK] Demo 1 complete: Basic Aggregations.' AS status;
