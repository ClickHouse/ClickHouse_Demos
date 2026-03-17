-- ============================================================
-- Section 05: Demo 4 — Advanced Queries
-- ============================================================
-- Scenario : Complex analytical patterns combining CTEs,
--            subqueries, array functions, EXPLAIN, and
--            ClickHouse-specific features
-- Concepts : WITH (CTE), subqueries, arrayJoin, CASE WHEN,
--            EXPLAIN, multiIf, dictionaries, FORMAT
-- ============================================================

USE nyc_taxi_analytics;


-- ============================================================
-- 4.1  CTEs (WITH clause) — readable multi-step analysis
-- ============================================================
SELECT '==== 4.1  CTEs: Step-by-step trip analysis ====' AS step;

-- Find neighborhoods where the average tip rate is above the global average
WITH
    global_avg AS (
        SELECT avg(tip_amount / nullIf(fare_amount, 0)) AS global_avg_tip_rate
        FROM trips
        WHERE fare_amount > 0
          AND payment_type = 'CSH'  -- Credit card only (cash tips not recorded)
    ),
    zone_stats AS (
        SELECT
            pickup_ntaname,
            count()                                                 AS trips,
            round(avg(tip_amount / nullIf(fare_amount, 0)) * 100, 1) AS tip_rate_pct
        FROM trips
        WHERE fare_amount > 0
          AND payment_type = 'CSH'
          AND pickup_ntaname != ''
        GROUP BY pickup_ntaname
        HAVING trips > 5000
    )
SELECT
    z.pickup_ntaname,
    z.trips,
    z.tip_rate_pct,
    round((z.tip_rate_pct / (g.global_avg_tip_rate * 100) - 1) * 100, 1) AS pct_above_avg
FROM zone_stats z
CROSS JOIN global_avg g
WHERE z.tip_rate_pct > g.global_avg_tip_rate * 100
ORDER BY z.tip_rate_pct DESC
LIMIT 15;

-- ============================================================
-- TALKING POINT
-- ============================================================
-- WITH clauses (CTEs) make complex queries readable by naming
-- intermediate results. In ClickHouse, CTEs are inlined (not
-- materialised) unless you use MATERIALIZED CTE.
-- nullIf(x, 0) returns NULL when x=0, avoiding division-by-zero.
-- ============================================================


-- ============================================================
-- 4.2  Subqueries — filter based on aggregated results
-- ============================================================
SELECT '==== 4.2  Subquery: trips from high-value zones ====' AS step;

-- Find individual trips FROM the top-10 revenue neighborhoods
SELECT
    pickup_ntaname,
    pickup_datetime,
    fare_amount,
    tip_amount,
    trip_distance
FROM trips
WHERE pickup_ntaname IN (
    SELECT pickup_ntaname
    FROM trips
    WHERE pickup_ntaname != ''
    GROUP BY pickup_ntaname
    ORDER BY sum(total_amount) DESC
    LIMIT 10
)
ORDER BY fare_amount DESC
LIMIT 20;


-- ============================================================
-- 4.3  CASE WHEN — conditional bucketing
-- ============================================================
SELECT '==== 4.3  CASE WHEN: fare buckets ====' AS step;

SELECT
    CASE
        WHEN fare_amount < 5    THEN 'under $5'
        WHEN fare_amount < 15   THEN '$5–$15'
        WHEN fare_amount < 30   THEN '$15–$30'
        WHEN fare_amount < 60   THEN '$30–$60'
        ELSE 'over $60'
    END                                 AS fare_bucket,
    count()                             AS trips,
    round(avg(trip_distance), 2)        AS avg_distance,
    round(avg(tip_amount), 2)           AS avg_tip,
    bar(count(), 0, 3000000, 40)        AS chart
FROM trips
WHERE fare_amount > 0
GROUP BY fare_bucket
ORDER BY min(fare_amount);

-- ============================================================
-- TALKING POINT
-- ============================================================
-- ClickHouse also has multiIf(cond1, val1, cond2, val2, ..., else)
-- which is a more compact/efficient alternative to nested CASE WHEN:
--
--   multiIf(fare_amount < 5,  'under $5',
--           fare_amount < 15, '$5-$15',
--           'over $15')
--
-- For large datasets, multiIf can be faster than CASE WHEN.
-- ============================================================


-- ============================================================
-- 4.4  EXPLAIN — inspect the query execution plan
-- ============================================================
SELECT '==== 4.4  EXPLAIN: understand query execution ====' AS step;

-- Basic EXPLAIN (shows logical plan)
EXPLAIN
SELECT
    payment_type,
    count() AS trips
FROM trips
GROUP BY payment_type;

-- EXPLAIN with index usage (critical for understanding performance)
EXPLAIN indexes = 1
SELECT count()
FROM trips
WHERE pickup_datetime >= '2015-09-01'
  AND pickup_datetime <  '2015-12-01';

-- ============================================================
-- TALKING POINT
-- ============================================================
-- EXPLAIN indexes=1 shows exactly which granules (8192-row blocks)
-- are read. Look for:
--   "Granules: X/Y" — X granules read out of Y total.
-- A ratio close to 1.0 means the primary key is NOT helping.
-- A low ratio (e.g. 5/950) means the index is working well.
-- For this query on pickup_datetime (first ORDER BY column),
-- the index should eliminate most of the table.
-- ============================================================


-- ============================================================
-- 4.5  Array functions — multiple values per row
-- ============================================================
SELECT '==== 4.5  Array functions ====' AS step;

SELECT '---- arrayJoin: expand top fares per zone ----' AS note;

SELECT
    pickup_ntaname,
    arrayJoin(top_fares) AS top_fare
FROM (
    SELECT
        pickup_ntaname,
        arraySlice(arrayReverseSort(groupArray(fare_amount)), 1, 5) AS top_fares
    FROM trips
    WHERE pickup_ntaname IN ('Midtown-Midtown South',
                             'Hudson Yards-Chelsea-Flat Iron-Union Square')
      AND fare_amount > 0
    GROUP BY pickup_ntaname
)
ORDER BY pickup_ntaname, top_fare DESC;

-- ============================================================
-- TALKING POINT
-- ============================================================
-- groupArray(x)          → collapses many rows into one Array per group
-- arrayReverseSort(arr)  → sorts the array descending
-- arraySlice(arr, 1, 5)  → takes the first 5 elements
-- arrayJoin(arr)         → expands the array back into individual rows
--
-- groupArray + arrayJoin is effectively a round-trip: collect → process
-- → expand. This is useful when you need to apply array functions
-- (sort, filter, transform) on a group's values before returning rows.
--
-- One arrayJoin() per SELECT is safe. Two arrayJoin() calls in the
-- same SELECT produce a CROSS JOIN of both arrays (N×M rows).
-- To unnest two parallel arrays together, use the ARRAY JOIN clause:
--   SELECT a, b FROM t ARRAY JOIN arr1 AS a, arr2 AS b;
-- ============================================================


-- ============================================================
-- 4.6  FORMAT — control output presentation
-- ============================================================
SELECT '==== 4.6  Output formats ====' AS step;

-- Default (TabSeparated in client)
SELECT payment_type, count() AS trips
FROM trips
GROUP BY payment_type
ORDER BY trips DESC
FORMAT Pretty;

-- ClickHouse has 70+ output formats including:
--   FORMAT JSON         → REST APIs, applications
--   FORMAT CSV          → spreadsheets, exports
--   FORMAT Parquet      → data lake / analytics platforms
--   FORMAT Vertical     → one value per line (great for wide rows)
--   FORMAT PrettyCompact → compact table for terminal
--   FORMAT Markdown     → documentation

SELECT '[OK] Demo 4 complete: Advanced Queries.' AS status;
