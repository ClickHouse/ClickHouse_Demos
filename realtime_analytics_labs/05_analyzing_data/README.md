# Section 05: Analyzing Data

**Dataset**: NYC Taxi Rides | **Database**: `nyc_taxi_analytics`

Progressive demos covering ClickHouse's analytical SQL capabilities — from simple aggregations to window functions and advanced query patterns.

---

## Setup

```bash
ch --queries-file 00_setup.sql
```

> ⏱ Data loading takes ~3–5 minutes for ~8 million rows from S3.

---

## Demo 1: Basic Aggregations

**File**: `01_basic_aggregations.sql`
**Scenario**: Answer common business questions about taxi operations.
**Key Concept**: ClickHouse executes GROUP BY on compressed columnar data — scanning hundreds of millions of rows per second.

```
┌─────────────────────────────┐
│  Raw table: nyc_taxi.trips  │
│  ~8M rows, columnar storage │
│                             │
│  SELECT payment_type,       │
│         count(), avg(fare)  │
│  GROUP BY payment_type      │
└─────────────────────────────┘
```

<details>
<summary>▶ Step-by-Step Instructions</summary>

1. Run the full file: `ch --queries-file 01_basic_aggregations.sql`
2. Observe the row count and compression ratio
3. Run each aggregation query block one at a time using `--query`
4. Point out elapsed time shown in the ClickHouse client output

</details>

**What You Learn**
- `COUNT()`, `SUM()`, `AVG()`, `MIN()`, `MAX()`, `uniq()`, `median()`
- `GROUP BY` on low-cardinality columns (payment_type, passenger_count)
- `HAVING` to filter aggregated results
- `topK()` and `quantile()` as ClickHouse-specific aggregate functions
- How columnar storage makes aggregations fast

---

## Demo 2: Time Series Analysis

**File**: `02_time_series.sql`
**Scenario**: Analyze taxi demand patterns over time.
**Key Concept**: ClickHouse has rich date/time functions purpose-built for time-series workloads.

**What You Learn**
- `toStartOfHour()`, `toStartOfDay()`, `toStartOfMonth()`, `toYear()`
- `formatDateTime()` for human-readable output
- `dateDiff()` to compute trip durations
- Binning rides into hourly/daily/monthly buckets
- How `ORDER BY` on `pickup_datetime` makes time-range queries fast

---

## Demo 3: Window Functions

**File**: `03_window_functions.sql`
**Scenario**: Rank neighborhoods by revenue and compute running totals.
**Key Concept**: Window functions in ClickHouse run after aggregation, enabling powerful analytical patterns without self-joins.

**What You Learn**
- `RANK()`, `ROW_NUMBER()`, `DENSE_RANK()` with `OVER (ORDER BY ...)`
- `PARTITION BY` to rank within groups
- `SUM() OVER (ORDER BY ...)` for running totals
- `LAG()` and `LEAD()` for period-over-period comparisons
- `ROWS BETWEEN` frame specification

---

## Demo 4: Advanced Queries

**File**: `04_advanced_queries.sql`
**Scenario**: Complex analytical patterns combining multiple ClickHouse features.
**Key Concept**: CTEs, subqueries, and array functions unlock expressive, readable SQL.

**What You Learn**
- `WITH` clause (CTEs) for readable multi-step analysis
- Subqueries in `WHERE` and `FROM`
- `arrayJoin()` and `array*` functions
- `CASE WHEN` for conditional logic
- `EXPLAIN` to inspect query execution plans
- `FORMAT Pretty` / `FORMAT Vertical` output modes

---

## Lab Exercises

**File**: `05_lab_exercises.sql`
**Instructions**: Try each exercise independently. An answer key is included at the bottom of the file (hidden in SQL comments).

| Exercise | Topic | Difficulty |
|----------|-------|-----------|
| 1 | Count rides by hour of day | ★☆☆ |
| 2 | Top 10 pickup neighborhoods by total revenue | ★☆☆ |
| 3 | Average tip percentage by payment type | ★★☆ |
| 4 | Rank all neighborhoods by average trip distance using window functions | ★★☆ |
| 5 | Find the busiest hour for each day of the week (CTE required) | ★★★ |
| 6 | Monthly revenue trend with month-over-month % change using LAG() | ★★★ |

---

## Cleanup

```bash
ch --queries-file 99_cleanup.sql
```
