# Section 08: Query Acceleration Techniques

**Dataset**: NYC Taxi Rides | **Database**: `nyc_taxi_perf`

Progressive demos covering the four main query acceleration tools in ClickHouse: primary key tuning, skip indexes, projections, and materialized views — each demonstrated with before/after performance comparisons using real data.

---

## Setup

```bash
ch --queries-file 00_setup.sql
```

> ⏱ Data loading takes ~3–5 minutes. If you completed Section 05, you can skip the data load by commenting out the INSERT step — the schema is recreated in a new database (`nyc_taxi_perf`).

---

## Demo 1: Primary Key Optimization

**File**: `01_primary_key.sql`
**Scenario**: Understand how `ORDER BY` drives query performance and how to choose the right key columns.
**Key Concept**: ClickHouse's primary index is *sparse* — it stores one entry per 8192-row granule, not per row. The key order dramatically affects how many granules are read.

```
ORDER BY (pickup_datetime, dropoff_datetime)
         ↑ range scans on this → fast
                                  ↑ range scans on this alone → slow
                                    (must read all granules)
```

**What You Learn**
- How sparse primary indexes work (granule-level filtering)
- Why column order in `ORDER BY` matters
- Comparing key-aligned vs. non-key queries with `EXPLAIN indexes=1`
- The role of granule size (`index_granularity = 8192`)
- When to use composite primary keys

---

## Demo 2: Skip Indexes

**File**: `02_skip_indexes.sql`
**Scenario**: Accelerate queries on non-primary-key columns using secondary indexes.
**Key Concept**: Skip indexes store aggregate metadata per granule (min/max, set of values, bloom filter). They let ClickHouse skip entire data blocks without reading them.

```
Without skip index: scan ALL granules → check each row
With minmax index:  skip granules where max(fare) < threshold
With set index:     skip granules that can't contain pickup_ntaname = X
With bloom filter:  skip granules unlikely to contain a token
```

**What You Learn**
- `minmax` index for numeric range queries
- `set(N)` index for low-cardinality equality filters
- `bloom_filter` for high-cardinality string equality/IN
- `ngrambf_v1` for LIKE/substring search
- How to measure skip index effectiveness with `system.query_log`

---

## Demo 3: Projections

**File**: `03_projections.sql`
**Scenario**: Pre-sort data for queries that use a different sort order than the primary key.
**Key Concept**: Projections are hidden sub-tables with a different sort order (and optional pre-aggregation). ClickHouse automatically uses the best projection for each query.

```
trips (ORDER BY pickup_datetime)
  └─ projection: order by dropoff_ntaname   ← used when filtering on dropoff
  └─ projection: order by fare_amount DESC  ← used for top-fare queries
```

**What You Learn**
- Creating projections with `ALTER TABLE ... ADD PROJECTION`
- Materializing projections with `ALTER TABLE ... MATERIALIZE PROJECTION`
- How ClickHouse automatically selects projections
- Pre-aggregating projections for GROUP BY queries
- Trade-offs: storage overhead vs. query speedup

---

## Demo 4: Materialized Views

**File**: `04_materialized_views.sql`
**Scenario**: Pre-aggregate expensive GROUP BY queries so dashboards return instantly.
**Key Concept**: A Materialized View in ClickHouse is an INSERT trigger — it transforms and aggregates data as it arrives, storing results in a separate table.

```
INSERT → trips_raw → MV trigger → trips_hourly_agg
                                  (pre-summed per hour/zone)
                                  ← Dashboard queries hit this
```

**What You Learn**
- Creating a `SummingMergeTree` target table
- Creating a Materialized View with `SELECT ... GROUP BY`
- Verifying MV triggers on INSERT
- Query performance: raw table vs. MV target
- When to use MVs vs. projections

---

## Demo 5: Query Profiling

**File**: `05_query_profiling.sql`
**Scenario**: Use ClickHouse built-in profiling tools to diagnose slow queries.
**Key Concept**: `EXPLAIN`, `system.query_log`, and `system.query_thread_log` give complete visibility into what the query engine is doing.

**What You Learn**
- `EXPLAIN` plan levels: syntax, pipeline, indexes
- `system.query_log` for historical query analysis
- `read_rows`, `read_bytes`, `elapsed_ms` as key performance metrics
- Finding the most expensive queries in your workload
- Identifying missed index opportunities

---

## Lab Exercises

**File**: `06_lab_exercises.sql`

| Exercise | Topic | Difficulty |
|----------|-------|-----------|
| 1 | Run EXPLAIN on two queries, explain which uses the index better | ★☆☆ |
| 2 | Add a minmax skip index on `trip_distance` and test it | ★★☆ |
| 3 | Create a projection for queries filtering on `dropoff_ntaname` | ★★☆ |
| 4 | Build a materialized view for daily revenue per neighborhood | ★★★ |
| 5 | Use `system.query_log` to find the slowest query from the session | ★★★ |

---

## Cleanup

```bash
ch --queries-file 99_cleanup.sql
```
