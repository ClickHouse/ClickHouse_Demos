# Worksheet 1: MergeTree Engine Selection

**Time estimate:** 15–20 minutes
**Reference:** [`docs/mergetree_guide.md`](../docs/mergetree_guide.md)

---

## Concept

In Snowflake, you create a table and Snowflake decides how to store it. In ClickHouse, you choose a storage engine — and this choice determines correctness, not just performance.

The three engines you need for this lab:

**MergeTree** — The base engine. Data is stored in sorted columnar files. No deduplication. Use this when the table is insert-only or when your pipeline manages updates externally (e.g., full reload on every dbt run).

**ReplacingMergeTree(version_col)** — Extends MergeTree with background deduplication. When rows with the same `ORDER BY` key exist in multiple parts, only the row with the highest `version_col` value is kept after a merge. Use this when rows can be updated and you have a column that increases monotonically on every update (e.g., `updated_at` timestamp).

> **Critical gotcha:** Deduplication is asynchronous. Until ClickHouse runs a background merge, both the old and new version of a row coexist. Always use `SELECT ... FINAL` on ReplacingMergeTree tables to force deduplication at query time.

**AggregatingMergeTree** — Extends MergeTree with partial aggregate state merging. Use this when the table stores combinable aggregate states (e.g., HyperLogLog sketches, quantile digests) and you need background aggregation. Not needed for the NYC Taxi lab — the aggregation tables are rebuilt by dbt, not accumulated.

### Decision Tree

```
Does the table receive UPDATE or DELETE operations?
│
├── No (insert-only)
│   └─► MergeTree()
│
└── Yes
    ├── Is there a timestamp/version column that increases on every update?
    │   ├── Yes → ReplacingMergeTree(version_col)
    │   └── No (e.g., full-reload dimension tables)
    │       └─► MergeTree()  — dbt handles upsert via atomic table swap (full rebuild)
    │
    └── Does the table store partial aggregate states (AggregateFunction types)?
        └── Yes → AggregatingMergeTree()
```

---

## Exercise: Engine Selection for NYC Taxi Tables

For each table below:
1. Determine the update pattern from the description
2. Identify the version column (if any)
3. Select the correct engine
4. Write a one-sentence reasoning

Fill in the `?` cells:

| Table | Update Pattern | Version Column | Recommended Engine | Reasoning |
|-------|----------------|----------------|--------------------|-----------|
| `trips_raw` | Python migration script writes rows in batches; the script may be retried, re-inserting the same `trip_id` | ? | ? | *(think: can the same trip_id appear twice?)* |
| `fact_trips` | Trips can be corrected after the fact (fare adjustments, status changes) — same `trip_id` re-inserted with new values | ? | ? | ? |
| `agg_hourly_zone_trips` | dbt recalculates the last 2 hours on each run and re-inserts results | ? | ? | ? |
| `dim_taxi_zones` | dbt runs a full rebuild on each run (atomic table swap (full rebuild)) | — | ? | ? |
| `dim_payment_type` | dbt runs a full rebuild on each run (atomic table swap (full rebuild)) | — | ? | ? |
| `dim_vendor` | dbt runs a full rebuild on each run (atomic table swap (full rebuild)) | — | ? | ? |
| `mv_hourly_revenue` | ClickHouse REFRESHABLE MV recalculates on a schedule | — | ? | ? |

**Hints:**
- `trips_raw`: The Python migration script (`scripts/02_migrate_trips.py`) loads rows in batches and supports `--resume` for retries. A retried batch may re-insert rows with the same `trip_id`. After cutover, the live producer can also retry on transient failures. Is there a column that captures insertion time?
- `fact_trips`: What does "same trip_id re-inserted with new values" mean for deduplication?
- `agg_hourly_zone_trips`: What happens when dbt inserts a new row for `(2024-01-01 14:00, zone_id=1)` that already exists from the last run?
- `dim_*` tables: dbt's `table` materialization in dbt-clickhouse does atomic table swap (full rebuild), replacing all rows on every run. Does this require deduplication?
- `mv_hourly_revenue`: A REFRESHABLE MV replaces its entire result set on each refresh. What engine fits a table that is always fully replaced?

---

## Reflection Questions

Before checking the answer key, answer these questions in your own words:

1. Why can't you use `MergeTree()` for `fact_trips` if trips can be updated?

2. What happens if you query `fact_trips` without `FINAL` on a `ReplacingMergeTree` table?

3. Why is `AggregatingMergeTree` not needed for `agg_hourly_zone_trips` even though it's an aggregation table?

---

<details>
<summary>▶ Answer Key — try the exercises first before expanding</summary>

| Table | Update Pattern | Version Column | Recommended Engine | Reasoning |
|-------|----------------|----------------|--------------------|-----------|
| `trips_raw` | Migration retries + producer retries | `_synced_at` | `ReplacingMergeTree(_synced_at)` | The Python migration script may retry a batch and re-insert the same `trip_id`. After cutover, the live producer may also retry on transient failures. `_synced_at DateTime DEFAULT now()` is set automatically on INSERT — a later retry has a higher value, so RMT keeps the most recent write. `stg_trips` queries with `FINAL` to guarantee dedup before any analytics model sees the data. |
| `fact_trips` | Upsert (trip corrections) | `updated_at` | `ReplacingMergeTree(updated_at)` | Same trip_id can arrive twice with different fare/status values. RMT keeps the row with the highest `updated_at`. Always query with `FINAL`. |
| `agg_hourly_zone_trips` | Upsert (rolling 2-hr recalculation) | `updated_at` | `ReplacingMergeTree(updated_at)` | dbt recalculates the last 2 hours and re-inserts. Without RMT, old and new aggregates accumulate and double-count. `updated_at` set to `now()` on each dbt run ensures the latest values win. |
| `dim_taxi_zones` | Full reload | — | `MergeTree()` | dbt's `table` materialization does atomic table swap (full rebuild), so there is never more than one generation of rows. No deduplication needed. |
| `dim_payment_type` | Full reload | — | `MergeTree()` | Same as `dim_taxi_zones`. |
| `dim_vendor` | Full reload | — | `MergeTree()` | Same as `dim_taxi_zones`. |
| `mv_hourly_revenue` | Full replacement on each REFRESH | — | `MergeTree()` | REFRESHABLE MVs replace their entire result set atomically. No duplicates can accumulate. |

**Reflection answers:**

1. With `MergeTree`, every version of a trip row is kept permanently. If trip `T001` is inserted twice with different fare amounts, both rows appear in `SELECT * ... WHERE trip_id = 'T001'`, and `SUM(fare_amount)` doubles. ReplacingMergeTree deduplicates on merge so only the latest version is retained.

2. Without `FINAL`, ClickHouse may return multiple versions of the same row — both the original insert and any updates — because background merges are asynchronous. Aggregates (SUM, COUNT) over the full table will over-count until the next merge runs. `FINAL` forces synchronous deduplication at read time.

3. `AggregatingMergeTree` is for tables where rows store *partial aggregate states* (binary `AggregateFunction` columns) that need to be merged across rows. `agg_hourly_zone_trips` stores final values (INT, FLOAT) that must be deduplicated via upsert — the latest row wins, it is not combined with prior rows. `ReplacingMergeTree` is correct here; `AggregatingMergeTree` would require rewriting all queries to use `-Merge` suffix combiners.

</details>

---

## Transfer to migration-plan.md

Once you have filled in this worksheet, copy your engine decisions to Section 3 of `migration-plan.md` and check off:

```
- [ ] Engine selection: completed
```
