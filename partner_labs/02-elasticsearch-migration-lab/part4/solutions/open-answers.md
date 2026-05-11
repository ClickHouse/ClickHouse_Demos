# Part 4 — Open-Ended Model Answers

> **Don't open this until you've completed all five exercises in [`exercises/open-questions.md`](../exercises/open-questions.md).** These are *model* answers, not the only correct answers — your reasoning matters more than matching the wording. The self-grade rubric below each exercise tells you what counts as "passed".

---

## Exercise 1 — Schema design

### Model `CREATE TABLE`

```sql
CREATE TABLE IF NOT EXISTS clickstream_events
(
    `event_id`   UUID                                                  CODEC(ZSTD(1)),
    `event_time` DateTime64(3)                                          CODEC(Delta, ZSTD(1)),
    `event_date` Date          DEFAULT toDate(event_time),
    `user_id`    UUID                                                  CODEC(ZSTD(1)),
    `session_id` UUID                                                  CODEC(ZSTD(1)),
    `product_id` LowCardinality(String)                                CODEC(ZSTD(1)),
    `event_type` LowCardinality(String)                                CODEC(ZSTD(1)),
    `value_usd`  Decimal(18, 4)                                        CODEC(ZSTD(1)),
    `attributes` Map(LowCardinality(String), String)                   CODEC(ZSTD(1)),

    -- Skip indexes for non-prefix point-lookups
    INDEX idx_user_id    user_id    TYPE bloom_filter(0.01) GRANULARITY 4,
    INDEX idx_session_id session_id TYPE bloom_filter(0.01) GRANULARITY 4
)
ENGINE = MergeTree
PARTITION BY event_date
ORDER BY (event_type, product_id, event_time)
TTL event_date + INTERVAL 90 DAY DELETE
SETTINGS ttl_only_drop_parts = 1, index_granularity = 8192;
```

### Rationale

- **Type choices:**
  - `event_id` → `UUID` (16 bytes vs. 36 if stored as `String`; the column is rarely filtered on, just stored).
  - `user_id` / `session_id` → `UUID` for the same reason; ~50 M / ~500 M cardinality means *not* `LowCardinality`.
  - `event_type` → `LowCardinality(String)` — 12 distinct values is the textbook case. Could also use `Enum`, but `LowCardinality(String)` interoperates better with the inevitable case where someone adds a 13th event type without redeploying schema.
  - `product_id` → `LowCardinality(String)` is borderline (2 M values is at the upper edge); benchmark before committing. If the cardinality grows past 10 M, switch to plain `String`.
  - `attributes` → `Map(LowCardinality(String), String)` is the schema-evolution-safe default. Promote hot keys to MATERIALIZED columns over time.
  - `value_usd` → `Decimal(18, 4)` to avoid floating-point rounding on revenue numbers. NULL-or-zero behavior is fine; no need for `Nullable`.

- **`ORDER BY (event_type, product_id, event_time)`:**
  - Query #1 (funnel by product_id) starts with `WHERE event_type IN ('view','add_to_cart','purchase') AND product_id = X` — both prefix columns hit. Massive granule pruning.
  - Query #3 (revenue by minute) filters `WHERE event_type = 'purchase'` — also prefix.
  - Query #2 (per-user history) doesn't use the prefix at all — that's what the `bloom_filter` skip index on `user_id` is for.
  - **Cardinality ordering** rule: low (12 values) → medium (2 M) → high/range (timestamp). Low-cardinality leading columns give the index more skipping power.

- **`PARTITION BY event_date`:**
  - One partition per day; with 5 B events/day the partition stays large enough to avoid the "thousands of tiny partitions" anti-pattern.
  - Critically, this aligns with the TTL boundary so `ttl_only_drop_parts = 1` can drop entire partitions cheaply.
  - **Don't** partition by hour or by `event_type`; either creates partition-count explosion.

- **TTL `event_date + INTERVAL 90 DAY DELETE`** with `ttl_only_drop_parts = 1`: drops entire 90-day-old daily partitions in one operation. No row-by-row scan.

- **Skip indexes:** `bloom_filter` on `user_id` (Query #2's join condition) and `session_id` (likely future need). Don't use `tokenbf_v1` here — these are UUIDs, not text. Granularity = 4 means each bloom covers 4 × 8192 = ~32K rows; tune by measuring lookup latency.

- **AggregatingMergeTree for Query #3?** *Yes* — define an MV:
  ```sql
  CREATE MATERIALIZED VIEW revenue_by_minute_mv
  TO revenue_by_minute AS
  SELECT
      toStartOfMinute(event_time) AS minute,
      product_id,
      sumState(value_usd)         AS revenue,
      countState()                AS purchase_count
  FROM clickstream_events
  WHERE event_type = 'purchase'
  GROUP BY minute, product_id;
  ```
  At 5 B events/day, dashboard queries against the raw table will be slow under contention; the AggMergeTree pre-aggregation moves the cost to insert time. Query #1 (funnel) and Query #2 (per-user) shouldn't get an MV — they're too high-cardinality and too varied to pre-aggregate.

### Self-grade

You **pass** this exercise if your answer:
- Uses `MergeTree` with a sensible `ORDER BY` whose prefix matches Query #1 or Query #3 (i.e., not `(event_id, event_time)` or `(event_time, …)`)
- Partitions by date (or week), not by event_type or by hour
- Uses `Map` (or JSON) for `attributes`, not one column per attribute
- Has at least one bloom_filter skip index on a high-cardinality column used by Query #2
- Includes a TTL clause aligned with the partition boundary

You **fail** if your answer puts `event_time` first in `ORDER BY` (a common ES instinct that hurts ClickHouse), or partitions by anything other than a date-derived column.

---

## Exercise 2 — Migration plan

### Model 6-month plan

**Phase 1 (weeks 1–2): Discovery & sandbox.**
Stand up a ClickHouse Cloud Production-tier service in the same region as ES. Provision a parallel OTel Collector cluster (3 nodes, behind LB) that reads from a tap into the existing ingest path — initially write-only to CH, NOT routed to from production traffic. Goal: validate connection, baseline costs at 200K events/sec sustained, prove backpressure handling.
**Exit criterion:** CH ingests 1 hour of dual-streamed data with parity check (ES count vs CH count) ≤ 1% drift.

**Phase 2 (weeks 3–6): Schema design + first 5 high-value indexes.**
Map the top 5 most-queried Kibana dashboards' field requirements. Build `CREATE TABLE` statements with `Map(LowCardinality(String), String)` for the 60+ grok-extracted fields by default, but materialize the top 10 hot fields (those referenced by ≥3 dashboards) as dedicated columns. Define the materialized-view fan-out for any AggregatingMergeTree pre-aggregations needed.
**Exit criterion:** All 20 Kibana dashboards' core queries have a working CH equivalent and return results within the same wall-clock as ES (or faster).

**Phase 3 (weeks 7–12): Parallel run + dashboard migration.**
Switch the OTel Collector to dual-write (ES + CH). Migrate dashboards in waves of ~50 saved searches per week, prioritizing read-heavy ones (those that scan >1 TB). Run [validation scripts](../../part3/scripts/validate_migration.sh) twice daily; investigate any service whose ES vs CH count drifts > 5%.
**Exit criterion:** All 300 saved searches functioning on CH; query latency p95 ≤ ES p95 for 7 consecutive days.

**Phase 4 (weeks 13–18): Logstash decommission.**
Replace Logstash with OTel Collector. The 60+ grok patterns become OTel `transform` processors and CH MATERIALIZED columns. (See Logstash decision below.) Run Logstash + OTel Collector in parallel for 2 weeks; drop Logstash once parity holds.
**Exit criterion:** Zero traffic flowing through Logstash for 7 consecutive days; alerting rules migrated to HyperDX Alerts (or equivalent).

**Phase 5 (weeks 19–22): Cutover.**
Stop Filebeat → Logstash. Delete Logstash. Stop ES new-data ingest (keep cluster running, read-only). All new data flows ES-free. Monitor cost dashboards and dashboard latency.
**Exit criterion:** ES is read-only for 14 consecutive days with no ingest, no incident, no rollback request.

**Phase 6 (weeks 23–26): Decommission & compliance handover.**
Final ES snapshot to S3 (one-shot, full snapshot at cutover-date). Tear down ES cluster. Document the S3 bucket location for compliance/audit. Switch the team's mental model from "ES is the source of truth" to "ES is the 90-day rear-view mirror".
**Exit criterion:** ES cluster destroyed, S3 snapshot validated by compliance team, runbook updated.

---

### Schema strategy for 60+ grok-extracted fields

- **Month 1:** All 60 fields land in `LogAttributes Map(LowCardinality(String), String)`. No special handling.
- **Month 2:** Identify the top 10 fields by dashboard reference count. Promote them via `ALTER TABLE … ADD COLUMN field MATERIALIZED LogAttributes['field']`.
- **Month 4:** Re-run the dashboard-reference query; promote any *new* hot fields. By this point the long tail is in Map and stays there.
- **Month 6:** Run a final dashboard query-pattern audit. The schema typically converges at 15–25 promoted columns + the Map for the long tail.

This avoids the "design the perfect schema upfront" trap that kills migration projects.

---

### Logstash decision: **replace with OTel Collector**

The 60+ grok patterns are the cost driver in Logstash, both compute and operational complexity. OTel Collector's `transform` processor (OTTL) covers ~80% of grok use cases natively, and the remaining 20% become MATERIALIZED columns in the target schema (effectively shifting the parsing cost from the collector path to the storage layer, which in ClickHouse means *zero* runtime cost — the parsed value is computed once at insert and read for free thereafter).

Vector is a viable alternative — it has VRL (Vector Remap Language) which is more familiar to Logstash users — but introduces a third tool to operate. Stick with OTel Collector for ecosystem consistency.

**Don't** keep Logstash. The lab's narrative is one less tool, not "ES + Logstash + Kibana → CH + Logstash + HyperDX".

---

### Compliance retention (1-year cold tier)

CH Cloud has no built-in S3-snapshot ILM, but two patterns work:

1. **Daily export to customer-owned S3.** Run a scheduled `INSERT INTO FUNCTION s3('s3://archive/year/month/day.parquet') SELECT * FROM otel_logs_v2 WHERE event_date = today() - 1`. The customer's S3 bucket holds 1 year of Parquet, queryable via Athena, Trino, or `s3()` table function from a temporary CH service. Storage is ~$23/TB/month for S3 IA, much cheaper than keeping 1 year hot in CH.
2. **Use ClickHouse Cloud's tiered storage** (where available — typically Production tier on certain providers). Storage cost beyond 90 days drops by ~70%, but compliance auditing is more complex than the S3-Parquet approach because the data is still inside CH.

Recommend (1) for compliance — the auditor wants "show me all logs from Aug 2025" to be a self-service S3 query, not a "let's spin up a CH cluster" project.

---

### Rollback plan if cutover fails 3 weeks in

Rollback assumes (a) you're already in dual-write mode and (b) a portion of dashboards have been migrated.

1. **Stop CH-only writes** — flip the OTel Collector back to single-write to ES; CH stops growing.
2. **Switch dashboards back to Kibana** — the original Kibana saved searches still exist. Re-enable them.
3. **Keep CH running for read-only forensics** — the data ingested during the parallel-run window is valuable for debugging the cutover failure.
4. **Don't delete the migrated artifacts** — the new CH schema, OTel Collector configs, etc. remain. Once the root cause is fixed, restart from Phase 4 (Logstash decommission), don't re-do Phases 1–3.

The single most important property of the migration: **at any point in the first 5 phases, you can fall back to ES with one OTel Collector config change**.

---

### Top 3 risks + mitigations

1. **Risk: schema design locks in too early.** Mitigation: explicitly plan a "hot-fields review" at month 2, 4, 6. Promotions are non-destructive ALTER TABLE statements.
2. **Risk: 5 alerting rules behave differently in CH.** Mitigation: build the alert MVs (per [Part 3 Step 8 Option B](../../part3/README.md#option-b-pre-computed-alert-table)) in week 8 and run them in shadow mode for 4 weeks before enabling pages. Compare fire counts to Kibana's existing alert history.
3. **Risk: customer's grok patterns include unsupported regex constructs.** Mitigation: enumerate the 60 patterns in week 1, identify the unsupported ones (typically lookbehind, named-conditional, etc.), rewrite as OTTL or as CH `regexpExtract` materialized columns. Don't discover this in week 14.

### Self-grade

You **pass** this exercise if your plan:
- Has clear phases, each with an exit criterion
- Replaces Logstash with OTel Collector (or Vector) — keeping Logstash through cutover is a fail
- Uses Map + selective promotion for the 60 fields, not "create all 60 columns upfront" or "JSON column for everything"
- Names a specific compliance-retention pattern (S3 Parquet export, tiered storage, or equivalent) — "we'll figure out compliance later" is a fail
- Identifies a rollback path that doesn't require re-running a CSV import

---

## Exercise 3 — Debugging A (p95 latency drift)

### Model answer

Three most likely causes, in order:

**Cause 1: The columns being aggregated have different units.**
ES typically stored the latency as a milliseconds float (`response_time` in seconds × 1000 in some ES setups, or already-ms). ClickHouse's `Duration` field for traces is in nanoseconds; if your CH query computes `quantile(0.95)(Duration)` without dividing by 1e6, you'll get a number ~1000× larger. Or — more subtly — if your CH query reads `LogAttributes['run_time']` which the application emits in seconds while ES had a parsed `run_time_ms` field already converted, the units differ silently.
**Check:** Run `SELECT min(Duration), max(Duration), avg(Duration) FROM otel_traces` and compare to the ES `min/max/avg` values for the same window. If the orders of magnitude differ, it's units.

**Cause 2: Different quantile algorithms.**
Elasticsearch's `percentiles` aggregation uses HDR Histogram or T-Digest depending on version — both *approximate* algorithms. ClickHouse's `quantile()` is also approximate (a deterministic but different algorithm). For long-tailed latency distributions, the two approximations can diverge by 10–25% even with identical input data. ClickHouse's `quantileExact()` is exact but slow; `quantileTDigest()` matches ES more closely.
**Check:** Run the same query with `quantileExact(0.95)(Duration)` (slow but precise). If the exact value lands between ES and CH approximations, the algorithms simply chose different quantile-estimation strategies.

**Cause 3: Different time windows or sample sets due to clock skew or filter mismatch.**
The CH query may be including a few seconds of additional traffic at either end of the window because `now() - INTERVAL 1 HOUR` resolves to a different wall-clock instant than the ES query did, or because the materialized `TimestampTime` column is `DateTime` (second precision) while ES filtered on millisecond-precision `@timestamp`. With a long tail, even a 5-second skew can shift p95 by 20%.
**Check:** Pin the time window with explicit `BETWEEN '2026-05-09 04:00:00' AND '2026-05-09 05:00:00'` on both sides. Re-run. If results converge, it was time-range alignment.

### Self-grade

You **pass** if your answer includes at least:
- One unit-conversion / type-mismatch cause (Duration in ns vs ms)
- Either the quantile-algorithm-difference cause OR the time-window alignment cause
- A specific debug check for each cause, not just "investigate"

---

## Exercise 4 — Debugging B (slow Map-key lookup)

### Diagnosis

`LogAttributes['request_id']` is a runtime *map dereference* — for each row scanned, ClickHouse has to look up the key `'request_id'` inside the Map. There's no skip index that helps, and the column store still has to materialize the Map column for every row in the scan range. In contrast, `TraceId` is a top-level `String` column with a `bloom_filter` skip index in [otel_logs_v2's schema](../../part3/clickhouse/schema.sql), so the per-granule pre-check eliminates ~99.9% of granules before reading any data.

So the 200× slowdown isn't because Map is slow — it's because the *skip-index optimization* doesn't apply to map keys.

### Quick fix: bloom-filter skip index on `mapKeys` / `mapValues`

```sql
ALTER TABLE otel_logs_v2 ADD INDEX idx_request_id
    mapValues(LogAttributes)
    TYPE bloom_filter(0.01)
    GRANULARITY 4;

ALTER TABLE otel_logs_v2 MATERIALIZE INDEX idx_request_id;
```

This builds a per-granule bloom of all map values; queries against any specific value (including `request_id` lookups) get the skip-index benefit. Trade-off: the index covers *all* map values, so collisions are higher than a dedicated bloom on a single column — but it works for any of the 30+ keys in `LogAttributes` without separate indexes. Quick because it's one ALTER, no downtime.

### Durable fix: promote `request_id` to a materialized column

```sql
ALTER TABLE otel_logs_v2 ADD COLUMN request_id String MATERIALIZED LogAttributes['request_id'];
ALTER TABLE otel_logs_v2 ADD INDEX idx_request_id request_id TYPE bloom_filter(0.01) GRANULARITY 4;
ALTER TABLE otel_logs_v2 MATERIALIZE COLUMN request_id, INDEX idx_request_id;
```

Now `request_id` is a top-level column with its own compression and its own bloom filter. Query: `WHERE request_id = 'abc123'` instead of `WHERE LogAttributes['request_id'] = 'abc123'`. Trade-off: each promotion costs one column-rewrite (a one-time MATERIALIZE pass). Worth it for hot keys; not worth it for the long-tail keys in `LogAttributes`.

This is exactly the schema-evolution pattern from [Part 2's ADR Decision 3](../../part2/solutions/adr-solution.md): default to Map, promote hot keys.

### Self-grade

You **pass** if your answer:
- Identifies that the issue is "no skip-index path on map dereference", not "ClickHouse is slow"
- Names a specific quick fix (Map-level bloom OR full scan acknowledgment OR specifically targeted regex skip index)
- Names the promotion-to-column durable fix
- Acknowledges the trade-off of each (Map index = broader collisions; promotion = ALTER cost)

---

## Exercise 5 — Trade-off analysis (alerting designs)

### Comparison table — model answer

| Property | Design A (query-time) | Design B (pre-aggregated MV) |
|---|---|---|
| Read cost per evaluation | High — every 60s, ClickHouse scans 5 minutes of raw rows. At 1M events/s × 300s = 300M rows scanned per evaluation. | Low — the MV produces ~1 row/minute per service. The 5-minute query reads ~5 rows. Microsecond response. |
| Write cost per ingested row | Zero — no per-row work | Small but non-zero — each insert triggers the MV's per-bucket aggregation update. Cost is bounded by the bucket count, not row count. Empirically <5% overhead. |
| Update lag | ~60s (poll interval) + the query latency | ~60s (poll interval); the MV itself is updated synchronously per insert |
| Cardinality scaling (100 svc × 50 endpoints) | Same scan cost regardless of cardinality (scans raw table) | The MV's row count = 5000 service-endpoint groups × 1/min = 7.2 M rows/day. Still tiny vs raw events; no scaling concern |

### Recommendation at 1M events/sec — **Design B wins**

The break-even point between A and B comes when raw-table scan cost crosses the per-row MV update cost. At 1M events/sec:

- Design A scans 300M rows per evaluation. Even at 1 GB/s per CPU on cold cache, that's measured in seconds per query, every 60 seconds, all day. Over a week, this is millions of CPU-seconds of pure alerting overhead.
- Design B amortizes the same aggregation across the insert path, where the cost is paid *once per row* rather than *re-paid every minute*. Insert-time aggregation also benefits from columnar batching; the per-bucket state update is essentially free at modern SIMD throughput.

**The right intuition:** alerting via raw-data re-scan is the *Elasticsearch transform pattern*. Alerting via pre-aggregated MVs is the *ClickHouse-native pattern*. Volume turns the latter from a nicety into a requirement around 100K events/sec; at 1M events/sec, Design A would consume more CPU than the actual workload it's supposed to monitor.

### Side note: where Design A still makes sense

- Low-volume environments (<10K events/sec) where evaluation cost is irrelevant
- Ad-hoc or temporary alerts where you don't want to provision an MV
- Alerts whose query shape changes weekly (the MV's schema is fixed; ad-hoc queries are flexible)

### Self-grade

You **pass** if your answer:
- Correctly identifies Design B's read cost as O(buckets) and Design A's as O(raw events scanned)
- Recognizes the symmetric trade-off: A pays nothing on write, lots on read; B pays a little on write, almost nothing on read
- Picks Design B at 1M events/s and explains it via amortization (not just "MVs are faster")
- Gets bonus credit for noting that this is the *insert-time vs query-time* aggregation pattern, which is the same pattern Part 3's `logs_summary_1min` AggregatingMergeTree demonstrates

---

## Total scoring

If you passed ≥ 4 of 5 exercises by the rubrics above, you're cleared on Part 4. If you passed 3 or fewer, the most common gaps are:

- **Exercise 1:** treating ClickHouse like Elasticsearch (storing one column per attribute, partitioning by event_type, ordering by timestamp first). Re-read [schema.sql](../../part3/clickhouse/schema.sql) and Part 2's worksheet.
- **Exercise 2:** missing the rollback story or the compliance answer. The lab's narrative repeatedly emphasizes that *the migration is reversible until decommission* — that's the property to internalize.
- **Exercise 4:** assuming Map types are inherently slow. They're not — the issue is purely about which optimizations apply.

**Done!** Don't forget to clean up: `bash ../../common/cleanup.sh`.
