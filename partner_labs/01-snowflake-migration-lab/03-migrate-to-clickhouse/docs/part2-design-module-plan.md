# New Part 2: Migration Planning & Design — Plan

## Context

The lab currently jumps directly from Part 1 (Snowflake environment setup) into hands-on migration execution. The CHMA Vision Document (PRISM framework) identifies this as the most common reason migrations underperform: partners start moving data before they understand the fundamental architectural decisions that determine whether the target system performs correctly at all.

The fix: insert a new **Part 2 — Plan & Design** module between the existing Part 1 and the current Part 2 (which becomes Part 3). This new module embodies PRISM Phases **P (Profile)** and **R (Re-architect)** — helping partners understand the differences between Snowflake and ClickHouse, profile their actual workload, and make explicit engine/schema/key design decisions *before* writing a single line of migration code.

**Structural change:**
- `01-setup-snowflake/` → **Part 1** (unchanged)
- NEW `02-plan-and-design/` → **Part 2** (this module)
- `02-migrate-to-clickhouse/` → renamed `03-migrate-to-clickhouse/` **Part 3**

---

## What the New Part 2 Contains

### No infrastructure to provision — this module is entirely knowledge-driven.

Partners use the **live Part 1 Snowflake environment** as their profiling target. The module produces a completed `migration-plan.md` that Part 3 then executes.

---

## File Structure

```
02-plan-and-design/
├── README.md
├── migration-plan.md                       # THE OUTPUT: template → partner fills in → Part 3 uses it
├── scripts/
│   ├── 01_profile_snowflake.sh             # Auto-profiling: inventory, query history, table stats → profile_report.md
│   └── 02_query_history.sql                # Snowflake SQL for ACCOUNT_USAGE.QUERY_HISTORY
├── worksheets/
│   ├── 01_mergetree_engine_selection.md    # Guided: engine per table + decision reasoning
│   ├── 02_sort_key_design.md               # Guided: ORDER BY from query workload
│   ├── 03_schema_translation.md            # Guided: type + function mapping per column
│   └── 04_migration_wave_plan.md           # Guided: dependency order + complexity grades
├── docs/
│   ├── snowflake_vs_clickhouse.md          # Architecture + SQL dialect comparison (reference)
│   └── mergetree_guide.md                  # MergeTree family deep-dive (reference)
└── examples/
    └── nyc_taxi_completed_plan.md          # Worked example: all decisions pre-filled for NYC Taxi
```

---

## File Detail

### `README.md`
- Explains PRISM Phases P + R in partner-accessible language
- States the module's single output: a completed `migration-plan.md`
- Flow: Run profiling script → Work through 4 worksheets → Fill in migration-plan.md → Proceed to Part 3
- No credentials beyond what Part 1 already set up (uses same `.env` + Snowflake access)
- Estimated time: 90–120 minutes

### `scripts/01_profile_snowflake.sh`
Connects to Part 1's Snowflake via `snowsql` and generates `profile_report.md`. Sections:

1. **Object Inventory** — queries `INFORMATION_SCHEMA.TABLES`, `SHOW STREAMS`, `SHOW TASKS`, `SHOW VIEWS`; outputs table with: object_name, type, schema, row_count, has_variant_columns, complexity_grade (A/B/C/D)
2. **Query Workload** — queries `SNOWFLAKE.ACCOUNT_USAGE.QUERY_HISTORY` for top 10 queries by total_elapsed_time; lists: query_text (truncated), executions, avg_ms, max_ms, bytes_scanned
3. **Table Statistics** — for each analytics table: row count, date range, null rates for key columns, VARIANT column usage frequency
4. **Schema Compatibility Gaps** — automatically flags: any VARIANT columns (require JSONExtract*), any QUALIFY usage in stored queries, any MERGE INTO patterns, presence of Streams/Tasks (require ClickPipes replacement)

Outputs: `./profile_report.md` in the lab directory.

Falls back gracefully if ACCOUNT_USAGE is unavailable (requires 1-hr data propagation delay): uses INFORMATION_SCHEMA alternatives.

### `scripts/02_query_history.sql`
Plain SQL to run manually in Snowflake UI if the shell script ACCOUNT_USAGE query is blocked by permissions:
```sql
SELECT query_text, execution_status, total_elapsed_time, bytes_scanned, ...
FROM SNOWFLAKE.ACCOUNT_USAGE.QUERY_HISTORY
WHERE database_name = 'NYC_TAXI_DB'
  AND start_time >= DATEADD(day, -7, CURRENT_TIMESTAMP())
ORDER BY total_elapsed_time DESC
LIMIT 20;
```

### `worksheets/01_mergetree_engine_selection.md`
Guided worksheet teaching partners *why* engine selection is a correctness decision, not a performance tuning knob.

Structure:
- Brief theory: 3 paragraphs on MergeTree, ReplacingMergeTree, AggregatingMergeTree — when each is right
- Decision tree (ASCII): Does the table get UPDATEs? → Is there a version/timestamp column? → ReplacingMergeTree. No UPDATEs? → MergeTree. Pre-aggregated with partial states? → AggregatingMergeTree.
- **Fill-in table** — one row per table, partner fills in: update_pattern, version_column, recommended_engine, reasoning

| Table | Update Pattern | Version Column | Recommended Engine | Reasoning |
|-------|----------------|----------------|--------------------|-----------|
| `trips_raw` | Insert-only (ClickPipes) | — | `MergeTree` | _partner fills_ |
| `fact_trips` | Upsert (trip updates) | `updated_at` | _?_ | _partner fills_ |
| `agg_hourly_zone_trips` | Upsert (rolling recalc) | `updated_at` | _?_ | _partner fills_ |
| `dim_taxi_zones` | Full reload | — | _?_ | _partner fills_ |
| `dim_date` | Full reload | — | _?_ | _partner fills_ |
| `dim_payment_type` | Full reload | — | _?_ | _partner fills_ |

- **Answer key** in a collapsible note at the bottom (not visible until partner scrolls past a divider)

### `worksheets/02_sort_key_design.md`
Teaches partners to derive ORDER BY from the query workload — not from the source schema.

Structure:
- Theory: 3 paragraphs. Key point: ORDER BY in ClickHouse is the primary index (sparse, binary search). Low-cardinality columns first, high-cardinality last. Query filter columns drive the design.
- **Filter column extraction exercise**: Show abridged versions of Q1-Q7. For each query, partner identifies:
  - What columns are in WHERE / GROUP BY / ORDER BY?
  - What's the cardinality of each? (borough = low, trip_id = high)
  - What column is most commonly filtered?
- **Fill-in sort key table**:

| Table | Most Common Filter | Cardinality Note | Proposed ORDER BY |
|-------|-------------------|-----------------|-------------------|
| `fact_trips` | `pickup_at` (all 7 queries) | pickup_at: medium; trip_id: UUID high | _partner fills_ |
| `agg_hourly_zone_trips` | `hour_bucket, zone_id` | hour_bucket: medium; zone_id: 265 values | _partner fills_ |
| `trips_raw` | `trip_id` (CDC lookups) | — | _partner fills_ |

- **Answer key** in collapsible section

### `worksheets/03_schema_translation.md`
Type-by-type and function-by-function translation, applied to the actual NYC Taxi columns.

Structure:
- **Type mapping table**: Partner fills in ClickHouse type for each Snowflake column of `TRIPS_RAW` and `FACT_TRIPS`

| Snowflake Column | SF Type | ClickHouse Type | Notes |
|------------------|---------|-----------------|-------|
| `TRIP_ID` | VARCHAR(36) | String | No native UUID type; String is fine |
| `PICKUP_DATETIME` | TIMESTAMP_NTZ | DateTime64(3, 'UTC') | _partner fills reasoning_ |
| `TRIP_METADATA` | VARIANT | String | _partner explains why String + JSONExtract*_ |
| `FARE_AMOUNT` | FLOAT | Float64 | |
| `VENDOR_ID` | INTEGER | UInt8 | _partner fills: why UInt8 not Int32?_ |

- **Function translation exercise**: Show 6 Snowflake expressions from Q1-Q7. Partner writes ClickHouse equivalent:

| Snowflake Expression | ClickHouse Equivalent |
|----------------------|-----------------------|
| `DATE_TRUNC('hour', pickup_at)` | _partner fills_ |
| `TRIP_METADATA:driver.rating::FLOAT` | _partner fills_ |
| `QUALIFY ROW_NUMBER() OVER (...) <= 10` | _partner fills_ |
| `DATEDIFF('minute', pickup_at, dropoff_at)` | _partner fills_ |
| `MERGE INTO ... WHEN MATCHED THEN UPDATE` | _partner fills_ |
| `METADATA$ACTION from CDC stream` | _partner fills_ |

- **Answer key** in collapsible section

### `worksheets/04_migration_wave_plan.md`
Dependency mapping and wave sequencing.

Structure:
- Theory: objects must migrate in dependency order (dimensions before facts, base tables before MVs)
- **Dependency DAG** (ASCII):
  ```
  Wave 0: trips_raw (base table, no deps)
            dim_taxi_zones, dim_date, dim_payment_type, dim_vendor
  Wave 1: stg_trips, stg_taxi_zones (depend on raw tables)
  Wave 2: int_trips_enriched (depends on staging)
  Wave 3: fact_trips, agg_hourly_zone_trips (depend on intermediate)
  Wave 4: mv_live_trip_feed (depends on fact_trips)
  ```
- **Complexity grading exercise**: Rate each object A (trivial) → D (requires redesign) and explain
- **Fill-in migration wave table**: partner assigns each object to a wave with dependencies listed
- **ClickPipes-specific note**: bulk load must complete before Wave 2-4 dbt models run (schema-first approach)

### `docs/snowflake_vs_clickhouse.md`
Reference document. 5 sections:

1. **Architecture Comparison**: Physical storage decisions (ClickHouse explicit vs Snowflake automatic), query execution model, concurrency model, cost model
2. **6 SQL Dialect Gaps** — with side-by-side examples for each Q1-Q7 challenge:
   - QUALIFY → subquery wrapper
   - VARIANT colon-path → JSONExtract*
   - LATERAL FLATTEN → pre-flatten or JSONExtract
   - MERGE INTO → delete_insert incremental or ReplacingMergeTree
   - Snowflake Streams → ClickPipes CDC
   - DATEADD/DATE_TRUNC → ClickHouse date functions
3. **Data Movement Options**: ClickPipes (Snowflake connector), remoteSecure(), object storage relay — when to use each
4. **CDC Approaches**: Snowflake Streams → ClickPipes vs Debezium → Kafka → ClickHouse
5. **Cost Model**: Snowflake credits (warehouse-based) vs ClickHouse Cloud (compute + storage separate)

### `docs/mergetree_guide.md`
Deep dive on the MergeTree family specifically for partners coming from Snowflake (no equivalent concept).

Covers: MergeTree, ReplacingMergeTree (deduplication semantics + FINAL), AggregatingMergeTree (partial states), CollapsingMergeTree (sign pattern), MergeTree with TTL. For each: when to use, key gotcha (especially RMT deduplication lag), ClickHouse-specific correctness risks.

### `examples/nyc_taxi_completed_plan.md`
A **fully worked example** of all 4 worksheets, completed with correct answers for the NYC Taxi workload. Partners compare their worksheet answers against this after finishing each section.

This also serves as the "answer key" for facilitators running the lab in a group setting.

### `migration-plan.md`
The primary **output artifact** of this module. Partners fill this in as they complete the worksheets.

Structure: 9 sections (matching the worksheets), each ending with a **completion checkbox**:
- `- [ ] Engine selection: completed` → partner changes to `- [x]` when done
- `- [ ] Sort key design: completed`
- `- [ ] Schema translation: completed`
- `- [ ] Migration wave plan: completed`

Sections:
1. **Profile Summary** — paste key numbers from `profile_report.md`
2. **Object Inventory** — list of tables being migrated, complexity grades
3. **Engine Selection Decisions** — one row per table (from Worksheet 1)
4. **Sort Key Design** — ORDER BY and PARTITION BY per table (from Worksheet 2)
5. **Schema Translation Notes** — any non-obvious type choices (from Worksheet 3)
6. **Migration Waves** — ordered wave table (from Worksheet 4)
7. **Known Dialect Gaps** — the 6 SQL constructs that need rewriting, with their ClickHouse equivalents
8. **Migration Strategy** — ClickPipes bulk + CDC (pre-selected for this lab; partners note why)
9. **Cutover Criteria** — row count parity ≥ 99.9%, checksum match on 10K sample, CDC lag < 60s

---

---

## Part 3 Changes (Consequential from this redesign)

### `setup.sh` scope: **Terraform only**
The current Part 3 `setup.sh` orchestrates 8 steps (Terraform → dbt → ClickPipes pause → verify → dbt again → dictionary → CDC pause → Superset → benchmark). This gets replaced with a minimal setup.sh that only provisions the ClickHouse Cloud cluster:

```bash
# New setup.sh behavior:
# Step 1: Validate env vars
# Step 2: terraform apply → output CLICKHOUSE_HOST, CLICKHOUSE_PORT
# Step 3: Write .clickhouse_state
# Step 4: Print: "Cluster provisioned. Follow README.md (Section 7) for all remaining steps."
```

Everything else (dbt, migration script, verification, dictionary, Superset, benchmark) is **documented in `README.md` (Section 7)** as explicit manual steps — partners run each individual script themselves, following the guide.

### Why this is better
- Migration decisions (dbt run, ClickPipes config, cutover) require partner judgment — automating them hides the reasoning
- Aligns with CHMA's principle: "Humans authorize the outcomes"
- Partners arrive at Part 3 with a completed migration-plan.md — the runbook maps directly to those decisions
- Individual scripts (`01_verify_migration.sh`, `run_benchmark.sh`, etc.) remain as standalone tools

### What stays the same in Part 3
- All individual scripts unchanged
- `teardown.sh` unchanged (still automates destroy — destroying infra is not a learning moment)
- `dbt/`, `terraform/`, `superset/` unchanged
- `README.md` (Section 7) is the **primary interface** for partners (comprehensive, 8 steps)

### What changes in Part 3
- `setup.sh`: strip all steps after Terraform; add pre-flight Part 2 check; print runbook pointer at end
- `README.md`: update Quickstart section — step 4 becomes "Follow README.md Section 7" instead of "Run setup.sh"
- `README.md`: add "Decision Alignment" table (from plan above)

---

## Directory Restructuring

### Files to rename/move:
- `01-snowflake-migration-lab/02-migrate-to-clickhouse/` → `01-snowflake-migration-lab/03-migrate-to-clickhouse/`
- Update README cross-reference links in both Part 1 and the moved Part 3

### Files to create (new):
- `01-snowflake-migration-lab/02-plan-and-design/` — all new files described above

---

## Implementation Chunks

### Chunk 1: Directory restructure + README (30 min)
- Rename `02-migrate-to-clickhouse/` → `03-migrate-to-clickhouse/`
- Update Part 1 README references to Part 2 → Part 3
- Update Part 3 README to reference Part 2 as a prerequisite
- Create `02-plan-and-design/README.md`

### Chunk 2: Reference documentation (45 min)
- `docs/snowflake_vs_clickhouse.md`
- `docs/mergetree_guide.md`

### Chunk 3: Profiling script (30 min)
- `scripts/01_profile_snowflake.sh`
- `scripts/02_query_history.sql`

### Chunk 4: Worksheets (60 min)
- `worksheets/01_mergetree_engine_selection.md`
- `worksheets/02_sort_key_design.md`
- `worksheets/03_schema_translation.md`
- `worksheets/04_migration_wave_plan.md`

### Chunk 5: Examples + migration plan template (30 min)
- `examples/nyc_taxi_completed_plan.md`
- `migration-plan.md` (blank template)

---

## Critical Design Decisions

1. **No new infrastructure** — partners use their existing Part 1 Snowflake credentials; no new `.env` needed
2. **Worksheets are fill-in, not read-only** — each worksheet has explicit blanks partners fill in; the point is reasoning, not reading
3. **Answer keys are present but deferred** — collapsible sections let partners check their work but don't hand them the answer
4. **`migration-plan.md` is a learning artifact, not a config file** — Part 3's decisions are pre-baked in the dbt models and terraform. The plan document exists so partners *reason through* the architecture before they see the implementation.
5. **Profiling script is best-effort** — ACCOUNT_USAGE requires ACCOUNTADMIN and has 1–3hr lag; script falls back to INFORMATION_SCHEMA alternatives with a clear warning

---

## How Part 2 and Part 3 Connect

This is the most important design question. The connection is **pedagogical, not mechanical** — but Part 3 enforces it with a soft gate.

### Part 3's `setup.sh` pre-flight check (new addition to Part 3)
At the very top of Part 3's `setup.sh`, before any Terraform runs, add:
```bash
# ── Part 2 prerequisite check ─────────────────────────────────
PART2_PLAN="${SCRIPT_DIR}/../02-plan-and-design/migration-plan.md"
if [[ ! -f "${PART2_PLAN}" ]]; then
  warn "Part 2 migration-plan.md not found at ${PART2_PLAN}"
  warn "It is strongly recommended to complete Part 2 (Plan & Design) before continuing."
else
  # Check completion markers
  COMPLETED=$(grep -c '\- \[x\]' "${PART2_PLAN}" || echo 0)
  TOTAL=$(grep -c '\- \[[ x]\]' "${PART2_PLAN}" || echo 0)
  info "Part 2 migration plan: ${COMPLETED}/${TOTAL} sections completed"
  if [[ "${COMPLETED}" -lt "${TOTAL}" ]]; then
    warn "Not all Part 2 sections are marked complete — consider finishing the worksheets first."
  fi
fi
```
This warns but never blocks. Partners who already know ClickHouse can skip Part 2.

### Part 3's README "Decision Alignment" section (new section to add to Part 3 README)
A new section that makes the connection explicit: "This lab implements the following architecture decisions. Compare these to your migration-plan.md to understand why each choice was made:"

| Decision | This Lab Implements | Why |
|----------|---------------------|-----|
| `trips_raw` engine | `MergeTree()` | Insert-only via ClickPipes; no updates needed |
| `fact_trips` engine | `ReplacingMergeTree(updated_at)` | Trips can be updated (fare corrections); version col = updated_at |
| `fact_trips` ORDER BY | `(toStartOfMonth(pickup_at), pickup_at, trip_id)` | Q1-Q7 all filter on pickup_at; trip_id for uniqueness |
| `agg_hourly_zone_trips` engine | `ReplacingMergeTree(updated_at)` | Rolling recalculation = upsert pattern |
| `dim_*` engine | `MergeTree()` | Full reload on each dbt run; no upserts |
| VARIANT → | `String` + `JSONExtract*` | Preserves raw JSON; JSONExtract at query time |
| QUALIFY → | Subquery wrapping ROW_NUMBER() | ClickHouse has no QUALIFY |
| MERGE INTO → | `delete_insert` incremental | dbt-clickhouse's idiomatic upsert strategy |

Partners who answered differently in Part 2 see here why the lab chose these specific options — which reinforces the learning even if they would have chosen differently.

---

## Verification

The module is complete when:
- `scripts/01_profile_snowflake.sh` runs against a live Part 1 environment and produces a `profile_report.md` with all 4 sections
- Each worksheet is self-contained (readable without running any scripts)
- `examples/nyc_taxi_completed_plan.md` has correct, verified answers for all 4 worksheets
- `migration-plan.md` template has exactly the sections that Part 3's README references as prerequisites
- `docs/snowflake_vs_clickhouse.md` covers all 6 SQL dialect gaps with correct ClickHouse equivalents
- Part 1 and Part 3 READMEs correctly reference the new module numbering
