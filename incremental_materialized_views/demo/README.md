# ClickHouse Demo

Single-file demo covering key ClickHouse features:
- Incremental Materialized Views
- AggregatingMergeTree (the AVG problem)
- Dictionaries
- Medallion Architecture

## Prerequisites

- ClickHouse Cloud connection (or local ClickHouse)
- ClickHouse client (`clickhouse-client` or cloud console)

## Quick Start

### 1. Run Setup (SECTION 0)

Run SECTION 0 to create database, tables, dictionaries, and seed data:

```bash
# Option A: Run entire setup section
clickhouse-client --queries-file webinar_demo.sql --multiquery

# Option B: Copy/paste SECTION 0 into ClickHouse console
```

Verify setup completed:
```sql
USE webinar_demo;
SELECT count() FROM bronze_events;  -- Should show ~50,000
```

### 2. Work Through Sections 1-4

Execute queries section-by-section. Each section is clearly marked:

| Section | Topic | Key Message |
|---------|-------|-------------|
| 0. Setup | Pre-run | Creates tables and sample data |
| 1. Incremental MVs | MV = INSERT trigger | Zero orchestration |
| 2. AVG Problem | avg(avg) is WRONG | State/Merge functions |
| 3. Dictionaries | O(1) lookups | dictGet vs JOIN |
| 4. Medallion Finale | All combined | 50K rows -> 3 rows |

### 3. Cleanup (Optional)

```sql
DROP DATABASE IF EXISTS webinar_demo;
```

## File Structure

```
demo/
|-- README.md           # This file
|-- webinar_demo.sql    # Single file with all demo sections
```

## Key Concepts by Section

### Section 1: Incremental MVs
> "MVs are INSERT triggers, not cached queries. Zero orchestration - data flows in real-time!"

Tables: `raw_logs` -> `log_summary`

### Section 2: The AVG Problem - KEY TEACHING MOMENT
> "avg(avg) is WRONG! State functions preserve the math needed to combine correctly."

This is the most important demo:
- Batch 1: 3 requests [10,10,10], avg = 10ms
- Batch 2: 1 request [50], avg = 50ms
- Wrong: avg(10, 50) = 30ms
- Right: (30+50)/(3+1) = 20ms (50% error!)

Tables: `tt_avg_latency_wrong` vs `tt_avg_latency`

### Section 3: Dictionaries
> "O(1) hash table lookups from memory. 10-100x faster than JOINs at scale!"

Compare JOIN vs dictGet - same result, different performance.

### Section 4: Medallion Architecture
> "All features combined: Incremental MVs + dictGet + AggregatingMergeTree. No Spark, no Airflow!"

Key insight: 50,000+ events reduced to 3 daily aggregate rows.

## Troubleshooting

### Setup takes too long
The seed data generation (50K events) should take < 30 seconds. If slow, reduce to 10K:
```sql
-- Change this line in SECTION 0:
FROM numbers(50000)
-- To:
FROM numbers(10000)
```

### Dictionary not loading
Force reload:
```sql
SYSTEM RELOAD DICTIONARY webinar_demo.products_dict;
SYSTEM RELOAD DICTIONARY webinar_demo.customers_dict;
```

### Need to reset and re-run
```sql
DROP DATABASE IF EXISTS webinar_demo;
-- Then run SECTION 0 again
```

## Connecting to ClickHouse Cloud

```bash
# Using clickhouse-client
clickhouse-client --host YOUR_HOST --port 8443 --secure \
    --user default --password YOUR_PASSWORD

# Then run queries interactively
```

Or use the ClickHouse Cloud SQL console directly.
