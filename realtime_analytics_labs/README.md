# Real-time Analytics with ClickHouse — Hands-On Labs

> **Course**: Real-time Analytics with ClickHouse (ClickHouse Academy)
> **Sections covered**: 05 Analyzing Data · 08 Query Acceleration Techniques
> **Dataset**: NYC Taxi Rides (~8 million rows, public S3)

---

## Overview

This repository contains hands-on demo scripts and lab exercises for two core sections of the *Real-time Analytics with ClickHouse* training course. Each section follows a progressive structure: you create a database, load real data, run guided demos, and then complete independent exercises.

| Section | Topic | Estimated Time |
|---------|-------|---------------|
| 05 | Analyzing Data | 60–75 min |
| 08 | Query Acceleration Techniques | 60–75 min |

---

## Prerequisites

You need a running ClickHouse instance (local or Cloud) with network access to AWS S3.

### 1. Configure Connection

```bash
cp .env.example .env
# Edit .env with your ClickHouse host, user, and password
```

### 2. Source Environment and Create Alias

```bash
source .env
```

Choose **one** alias based on your setup:

```bash
# Local ClickHouse (no password)
alias ch="clickhouse-client --host=$CLICKHOUSE_HOST --user=$CLICKHOUSE_USER"

# Local ClickHouse (with password)
alias ch="clickhouse-client --host=$CLICKHOUSE_HOST --user=$CLICKHOUSE_USER --password=$CLICKHOUSE_PASSWORD"

# ClickHouse Cloud
alias ch="clickhouse-client --host=$CLICKHOUSE_HOST --port=9440 --user=$CLICKHOUSE_USER --password=$CLICKHOUSE_PASSWORD --secure"
```

### 3. Verify Connection

```bash
ch --query "SELECT version()"
```

---

## Quick Start

Run Section 05 demos:

```bash
cd 05_analyzing_data
ch --queries-file 00_setup.sql
ch --queries-file 01_basic_aggregations.sql
ch --queries-file 02_time_series.sql
ch --queries-file 03_window_functions.sql
ch --queries-file 04_advanced_queries.sql
```

Run Section 08 demos:

```bash
cd 08_query_acceleration
ch --queries-file 00_setup.sql
ch --queries-file 01_primary_key.sql
ch --queries-file 02_skip_indexes.sql
ch --queries-file 03_projections.sql
ch --queries-file 04_materialized_views.sql
ch --queries-file 05_query_profiling.sql
```

Clean up all databases when done:

```bash
ch --queries-file 05_analyzing_data/99_cleanup.sql
ch --queries-file 08_query_acceleration/99_cleanup.sql
```

---

## Dataset: NYC Taxi Rides

All labs use the **NYC Taxi** public dataset — one of the most commonly used datasets for ClickHouse demonstrations. It contains real taxi trip records from New York City.

| Attribute | Value |
|-----------|-------|
| Rows loaded (labs) | ~3 million (files 0–2) |
| Full dataset | ~3 billion rows |
| Source | AWS S3 (ClickHouse public bucket) |
| Key columns | `pickup_datetime`, `fare_amount`, `trip_distance`, `passenger_count`, `payment_type`, `pickup_ntaname`, `dropoff_ntaname` |

The full 3-billion-row dataset is available at `trips_{0..199}.gz` — a great talking point about ClickHouse scale.

---

## Lab Structure

Each section folder contains:

```
NN_section_name/
├── README.md              # Section overview and What You Learn
├── 00_setup.sql           # Create database, table, load data
├── 01_topic.sql           # Demo script (run by instructor)
├── 02_topic.sql           # Demo script (run by instructor)
├── ...
├── NN_lab_exercises.sql   # Student exercises with answer key
└── 99_cleanup.sql         # Drop all objects created
```

SQL files include `-- TALKING POINT` comments to guide instructor narration and `SELECT '...' AS step;` markers so students can follow progress in their terminal.

---

## Sections

- **[05 Analyzing Data](./05_analyzing_data/README.md)** — Aggregations, time-series, window functions, CTEs, array/map functions, and EXPLAIN
- **[08 Query Acceleration Techniques](./08_query_acceleration/README.md)** — Primary key optimization, skip indexes, projections, materialized views, and query profiling
