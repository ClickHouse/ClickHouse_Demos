# Exercise 2A — Data Model Mapping Worksheet

Fill in every field below using the inspection commands from [../README.md](../README.md#inspection-commands). **Do not copy from the solution.** Each value should come from a command you ran.

If a field does not apply (e.g. no enrichment pipeline), write `N/A` and briefly say why.

---

## 1. Data Stream: `logs-web_access-lab`

### Current state
- Total documents: `___`
- Number of backing indices: `___`
- Total primary-shard size (human-readable): `___`
- Number of shards per backing index: `___`
- Replica count per backing index: `___`
- Number of unique fields in mapping: `___`
- High-cardinality fields (≥ 1 000 unique values, list them): `___`
- Fields used in most queries (from Part 1 dashboards, list them): `___`
- ILM policy name: `___`
- ILM phases configured (list each with its `min_age` and actions): `___`
- Rollover condition (size / age): `___`
- Delete age: `___`
- Ingest pipeline name(s) applied to this stream: `___`
- Pipeline processors (list each processor type and its purpose in one line): `___`
- Enriched fields added by pipeline (list them): `___`

### Proposed ClickHouse table design
- Table name: `___`
- Partition key (and why): `___`
- `ORDER BY` key (and why — think about which columns co-locate rows for the dashboards you built): `___`
- Which fields belong as top-level columns vs. inside the `LogAttributes` Map (and why): `___`
- Which fields need materialized columns for query performance (and why): `___`
- How will you replicate GeoIP enrichment — dictionary with `dictGet()`, OTel `geoip` processor, or drop? `___`
- How will you replicate user-agent parsing — materialized column with regex, OTel `user_agent` processor, or drop? `___`
- TTL policy (map from the ILM delete phase): `___`
- Which ILM phases become unnecessary in ClickHouse Cloud, and why: `___`

---

## 2. Data Stream: `logs-application-lab`

### Current state
- Total documents: `___`
- Number of backing indices: `___`
- Total primary-shard size: `___`
- Shards / replicas per backing index: `___`
- Number of unique fields in mapping: `___`
- High-cardinality fields: `___`
- Fields used in most queries: `___`
- ILM policy name: `___`
- ILM phases configured: `___`
- Rollover condition: `___`
- Delete age: `___`
- Ingest pipeline name(s): `___`
- Pipeline processors and purposes: `___`
- Enriched fields added by pipeline: `___`

### Proposed ClickHouse table design
- Table name: `___`
- Partition key: `___`
- `ORDER BY` key: `___`
- Top-level columns vs. Map attributes: `___`
- Materialized columns: `___`
- How will trace_id / span_id be stored (plain column, Map, or materialized column)? Why: `___`
- TTL policy: `___`
- ILM phases unnecessary in CH Cloud: `___`

---

## 3. Data Stream: `logs-infrastructure-lab`

### Current state
- Total documents: `___`
- Number of backing indices: `___`
- Total primary-shard size: `___`
- Shards / replicas per backing index: `___`
- Number of unique fields in mapping: `___`
- High-cardinality fields: `___`
- Fields used in most queries: `___`
- ILM policy name: `___`
- ILM phases configured: `___`
- Rollover condition: `___`
- Delete age: `___`
- Ingest pipeline name(s): `___`
- Pipeline processors and purposes: `___`
- Enriched fields added by pipeline: `___`

### Proposed ClickHouse table design
- Table name: `___`
- Partition key: `___`
- `ORDER BY` key: `___`
- Top-level columns vs. Map attributes: `___`
- Materialized columns: `___`
- How will you replicate `grok` syslog parsing (OTel `regex` operator, materialized column with `extractAllGroupsVertical()`, or MV)? Why: `___`
- TTL policy: `___`
- ILM phases unnecessary in CH Cloud: `___`

---

## 4. Data Streams: `traces-apm-*` and `logs-apm.*` (Workload 2 — OTel Demo)

The OTel Demo microservices emit traces, metrics and structured logs via OTLP through `otelcol-demo` to the Elastic APM Server, which indexes them into `traces-apm-*` and per-service `logs-apm.app.*` data streams. Unlike Workload 1, this workload is *already* OTel-native — the migration for it is mostly a backend swap (APM Server → ClickHouse exporter).

### 4a. Traces: `traces-apm-*`
- Total documents: `___`
- Number of backing indices: `___`
- Unique leaf fields in mapping: `___`
- Number of distinct `service.name` values: `___`
- `processor.event` distribution (% `transaction` vs. % `span`): `___`
- Instrumentation languages in `service.language.name` (list): `___`
- Transaction `transaction.duration.us` p50 / p95 / p99 (from the percentiles command in the README): `___`
- `event.outcome` distribution (success / failure / unknown %): `___`

### 4b. Logs: `logs-apm.app.*` and `logs-apm.error-default`
- How many `logs-apm.*` data streams exist? `___`
- Total docs across all `logs-apm.*`: `___`
- Top 3 services by log volume: `___`

### 4c. Proposed ClickHouse target: `otel_traces`
- Use the OTel Collector `clickhouseexporter` default schema, or custom? Why: `___`
- `ORDER BY` key (remember Query 5's trace-lookup pattern from Exercise 2B): `___`
- Post-creation ALTERs needed (skip indices, TTL, …): `___`
- TTL: `___`

### 4d. Proposed ClickHouse target: `otel_logs` (APM application logs)
- Single table, or one per service (matching the ~18 `logs-apm.app.*` streams)? Why: `___`
- `ORDER BY`: `___`
- How will `trace.id` → `TraceId` correlation be preserved across the logs and traces tables? `___`
- TTL: `___`

---

## 5. Query Latency Baseline

Use the commands from [../README.md](../README.md#record-a-query-latency-baseline) and record the **median** `.took` from 3 runs per query. You'll replay these in Part 3 against ClickHouse and compare.

| Query | ES `.took` (median, ms) | Notes / cache state |
|---|---|---|
| Q1 — Top 10 request paths (status 200) | `___` | `___` |
| Q2 — 5xx count per 1m in last 1h    | `___` | `___` |
| Q3 — Trace lookup by `trace.id`      | `___` | `___` |

Record the full 3-run sequence if you want to distinguish cold-cache from warm-cache behavior:
- Q1 runs: `___ / ___ / ___`
- Q2 runs: `___ / ___ / ___`
- Q3 runs: `___ / ___ / ___`

---

## 6. Cross-cutting observations

- **Total ES storage across all 3 data streams**: `___`
- **How many of your ingest pipeline processors perform work that could live client-side (in the OTel Collector) instead of server-side?** `___`
- **Which ILM actions (rollover, shrink, forcemerge, set_priority, migrate, delete) still have a purpose in ClickHouse Cloud, and which do not?** List each with a one-line justification: `___`
- **If you had to cap ClickHouse storage at 30% of current ES usage, which fields would you drop or downsample first, and why?** `___`
