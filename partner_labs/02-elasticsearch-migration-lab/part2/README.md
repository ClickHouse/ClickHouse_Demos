# Part 2: Architectural Analysis — Elasticsearch vs. ClickHouse

**Estimated time: 60–90 minutes**

## Overview

Before touching any migration tooling, you will analyze your running Elasticsearch environment, map its concepts to ClickHouse (the target), translate representative queries to ClickHouse SQL, and write an Architecture Decision Record (ADR) documenting your migration strategy. No infrastructure changes happen in this part — this is a thinking exercise that drives the execution plan in Part 3.

## Prerequisites

- Part 1 environment is running (see [Part 1 Checkpoint](../part1/README.md#checkpoint))
- All validation checks passing: `bash ../part1/validation/check.sh`
- `jq` installed (`brew install jq` on macOS, `apt-get install jq` on Debian/Ubuntu)

## What You'll Produce

Three deliverables, all in [exercises/](exercises/):

| # | Deliverable | File |
|---|---|---|
| 2A | Data-model worksheet analyzing your ES environment, proposing a ClickHouse design, and recording a query-latency baseline for Part 3 comparison | [exercises/worksheet.md](exercises/worksheet.md) |
| 2B | ClickHouse SQL for 5 representative Elasticsearch queries | [exercises/query-translation.md](exercises/query-translation.md) |
| 2C | Architecture Decision Record covering 7 strategic decisions | [exercises/adr-template.md](exercises/adr-template.md) |

Model answers live in [solutions/](solutions/). **Complete each exercise before peeking.**

---

## Reference: Core Concept Mapping

Use this table as you work through Exercise 2A and 2C.

| Elasticsearch Concept | ClickHouse Equivalent | Key Difference |
|---|---|---|
| Index | Table | ES creates new indices per time period (ILM rollover); CH uses a single table with partitions |
| **Data Stream** | **Single MergeTree Table** | **ES abstracts rolling indices + ILM + auto-rollover; CH replaces this with a single partitioned table — no rolling, no stream management** |
| Document | Row | ES documents are schema-flexible; CH rows are schema-bound (with JSON/Map types for flexibility) |
| Field | Column | ES fields can be dynamically added; CH columns are explicitly defined (except with `JSON` type) |
| Mapping / Composable Index Template | Table Schema (DDL) | ES composes component templates; CH uses a single `CREATE TABLE` |
| Shard | Shard (logical) | ES shards are physical Lucene structures tied to JVM heap; CH shards are logical, vertically scalable |
| Replica | Replica | ES uses synchronous primary-replica replication; CH uses asynchronous by default |
| Inverted Index | Primary Key + Skip Indices | ES indexes every field by default; CH uses sorted primary keys and optional `bloom_filter` / `text` skip indices (`tokenbf_v1` / `ngrambf_v1` deprecated >= 26.2) |
| **ILM (hot → warm → cold → delete)** | **TTL + Partitions (deletion only)** | **ClickHouse Cloud stores all data on object storage with automatic local caching — hot/warm/cold tiers are irrelevant. Only `TTL … DELETE` is needed for expiration.** |
| **Ingest Pipeline** (`geoip`, `user_agent`, `grok`, `script`) | **Materialized Views + Materialized Columns + Dictionaries** | **ES transforms data pre-index with processors; CH uses materialized views as insert-time triggers, materialized columns for per-row expressions, and dictionaries for enrichment lookups** |
| Elasticsearch Transforms (rollups) | Incremental Materialized Views + AggregatingMergeTree | ES transforms re-aggregate periodically; CH uses incremental partial aggregation states that auto-merge |
| **Kibana Alerting Rules** | **Grafana Alerting / HyperDX Alerts / pre-computed MVs** | **ClickHouse has no built-in alerting — use Grafana with the ClickHouse datasource, HyperDX alerts, or pre-compute alert conditions via materialized views** |
| Kibana | HyperDX | Both are observability UIs; HyperDX is OTel-native |
| Elastic Agent / Filebeat | OpenTelemetry Collector | ES uses proprietary agents; ClickStack uses vendor-neutral OTel Collector |
| ECS (Elastic Common Schema) | OTel Semantic Conventions | Different attribute naming; ECS is merging into OTel spec |

---

## Reference: ECS → OTel Semantic Convention Mapping

In Part 3 you'll configure the OTel Collector to emit OTel-native fields. Use this table to translate each ECS field you documented in Exercise 2A. Fields left in the OTel collector's `LogAttributes` / `SpanAttributes` / `ResourceAttributes` Maps are noted as such — promote them to top-level columns only when query patterns justify it (see ADR Decision 3).

| ECS field (Elasticsearch)                            | OTel equivalent (ClickHouse)                             | Notes |
|---|---|---|
| `@timestamp`                                          | `Timestamp` (top-level `DateTime64(9)`)                  | |
| `message`                                             | `Body` (top-level `String`)                              | |
| `log.level` / `level`                                 | `SeverityText` + `SeverityNumber`                        | OTel adds numeric level 1–24 |
| `event.severity`                                      | `SeverityText`                                            | |
| `service` / `service.name`                            | `ServiceName` (top-level `LowCardinality(String)`)       | |
| `service.version`                                     | `ServiceVersion` / `ResourceAttributes['service.version']` | |
| `host.hostname` / `hostname`                          | `host.name` (`ResourceAttributes['host.name']`)          | ECS aliases `host.name`; OTel uses only `host.name` |
| `host.ip`                                             | `host.ip` (resource attribute)                            | |
| `trace.id`                                            | `TraceId` (top-level `String`)                            | |
| `span.id` / `transaction.id`                          | `SpanId`                                                  | |
| `parent.id`                                           | `ParentSpanId`                                            | |
| `http.request.method`                                 | `http.request.method`                                     | Same in both — OTel adopted ECS naming |
| `http.response.status_code`                           | `http.response.status_code`                               | Same |
| `client.ip` / `source.ip` / `remote_addr`             | `client.address`                                          | Stored as string |
| `user_agent.original` / `user_agent`                  | `user_agent.original`                                     | |
| `user_agent.name` / `user_agent_parsed.name`          | `user_agent.name`                                         | Output of UA parser |
| `error.message`                                       | `exception.message` (span event)                          | |
| `error.stack_trace`                                   | `exception.stacktrace`                                    | |
| `transaction.name`                                    | `span.name` (for transactions)                            | |
| `transaction.duration.us`                             | `Duration` (`Int64` **nanoseconds**)                      | ⚠ unit change: µs → ns |
| `labels.*`                                            | `ResourceAttributes` / `SpanAttributes` / `LogAttributes` | Map type |

---

## Exercise 2A: Data Model Mapping

**Deliverable:** Complete [exercises/worksheet.md](exercises/worksheet.md).

Examine your running Elasticsearch environment and fill in one worksheet section per data stream (`logs-web_access-lab`, `logs-application-lab`, `logs-infrastructure-lab`). Then propose a ClickHouse table design for each.

### Inspection commands

Run these from any host that can reach Elasticsearch at `http://localhost:9200` (the same host you ran Part 1 on).

**List data streams and their backing indices:**
```bash
curl -s "http://localhost:9200/_data_stream/logs-*" | jq '.data_streams[] | {name, indices: [.indices[].index_name], generation}'
```

**Get the mapping for one data stream:**
```bash
curl -s "http://localhost:9200/logs-web_access-lab/_mapping" | jq .
```

**Count the unique leaf fields in the mapping** — excludes ES meta-fields (`_id`, `_index`, `_source`, `_doc_count`, etc.) that would otherwise inflate the count by ~14:
```bash
curl -s "http://localhost:9200/logs-web_access-lab/_field_caps?fields=*" \
  | jq '[.fields | to_entries[] | select(.key | startswith("_") | not)] | length'
```

**Get index stats (doc count, size):**
```bash
curl -s "http://localhost:9200/logs-web_access-lab/_stats" \
  | jq '.indices | to_entries[] | {index: .key, docs: .value.primaries.docs.count, size_bytes: .value.primaries.store.size_in_bytes}'
```

**Get shard allocation:**
```bash
curl -s "http://localhost:9200/_cat/shards/logs-web_access-*?v"
```

**Inspect the ILM policy and current phase:**
```bash
curl -s "http://localhost:9200/_ilm/policy/lab-observability-policy" | jq .
curl -s "http://localhost:9200/logs-web_access-lab/_ilm/explain" \
  | jq '.indices | to_entries[] | {index: .key, phase: .value.phase, age: .value.age}'
```

**Inspect the ingest pipelines:**
```bash
for p in default-enrichment web-access-enrichment app-log-enrichment infra-log-parsing; do
  echo "=== $p ==="
  curl -s "http://localhost:9200/_ingest/pipeline/$p" | jq ".\"$p\".processors"
done
```

**Check pipeline stats (docs processed, failures):**
```bash
curl -s "http://localhost:9200/_nodes/stats/ingest?filter_path=nodes.*.ingest.pipelines" \
  | jq '.nodes | to_entries[0].value.ingest.pipelines'
```

**Verify enrichment is working:**
```bash
curl -s "http://localhost:9200/logs-web_access-lab/_search?size=1" \
  | jq '.hits.hits[0]._source | {remote_addr, geo, user_agent_parsed, "event.severity": ."event".severity}'
```

**Count unique values in high-cardinality fields:**
```bash
curl -s -H 'Content-Type: application/json' "http://localhost:9200/logs-web_access-lab/_search?size=0" -d '{
  "aggs": {
    "services": {"cardinality": {"field": "service"}},
    "paths":    {"cardinality": {"field": "request_path.keyword"}},
    "ips":      {"cardinality": {"field": "remote_addr"}}
  }
}' | jq .aggregations
```

### Inspection: Workload 2 (APM traces and logs)

The OTel Demo writes to `traces-apm-*` and ~18 `logs-apm.app.*` data streams. Worksheet Section 4 covers this workload.

**List APM data streams:**
```bash
curl -s "http://localhost:9200/_data_stream/traces-apm*,logs-apm*" \
  | jq '.data_streams[] | {name, generation}'
```

**Sample trace document top-level keys:**
```bash
curl -s "http://localhost:9200/traces-apm-*/_search?size=1" \
  | jq '.hits.hits[0]._source | keys'
```

**Trace-field cardinalities and event-type split:**
```bash
curl -s -H 'Content-Type: application/json' "http://localhost:9200/traces-apm-*/_search?size=0" -d '{
  "aggs": {
    "services":    {"cardinality": {"field": "service.name"}},
    "transactions":{"cardinality": {"field": "transaction.name"}},
    "languages":   {"terms": {"field": "service.language.name"}},
    "event_types": {"terms": {"field": "processor.event"}}
  }
}' | jq .aggregations
```

**Transaction-duration percentiles (use for Section 4a p50/p95):**
```bash
curl -s -H 'Content-Type: application/json' "http://localhost:9200/traces-apm-*/_search?size=0" -d '{
  "query": {"term": {"processor.event": "transaction"}},
  "aggs": {
    "dur_pct": {"percentiles": {"field": "transaction.duration.us", "percents": [50, 95, 99]}}
  }
}' | jq .aggregations
```

**Log-stream docs per OTel Demo service:**
```bash
curl -s "http://localhost:9200/_cat/indices/logs-apm.app.*?h=index,docs.count&format=json" \
  | jq 'sort_by(-(."docs.count" | tonumber)) | .[] | {index, docs: .["docs.count"]}'
```

### Record a query latency baseline

Before you design the target schema, record how long 3 canonical queries take against the current ES stack. In Part 3 you'll rerun the equivalent ClickHouse queries and compare. Use the `.took` field (milliseconds ES spent serving the query, excluding network/serialization).

```bash
# Q1 — Top 10 request paths (status 200)
for i in 1 2 3; do
  curl -s -H 'Content-Type: application/json' \
    "http://localhost:9200/logs-web_access-lab/_search?size=0" -d '{
      "query":{"bool":{"filter":[{"term":{"status":"200"}}]}},
      "aggs":{"top_paths":{"terms":{"field":"request_path.keyword","size":10}}}
    }' | jq '.took'
done

# Q2 — 5xx count per 1m in last 1h
for i in 1 2 3; do
  curl -s -H 'Content-Type: application/json' \
    "http://localhost:9200/logs-web_access-lab/_search?size=0" -d '{
      "query":{"bool":{"filter":[
        {"range":{"status":{"gte":"500"}}},
        {"range":{"@timestamp":{"gte":"now-1h"}}}]}},
      "aggs":{"errors":{"date_histogram":{"field":"@timestamp","fixed_interval":"1m"}}}
    }' | jq '.took'
done

# Q3 — Trace lookup by any trace.id (grab a real one first)
TID=$(curl -s "http://localhost:9200/traces-apm-*/_search?size=1" | jq -r '.hits.hits[0]._source.trace.id')
for i in 1 2 3; do
  curl -s -H 'Content-Type: application/json' \
    "http://localhost:9200/traces-apm-*/_search?size=10" -d "{\"query\":{\"term\":{\"trace.id\":\"$TID\"}}}" \
    | jq '.took'
done
```

Record the **median** of the 3 runs for each query in [exercises/worksheet.md](exercises/worksheet.md#5-query-latency-baseline) Section 5.

> **Tip:** When proposing the ClickHouse ORDER BY key, think about which field combination best co-locates rows that are queried together. The dashboards you built in Part 1 are your primary access pattern guide.

---

## Exercise 2B: Query Pattern Translation

**Deliverable:** Complete [exercises/query-translation.md](exercises/query-translation.md).

The exercise file contains 5 representative Elasticsearch queries drawn from your Part 1 dashboards and APM UI. For each, write the equivalent ClickHouse SQL against a target schema named `otel_logs` (or `otel_traces` for trace lookups) with the following columns:

| Column | Type | Maps from ES field |
|---|---|---|
| `Timestamp` | `DateTime64(9)` | `@timestamp` |
| `ServiceName` | `LowCardinality(String)` | `service` |
| `Body` | `String` | `message` / raw log line |
| `SeverityText` | `LowCardinality(String)` | `event.severity` |
| `LogAttributes` | `Map(LowCardinality(String), String)` | all other log fields (`request_path`, `status`, `remote_addr`, …) |
| `TraceId` | `String` | `trace.id` (traces table only) |

> **Why Map instead of individual columns?** The OTel schema uses a `Map` for flexible, vendor-neutral attribute storage. In Part 3 you'll extract frequently-queried fields (like `status`) into materialized columns for performance.

---

## Exercise 2C: Architecture Decision Record

**Deliverable:** Complete [exercises/adr-template.md](exercises/adr-template.md).

Write a short ADR covering seven strategic decisions. There is no single "right" answer — the goal is that your rationale is consistent with both your current ES setup (from Exercise 2A) and the constraints of ClickHouse Cloud (columnar storage, object-backed, no built-in alerting).

The seven decisions:

1. **Migration approach** — parallel run, cut-over, or phased by log type?
2. **Agent strategy** — OTel Collector direct, Filebeat → Vector → OTel bridge, or Filebeat → Kafka → Vector → OTel?
3. **Schema strategy** — default OTel schema, custom with materialized columns, or Null source table + MVs? Specify for each of the 3 log types plus APM traces, propose an `ORDER BY` key for each, **and decide how new fields appearing post-launch are handled** (Map-only, auto-promote, JSON column, or hybrid).
4. **Ingest pipeline translation** — where does each ES processor land? GeoIP (dictionary vs. collector processor), user-agent parsing, grok parsing, severity derivation, ingest timestamp.
5. **Data lifecycle** — ILM phases that become unnecessary (explain why), retention period, and TTL clause.
6. **Alerting migration** — tool choice and how to recreate the 2 Kibana alerting rules (`High Error Rate`: >5% 5xx in 5m; `Service Heartbeat`: zero logs from a service for >3m).
7. **Historical data strategy** — backfill all ~62 M ES docs into ClickHouse, start fresh and keep ES read-only, or a hybrid? If backfilling, which tool and how do you dedup?

---

## Checkpoint

Before proceeding to [Part 3](../part3/README.md), verify:

- [ ] `exercises/worksheet.md` has every field filled in for all 3 data streams
- [ ] Each data stream section includes a proposed ClickHouse table design (partition key, ORDER BY, materialized columns, TTL)
- [ ] You have identified which ILM phases become unnecessary in ClickHouse Cloud and why
- [ ] `exercises/query-translation.md` has working ClickHouse SQL for all 5 queries
- [ ] `exercises/adr-template.md` has a rationale for each of the 7 decisions (not just a choice)
- [ ] You compared your worksheet and ADR answers against [solutions/](solutions/) and can explain any deliberate deviations

---
**Next:** [Part 3: Migration Execution →](../part3/README.md)
