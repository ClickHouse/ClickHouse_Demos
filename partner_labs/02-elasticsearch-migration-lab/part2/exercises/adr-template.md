# Exercise 2C — Architecture Decision Record (ADR)

Write a short ADR covering the six decisions below. For each decision:

1. State your **Choice** (one of the listed options, or your own).
2. Write a **Rationale** of 2–4 sentences — the *why*, not the *what*. Tie it back to concrete constraints you discovered in Exercise 2A (fleet size, enrichment work, ILM phases, alerting rules, etc.).
3. Where the template asks for a concrete artifact (e.g. a `TTL` clause, an `ORDER BY` key), fill it in.

---

# ADR: Observability Migration Strategy (Elasticsearch → ClickHouse)

**Author:** `___`
**Date:** `___`
**Status:** Proposed

## Context

Our current observability stack uses Elasticsearch 8.15 + Kibana + Filebeat, with the following workloads (summarize from Exercise 2A):

- **Workload 1 — Agent-collected logs** (Filebeat): `___` data streams, `___` docs, `___` size on disk
- **Workload 2 — OTel-instrumented microservices**: `___` services, traces to Elastic APM Server

Primary pain points driving the migration: `___`

Target: ClickHouse Cloud + HyperDX as the observability UI.

---

## Decision 1: Migration Approach

**Choice:** `[ Parallel run / Cut-over / Phased by log type ]`

**Rationale:**
> `___`

**Rollback strategy if the target goes bad:** `___`

---

## Decision 2: Agent Strategy

**Choice:** `[ OTel Collector direct / Filebeat → Vector → OTel bridge / Filebeat → Kafka → Vector → OTel ]`

**Rationale (consider: fleet size, ECS vs. OTel semantic differences, in-flight buffering, operational risk):**
> `___`

**What happens to the 16-service OTel Demo (Workload 2)?** (hint: it already emits OTLP)
> `___`

---

## Decision 3: Schema Strategy

For each log type, specify the approach *and* the proposed `ORDER BY` key:

| Log type | Approach | Proposed `ORDER BY` | Justification (1 line) |
|---|---|---|---|
| `logs-web_access-lab` | `[ default OTel / custom with materialized cols / Null table + MV ]` | `___` | `___` |
| `logs-application-lab` | `___` | `___` | `___` |
| `logs-infrastructure-lab` | `___` | `___` | `___` |
| APM traces | `___` | `___` | `___` |

**Overall rationale — why this balance of columns / Map / materialized columns:**
> `___`

**Schema evolution — how do you handle fields that appear AFTER initial schema creation?** (e.g., a new `feature_flag.*` attribute the collector starts emitting next month)

- **Choice:** `[ Everything new goes into LogAttributes Map / Auto-promote via DDL generator + schema registry / JSON column type for flex attrs / Hybrid: Map default, promote hot fields via ALTER ]`
- **Who owns the promotion decision** (platform team vs. service team): `___`
- **Detection mechanism** (e.g. `SELECT mapKeys(LogAttributes) AS k, count() FROM otel_logs WHERE Timestamp > now() - INTERVAL 7 DAY GROUP BY k ORDER BY count() DESC`): `___`
- **Promotion threshold** (e.g. field appears on ≥ 30% of rows AND is referenced in ≥ 2 dashboards/alerts): `___`
- **Rationale:** `___`

---

## Decision 4: Ingest Pipeline Translation

For each ES processor, specify where the work will live in ClickHouse:

| ES processor (pipeline) | Equivalent in target stack | Why this location |
|---|---|---|
| `geoip` on `remote_addr` (web-access-enrichment) | `[ dictionary + dictGet() in materialized col / OTel geoip processor / drop ]` | `___` |
| `user_agent` on `user_agent` (web-access-enrichment) | `[ materialized col with regex / OTel user_agent processor / drop ]` | `___` |
| `grok` syslog parse (infra-log-parsing) | `[ OTel regex operator / materialized col with extractAllGroupsVertical() / MV ]` | `___` |
| `script` severity derivation (all pipelines) | `[ multiIf() in materialized col / OTel processor / MV ]` | `___` |
| `set event.ingested = _ingest.timestamp` (default-enrichment) | `[ DEFAULT now() / MATERIALIZED now() / OTel ]` | `___` |
| `dissect` on app `message` (app-log-enrichment) | `___` | `___` |

**Which of these could you eliminate entirely? Why?**
> `___`

---

## Decision 5: Data Lifecycle

**Current ES ILM policy** (copy from worksheet): `___`

**Phase-by-phase migration verdict:**

| ILM action | Still needed in ClickHouse Cloud? | Replacement (if any) |
|---|---|---|
| `rollover` at 5 GB / 1 d | `[ yes / no ]` — `___` | `___` |
| `shrink` to 1 shard at 2 d | `[ yes / no ]` — `___` | `___` |
| `forcemerge` to 1 segment at 2 d | `[ yes / no ]` — `___` | `___` |
| `set_priority` 100 → 50 | `[ yes / no ]` — `___` | `___` |
| Cold/frozen tier migration | `[ yes / no ]` — `___` | `___` |
| `delete` at 30 d | `[ yes / no ]` — `___` | `___` |

**Retention period:** `___` days

**TTL clause to add to each table:**
```sql
TTL ___
```

**Rationale — how this simplifies operations compared to ILM:**
> `___`

---

## Decision 6: Alerting Migration

The 2 Kibana alerting rules from Part 1:

| Rule | Condition | Schedule |
|---|---|---|
| **High Error Rate** | > 5 % of `logs-web_access-lab` requests have `status >= 500` over a 5-minute window | every 1 min |
| **Service Heartbeat** | zero logs from any known `service` value for > 3 min | every 1 min |

**Tool choice for ClickHouse-side alerting:** `[ Grafana Alerting + CH datasource / HyperDX alerts / Pre-computed MV + cron / Other ]`

**Rationale:**
> `___`

**High Error Rate — concrete implementation:**
```sql
-- ClickHouse query that returns a single row when the alert should fire
___
```
Schedule / evaluation window: `___`

**Service Heartbeat — concrete implementation:**
```sql
___
```
Schedule / evaluation window: `___`

**What new capability (if any) does ClickHouse unlock for alerting that Kibana could not do?**
> `___`

---

## Decision 7: Historical Data Strategy

**Choice:** `[ Start fresh (ClickHouse gets new data only; ES stays read-only for history) / Full backfill (replay all ~62 M ES docs into ClickHouse) / Partial backfill (last N days) / Dual-source dashboards during transition ]`

**Rationale** (consider: trend-history value, migration window, storage/compute cost, tooling risk):
> `___`

**If backfilling, concrete plan:**
- Tool: `[ elasticdump / Logstash (elasticsearch input + clickhouse output) / custom scroll-API + clickhouse-client / OSS elastic-to-clickhouse / other ]`
- Rate (events/s): `___`
- ETA for full replay: `___`
- Dedup strategy for out-of-order or retried rows (e.g. `INSERT INTO live SELECT DISTINCT ON (ServiceName, Timestamp, Body) * FROM staging` for one-shot replays, or `ReplacingMergeTree` with a `MATERIALIZED` version column for incremental backfills): `___`
- How do you know the backfill is complete and correct (doc count parity? sampling?): `___`

**If starting fresh:**
- When does ES become read-only? `___`
- How long do you keep ES alive for historical queries? `___`
- How do dashboards handle the "old data in ES, new data in CH" split? `___`
- Final archive plan (snapshot → S3, tear down, …): `___`

---

## Risks & Open Questions

List any risks or open questions you want to resolve before starting Part 3:

- `___`
- `___`
- `___`
