# Exercise 2C — ADR Solution (Model Answer)

> **Note:** This is one reasonable answer given the lab's specific constraints. Your ADR may legitimately differ — what matters is that your *rationale* is grounded in the ES state you documented in Exercise 2A.

---

# ADR: Observability Migration Strategy (Elasticsearch → ClickHouse)

**Author:** Migration Team
**Date:** 2026-04-21
**Status:** Proposed

## Context

Our observability stack runs Elasticsearch 8.15 + Kibana + Filebeat, with two workloads:

- **Workload 1 — Agent-collected logs** (Filebeat): 3 data streams (`logs-web_access-lab`, `logs-application-lab`, `logs-infrastructure-lab`), ~62 M docs, ~16 GB primary-shard storage over 5 days. Four ingest pipelines perform GeoIP lookup, user-agent parsing, grok syslog parsing, and severity derivation.
- **Workload 2 — OTel-instrumented microservices**: 16 services from the OpenTelemetry Demo, instrumented with OTel SDKs in 10 languages, emitting OTLP traces / metrics / logs through the Elastic APM Server into `traces-apm-*` / `logs-apm.*` / `metrics-apm.*` indices.

**Primary pain points:**
- Storage cost grows linearly — ILM's hot → warm → delete is operationally complex but doesn't actually reduce cost until cold tier (which we don't run here). JVM heap pressure from inverted indices on every field.
- Two collection paths (Filebeat + Elastic APM agents) use two proprietary schemas (ECS vs. OTel) — double the schema maintenance.
- Kibana alerting has limited SQL expressiveness — no joins, no window functions, no deep aggregations.
- No built-in cost control on high-cardinality fields (`remote_addr`, `trace_id`) which are always indexed.

**Target:** ClickHouse Cloud + HyperDX as the observability UI.

---

## Decision 1: Migration Approach

**Choice:** Parallel run for 2 weeks, then phased cut-over per data stream.

**Rationale:**

A hard cut-over is too risky — we have 2 live alerting rules that cannot regress, and our dashboards are used by on-call. A **parallel run** (dual-write to both ES and ClickHouse) lets us compare document counts, run query parity checks daily, and exercise the target with real traffic. Once we have 7 days of clean parity, we cut over one data stream at a time (`logs-infrastructure-lab` first — lowest query volume, easiest to rollback), keeping the old ES dashboards read-only for another 2 weeks as a safety net.

**Rollback strategy:** Target failure during parallel run → redirect collector `otlphttp` exporter back to Elastic APM Server; ES is still being written. Target failure post-cutover within the 2-week read-only window → flip HyperDX/Grafana datasource from ClickHouse back to ES for the affected stream.

---

## Decision 2: Agent Strategy

**Choice:** **OTel Collector direct** for Workload 2 (already native); **Filebeat → Vector → OTel Collector bridge** for Workload 1.

**Rationale:**

- Workload 2 is already emitting OTLP. We simply re-point the Demo's `otelcol-demo` to a new ClickHouse exporter pipeline — zero agent reconfiguration.
- Workload 1's three log generators already use Filebeat. Replacing every Filebeat instance with the OTel Collector *right now* is high-risk change on a running pipeline. **Vector** is the safe intermediate: Filebeat continues shipping to Vector over the Beats input, Vector transforms ECS attribute names to OTel semantic conventions (`host.hostname` → `host.name`, `service` → `service.name`, etc.), and emits OTLP downstream. This also gives us a place to buffer and tee traffic during parallel run.
- Once we're fully cut over we can retire Vector and replace Filebeat with a thin OTel Collector `filelog` receiver per host. That's a low-risk, one-line swap post-cutover.
- **Workload 2 (OTel Demo):** no change — the existing `otelcol-demo` gets a second exporter pointing at ClickHouse (via the `clickhouseexporter` contrib component) alongside the APM Server one.

---

## Decision 3: Schema Strategy

| Log type | Approach | Proposed `ORDER BY` | Justification |
|---|---|---|---|
| `logs-web_access-lab` | **Custom with materialized columns** | `(ServiceName, Status, toUnixTimestamp(Timestamp))` | Hot dashboards filter by service + status. Materialized `Status`, `RequestPath`, `RunTime`, `CountryName` give columnar scan speed. |
| `logs-application-lab` | **Custom with materialized columns + bloom filter on `TraceId`** | `(ServiceName, SeverityText, toUnixTimestamp(Timestamp))` | Trace-correlation lookups (`WHERE TraceId = ?`) use a skip index; keeping `TraceId` out of the primary key preserves time-range scan locality. |
| `logs-infrastructure-lab` | **Custom** (pre-parsed by OTel) | `(Hostname, Process, toUnixTimestamp(Timestamp))` | Both lead columns are low cardinality (~10 hosts, ~10 processes) — excellent compression and per-host query pruning. |
| APM traces (`traces-apm-*`) | **Default OTel schema** + post-creation bloom filter on `TraceId` | `(ServiceName, Timestamp, TraceId)` | Default OTel schema maps 1:1 from the OTel Collector `clickhouseexporter`, but the exporter does NOT add a skip index on `TraceId` — we add it ourselves (see below). |

**Overall rationale:**

We intentionally avoid the Null-table + MV pattern here. It adds a layer of indirection and ingest-time CPU cost that's only justified when you're duplicating a raw stream into multiple aggregated destinations. For this migration we write directly to the target MergeTree; expensive derivations (geo lookup, user-agent regex) live in materialized columns so they compute at insert and reuse on every query.

**Mandatory post-creation step for `otel_traces`:** the default `clickhouseexporter` schema ships without a skip index on `TraceId`. Trace-ID lookups (Query 5 in Exercise 2B) would otherwise full-scan. Run once after the exporter creates the table:

```sql
ALTER TABLE otel_traces ADD INDEX trace_id_bf TraceId TYPE bloom_filter(0.01) GRANULARITY 4;
ALTER TABLE otel_traces MATERIALIZE INDEX trace_id_bf;    -- backfill the index on existing granules
```

We do the same for `SpanId` if cross-span lookups become a hot path.

**Schema evolution — our choice:** **Hybrid — Map default, promote hot fields via periodic review.**

All unknown attributes land in `LogAttributes` / `SpanAttributes` Map by default (exporter behavior). Once per sprint the platform team runs the following query against the last week of data:

```sql
SELECT mapKeys(LogAttributes) AS keys, count() AS n
FROM otel_logs ARRAY JOIN mapKeys(LogAttributes) AS key
WHERE Timestamp > now() - INTERVAL 7 DAY
GROUP BY keys
ORDER BY n DESC
LIMIT 50;
```

Any key that (a) appears on ≥ 30 % of rows AND (b) is referenced in ≥ 2 dashboards or alerts is promoted with:

```sql
ALTER TABLE otel_logs ADD COLUMN <NewCol> String MATERIALIZED LogAttributes['<key>'];
```

Promotion is a platform-team decision (not per-service) to keep the schema small and consistent.

**Why not auto-promote via a DDL generator?** Running ALTERs in response to ingest-time signals is risky — a misclassified noisy attribute (`request_id`, trace-id-like UUIDs) becomes a high-cardinality column and wrecks compression. The human-in-the-loop cadence is cheap insurance and avoids unintended schema bloat.

**Why not the `JSON` column type?** `Map(LowCardinality(String), String)` is the older, more widely-deployed option with predictable behavior for `mapContains` / `LogAttributes['key']` access patterns. The `JSON` type may perform better at scale, but we haven't benchmarked it on this specific workload yet. Revisit if the Map key space grows past ~200 distinct keys, which would start to hurt `LowCardinality` dictionary efficiency.

**What this means for Workload 2 (OTel Demo):** the `clickhouseexporter` handles dynamic attributes natively — new span attributes emitted by a service appear in `SpanAttributes` the first time they're seen, no collector or schema change needed. The promotion workflow is identical to the logs case.

---

## Decision 4: Ingest Pipeline Translation

| ES processor | Target-stack equivalent | Why |
|---|---|---|
| `geoip` on `remote_addr` | **Dictionary (`IP_TRIE` layout over GeoLite2 CSV) + `dictGet()` in materialized column** | Keeps collectors stateless. Dictionary updates at the backend are a single DDL; we don't have to redeploy every Vector/Filebeat node. |
| `user_agent` on `user_agent` | **OTel Collector `user_agent` processor** | The processor maps 1:1 to Elastic's implementation, emits parsed attributes (`user_agent.name`, `user_agent.os.name`, etc.). Backend schema stays minimal. |
| `grok` syslog parse | **OTel Collector `regex_parser` operator in the `filelog` receiver** | Parse once at the edge. ClickHouse sees already-structured rows. CH-side regex would be higher CPU at ingest scale and harder to iterate on. |
| `script` severity derivation | **`MATERIALIZED multiIf(...)` column** | The logic (HTTP status → severity, keyword → severity) is trivial in SQL, runs once at insert, and stays alongside the schema so it can't drift from the table. |
| `set event.ingested = _ingest.timestamp` | **`DEFAULT now()` column** | Same semantics, one keyword. No processor needed. |
| `dissect` on app `message` | **Drop it** — rarely matches in practice and the fields it *would* extract aren't queried. | Dead processor identified during Exercise 2A. |

**Which processors could be eliminated entirely?** The `dissect` processor is dead weight. The `set event.ingested` pipeline is redundant once we use a `DEFAULT now()` column. That removes 2 pipelines from the migration.

---

## Decision 5: Data Lifecycle

**Current ES ILM policy (`lab-observability-policy`):**
- Hot: `rollover` at 5 GB or 1 day, priority 100
- Warm (at 2 days): `shrink` to 1 shard, `forcemerge` to 1 segment, priority 50
- Delete: at 30 days

**Phase-by-phase verdict:**

| ILM action | Still needed? | Replacement |
|---|---|---|
| `rollover` at 5 GB / 1 d | **No** — ClickHouse has one table with date-range partitions; no rolling indices to manage. | Partition by `toYYYYMM(Timestamp)` |
| `shrink` to 1 shard at 2 d | **No** — logical shards auto-scale; no physical shard surgery needed. | N/A |
| `forcemerge` to 1 segment at 2 d | **No** — MergeTree's background merge process handles this automatically and continuously. | N/A |
| `set_priority` 100 → 50 | **No** — ClickHouse Cloud has no node-tier priority concept. | N/A |
| Cold/frozen tier migration | **No** — ClickHouse Cloud stores *all* data on object storage with an automatic local read cache. There is no "cold" tier to migrate to; old data just stays on the same object store and is cached on demand. | N/A |
| `delete` at 30 d | **Yes** — still the only lifecycle action with semantic purpose. | `TTL … DELETE` |

**Retention period:** 30 days (match current ES).

**TTL clause (per table):**
```sql
TTL toDateTime(Timestamp) + INTERVAL 30 DAY DELETE
```

**Rationale:**

Five of the six ILM actions evaporate. This is the single biggest operational simplification from the migration — ILM was 200+ lines of policy + phase-transition monitoring + index-state dashboards, all of which are replaced by one `TTL` clause per table. No more "why is this index stuck in warm?" tickets.

---

## Decision 6: Alerting Migration

**Tool choice:** **Grafana Alerting with the ClickHouse datasource** for both rules.

**Rationale:**

- Grafana has a first-class ClickHouse datasource and a mature alerting engine (scheduling, deduplication, silences, routing to Slack/PagerDuty). No need to invent our own.
- HyperDX has alerts but they're less full-featured than Grafana Alerting for numeric-threshold + time-window rules.
- Pre-computed MVs are a valid alternative for expensive alert queries; for these two rules the query is cheap so Grafana-side SQL is fine.

**High Error Rate — implementation:**
```sql
-- Returns a single row IFF 5xx rate exceeded 5% in the last 5 minutes.
SELECT
    countIf(Status >= 500)                     AS errors,
    count()                                     AS total,
    (errors / total) * 100                      AS error_rate_pct
FROM otel_logs_web_access
WHERE Timestamp >= now() - INTERVAL 5 MINUTE
HAVING total > 100                              -- suppress false alerts on low volume
   AND error_rate_pct > 5.0;
```
**Schedule:** Evaluate every 1 minute, pending for 2 consecutive evaluations before firing (avoids single-blip pages).

**Service Heartbeat — implementation:**
```sql
-- Returns one row per service that has emitted no logs in the last 3 minutes.
WITH known_services AS (
    SELECT DISTINCT ServiceName FROM otel_logs_application
    WHERE Timestamp >= now() - INTERVAL 1 DAY
)
SELECT ks.ServiceName AS service
FROM known_services ks
LEFT JOIN (
    SELECT ServiceName, max(Timestamp) AS last_seen
    FROM otel_logs_application
    WHERE Timestamp >= now() - INTERVAL 10 MINUTE
    GROUP BY ServiceName
) recent ON recent.ServiceName = ks.ServiceName
WHERE recent.last_seen IS NULL
   OR recent.last_seen < now() - INTERVAL 3 MINUTE;
```
**Schedule:** Evaluate every 1 minute. Each emitted row triggers a separate alert instance routed by service name.

**New capability ClickHouse unlocks:**

- **Joins in alert queries** (Kibana can't do this). The heartbeat rule above joins a "known services" set against a "recently seen" set. In ES we would have to maintain the known-services list out of band.
- **Window functions and `sequenceMatch`** for complex anomaly detection — "pages that fire when a service has 3 consecutive 5xx spikes within 10 minutes."
- **CTEs + subqueries** — richer alert conditions without resorting to Watcher scripts.

---

## Decision 7: Historical Data Strategy

**Choice:** **Start fresh.** ClickHouse receives new data only; ES stays read-only for 90 days, then is snapshotted and decommissioned.

**Rationale:**

- **Trend-history value is limited for this workload.** Logs and traces are primarily used for incident response (last 24 h) and weekly trend review. 30 d retention is plenty; 90 d of ES-read-only covers any "look back a quarter" need during the transition.
- **Backfill tooling risk is real.** `elasticdump` or a scroll-API script can move ~62 M docs, but the dedup story is fragile — any retry or partial failure leaks duplicates into `otel_logs`. Running `ReplacingMergeTree` to dedup is possible but defers the problem and complicates query semantics during the replay window.
- **Storage cost of duplicating ~62 M docs** is non-trivial during the cutover window and adds no operational value post-cutover once ES is read-only.
- **The 90-day read-only ES window is cheap insurance.** If we discover a dashboard regression or need longer history for an investigation, ES is still queryable. HyperDX supports multiple datasources, so we can route "last 30 days" to ClickHouse and older queries to ES transparently.

**ES read-only plan:**

| Day | Action |
|---|---|
| **0 (cutover)** | Stop Filebeat writes. Stop APM-server writes. Leave ES cluster running. Flip HyperDX/Grafana default datasource from ES → ClickHouse. |
| **0 – 90** | Dashboards serve "last 30 days" from CH. Long-range historical lookups route to ES as a secondary HyperDX datasource. |
| **90** | `POST _snapshot/backup_repo/final_snapshot` → S3. Tear down the ES cluster. Snapshot can be restored to a temporary ES instance if ever needed. |

**If backfill becomes required later (escape hatch):**

Reuse the Vector bridge from Decision 2:
```bash
elasticdump \
  --input=http://es:9200/logs-web_access-lab \
  --output=http://vector:8686/_bulk \
  --type=data \
  --limit=10000
```
- **Expected rate:** ~50 k docs/s sustained (Vector batches to the ClickHouse exporter).
- **Full-replay ETA:** ~62 M docs ≈ 20 minutes end-to-end.
- **Dedup strategy:** ingest the ES replay into a staging table (same schema as `otel_logs`), then merge into the live table with `SELECT DISTINCT ON` to collapse retried rows. Simple, no engine surgery:

  ```sql
  -- 1. Staging table with the live table's schema
  CREATE TABLE otel_logs_staging AS otel_logs;

  -- 2. Re-point Vector at otel_logs_staging and run the elasticdump replay.

  -- 3. Merge, keeping one row per identifying tuple
  INSERT INTO otel_logs
  SELECT DISTINCT ON (ServiceName, Timestamp, Body) *
  FROM otel_logs_staging;

  DROP TABLE otel_logs_staging;
  ```
  For a 20-minute, ~62 M-row one-shot replay this beats a `ReplacingMergeTree` staging engine: fewer moving parts, no background-merge timing to coordinate, and the dedup key is explicit at the `DISTINCT ON` site.
- **For longer-running or incremental backfills** (e.g. rolling replay of months of data) a `ReplacingMergeTree` staging table with a `MATERIALIZED ContentHash` version column in `ORDER BY` is the right tool — see the ClickHouse docs on `ReplacingMergeTree`; that DDL is out of scope for this lab.
- **Completeness check:** compare doc counts per day between ES (`_count` with `range` query) and CH (`count() WHERE toDate(Timestamp) = ...`). Expect ≤ 0.01 % discrepancy from ingest timing.

---

## Risks & Open Questions

- **Vector as a bridge** adds one more hop and one more failure domain during parallel run. Acceptable for 2 weeks; we want it retired before we declare full cutover.
- **GeoIP dictionary refresh cadence** — MaxMind GeoLite2 is updated weekly; we need a cron/Airflow job to pull the latest CSV and `SYSTEM RELOAD DICTIONARY` nightly.
- **High-cardinality fields** (e.g. `trace.id`, `remote_addr`) — need to confirm bloom filter + skip index performance on a 30-day rolling window. Plan to load-test with `clickhouse-benchmark` against 30× current daily volume before cutover.
- **Kibana dashboards → HyperDX/Grafana migration** — HyperDX ingests OTel-native schemas cleanly but we'll have to rebuild the 6 dashboards by hand. Budget: 1 engineer-day per dashboard.
- **Cost model under ClickHouse Cloud** — we need a sizing estimate based on post-compression volume (expected ~10× reduction), query QPS, and compute scale-out tier.
