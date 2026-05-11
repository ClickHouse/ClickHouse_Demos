# Exercise 2A — Worksheet Solution

> **Note:** Doc counts, sizes, and index names will differ on your environment — the values below are from one reference run after ~5 days of data generation. What matters is the **shape** of the answers: types, cardinalities, enrichment purpose, and table-design rationale.

---

## 1. Data Stream: `logs-web_access-lab`

### Current state
- Total documents: **~31 M**
- Number of backing indices: **3** (generation 3, one rolled per day or 5 GB)
- Total primary-shard size: **~8.5 GB** (across 3 backing indices)
- Shards per backing index: **2 primary** (from the `lab-logs-settings` component template)
- Replicas per backing index: **1 replica** configured — `UNASSIGNED` in single-node lab (expected; no second node to host it)
- Unique fields in mapping: **~69** user fields (excluding ES meta-fields like `_id`, `_index`). Counts dynamically added `geo.*`, `user_agent_parsed.*` subfields and `.keyword` subfields.
- **High-cardinality fields:** `remote_addr` (~65 k), `request_path` (~200 with Zipfian — moderate), `trace.id` (if APM-joined), `user_agent` (several hundred)
- **Fields used in most queries** (from Part 1 dashboards): `@timestamp`, `status`, `request_path`, `request_type`, `service`, `geo.country_name`, `user_agent_parsed.name`, `event.severity`, `run_time`
- ILM policy: **`lab-observability-policy`**
- ILM phases: **hot** (priority 100, rollover at `max_age=1d` or `max_size=5gb`), **warm** (at 2 d — shrink to 1 shard, forcemerge to 1 segment, priority 50), **delete** (at 30 d — delete + snapshot delete)
- Rollover condition: **max_size=5 GB OR max_age=1 d**
- Delete age: **30 days**
- Ingest pipelines: **`default-enrichment`** (default, set on all data streams) → **`web-access-enrichment`** (applied via Filebeat conditional routing)
- **Pipeline processors:**
  - `set event.ingested = _ingest.timestamp` *(default-enrichment)* — stamp ingest time
  - `geoip on remote_addr → geo.*` — add country_name, city_name, location geopoint
  - `user_agent on user_agent → user_agent_parsed.*` — parse name, version, os, device
  - `set event.severity = "info"` — default severity
  - `script (Painless)` — override severity to `warn` on 4xx, `error` on 5xx
- Enriched fields: `geo.country_name`, `geo.city_name`, `geo.location`, `user_agent_parsed.name/version/os.*/device.name`, `event.severity`, `event.ingested`

### Proposed ClickHouse table design

```sql
CREATE TABLE otel_logs_web_access
(
    Timestamp       DateTime64(9) CODEC(Delta, ZSTD),
    ServiceName     LowCardinality(String),
    SeverityText    LowCardinality(String),
    Body            String CODEC(ZSTD(3)),
    LogAttributes   Map(LowCardinality(String), String),
    -- materialized columns for hot-path dashboard fields
    RemoteAddr      IPv4      MATERIALIZED toIPv4OrDefault(LogAttributes['remote_addr']),
    Status          UInt16    MATERIALIZED toUInt16OrZero(LogAttributes['status']),
    RequestPath     String    MATERIALIZED LogAttributes['request_path'],
    RequestMethod   LowCardinality(String) MATERIALIZED LogAttributes['request_type'],
    RunTime         Float32   MATERIALIZED toFloat32OrZero(LogAttributes['run_time']),
    CountryName     LowCardinality(String) MATERIALIZED dictGetOrDefault('geo_ip_dict', 'country_name', RemoteAddr, ''),
    UserAgentName   LowCardinality(String) MATERIALIZED extract(LogAttributes['user_agent'], '^([A-Za-z]+)')
)
ENGINE = MergeTree
PARTITION BY toYYYYMM(Timestamp)        -- 1 month per partition; matches retention granularity
ORDER BY (ServiceName, Status, Timestamp)  -- dashboards filter by service + status
TTL toDateTime(Timestamp) + INTERVAL 30 DAY DELETE;
```

- **Partition key:** `toYYYYMM(Timestamp)` — one partition per month. Fine granularity for efficient drops; too fine (`toDate`) creates thousands of parts. Matches the 30-day retention.
- **ORDER BY:** `(ServiceName, Status, Timestamp)` — dashboard queries (request rate by service, 5xx count) filter by `ServiceName` first, then `Status`. `Timestamp` last gives time-range pruning within each (service, status) sort block.
- **Top-level columns vs. Map:** Put fields in hot dashboard queries as **materialized columns** (`Status`, `RequestPath`, `RunTime`); keep cold / diagnostic fields (`referer`, `size`, `user_agent_parsed.device.name`) in `LogAttributes`. Materialized columns are computed at insert time and stored as real columns, giving full columnar-scan performance.
- **GeoIP enrichment:** Dictionary (`IP_TRIE` layout over MaxMind GeoLite2 CSV) + `dictGet()` in a materialized column. Push enrichment to the storage engine so ingest agents stay stateless and schema changes don't require redeploying collectors.
- **User-agent parsing:** Either (a) the OTel `user_agent` processor (simple: emits parsed name/os/device as attributes) or (b) a materialized column with `extract()` / regex. The OTel processor is cleaner for the migration because Elastic's `user_agent` processor is pre-existing and maps 1:1.
- **TTL:** `Timestamp + INTERVAL 30 DAY DELETE`. Matches ILM delete age.
- **ILM phases unnecessary in CH Cloud:** rollover (no indices to roll), shrink (logical shards auto-scale), forcemerge (background merges handle this automatically), set_priority (no tiered caches exposed), tier migration (hot/warm/cold is irrelevant — all data is on object storage with auto-caching).

---

## 2. Data Stream: `logs-application-lab`

### Current state
- Total documents: **~12 M**
- Backing indices: **2**
- Primary-shard size: **~3.9 GB**
- Shards/replicas: **2 primary / 1 replica** (replica unassigned — single-node)
- Mapping fields: **~42** user fields
- **High-cardinality fields:** `trace_id`, `span_id`, `message`, `error.stack` (where present)
- **Common-query fields:** `@timestamp`, `level`, `service`, `event.severity`, `trace_id`, `message`
- ILM policy: **`lab-observability-policy`** (same as web)
- ILM phases / rollover / delete age: **same** (hot at 5 GB/1 d, warm at 2 d, delete at 30 d)
- Ingest pipelines: **`default-enrichment`** → **`app-log-enrichment`**
- **Pipeline processors:**
  - `set event.ingested`
  - `set event.severity = {{level}}` — copy level
  - `lowercase event.severity`
  - `dissect on message` (rarely matches; leaves `_tmp.*`)
  - `remove _tmp*` — cleanup
- Enriched fields: `event.severity`, `event.ingested`

### Proposed ClickHouse table design

```sql
CREATE TABLE otel_logs_application
(
    Timestamp      DateTime64(9) CODEC(Delta, ZSTD),
    ServiceName    LowCardinality(String),
    SeverityText   LowCardinality(String),
    Body           String CODEC(ZSTD(3)),
    TraceId        String,
    SpanId         String,
    LogAttributes  Map(LowCardinality(String), String),
    -- skip index for trace-ID needle-in-haystack lookup
    INDEX trace_id_bf TraceId TYPE bloom_filter(0.01) GRANULARITY 4,
    -- full-text skip index for `Body` (text index preferred >= 26.2; tokenbf_v1 deprecated)
    INDEX body_tokens Body TYPE text GRANULARITY 4
)
ENGINE = MergeTree
PARTITION BY toYYYYMM(Timestamp)
ORDER BY (ServiceName, SeverityText, Timestamp)
TTL toDateTime(Timestamp) + INTERVAL 30 DAY DELETE;
```

- **Partition key:** `toYYYYMM(Timestamp)` — same reasoning.
- **ORDER BY:** `(ServiceName, SeverityText, Timestamp)` — "show me errors for service X in the last 10 minutes" is the dominant query.
- **`TraceId` / `SpanId`:** Top-level columns with a `bloom_filter` skip index on `TraceId`. NOT in the primary key — that would destroy data locality for time-range scans. The bloom filter accelerates point lookups by trace ID without affecting sort order.
- **Severity derivation:** A `MATERIALIZED lowerUTF8(LogAttributes['level'])` column would replicate the ES `lowercase` processor. For this lab we store it directly as `SeverityText` at collector level (OTel `severityparser`).
- **`dissect`:** Skip it — it rarely matches. If structured parsing is needed, use OTel `regex_parser` operator, not a CH MV (keeps the schema clean).
- **TTL:** 30 days, same as ES.
- **ILM phases unnecessary:** same as web_access — all except `delete`.

---

## 3. Data Stream: `logs-infrastructure-lab`

### Current state
- Total documents: **~18.6 M**
- Backing indices: **2**
- Primary-shard size: **~3.7 GB**
- Shards/replicas: **2 primary / 1 replica**
- Mapping fields: **~35** user fields
- **High-cardinality fields:** `message` (unstructured), `log_message` (parsed), `pid`
- **Low-cardinality fields:** `hostname` (~10 values — k8s-node-01..10), `process` (~10 values)
- **Common-query fields:** `@timestamp`, `hostname`, `process`, `event.severity`, `log_message`
- ILM policy / phases / delete age: **same** (`lab-observability-policy`)
- Ingest pipelines: **`default-enrichment`** → **`infra-log-parsing`**
- **Pipeline processors:**
  - `set event.ingested`
  - `grok %{SYSLOGTIMESTAMP}%{HOSTNAME}%{WORD:process}[...]` — extract 4 fields
  - `set event.severity = "info"`
  - `script (Painless)` — upgrade severity to warn/error based on keywords in `log_message`
- Enriched fields: `syslog_timestamp`, `hostname`, `process`, `pid`, `log_message`, `event.severity`, `event.ingested`

### Proposed ClickHouse table design

```sql
CREATE TABLE otel_logs_infrastructure
(
    Timestamp      DateTime64(9) CODEC(Delta, ZSTD),
    Hostname       LowCardinality(String),
    Process        LowCardinality(String),
    Pid            UInt32,
    SeverityText   LowCardinality(String),
    Body           String CODEC(ZSTD(3)),                  -- raw syslog line
    LogMessage     String CODEC(ZSTD(3)),                   -- extracted message body
    LogAttributes  Map(LowCardinality(String), String),
    INDEX body_tokens LogMessage TYPE text GRANULARITY 4  -- text index preferred >= 26.2; tokenbf_v1 deprecated
)
ENGINE = MergeTree
PARTITION BY toYYYYMM(Timestamp)
ORDER BY (Hostname, Process, Timestamp)
TTL toDateTime(Timestamp) + INTERVAL 30 DAY DELETE;
```

- **Partition/ORDER BY:** Dashboards filter by `Hostname` → `Process` → time. Both are low cardinality, so they compress extremely well and the sort key offers excellent time-range pruning.
- **`grok` replacement:** Push the regex to the OTel Collector (`regex_parser` operator) at ingestion. This keeps the parsing close to the source and lets ClickHouse store already-structured fields. Alternative: a MATERIALIZED column with `extractAllGroupsVertical()`, but that pays the CPU cost on every insert in ClickHouse — better to spend it once in the collector.
- **Severity:** A `MATERIALIZED multiIf(positionCaseInsensitive(LogMessage, 'error') > 0, 'error', positionCaseInsensitive(LogMessage, 'warn') > 0, 'warn', 'info')` column replicates the Painless script exactly.
- **TTL:** 30 days.
- **ILM phases unnecessary:** same story — only `delete` is needed.

---

## 4. Data Streams: `traces-apm-*` and `logs-apm.*` (Workload 2 — OTel Demo)

### 4a. Traces: `traces-apm-*`
- Total documents: **~13.8 M on day 1, growing to ~14 M/day** as the load generator runs (1 backing index per day)
- Backing indices: **2–3** depending on run time
- Unique leaf fields in mapping: **~200** (OTel resource + span attributes + Elastic APM bookkeeping fields)
- Distinct `service.name`: **19** values (16 OTel Demo microservices + `frontend-proxy`, `frontend-web`, `sample-order-app`)
- `processor.event` distribution: **~50 %** `transaction`, **~50 %** `span`
- Instrumentation languages (`service.language.name`, 10 values including `unknown` for Envoy-emitted spans): **nodejs, cpp, python, dotnet, rust, java, php, ruby, go, unknown**
- `transaction.name` cardinality: **~7 000** distinct names (mostly HTTP-verb + route combinations)
- `transaction.duration.us` percentiles: **p50 ≈ 2.5 ms · p95 ≈ 75 ms · p99 ≈ 1.4 s** (long tail from cross-service calls and load-generator-induced hotspots)
- `event.outcome`: **~99.5 % success · ~0.5 % failure** (flagd feature flags default to `off`, so no fault injection)

### 4b. Logs: `logs-apm.app.*` and `logs-apm.error-default`
- `logs-apm.*` data streams: **19** total (18 per-service `logs-apm.app.<service>-default` + 1 `logs-apm.error-default`)
- Total docs across all `logs-apm.*`: **tens of millions** — grows fast (`frontend_web` alone is ~28 M after a day of load)
- Top 3 services by log volume: **`frontend_web` (~27.8 M) · `frontend_proxy` (~2.0 M) · `product_catalog` (~1.1 M)**. Next tier: `cart` (~1 M), `currency` (~400 k), `recommendation` (~300 k). Most per-service streams stay under 100 k docs.
- **Observation:** `frontend_web` dominates by 10× because Next.js emits a log per page render. Keep this in mind when sizing the target `otel_logs` table — service volume is extremely skewed.

### 4c. Proposed ClickHouse target: `otel_traces`

Use the **OTel Collector `clickhouseexporter` default schema**. It maps 1:1 from OTLP, handles batching and schema migration, and there's no reason to hand-write the DDL for the standard case.

```sql
-- What the exporter creates for you (abridged):
CREATE TABLE otel_traces
(
    Timestamp             DateTime64(9) CODEC(Delta, ZSTD(1)),
    TraceId               String CODEC(ZSTD(1)),
    SpanId                String CODEC(ZSTD(1)),
    ParentSpanId          String CODEC(ZSTD(1)),
    TraceState            String CODEC(ZSTD(1)),
    SpanName              LowCardinality(String) CODEC(ZSTD(1)),
    SpanKind              LowCardinality(String) CODEC(ZSTD(1)),
    ServiceName           LowCardinality(String) CODEC(ZSTD(1)),
    ResourceAttributes    Map(LowCardinality(String), String) CODEC(ZSTD(1)),
    SpanAttributes        Map(LowCardinality(String), String) CODEC(ZSTD(1)),
    Duration              Int64 CODEC(ZSTD(1)),
    StatusCode            LowCardinality(String) CODEC(ZSTD(1)),
    StatusMessage         String CODEC(ZSTD(1)),
    Events.Name           Array(String) CODEC(ZSTD(1)),
    Events.Timestamp      Array(DateTime64(9)) CODEC(Delta, ZSTD(1)),
    Events.Attributes     Array(Map(LowCardinality(String), String)) CODEC(ZSTD(1)),
    Links.TraceId         Array(String) CODEC(ZSTD(1)),
    Links.SpanId          Array(String) CODEC(ZSTD(1)),
    Links.TraceState      Array(String) CODEC(ZSTD(1)),
    Links.Attributes      Array(Map(LowCardinality(String), String)) CODEC(ZSTD(1)),
    INDEX idx_trace_id TraceId TYPE bloom_filter(0.001) GRANULARITY 1  -- ← we add this
)
ENGINE = MergeTree
PARTITION BY toDate(Timestamp)
ORDER BY (ServiceName, SpanName, Timestamp)
TTL toDateTime(Timestamp) + INTERVAL 30 DAY DELETE;
```

- **ORDER BY:** `(ServiceName, SpanName, Timestamp)` — matches typical "show me latency for `frontend.checkout` over the last 15 minutes" queries. Default from the exporter.
- **Mandatory post-creation step:** the default exporter schema ships **without** a skip index on `TraceId`, so trace-ID point lookups (Query 5 from Exercise 2B) would full-scan. Run once:
  ```sql
  ALTER TABLE otel_traces ADD INDEX trace_id_bf TraceId TYPE bloom_filter(0.01) GRANULARITY 4;
  ALTER TABLE otel_traces MATERIALIZE INDEX trace_id_bf;
  ```
- **TTL:** 30 days, matching the source ILM.

### 4d. Proposed ClickHouse target: `otel_logs` (APM application logs)

**One `otel_logs` table, not 19.**

Elasticsearch splits by service because each backing index adds per-index mapping overhead but gives tight per-index storage and fine-grained ILM. ClickHouse inverts that trade-off — a single table:

- Compresses `ServiceName` as `LowCardinality(String)` at ~1 byte/row regardless of how many distinct services.
- Lets one `ORDER BY (ServiceName, ...)` prune by service for free on every query.
- Avoids 19 separate tables' worth of part/merge/background overhead.
- Enables trivial cross-service `JOIN otel_logs USING (TraceId) otel_traces` for log↔trace correlation.

```sql
CREATE TABLE otel_logs
(
    Timestamp       DateTime64(9) CODEC(Delta, ZSTD),
    TraceId         String,
    SpanId          String,
    SeverityText    LowCardinality(String),
    SeverityNumber  Int32,
    ServiceName     LowCardinality(String),
    Body            String CODEC(ZSTD(3)),
    ResourceAttributes Map(LowCardinality(String), String),
    LogAttributes      Map(LowCardinality(String), String),
    INDEX idx_trace_id TraceId TYPE bloom_filter(0.01) GRANULARITY 4
)
ENGINE = MergeTree
PARTITION BY toYYYYMM(Timestamp)
ORDER BY (ServiceName, SeverityText, Timestamp)
TTL toDateTime(Timestamp) + INTERVAL 30 DAY DELETE;
```

- **`ORDER BY (ServiceName, SeverityText, Timestamp)`** — covers "errors for service X in the last N minutes," which is the dominant query.
- **`TraceId` correlation preserved** via top-level column + bloom filter. Logs and traces share both the `TraceId` value and the column name, so the join is a one-liner.
- **TTL:** 30 days.

---

## 5. Query Latency Baseline

Measured median of 3 runs each against the running ES cluster at `localhost:9200`.

| Query | ES `.took` (median, ms) | Notes |
|---|---|---|
| Q1 — Top 10 request paths (status 200)     | **1**   | Cold-cache first run 200–1 500 ms; warm runs drop to ~1 ms. Terms agg over `request_path.keyword`. |
| Q2 — 5xx count per 1m in last 1h          | **4**   | Smaller time window + lower cardinality makes this cheap even cold. |
| Q3 — Trace lookup by `trace.id`            | **1**   | Cold 100–200 ms, warm ~1 ms. Inverted index point lookup — essentially free once the index segment is in memory. |

(First-run cold-cache values typically look like **Q1=254 ms · Q2=6 ms · Q3=158 ms**. Run each query twice before recording to measure steady-state warm performance.)

> **Teaching note:** these numbers vary wildly by cache state. What matters is the **shape**: Q3 is basically free in ES because every `trace.id` value is in an always-on inverted index. In ClickHouse we have to *earn* that performance with a `bloom_filter` skip index (see ADR solution Decision 3). Without it, a `WHERE TraceId = ?` would full-scan the `otel_traces` table.

Part 3 will replay these three queries against ClickHouse. Expect Q1 and Q2 to be faster in ClickHouse (columnar scan of a tiny `Status` materialized column beats postings intersection), and Q3 to be *close* to ES once the bloom-filter index is in place. If Q3 is > 10× ES, the bloom filter probably wasn't materialized over historical granules — re-check Section 4c.

---

## 6. Cross-cutting observations

- **Total ES storage across all 3 streams:** ~16 GB primaries (web 8.5 + app 3.9 + infra 3.7). ClickHouse typically compresses logs at ~10–15× better ratio; expect 1–2 GB post-migration for the same dataset.
- **Processors that could live client-side:** `geoip`, `user_agent`, `grok`, `set event.ingested`, severity derivation — **all of them** can live in the OTel Collector (processors: `transform`, `geoip`, `user_agent`, `regex_parser`, `attributes`). Doing enrichment at the edge keeps the backend schema minimal. The trade-off: collector CPU cost scales with fleet size, whereas a CH dictionary-based `dictGet()` runs centrally.
- **ILM action verdict:**
  - `rollover` (size/age) — **Not needed.** ClickHouse has one table with partitions; no rolling indices to manage.
  - `shrink` (reduce shards) — **Not needed.** Logical shards auto-scale.
  - `forcemerge` — **Not needed.** Background merges are automatic.
  - `set_priority` — **Not needed.** No node-tier priority concept.
  - `migrate` (data-tier routing hot→warm→cold) — **Not needed.** ClickHouse Cloud stores everything on object storage with an automatic local read cache; there are no node tiers to migrate between.
  - `delete` at 30 d — **Still needed.** Replicated by `TTL`.
- **Capping storage at 30 % of ES usage:** drop `size` (fixed-width numeric, easy to compute if needed), drop `referer` (low query value, high cardinality), drop stack-trace `Body` after 7 days using a `TTL … RECOMPRESS` or a per-column TTL (`TTL Timestamp + INTERVAL 7 DAY DELETE WHERE SeverityText = 'info'`), and downsample `@timestamp` to `DateTime` (4 bytes) instead of `DateTime64(9)` (8 bytes) for streams that don't need sub-second precision.
