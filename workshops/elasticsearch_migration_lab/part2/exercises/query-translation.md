# Exercise 2B — Query Pattern Translation

Translate each of the 5 Elasticsearch queries below to ClickHouse SQL. Assume the target schema is:

```sql
-- otel_logs
CREATE TABLE otel_logs (
    Timestamp       DateTime64(9),
    ServiceName     LowCardinality(String),
    Body            String,
    SeverityText    LowCardinality(String),
    LogAttributes   Map(LowCardinality(String), String)
    -- ... additional OTel-standard columns
) ENGINE = MergeTree
ORDER BY (ServiceName, toUnixTimestamp(Timestamp));

-- otel_traces
CREATE TABLE otel_traces (
    Timestamp       DateTime64(9),
    TraceId         String,
    SpanId          String,
    ServiceName     LowCardinality(String),
    SpanName        LowCardinality(String),
    Duration        Int64,
    StatusCode      LowCardinality(String),
    SpanAttributes  Map(LowCardinality(String), String)
) ENGINE = MergeTree
ORDER BY (ServiceName, toUnixTimestamp(Timestamp), TraceId);
```

For each query, write (a) the ClickHouse SQL and (b) one or two sentences comparing how ES and ClickHouse answer the query internally (what index / scan pattern each uses).

> **Note:** ES fields like `request_path`, `status`, `request_type`, etc. land in `LogAttributes` under the same key name. Use `LogAttributes['request_path']` to access them. Cast to a numeric type (e.g. `toUInt16OrZero(LogAttributes['status'])`) when comparing numerically.

---

## Query 1: Top 10 Request Paths (status 200 only)

**Kibana / KQL:**
```
log_type: "web_access" and status: "200"
```
with a Terms aggregation on `request_path.keyword`, size 10.

**Elasticsearch DSL:**
```json
{
  "size": 0,
  "query": {
    "bool": {
      "filter": [
        { "term": { "log_type": "web_access" } },
        { "term": { "status": "200" } }
      ]
    }
  },
  "aggs": {
    "top_paths": {
      "terms": { "field": "request_path.keyword", "size": 10 }
    }
  }
}
```

**Your ClickHouse SQL:**
```sql
-- TODO
```

**How does each engine answer this internally?** (1–2 sentences)
> `___`

---

## Query 2: 5xx Error Count per Minute (last hour)

**Elasticsearch DSL:**
```json
{
  "size": 0,
  "query": {
    "bool": {
      "filter": [
        { "range": { "status": { "gte": 500 } } },
        { "range": { "@timestamp": { "gte": "now-1h" } } }
      ]
    }
  },
  "aggs": {
    "errors_over_time": {
      "date_histogram": { "field": "@timestamp", "fixed_interval": "1m" }
    }
  }
}
```

**Your ClickHouse SQL:**
```sql
-- TODO
```

**How does each engine answer this internally?**
> `___`

---

## Query 3: Full-Text Search — "connection timeout" in ERROR logs

**Kibana / KQL:**
```
message: "connection timeout" and level: "ERROR"
```

**Elasticsearch DSL:**
```json
{
  "size": 50,
  "query": {
    "bool": {
      "must": [
        { "match_phrase": { "message": "connection timeout" } },
        { "term": { "level": "ERROR" } }
      ]
    }
  },
  "sort": [{ "@timestamp": "desc" }]
}
```

**Your ClickHouse SQL:**
```sql
-- TODO
```

**How does each engine answer this internally?**
> `___`

---

## Query 4: Unique Services per Day (last 7 days)

**Elasticsearch DSL:**
```json
{
  "size": 0,
  "query": { "range": { "@timestamp": { "gte": "now-7d" } } },
  "aggs": {
    "daily": {
      "date_histogram": { "field": "@timestamp", "calendar_interval": "day" },
      "aggs": {
        "unique_services": { "cardinality": { "field": "service" } }
      }
    }
  }
}
```

**Your ClickHouse SQL (give two variants — one exact, one approximate — and note which you would put in a production dashboard and why):**
```sql
-- Variant A (exact):

-- Variant B (approximate):
```

**How does each engine answer this internally?**
> `___`

---

## Query 5: Trace Lookup by `trace.id`

**Elasticsearch DSL:**
```json
GET apm-*,traces-apm-*/_search
{
  "size": 1000,
  "query": { "term": { "trace.id": "<TRACE_ID>" } },
  "sort": [{ "@timestamp": "asc" }]
}
```

**Your ClickHouse SQL:**
```sql
-- TODO
```

**How does each engine answer this internally?** (specifically: why `TraceId` should NOT be the leading column in `ORDER BY`)
> `___`
