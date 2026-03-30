# AI-Powered Observability Workshop — Participant Guide

**Date:** March 2026 &nbsp;|&nbsp; **Duration:** 75 minutes &nbsp;|&nbsp; **Level:** Intermediate

---

## Your Login Credentials

| Your Seat | LibreChat Email | Password |
|-----------|----------------|----------|
| User 1 | `user1@clickhouse.com` | `Workshop2024!` |
| User 2 | `user2@clickhouse.com` | `Workshop2024!` |
| User 3 | `user3@clickhouse.com` | `Workshop2024!` |
| User 4 | `user4@clickhouse.com` | `Workshop2024!` |
| User 5 | `user5@clickhouse.com` | `Workshop2024!` |
| User 6 | `user6@clickhouse.com` | `Workshop2024!` |
| User 7 | `user7@clickhouse.com` | `Workshop2024!` |
| User 8 | `user8@clickhouse.com` | `Workshop2024!` |
| User 9 | `user9@clickhouse.com` | `Workshop2024!` |
| User 10 | `user10@clickhouse.com` | `Workshop2024!` |
| User 11 | `user11@clickhouse.com` | `Workshop2024!` |
| User 12 | `user12@clickhouse.com` | `Workshop2024!` |

---

## PART 1 — LibreChat AI Agent (35 min)

### Step 1: Open LibreChat

```
https://ec2-3-144-204-30.us-east-2.compute.amazonaws.com
```

> Accept the browser security warning (self-signed certificate → click **Advanced → Proceed**)

Log in with your credentials from the table above.

---

### Step 2: Select Your Model

In the model picker at the top, choose:
- **Gemini 2.5 Pro** — best reasoning, recommended for complex queries
- **Claude Sonnet 4.6** — great for structured SQL and explanations
- **Gemini 2.5 Flash** — fastest, good for simple lookups

> The **ClickHouse OTel Database** MCP tool is pre-connected. The AI can run SQL directly against `sql-clickhouse.clickhouse.com` → `otel_v2` database.

---

### Step 3: Guided Demo Script

Run these queries in order. Watch how the AI converts natural language → SQL → results.

**Discovery (5 min)**
```
What services are in the otel_v2 database? Show me their request volume and error rates in the last hour.
```

```
Which service has the highest p99 latency right now?
```

**Error Investigation (10 min)**
```
Show me error rates by service in the last hour. Which service is most broken?
```

```
For the service with the highest error rate, show me the actual error messages from logs.
```

**Latency Deep-Dive (10 min)**
```
Show me p50, p95, and p99 latency for all services. Format as a table sorted by p99.
```

```
Find the 10 slowest individual traces in the last hour. What were they doing?
```

**Distributed Tracing (10 min)**
```
Build a service dependency map — which services call which other services and at what error rate?
```

```
Find traces where the total duration was over 5 seconds. Which service caused the slowdown?
```

**SLA Check (5 min)**
```
Are any services violating their SLA targets? Show me actual vs target p95 latency for each service.
```

---

### Step 4: Try Your Own Questions

Use this format:
> *"Show me [metric] for [service] in the [time window] where [condition]"*

**Example ideas:**
- `"Why is the checkout service slow today? Show me root cause."`
- `"Compare error rates this hour vs 24 hours ago for all services"`
- `"Find database queries taking more than 2 seconds and which service runs them"`
- `"Show me a mermaid chart of request volume by service over the last 3 hours"`

---

## PART 2 — LangFuse: See Your AI Traces (10 min)

Every query you just ran was automatically traced. Open the LangFuse dashboard to see what happened behind the scenes.

```
https://us.cloud.langfuse.com
```

**Project:** JM's Observability-Demo

### What to look for:

1. **Traces** — Find the trace for your last LibreChat query
   - See the full prompt sent to the LLM
   - See the SQL the model generated
   - See the raw ClickHouse result

2. **Token usage** — How many tokens did each query cost?

3. **Latency** — How long did the LLM take vs ClickHouse execution?

4. **Quality Score** — Each response gets an automatic quality score (0–1) based on response depth

5. **Model comparison** — Run the same question on Gemini vs Claude. Compare cost and quality in LangFuse.

> **Key insight:** LangFuse makes AI behavior observable — the same way OpenTelemetry makes services observable.

---

## PART 3 — Direct SQL at sql-clickhouse.clickhouse.com (15 min)

Skip the AI. Query raw OpenTelemetry data directly.

### Connect

Open the ClickHouse SQL console:
```
https://sql.clickhouse.com
```

| Setting | Value |
|---------|-------|
| Host | `sql-clickhouse.clickhouse.com` |
| User | `demo` |
| Password | *(leave blank)* |
| Database | `otel_v2` |

---

### Walkthrough Queries

**1. What tables exist?**
```sql
SHOW TABLES FROM otel_v2
```

**2. Service overview — request volume + error rate**
```sql
SELECT
    ServiceName,
    count() AS total_requests,
    countIf(StatusCode = 'ERROR') AS errors,
    round(countIf(StatusCode = 'ERROR') / count() * 100, 2) AS error_rate_pct
FROM otel_v2.otel_traces
WHERE Timestamp >= now() - INTERVAL 1 HOUR
  AND SpanKind = 'SERVER'
GROUP BY ServiceName
ORDER BY error_rate_pct DESC
```

**3. Latency percentiles by service**
```sql
SELECT
    ServiceName,
    round(quantile(0.50)(Duration) / 1e6, 1) AS p50_ms,
    round(quantile(0.95)(Duration) / 1e6, 1) AS p95_ms,
    round(quantile(0.99)(Duration) / 1e6, 1) AS p99_ms
FROM otel_v2.otel_traces
WHERE Timestamp >= now() - INTERVAL 1 HOUR
  AND SpanKind = 'SERVER'
GROUP BY ServiceName
ORDER BY p99_ms DESC
```

**4. Top 10 slowest individual requests**
```sql
SELECT
    ServiceName,
    SpanName,
    round(Duration / 1e6, 1) AS duration_ms,
    StatusCode,
    formatDateTime(Timestamp, '%H:%M:%S') AS time
FROM otel_v2.otel_traces
WHERE Timestamp >= now() - INTERVAL 1 HOUR
  AND SpanKind = 'SERVER'
ORDER BY Duration DESC
LIMIT 10
```

**5. Error log messages with correlated trace context**
```sql
SELECT
    l.SeverityText,
    l.Body AS error_message,
    t.ServiceName,
    t.SpanName,
    round(t.Duration / 1e6, 1) AS duration_ms
FROM otel_v2.otel_logs l
JOIN otel_v2.otel_traces t
  ON l.TraceId = t.TraceId AND l.SpanId = t.SpanId
WHERE l.SeverityText IN ('ERROR', 'FATAL')
  AND l.Timestamp >= now() - INTERVAL 1 HOUR
ORDER BY l.Timestamp DESC
LIMIT 20
```

**6. Service dependency map**
```sql
SELECT
    parent_service,
    child_service,
    sum(call_count) AS total_calls,
    round(sum(error_count) / sum(call_count) * 100, 2) AS error_rate_pct,
    round(avg(avg_duration_ms), 1) AS avg_latency_ms
FROM otel_v2.otel_service_dependencies
WHERE timestamp_hour >= now() - INTERVAL 3 HOUR
GROUP BY parent_service, child_service
ORDER BY total_calls DESC
LIMIT 20
```

**7. Hourly request volume trend (last 24 hours)**
```sql
SELECT
    toStartOfHour(Timestamp) AS hour,
    ServiceName,
    count() AS requests,
    countIf(StatusCode = 'ERROR') AS errors
FROM otel_v2.otel_traces
WHERE Timestamp >= now() - INTERVAL 24 HOUR
  AND SpanKind = 'SERVER'
GROUP BY hour, ServiceName
ORDER BY hour DESC, requests DESC
```

**8. SLA compliance check**
```sql
SELECT
    t.ServiceName,
    round(quantile(0.95)(t.Duration) / 1e6, 1) AS p95_actual_ms,
    s.SLA_P95_ms AS p95_target_ms,
    if(quantile(0.95)(t.Duration) / 1e6 > s.SLA_P95_ms, 'VIOLATION', 'OK') AS status
FROM otel_v2.otel_traces t
LEFT JOIN otel_v2.otel_services s ON t.ServiceName = s.ServiceName
WHERE t.Timestamp >= now() - INTERVAL 1 HOUR
  AND t.SpanKind = 'SERVER'
GROUP BY t.ServiceName, s.SLA_P95_ms
ORDER BY p95_actual_ms DESC
```

> **ClickHouse tip:** Durations are stored in **nanoseconds**. Always divide by `1,000,000` to get milliseconds.

---

## PART 4 — Clickstack: Natural Language Search (15 min)

Clickstack is ClickHouse's AI-powered search layer over your telemetry data. No SQL needed.

```
https://play-clickstack.clickhouse.com/search
```

### 10 Things to Try

| # | What to type | What it shows |
|---|-------------|---------------|
| 1 | `errors in the last hour` | Recent error events grouped by service |
| 2 | `slowest requests today` | Top latency outliers with trace details |
| 3 | `payment service failures` | Filtered view of payment service error spans |
| 4 | `services with p99 above 1 second` | SLA breach candidates |
| 5 | `database queries slower than 2 seconds` | Slow DB spans with `db.statement` |
| 6 | `error rate trend last 24 hours` | Time-series error rate chart |
| 7 | `which services depend on user-service` | Upstream callers of a given service |
| 8 | `log messages containing timeout` | Full-text search across `otel_logs.Body` |
| 9 | `HTTP 500 errors by endpoint` | Grouped by `http.route` + `http.status_code` |
| 10 | `services with zero traffic in the last hour` | Detect dead or unreachable services |

### Compare: Clickstack vs LibreChat Agent

| | **Clickstack** | **LibreChat Agent** |
|---|---|---|
| Interface | Search bar | Conversational chat |
| SQL needed | No | No |
| Custom logic | Limited | Full SQL via MCP |
| Trace into LangFuse | No | Yes |
| Multi-step reasoning | No | Yes |

---

## Quick Reference

| Resource | URL |
|----------|-----|
| LibreChat (AI Agent) | `https://ec2-3-144-204-30.us-east-2.compute.amazonaws.com` |
| ClickHouse SQL | `https://sql.clickhouse.com` |
| Clickstack Search | `https://play-clickstack.clickhouse.com/search` |
| LangFuse Traces | `https://us.cloud.langfuse.com` |

| ClickHouse Connection | Value |
|-----------------------|-------|
| Host | `sql-clickhouse.clickhouse.com` |
| User | `demo` |
| Password | *(blank)* |
| Database | `otel_v2` |

**Key ClickHouse facts to remember:**
- Duration stored in **nanoseconds** → divide by `1,000,000` for ms
- Use `SpanKind = 'SERVER'` for user-facing requests
- Always add `WHERE Timestamp >= now() - INTERVAL N HOUR` for performance
- Prefer materialized views (`otel_service_metrics_1m`, `otel_service_health_hourly`) for aggregations

---

*Workshop materials: github.com/ClickHouse/ClickHouse_Demos/tree/main/agent_stack_builds/clickhouse_ai_obserability*
