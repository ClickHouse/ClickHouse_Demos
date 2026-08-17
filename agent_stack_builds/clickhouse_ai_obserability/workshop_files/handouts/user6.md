# AI Observability Workshop — User 6 Lab Guide

---

## Your Login Credentials

| | |
|---|---|
| **LibreChat URL** | `https://ec2-3-144-204-30.us-east-2.compute.amazonaws.com` |
| **Email** | `user6@clickhouse.com` |
| **Password** | `Workshop2024!` |
| **ClickHouse Host** | `sql-clickhouse.clickhouse.com` |
| **ClickHouse User** | `demo` · Password: *(blank)* · Database: `otel_v2` |
| **Langfuse** | `https://us.cloud.langfuse.com` — project: *JM's Observability-Demo* |
| **Clickstack** | `https://play-clickstack.clickhouse.com/search` |

> Open LibreChat in your browser → accept the certificate warning → **Advanced → Proceed**

---

## Part 1 — LibreChat AI Agent (35 min)

The AI is pre-connected to ClickHouse via an MCP tool. It reads your question, writes SQL, runs it, and explains the results — all in one step.

**Select your model** from the top picker:
- **Gemini 2.5 Pro** — best for complex multi-step reasoning
- **Claude Sonnet 4.6** — best for structured SQL and explanations
- **Gemini 2.5 Flash** — fastest, good for quick lookups

### Guided Demo Script

Run these in order. Watch the model write and execute SQL automatically.

**Discovery**
```
What services are in the database? Show request volume and error rates in the last hour.
```
```
Which service has the highest p99 latency right now?
```

**Error Investigation**
```
Show me error rates by service in the last hour — which service is most broken?
```
```
For the service with the highest error rate, show me the actual error messages from the logs.
```

**Latency Deep-Dive**
```
Show me p50, p95, and p99 latency for all services as a table sorted by p99.
```
```
Find the 10 slowest individual traces in the last hour. What were they doing?
```

**Distributed Tracing**
```
Build a service dependency map — which services call which, and at what error rate?
```
```
Find traces where total duration exceeded 5 seconds. Which service caused the slowdown?
```

**SLA Check**
```
Are any services violating their SLA targets? Show actual vs target p95 latency per service.
```

### Write Your Own

Format: *"Show me [metric] for [service] in the [time window] where [condition]"*

Try:
- `"Why is the checkout service slow? Show root cause."`
- `"Compare error rates this hour vs 24 hours ago for all services"`
- `"Show a mermaid chart of request volume by service over the last 3 hours"`
- `"Find database queries taking more than 2 seconds"`

---

## Part 2 — Langfuse: See Your AI Traces (10 min)

Every question you just asked was automatically traced. Open Langfuse to see inside the pipeline.

**URL:** `https://us.cloud.langfuse.com` → project **JM's Observability-Demo**

Look for:
1. **Traces** — find your last LibreChat query; see the full prompt, generated SQL, and raw result
2. **Token usage** — how many tokens did each query consume?
3. **Latency breakdown** — LLM time vs ClickHouse execution time
4. **Quality score** — automatic 0–1 score on each response
5. **Model comparison** — run the same question on Gemini vs Claude, compare cost in Langfuse

> **The point:** LangFuse makes AI behavior observable — the same way OpenTelemetry makes services observable.

---

## Part 3 — Direct SQL at sql-clickhouse.clickhouse.com (15 min)

Skip the AI. Query raw OpenTelemetry data yourself.

**Open:** `https://sql.clickhouse.com` → connect with host `sql-clickhouse.clickhouse.com`, user `demo`, password blank, database `otel_v2`

**1. What tables exist?**
```sql
SHOW TABLES FROM otel_v2
```

**2. Service overview — requests + error rate**
```sql
SELECT
    ServiceName,
    count()                                                    AS total_requests,
    countIf(StatusCode = 'ERROR')                             AS errors,
    round(countIf(StatusCode = 'ERROR') / count() * 100, 2)  AS error_rate_pct
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

**4. Top 10 slowest requests**
```sql
SELECT
    ServiceName,
    SpanName,
    round(Duration / 1e6, 1)            AS duration_ms,
    StatusCode,
    formatDateTime(Timestamp, '%H:%M:%S') AS time
FROM otel_v2.otel_traces
WHERE Timestamp >= now() - INTERVAL 1 HOUR
  AND SpanKind = 'SERVER'
ORDER BY Duration DESC
LIMIT 10
```

**5. Error logs correlated with traces**
```sql
SELECT
    l.SeverityText,
    l.Body                        AS error_message,
    t.ServiceName,
    t.SpanName,
    round(t.Duration / 1e6, 1)   AS duration_ms
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
    sum(call_count)                                    AS total_calls,
    round(sum(error_count) / sum(call_count) * 100, 2) AS error_rate_pct,
    round(avg(avg_duration_ms), 1)                     AS avg_latency_ms
FROM otel_v2.otel_service_dependencies
WHERE timestamp_hour >= now() - INTERVAL 3 HOUR
GROUP BY parent_service, child_service
ORDER BY total_calls DESC
LIMIT 20
```

**7. Hourly request trend (last 24 hours)**
```sql
SELECT
    toStartOfHour(Timestamp) AS hour,
    ServiceName,
    count()                   AS requests,
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
    s.SLA_P95_ms                                AS p95_target_ms,
    if(quantile(0.95)(t.Duration) / 1e6 > s.SLA_P95_ms,
       'VIOLATION', 'OK')                       AS status
FROM otel_v2.otel_traces t
LEFT JOIN otel_v2.otel_services s ON t.ServiceName = s.ServiceName
WHERE t.Timestamp >= now() - INTERVAL 1 HOUR
  AND t.SpanKind = 'SERVER'
GROUP BY t.ServiceName, s.SLA_P95_ms
ORDER BY p95_actual_ms DESC
```

> Duration is stored in **nanoseconds** — always divide by `1,000,000` (or `1e6`) to get milliseconds.

---

## Part 4 — Clickstack Search (15 min)

Natural language search over your telemetry — no SQL, no AI agent.

**URL:** `https://play-clickstack.clickhouse.com/search`

| # | Search for | What you see |
|---|-----------|-------------|
| 1 | `errors in the last hour` | Error events grouped by service |
| 2 | `slowest requests today` | Top latency outliers |
| 3 | `payment service failures` | Filtered error spans for payment |
| 4 | `services with p99 above 1 second` | SLA breach candidates |
| 5 | `database queries slower than 2 seconds` | Slow DB spans |
| 6 | `error rate trend last 24 hours` | Time-series error chart |
| 7 | `which services depend on user-service` | Upstream callers |
| 8 | `log messages containing timeout` | Full-text log search |
| 9 | `HTTP 500 errors by endpoint` | Grouped by route + status |
| 10 | `services with zero traffic in the last hour` | Dead or unreachable services |

---

## Reference Card

| Resource | URL |
|----------|-----|
| LibreChat (AI Agent) | `https://ec2-3-144-204-30.us-east-2.compute.amazonaws.com` |
| ClickHouse SQL Console | `https://sql.clickhouse.com` |
| Clickstack Search | `https://play-clickstack.clickhouse.com/search` |
| Langfuse Traces | `https://us.cloud.langfuse.com` |

| ClickHouse | Value |
|-----------|-------|
| Host | `sql-clickhouse.clickhouse.com` |
| User | `demo` |
| Password | *(blank)* |
| Database | `otel_v2` |

**Your credentials:** `user6@clickhouse.com` / `Workshop2024!`
