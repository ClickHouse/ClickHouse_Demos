# Exercise 3B — Queries That Weren't Possible in Elasticsearch

> **Database:** All Part 3 tables (`otel_logs_v2`, `otel_traces`, `otel_metrics_*`, `alert_error_rate`, `logs_summary_1min`) live in the `otel` database. Run `clickhouse client --database otel ...` or open the SQL console and start every session with `USE otel;` so the unqualified table names below resolve correctly.

Now that data is flowing into ClickHouse, this exercise demonstrates what's *newly possible* — queries that range from difficult to impossible in Elasticsearch but are natural in ClickHouse SQL.

For each exercise:
1. Read the **ES limitation** to understand what ES can and can't do
2. Write (or run) the **ClickHouse query** in the SQL console
3. Note the result and answer the reflection question

---

## Exercise 1: Cross-Signal JOIN — Logs + Traces

**ES limitation:** Elasticsearch DSL has no JOIN. ES|QL supports only restricted left outer joins. Correlating logs with their parent traces requires multiple API calls and client-side joining.

**Task:** Write a query that finds the 10 slowest traces in the last hour, then shows all log entries associated with those traces — in a single query.

Hints:
- `otel_traces` has `TraceId`, `ServiceName`, `SpanName`, `Duration`, `SpanKind`
- `otel_logs_v2` has `TraceId`, `Timestamp`, `Body`, `SeverityText`
- Use a CTE to find slow traces first, then JOIN
- `Duration` is stored in nanoseconds
- **Restrict to user-facing spans:** if you take "top 10 slowest" without a filter, the OTel demo's `flagd` feature-flag long-poll streams (`SpanName` containing `flagd`, `Duration ≈ 600,000,000,000` ns) dominate the top of the distribution and have **no** associated logs, so the JOIN returns zero rows. Filter to `SpanKind = 'Server'` (verify with `SELECT DISTINCT SpanKind FROM otel_traces` — values are `'Server'`/`'Client'`/`'Internal'`, not the proto-style `SPAN_KIND_*`) and exclude flagd by name.

```sql
-- Write your query here:

```

**Reflection:** In Kibana, how would you accomplish the same investigation? How many separate queries/actions would it take?

> Your answer: _______________________________________________

---

## Exercise 2: Window Functions — Anomaly Detection

**ES limitation:** Elasticsearch has no window functions. Detecting whether a metric spiked relative to its historical baseline requires external computation or complex scripted metric aggregations.

**Task:** Write a query using `LAG()` to find service-minute pairs where the error rate **jumped more than 20%** compared to the previous minute, over the last 1 hour.

Hints:
- Use a CTE to compute per-service, per-minute error rates first; bucket with `toStartOfMinute(Timestamp)`
- `LAG(error_rate_pct) OVER (PARTITION BY ServiceName ORDER BY minute)` gives you the previous minute's rate
- Filter for rows where `current_rate / prev_rate > 1.2`
- `RequestType != ''` limits to web access logs
- Add `HAVING total > 50` to skip low-sample minutes where the rate is statistically noisy

> **Why minutes / 1.2× and not hours / 3×?** The lab's log generators emit a deliberately steady ~5% error rate (Poisson noise around the mean). Hour-bucket spike ratios stay within ±3% in practice; even per-minute the worst observed swing is ~1.5×. A 20% threshold (1.2×) at minute resolution surfaces the natural noise envelope while still teaching the same `LAG` pattern. In a production rule you'd tune the threshold for your own traffic — typically 1.5×–3× over a 5-minute rolling window.

```sql
-- Write your query here:

```

**Reflection:** What tool or external system would you need to replicate this in an Elasticsearch-only stack?

> Your answer: _______________________________________________

---

## Exercise 3: Unbounded GROUP BY — Complete Endpoint Inventory

**ES limitation:** Elasticsearch Terms aggregations require a `size` parameter (default 10, max bounded by `max_buckets: 65,535`). Getting *all* unique values requires paginating through a composite aggregation.

**Task:** List ALL unique request paths (`RequestPage`) along with their total request count, error rate, and p95 latency — no `LIMIT`, no pagination.

Hints:
- `countIf(StatusCode >= 500)` for error count
- `quantile(0.95)(toFloat64OrZero(LogAttributes['run_time']))` for p95
- `round(error_count / total_requests * 100, 2)` for error rate
- `WHERE RequestType != ''` for web logs only
- `ORDER BY total_requests DESC`

```sql
-- Write your query here:

```

**Reflection:** Run the query and note how many unique paths are returned. Why can't you do this in Elasticsearch Terms aggregations?

> Unique paths returned: `_______`
>
> ES limitation: _______________________________________________

---

## Exercise 4: Sequence Detection — Request Flows Leading to Errors

**ES limitation:** Elasticsearch has no sequence matching across events. EQL supports sequences for security use cases but cannot handle arbitrary event chains across services.

**Task:** Find client IP addresses that experienced the sequence: `api-gateway` (200) → `order-service` (200) → `payment-service` (5xx) — all within 10 seconds.

Hints:
- `sequenceMatch('(?1).*(?2).*(?3)')` takes the timestamp column first, then boolean conditions for each step
- Use `TimestampTime` (type `DateTime`), not `Timestamp` (type `DateTime64`) — `sequenceMatch` requires `DateTime`
- Group by `RemoteAddr`, apply `HAVING sequenceMatch(...)`
- `StatusCode = 200` for success, `StatusCode >= 500` for server error
- Add a time window: `WHERE TimestampTime >= now() - INTERVAL 1 HOUR`

```sql
-- Write your query here:

```

**Reflection:** What does `(?1).*(?2).*(?3)` mean in the sequenceMatch pattern? What does `.*` match?

> Your answer: _______________________________________________

---

## Exercise 5: Conditional Aggregation — Multi-Metric Service Health

**ES limitation:** In Elasticsearch, each conditional metric requires a separate `filter` aggregation wrapping a `metric` sub-aggregation, leading to deeply nested query JSON (~150 lines for an equivalent query).

**Task:** Write a single query that computes for each service (web access logs only, last 1 hour):
- Total events
- 2xx success count
- 4xx client error count
- 5xx server error count
- Error rate %
- p50 and p95 latency for **successful requests only** (`StatusCode < 500`)
- Unique affected IPs for **failed requests** (`StatusCode >= 500`)
- First and last error timestamp

Hints:
- Use `-If` combinators: `countIf(condition)`, `quantileIf(0.95)(field, condition)`, `uniqIf(field, condition)`
- `minIf(Timestamp, condition)`, `maxIf(Timestamp, condition)`
- `RequestType != ''` for web logs

```sql
-- Write your query here:

```

**Reflection:** Count the lines in your ClickHouse query. Now estimate how many lines the equivalent Elasticsearch DSL JSON would be. What's the ratio?

> ClickHouse lines: `_____`  ES DSL estimate: `_____`  Ratio: `_____×`

---

## Exercise 6: CTE Root Cause Investigation

**ES limitation:** Elasticsearch DSL does not support subqueries or CTEs. Multi-step investigations require multiple API calls with client-side orchestration.

**Task:** Write a single CTE-based query that answers:
*"Which services had the highest error rate in the last hour, what were their top error messages, and which trace IDs were affected?"*

The query should:
1. **CTE 1** (`error_services`): Find top 3 services by error rate in the last hour (only services with > 10 errors)
2. **CTE 2** (`top_errors`): Get top 10 error message bodies for those services (JOIN with CTE 1)
3. **CTE 3** (`affected_traces`): Get up to 50 distinct TraceIds for those services (JOIN with CTE 1)
4. **Final SELECT**: Combine all three CTEs using `groupArray()` to collect sample messages and trace IDs per service

Hints:
- `groupArray(10)(t.Body)` collects up to 10 Body values into an array per group
- `groupArray(5)(a.TraceId)` for trace IDs
- Use `INNER JOIN` between CTEs (not a full outer join)
- `WHERE TraceId != ''` to exclude rows without traces

```sql
-- Write your query here:

```

**Reflection:** This single query replaces how many Kibana/ES API calls? Which CTE step was most valuable for your investigation?

> Number of equivalent ES calls: `_____`
>
> Most valuable step: _______________________________________________
