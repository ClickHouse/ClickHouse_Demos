# Exercise 3B — SQL Exercises Solution (Model Answers)

> **Database:** All tables below live in the `otel` database. Run with `clickhouse client --database otel ...` or `USE otel;` first.

---

## Exercise 1: Cross-Signal JOIN — Logs + Traces

```sql
WITH slow_traces AS (
    SELECT TraceId, ServiceName, SpanName, Duration
    FROM otel_traces
    WHERE Timestamp >= now() - INTERVAL 1 HOUR
      AND SpanKind = 'Server'             -- user-facing request spans only
      AND SpanName NOT LIKE '%flagd%'     -- exclude long-poll feature-flag streams
    ORDER BY Duration DESC
    LIMIT 10
)
SELECT
    t.TraceId,
    t.ServiceName       AS trace_service,
    t.SpanName,
    t.Duration          AS trace_duration_ns,
    l.Timestamp         AS log_time,
    l.ServiceName       AS log_service,
    l.SeverityText,
    l.Body
FROM slow_traces t
JOIN otel_logs_v2 l ON t.TraceId = l.TraceId
ORDER BY t.Duration DESC, l.Timestamp ASC;
```

**Teaching points:**
- This single query replaces a 3-step Kibana workflow: (1) query APM for slowest traces, (2) copy trace IDs, (3) search logs for each trace ID individually.
- ClickHouse executes the CTE once, then uses a hash join to match `l.TraceId` against the 10-row result set — extremely efficient.
- The `bloom_filter` skip index on `TraceId` in `otel_logs_v2` accelerates the join probe for each of the 10 slow trace IDs.
- `groupArray()` was not used here because we want individual log rows, not arrays. Exercise 6 uses `groupArray()` for aggregated output.

> **Why the two extra `WHERE` clauses?** Without `SpanKind = 'Server'` you'll find that the top 10 slowest spans are all internal `flagd` `EventStream` gRPC long-polls — bookkeeping spans that stay open for ~10 minutes (`Duration ≈ 600,000,000,000 ns`) and have **zero** associated log records, so the JOIN returns no rows. Restricting to `SpanKind = 'Server'` (incoming HTTP requests) plus excluding flagd by name surfaces real user-facing slow requests like the `frontend-proxy / ingress` spans that take ~8–9 seconds; each of those has matching application logs so the JOIN succeeds. The OTel demo's instrumentation is naturally noisy at the top end of the duration distribution — this is a useful real-world reminder that "top N by latency" almost always needs a kind/name filter to be meaningful.
>
> **Note on enum values:** ClickHouse's OTel collector exporter writes `SpanKind` as `'Server'`, `'Client'`, `'Internal'`, `'Producer'`, `'Consumer'` — not the OTel proto names like `'SPAN_KIND_SERVER'`. Always check what's actually stored: `SELECT DISTINCT SpanKind FROM otel_traces`.

**Expected output shape:** Multiple rows per trace (all log entries sharing that TraceId), ordered by trace duration then log timestamp.

---

## Exercise 2: Window Functions — Anomaly Detection

```sql
WITH minute_errors AS (
    SELECT
        ServiceName,
        toStartOfMinute(Timestamp)  AS minute,
        countIf(StatusCode >= 500)  AS errors,
        count()                     AS total,
        if(total > 0, errors / total * 100, 0) AS error_rate_pct
    FROM otel_logs_v2
    WHERE TimestampTime >= now() - INTERVAL 1 HOUR
      AND RequestType != ''
    GROUP BY ServiceName, minute
    HAVING total > 50         -- skip low-sample minutes where the rate is noise
),
with_lag AS (
    SELECT
        ServiceName,
        minute,
        error_rate_pct,
        LAG(error_rate_pct) OVER (PARTITION BY ServiceName ORDER BY minute) AS prev_minute_rate,
        if(prev_minute_rate > 0, error_rate_pct / prev_minute_rate, 0)      AS spike_ratio
    FROM minute_errors
)
SELECT *
FROM with_lag
WHERE spike_ratio > 1.2
ORDER BY spike_ratio DESC;
```

**Teaching points:**
- `LAG(error_rate_pct) OVER (PARTITION BY ServiceName ORDER BY minute)` compares each minute to the previous minute **for the same service**. Without `PARTITION BY`, you'd incorrectly compare across different services and end up with a meaningless cross-service ratio.
- In Elasticsearch: you'd need a `date_histogram` aggregation to get per-minute counts, pull the JSON client-side, then compute deltas in Python/JavaScript. That's at minimum 2 API calls + application code, plus state to remember the previous bucket.
- The `with minute_errors AS (...)` CTE computes the base metrics; the outer query applies the window function. This two-step structure keeps the intent clear and is often more efficient than a single complex query — ClickHouse can pipeline the two stages without materializing the full intermediate result.
- `HAVING total > 50` inside the first CTE is the unsung hero: without it, a minute with 3 requests and 1 error (33% rate!) would dwarf every other bucket. Always sample-size-filter your aggregations before applying ratio comparisons.

> **Why these specific knobs (minute / 1.2× / 1 hour)?** The lab's log generators emit a deliberately steady ~5% error rate per service (Poisson noise around the mean). Empirically:
>
> | Threshold | Per-minute pairs that crossed it (last 1 h, ~305 samples) |
> |---|---|
> | spike > 1.10 (10%) | 40 |
> | spike > 1.20 (20%) | 4 |
> | spike > 1.50 (50%) | 0 |
> | spike > 2.00 (2×)  | 0 |
> | spike > 3.00 (3×)  | 0 |
>
> A 20% jump (1.2×) at minute resolution is the smallest threshold that surfaces actual rare events rather than the everyday noise floor — typical sample output:
>
> ```
> ServiceName    minute               rate_pct  prev_minute_rate  spike_ratio
> web-frontend   2026-05-09 04:10:00  5.186     4.263             1.216
> web-frontend   2026-05-09 03:48:00  5.501     4.549             1.209
> api-gateway    2026-05-09 04:24:00  4.714     3.903             1.208
> web-frontend   2026-05-09 04:01:00  5.357     4.460             1.201
> ```
>
> In a production rule you'd tune to your own traffic patterns — typically 1.5×–3× over a 5-minute rolling window. Hour-bucket / 3× as a rule (a common "obvious incident" SRE threshold) is too coarse for this lab's data because the synthetic generator's hourly variation is ~3% relative; in production with real human user-driven traffic, 3× hourly is exactly the right "we have a real problem" threshold.

---

## Exercise 3: Unbounded GROUP BY — Complete Endpoint Inventory

```sql
SELECT
    RequestPage,
    count()                                              AS total_requests,
    countIf(StatusCode >= 500)                           AS errors,
    round(errors / total_requests * 100, 2)              AS error_rate_pct,
    quantile(0.95)(toFloat64OrZero(LogAttributes['run_time'])) AS p95_latency
FROM otel_logs_v2
WHERE RequestType != ''
GROUP BY RequestPage
ORDER BY total_requests DESC;
```

**Teaching points:**
- No `LIMIT` means ClickHouse returns every unique `RequestPage` value — 100, 10,000, or 1,000,000. Elasticsearch Terms agg with `size` would only return the top N.
- In Elasticsearch, the composite aggregation workaround paginates through results using `after_key`. Each page is a separate API call. ClickHouse does this in a single pass.
- `quantile(0.95)(toFloat64OrZero(...))` computes p95 latency **inline** with the GROUP BY — no separate sub-aggregation needed. In Elasticsearch, adding percentiles to a terms aggregation doubles the response payload and query complexity.
- `toFloat64OrZero()` safely handles rows where `run_time` is empty or non-numeric (returns 0 instead of throwing an error).

---

## Exercise 4: Sequence Detection — Request Flows Leading to Errors

```sql
SELECT
    RemoteAddr,
    count() AS occurrence_count
FROM otel_logs_v2
WHERE TimestampTime >= now() - INTERVAL 1 HOUR
GROUP BY RemoteAddr
HAVING sequenceMatch('(?1)(?t<=10).*(?2)(?t<=10).*(?3)')(
    TimestampTime,                                       -- sequenceMatch requires DateTime, not DateTime64
    ServiceName = 'api-gateway'   AND StatusCode = 200,
    ServiceName = 'order-service' AND StatusCode = 200,
    ServiceName = 'payment-service' AND StatusCode >= 500
)
ORDER BY occurrence_count DESC
LIMIT 20;
```

**Teaching points:**
- `(?1)(?t<=10).*(?2)(?t<=10).*(?3)` is a regular-expression-like pattern over event sequences:
  - `(?N)` matches an event satisfying condition N
  - `(?t<=10)` constrains the **time delta to the next event** to ≤ 10 seconds (inclusive)
  - `.*` matches zero or more intervening events between the two anchor conditions
  - The whole pattern requires conditions 1, 2, 3 to occur **in order**, with each successive matched condition arriving within 10 seconds of the previous one
- The function operates on rows grouped by `RemoteAddr`, treating each group as an ordered event sequence.
- `sequenceMatch()` is unique to ClickHouse. Neither Elasticsearch DSL nor ES|QL can express multi-step ordered event patterns across services.
- The time-delta operator `(?t op N)` supports `<`, `<=`, `==`, `>=`, `>`. Different from `windowFunnel()`, which takes the window as a separate numeric argument and reports *how far* through the funnel each row got.

> **Verified output:** Without the `(?t<=10)` time bound (i.e., `'(?1).*(?2).*(?3)'`), this query returns ~50 of 51 distinct IPs in this lab — every IP eventually hits the api-gateway → order-service → payment-service-5xx path within an hour, so the pattern matches almost everywhere and the answer isn't very discriminating. Adding the 10-second time bound between successive matched events drops the result to ~9 IPs, which is much closer to the kind of "real anomaly" signal you'd want in production.

> **If your query returns 0 rows:** the log generator's service names may differ. Run `SELECT DISTINCT ServiceName FROM otel_logs_v2 WHERE RequestType != ''` to find the actual web-access service names — the lab uses `api-gateway`, `order-service`, `payment-service`, `inventory-service`, and `web-frontend`.

---

## Exercise 5: Conditional Aggregation — Multi-Metric Service Health

```sql
SELECT
    ServiceName,
    count()                                                            AS total_events,
    countIf(StatusCode >= 200 AND StatusCode < 300)                   AS success_2xx,
    countIf(StatusCode >= 400 AND StatusCode < 500)                   AS client_errors_4xx,
    countIf(StatusCode >= 500)                                         AS server_errors_5xx,
    round(server_errors_5xx / total_events * 100, 2)                  AS error_rate_pct,
    quantileIf(0.50)(toFloat64OrZero(LogAttributes['run_time']), StatusCode < 500) AS p50_latency_ok,
    quantileIf(0.95)(toFloat64OrZero(LogAttributes['run_time']), StatusCode < 500) AS p95_latency_ok,
    quantileIf(0.50)(toFloat64OrZero(LogAttributes['run_time']), StatusCode >= 500) AS p50_latency_err,
    uniqIf(RemoteAddr, StatusCode >= 500)                             AS unique_affected_ips,
    minIf(Timestamp, StatusCode >= 500)                               AS first_error_at,
    maxIf(Timestamp, StatusCode >= 500)                               AS last_error_at
FROM otel_logs_v2
WHERE TimestampTime >= now() - INTERVAL 1 HOUR
  AND RequestType != ''
GROUP BY ServiceName
ORDER BY error_rate_pct DESC;
```

**Teaching points:**
- The `-If` combinator can be appended to *any* ClickHouse aggregate function: `countIf`, `avgIf`, `sumIf`, `quantileIf`, `uniqIf`, `minIf`, `maxIf`, etc.
- This single query performs 12 metrics across 7 different conditions in **one table scan**. In Elasticsearch, each conditional metric requires its own `filter` → `metric` nesting, producing ~150 lines of nested JSON for an equivalent query.
- `quantileIf(0.95)(latency, StatusCode < 500)` computes p95 **only for successful requests** — impossible to express in a single ES aggregation without `bucket_script`.
- `uniqIf(RemoteAddr, StatusCode >= 500)` counts distinct affected clients — useful for gauging blast radius. In ES, this would require a filtered `cardinality` sub-aggregation with its HyperLogLog approximation; ClickHouse's `uniqIf` is also approximate (HLL) but the syntax is far simpler.

---

## Exercise 6: CTE Root Cause Investigation

```sql
WITH
error_services AS (
    SELECT
        ServiceName,
        countIf(StatusCode >= 500) AS errors,
        count()                    AS total
    FROM otel_logs_v2
    WHERE TimestampTime >= now() - INTERVAL 1 HOUR
      AND RequestType != ''
    GROUP BY ServiceName
    HAVING errors > 10
    ORDER BY errors / total DESC
    LIMIT 3
),
top_errors AS (
    SELECT
        l.ServiceName,
        l.Body,
        count() AS occurrences
    FROM otel_logs_v2 l
    INNER JOIN error_services e ON l.ServiceName = e.ServiceName
    WHERE l.StatusCode >= 500
      AND l.TimestampTime >= now() - INTERVAL 1 HOUR
    GROUP BY l.ServiceName, l.Body
    ORDER BY occurrences DESC
    LIMIT 10
),
affected_traces AS (
    SELECT DISTINCT
        l.TraceId,
        l.ServiceName
    FROM otel_logs_v2 l
    INNER JOIN error_services e ON l.ServiceName = e.ServiceName
    WHERE l.StatusCode >= 500
      AND l.TraceId != ''
      AND l.TimestampTime >= now() - INTERVAL 1 HOUR
    LIMIT 50
)
SELECT
    e.ServiceName,
    e.errors,
    e.total,
    round(e.errors / e.total * 100, 2)  AS error_rate_pct,
    groupArray(10)(t.Body)              AS sample_error_messages,
    groupArray(5)(a.TraceId)            AS sample_trace_ids
FROM error_services e
LEFT JOIN top_errors t       ON e.ServiceName = t.ServiceName
LEFT JOIN affected_traces a  ON e.ServiceName = a.ServiceName
GROUP BY e.ServiceName, e.errors, e.total
ORDER BY error_rate_pct DESC;
```

**Teaching points:**
- This single query replaces 3 separate Elasticsearch API calls + client-side JSON stitching: (1) date-histogram error rate, (2) terms aggregation for error messages, (3) terms aggregation for trace IDs.
- **`groupArray(N)(expr)`** collects up to N values of `expr` into an array per group. It's ClickHouse's equivalent of collecting sample values — there is no clean ES equivalent (ES `top_hits` is closest but only works within terms sub-aggregations, not across JOINs).
- CTEs in ClickHouse are computed once and reused. `error_services` is referenced by three downstream CTEs and the final SELECT — ClickHouse materializes it once.
- `LEFT JOIN` in the final SELECT ensures a service row appears even if it has no matching trace IDs (e.g., all errors lacked a TraceId). An `INNER JOIN` would silently drop those services from the result.
- The `HAVING errors > 10` filter in `error_services` prevents noise from services with very low traffic that happen to have a single error. Adjust the threshold based on your ingestion rate.

> **Why is `sample_trace_ids` empty in this lab?** The query returns rows for `web-frontend`, `payment-service`, `order-service` (etc.) — these are the **file-based log generators** introduced in Part 1, and they don't propagate trace context, so every row in `otel_logs_v2` for these services has `TraceId = ''`. The `affected_traces` CTE filters on `TraceId != ''` and ends up empty, so `groupArray(5)(a.TraceId)` returns `['','','','','']` (the LEFT JOIN keeps the parent row but no actual TraceId values exist). To verify: `SELECT ServiceName, countIf(TraceId != '') AS rows_with_trace, count() AS total FROM otel_logs_v2 GROUP BY ServiceName ORDER BY total DESC` shows that file-based services have `rows_with_trace = 0`, while OTel-Demo services (`frontend-proxy`, `product-catalog`, `cart`, …) have non-zero — but those don't currently emit HTTP 5xx codes via the `StatusCode` column, so they don't surface in `error_services`. **In a real production environment**, the application emitting 5xx errors and the application emitting traces would be the same OTel-instrumented service, and `sample_trace_ids` would be a clickable jumping-off point for trace drill-down. The lab can't show that end-to-end with its current data sources, but the SQL pattern is exactly what you'd run against production.
