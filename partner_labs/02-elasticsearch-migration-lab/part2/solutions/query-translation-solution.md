# Exercise 2B — Query Translation Solution

---

## Query 1: Top 10 Request Paths (status 200)

```sql
SELECT
    LogAttributes['request_path']             AS request_path,
    count()                                    AS hits
FROM otel_logs
WHERE LogAttributes['log_type'] = 'web_access'
  AND LogAttributes['status']   = '200'
GROUP BY request_path
ORDER BY hits DESC
LIMIT 10;
```

**How each engine answers this:**

- **ES**: builds a Terms bucket from the keyword subfield `request_path.keyword`. The result is approximate because each shard returns its own top-N and they are merged — a path that is #11 on every shard can be missed. The `shard_size` parameter widens the pre-merge list to mitigate this.
- **ClickHouse**: streams the matching rows through a hash-aggregate. Result is **exact** — there is no per-shard approximation. Scan cost is proportional to rows where `status = '200'`, which benefits from the materialized `Status` column + the `(ServiceName, Status, Timestamp)` ORDER BY key.

**If this is a hot dashboard query**, promote `request_path` to a materialized column (or extract at the collector). The version using `LogAttributes['request_path']` works but scans the whole Map per row.

---

## Query 2: 5xx Error Count per Minute (last hour)

```sql
SELECT
    toStartOfMinute(Timestamp)                 AS minute,
    count()                                     AS error_count
FROM otel_logs
WHERE Timestamp >= now() - INTERVAL 1 HOUR
  AND toUInt16OrZero(LogAttributes['status']) >= 500
GROUP BY minute
ORDER BY minute;
```

**How each engine answers this:**

- **ES**: `date_histogram` walks the `@timestamp` field and creates a bucket per fixed interval. It has a `max_buckets` guard (default 65 536); huge time ranges or tiny intervals throw an error.
- **ClickHouse**: `toStartOfMinute()` is a cheap integer arithmetic on the `Timestamp` column. There is no bucket cap. With a materialized `Status UInt16` column, the filter becomes `Status >= 500` — a direct column scan, no Map decoding per row.

**Variant for a dashboard with a materialized `Status` column:**
```sql
SELECT toStartOfMinute(Timestamp) AS minute, count() AS error_count
FROM otel_logs
WHERE Timestamp >= now() - INTERVAL 1 HOUR AND Status >= 500
GROUP BY minute ORDER BY minute;
```

---

## Query 3: Full-Text Search — "connection timeout" in ERROR logs

```sql
SELECT Timestamp, ServiceName, Body
FROM otel_logs
WHERE SeverityText = 'ERROR'
  AND hasTokenCaseInsensitive(Body, 'connection')             -- accelerated by text skip index (>=26.2) or tokenbf_v1
  AND hasTokenCaseInsensitive(Body, 'timeout')                -- accelerated by text skip index (>=26.2) or tokenbf_v1
  AND positionCaseInsensitive(Body, 'connection timeout') > 0 -- enforces phrase adjacency
ORDER BY Timestamp DESC
LIMIT 50;
```

**Why three predicates, not two?** `match_phrase "connection timeout"` in ES requires the two tokens to be **adjacent and in order**. `hasTokenCaseInsensitive(X, 'connection') AND hasTokenCaseInsensitive(X, 'timeout')` alone is only equivalent to `match` with `AND`, not `match_phrase` — it would match `"timeout occurred, connection dropped"`. The `hasTokenCaseInsensitive` calls give us the skip-index acceleration; the `positionCaseInsensitive(...) > 0` check enforces the phrase constraint. ClickHouse evaluates predicates left-to-right, so cheap skip-index checks run first and the substring scan only runs on survivors.

**Why `hasTokenCaseInsensitive`, not `hasToken`?** ES's default `standard` analyzer lowercases text at index and query time, so `match_phrase "connection timeout"` matches "Connection Timeout", "CONNECTION TIMEOUT", etc. `hasToken` is case-sensitive and would miss those variants. `hasTokenCaseInsensitive` matches on case-folded token boundaries and is still accelerated by the skip index.

**How each engine answers this:**

- **ES**: `match_phrase` uses the inverted index on the tokenized `message` field. Every `message` posting list is always maintained — cheap for queries, expensive for writes and storage. A `match_phrase` query walks the postings for both tokens, intersects them, **and** verifies positional adjacency using the positional posting lists.
- **ClickHouse**: a skip index on `Body` eliminates *granules* (groups of 8 192 rows by default) that definitely don't contain the tokens, then the engine scans only survivors and applies the `positionCaseInsensitive` predicate for phrase verification. Writes and storage are much cheaper than ES's inverted index because granule-level skip indexes are tiny, but false positives exist — hence the need for the substring check on candidate granules. **Index type:** prefer `TYPE text` (>= 26.2, purpose-built for full-text search, deterministic token lookup); `tokenbf_v1` / `ngrambf_v1` are deprecated as of ClickHouse 26.2.

**`hasToken` / `hasTokenCaseInsensitive` vs. `LIKE` vs. `positionCaseInsensitive`:**
- `hasToken` matches whole tokens on punctuation/whitespace boundaries (case-sensitive). `hasTokenCaseInsensitive` does the same with case folding. Both are accelerated by a `text` skip index (>= 26.2, preferred) or `tokenbf_v1` (deprecated). Use the case-insensitive variant when translating ES queries that use the `standard` analyzer. Use as a **filter** for the cheap pass.
- `positionCaseInsensitive` / `LIKE '%substr%'` do true substring matching — not token-boundary aware, not skip-index accelerated. Use them as a **verifier** after `hasToken*` narrows the candidate granules. Used alone, they full-scan every row.

---

## Query 4: Unique Services per Day (last 7 days)

```sql
-- Variant A: EXACT
SELECT
    toDate(Timestamp)         AS day,
    uniqExact(ServiceName)    AS unique_services
FROM otel_logs
WHERE Timestamp >= now() - INTERVAL 7 DAY
GROUP BY day
ORDER BY day;

-- Variant B: APPROXIMATE (HyperLogLog-based, much faster on high-cardinality columns)
SELECT
    toDate(Timestamp)         AS day,
    uniq(ServiceName)         AS unique_services
FROM otel_logs
WHERE Timestamp >= now() - INTERVAL 7 DAY
GROUP BY day
ORDER BY day;
```

**Which to use in production:** **`uniq()`** (approximate) on dashboards and alerts. The error is ≤ 1.6 % with much lower memory and CPU. Reserve `uniqExact` for audit/billing queries where exact numbers matter. For *this* workload `ServiceName` has ~5 distinct values, so both are trivially cheap — but the pattern generalizes: if the cardinality is known to be small, prefer `uniqExact`; for unbounded/high-cardinality columns (user IDs, trace IDs), prefer `uniq` or `uniqCombined`.

**How each engine answers this:**

- **ES**: `cardinality` aggregation uses HyperLogLog++ with a tunable `precision_threshold`. Always approximate.
- **ClickHouse**: chooses for you. `uniq` → HLL, `uniqExact` → hash set, `uniqCombined` → HLL+hash hybrid.

---

## Query 5: Trace Lookup by `trace.id`

```sql
SELECT *
FROM otel_traces
WHERE TraceId = '<TRACE_ID>'
ORDER BY Timestamp ASC
LIMIT 1000;
```

**How each engine answers this:**

- **ES**: `trace.id` is a `keyword` field, indexed in the inverted index. Lookup is O(log N) via the term dictionary, then each posting is fetched — very fast.
- **ClickHouse**: `TraceId` is **not** in the primary `ORDER BY` key. A plain `WHERE TraceId = ?` without acceleration would full-scan. **The fix is a `bloom_filter` skip index on `TraceId`.** The index probes each granule's Bloom filter; most granules are eliminated cheaply, and only candidates are scanned.

```sql
ALTER TABLE otel_traces
ADD INDEX trace_id_bf TraceId TYPE bloom_filter(0.01) GRANULARITY 4;
ALTER TABLE otel_traces MATERIALIZE INDEX trace_id_bf;
```

**Why `TraceId` should NOT be the leading column in `ORDER BY`:**

Sort keys are an agreement with the query planner: *"most queries will filter from the left."* If you make `TraceId` the lead column, the rows for a single trace are co-located — but every other query pattern (time range by service, most-recent errors, dashboard tile queries) becomes a full scan because those queries don't know the TraceId. Trace lookup is a **needle-in-a-haystack** pattern: rare, point-lookup. Skip indices (Bloom filters) are designed for exactly this case — they give you sub-linear lookup without paying the storage-layout tax.
