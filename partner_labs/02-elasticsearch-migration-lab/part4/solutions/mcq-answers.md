# Part 4 — MCQ Answer Key

> **Don't open this file until you've answered every question in [`exercises/mcq.md`](../exercises/mcq.md).** The explanations below are designed to deepen your understanding regardless of whether you got the answer right or wrong — even on questions you got correct, read the explanation, because most are deliberately structured to surface a specific mental model.

| Q | Answer |
|---|---|
| 1 | **B** |
| 2 | **B** |
| 3 | **C** |
| 4 | **C** |
| 5 | **B** |
| 6 | **B** |
| 7 | **B** |
| 8 | **C** |
| 9 | **B** |
| 10 | **C** |
| 11 | **B** |
| 12 | **C** |
| 13 | **B** |
| 14 | **B** |
| 15 | **B** |

---

## Q1. Sort-key prefix matching — **B**

ClickHouse's primary index is *sparse* — one entry per `index_granularity` rows (default 8192). The engine uses this to skip granules that **cannot** contain matches based on the leading sort-key columns. A query that filters on:

- The full prefix `(ServiceName, SeverityText, TimestampTime)` — all three columns of the order key, and `TimestampTime` last (the trailing range column) — gives the index maximum power to skip granules.

**A** filters only on the trailing column — the engine can use the per-granule min/max for `TimestampTime` but cannot use the sparse index to its full effect because the leading columns are unconstrained. **C** uses `LIKE` on a column not in the order key — needs a skip index (e.g. `tokenbf_v1`) or a full scan. **D** filters on a column not in the sort key at all.

**Mental model:** "How much of the order-key prefix does my predicate constrain?"

---

## Q2. TTL drop granularity — **B**

`SETTINGS ttl_only_drop_parts = 1` is the magic. With it, ClickHouse waits until an **entire MergeTree part** has expired (i.e., its max `TimestampDate` has crossed `INTERVAL 30 DAY`), then unlinks the whole part on disk. This is dramatically cheaper than the default behavior (rewrite the part minus the expired rows), and it pairs naturally with date-based partitioning where each partition lives in a small number of parts.

**A** describes the *default* TTL behavior without `ttl_only_drop_parts = 1`. **C** would only happen with `DROP TABLE`. **D** is not a real ClickHouse mode.

**Mental model:** Time-series TTL should drop *parts*, not *rows* — make sure your partition key aligns with the TTL boundary.

---

## Q3. LowCardinality applicability — **C**

`LowCardinality(String)` builds a column-local dictionary mapping each distinct string to a small integer code, then stores the codes. This compresses extraordinarily well when there are very few distinct values (think `ServiceName`, `SeverityText`, `Region`, `EnvironmentName`). The break-even point is empirically around ~10,000 distinct values; beyond that, the dictionary itself starts dominating storage and lookup overhead.

**A** would defeat the dictionary entirely (each row gets a new code). **B** is a textbook case where you want full text storage plus a skip index. **D** is for `Date`/`DateTime`, not strings — and ClickHouse's native time types are already efficient.

---

## Q4. Null engine purpose — **C**

The `otel_logs (Null engine) → otel_logs_mv → otel_logs_v2 (MergeTree)` pattern is a classic ClickHouse idiom: it lets you accept inserts that match the *raw* OTel schema (which the OTel Collector knows how to write) while persisting only the *transformed* schema you actually want to query (with materialized columns for `GeoCountry`, `BrowserFamily`, `RequestPage`, etc.). Without the `Null` table you'd either have to (a) store the data twice, or (b) make the OTel Collector know about your bespoke target schema, breaking its plug-and-play property.

**A**/**B**/**D** are not the reason. The Collector can write to MergeTree directly — but that means the collector schema and storage schema are coupled, which is exactly what we don't want.

---

## Q5. Where attributes live in OTel — **B**

The OpenTelemetry data model splits attributes into:
- **Resource attributes** — describe the *entity* producing telemetry (host, container, service, k8s pod). Set once at SDK init time and propagated automatically.
- **Span / log / metric attributes** — describe a *specific event*. Vary per signal record.

`service.name`, `service.version`, `host.name`, `cloud.region`, `k8s.pod.name` are all resource attributes. The lab's `ServiceName` column is a top-level promotion of `ResourceAttributes['service.name']` so it can serve as the leading sort-key column.

---

## Q6. Skip indexes for non-prefix filters — **B**

A skip index supplements the primary key with per-granule "this granule cannot contain a match" hints for columns *not* in the order key. `bloom_filter(0.01)` is the standard for high-cardinality columns where you do equality lookups (TraceId, UserId, RequestId).

**A** would actually *hurt* lookup queries — coarser granules mean each "skip" is a bigger miss. **C** is impossible without a full table rebuild (`ORDER BY` is immutable in MergeTree). **D** doesn't change query plans.

**Mental model:** "Sort key for the dominant access pattern; skip indexes for the rest."

---

## Q7. Why dual-write at the collector — **B**

When both backends are populated by the same writer (the collector emits identical batches to ES and CH), parity comparisons are pure storage/query-engine differences. If you instead dual-read at the dashboard, you have to plumb cross-system query orchestration into the dashboard, you can't tell whether differences come from the storage path or the dashboard's own reasoning, and rolling back is more complex (you have to revert dashboard code, not just remove a collector exporter).

**A** is irrelevant. **C** is false (Grafana, HyperDX, even Kibana via remote-clusters can read from multiple sources). **D** is just wrong.

---

## Q8. AggregatingMergeTree purpose — **C**

This one is worth understanding deeply because it's where ClickHouse decisively beats Elasticsearch's transform pattern.

In Elasticsearch transforms, you periodically scan raw events and re-aggregate them into a summary index. The cost grows with raw volume.

In ClickHouse `AggregatingMergeTree`:
- Each insert produces a small *partial state* — e.g., `countState()` is just an integer, `avgState()` is a (sum, count) pair.
- Background merges combine adjacent parts' partial states (the algebraic property of count, sum, etc. makes this trivial; even quantile sketches like t-digest are mergeable).
- Queries combine remaining partial states with `*Merge` aggregates.

The raw rows are **never re-read** after the initial state has been computed. The cost of the rollup is paid at insert time, not at query time, and it doesn't grow with retention.

---

## Q9. Why IP_TRIE dictionaries are fast — **B**

Dictionaries in ClickHouse are first-class citizens that live in collector memory after `SYSTEM RELOAD DICTIONARY`. The `IP_TRIE` layout is specifically a radix-tree (a trie indexed by bit prefix), so a CIDR lookup is `O(prefix-length)` — about 32 bit-comparisons for IPv4. No disk, no scan, no per-row cost beyond the trie walk.

**A** is wrong (dictionaries are exposed via the planner; they just have specialized fast paths). **C** is implausible at MaxMind dataset sizes (~3 MB for country DB). **D** is true but doesn't explain the speed.

---

## Q10. ILM equivalence — **C**

ClickHouse Cloud's storage architecture eliminates most of ILM:
- **No node tiers** — all data lives on object storage with automatic local caching. There's no "hot vs warm" allocation to manage.
- **No rollover** — a single MergeTree table with date-partitioning replaces the rolling-index pattern.
- **No forcemerge schedule** — background merges are continuous and self-tuning.
- **TTL handles delete** — one clause replaces the entire 30-day-delete phase.

This is one of the highest-impact talking points in any ES → CH conversation. The customer's Ops complexity drops by an order of magnitude.

---

## Q11. Why filter to `SpanKind = 'Server'` — **B**

This was the gotcha you encountered in Exercise 1. The OTel demo's `flagd` long-poll EventStream spans last ~10 minutes by design — they're streaming connection bookkeeping, not user-facing requests. They dominate any naive "top 10 slowest" query.

In production you'll see the same pattern in any system with long-lived RPCs (SignalR, gRPC server streaming, GraphQL subscriptions, MQTT keep-alives). Always filter to either `SpanKind = 'Server'` (incoming requests) or to a specific `SpanName` pattern when looking for user-impactful latency.

**Mental model:** Server-kind spans = "what a user waited for". Internal/Client/Producer/Consumer = "what happened underneath".

---

## Q12. Indexing for full-text search — **C**

The `text` index is the modern, recommended approach. It went GA in ClickHouse 26.2 and is what [the lab's own `otel_logs_v2` schema](../../part3/clickhouse/schema.sql) uses on the `Body` column:

```sql
INDEX idx_body Body TYPE text(tokenizer='sparseGrams') GRANULARITY 8
```

Key properties of `text`:
- **Configurable tokenizer** — `tokens` (whitespace), `ngrams(N)` (substring), `sparseGrams` (smart variable-length tokens, lab default), or a custom regex.
- **Better precision than `tokenbf_v1`** — instead of bloom-filter approximation per granule, the `text` index keeps a real per-token postings list. False-positive rate at query time is ~0 vs. tokenbf_v1's `~0.025` default.
- **Proper case handling and CJK support** in newer tokenizers — tokenbf_v1 had no notion of Unicode segmentation.

**B** (`tokenbf_v1`) still works and is the right answer on CH < 26.2 — the question deliberately asks for the *modern* recommendation. Many existing production deployments still use tokenbf_v1; both indexes can coexist on the same column during a rollover. **A** (`minmax`) tracks per-granule min/max — useful for monotonic numeric/date columns, not text. **D** (untokenized `bloom_filter`) treats the whole string as one token; it only helps for whole-string equality (e.g., `WHERE Body = 'an exact full string'`), never substring matches.

**Mental model:** for substring/full-text on `String` columns: **`text` if you're on 26.2+, `tokenbf_v1` if you're on an older cluster**. For exact equality on a high-cardinality string column (e.g., TraceId): **`bloom_filter`**.

---

## Q13. Why ClickHouse doesn't ship an ES-style inverted index — **B**

The framing is **"different design point"**, not "missing feature". Both engines support full-text predicates; they're optimized for different access patterns.

### Elasticsearch's inverted index

Built on Lucene; it is the *primary* access path for text:

- **Granularity:** per-document. The index stores `token → posting list of (doc_id, term_frequency, positions)` for every doc containing the token.
- **Purpose:** find documents matching a query. Returns the actual matching docs, not a candidate set.
- **Ranking is built-in:** BM25 / TF-IDF scoring during the lookup; results are pre-ranked by relevance.
- **Mandatory by default:** every `text` field gets one — you can't have a text field without paying the index cost.
- **Storage cost:** typically 30–50% of the original document size.
- **Rich query semantics:** phrase-with-slop, fuzzy matching with Levenshtein distance, multi-field boosting, scoring DSL — all enabled by per-document granularity.

### ClickHouse's `text` index (GA in 26.2)

A *skip index* layered on columnar storage; not the primary access path:

- **Granularity:** per-granule (8192 rows by default). Tells the engine "this granule **might** contain rows with token X" — does not identify the specific matching rows.
- **Purpose:** prune granules from a scan. After pruning, ClickHouse reads candidate granules and applies the `LIKE` predicate row-by-row.
- **No relevance ranking.** Results return in storage order. No BM25, no TF-IDF, no "more relevant than".
- **Optional:** opt-in DDL on a single column. Tables without it just scan the column.
- **Storage cost:** ~1–5% of the column's size — one posting list per granule, not per row.
- **Query semantics:** SQL `LIKE`, `ILIKE`, `hasToken`, regex. No phrase-with-slop, no fuzzy, no scoring DSL.

### Walking through `WHERE Body LIKE '%timeout%'` on 1 B rows

| Step | Elasticsearch | ClickHouse |
|---|---|---|
| 1 | Tokenize `'timeout'` → `[timeout]` | Same |
| 2 | Look up token in the inverted index → posting list of doc IDs (say 2.3 M docs) | Walk the skip index — say 5 K granules of 122 K may contain `timeout`; skip the other 117 K |
| 3 | Fetch each matching doc, score it, return ranked results | Read columns from the 5 K candidate granules (~41 M rows) |
| 4 | (none) | Apply `LIKE '%timeout%'` row-by-row, get ~2.3 M matches |
| **Cost dominator** | Posting-list traversal | Row-scan within candidate granules (still ~24× less data than no index) |

### When each wins

| Use case | Better fit | Why |
|---|---|---|
| E-commerce product search ("running shoes $40–80") | **ES** | Relevance ranking is core; queries return small ranked sets |
| Documentation search-as-you-type | **ES** | Fuzzy matching, phrase queries, autocomplete |
| Log analytics ("5xx errors mentioning 'timeout' last hour") | **CH** | Filter-then-aggregate over billions of rows; ranking irrelevant |
| Security analytics ("alert on string 'malicious_pattern'") | **CH** | Scan-with-prune is a great fit |
| "Top-100 most-relevant logs given a free-text query" | **ES** | CH has no scoring — you'd write your own ranking SQL |
| "Sum bytes_transferred where path matches `/api/.*` over 90 d" | **CH** | Aggregation dominates; text predicate is just a filter |

### The customer-conversation summary

Both systems can find lines containing `'timeout'`. The *next* query reveals the design-point difference:

- After ES finds matches, the natural follow-up is **"rank by relevance"**.
- After ClickHouse finds matches, the natural follow-up is **"now compute p99 latency grouped by service for these rows"**.

The same physical data layout cannot be optimal for both questions. ES picks document-oriented + ranking; ClickHouse picks columnar + analytics.

---

## Q14. Schema-evolution strategy — **B**

This is straight from the Part 2 ADR Decision 3 model answer:

- **Map default** preserves flexibility — any new attribute key just lands in `LogAttributes` without DDL.
- **Promote hot keys** to dedicated columns when usage justifies the schema change (rule of thumb: ≥30% of rows reference the key, ≥2 dashboards/alerts use it).
- The promotion is non-destructive: `ALTER TABLE ... ADD COLUMN MATERIALIZED LogAttributes['X']` lets new and old rows coexist.

**A** (JSON column with no promotion) gives flexibility but no fast path; benchmarks show ~2× slower than Map at this lab's volume. **C** ("one column per possible attribute") fails as soon as a service emits a new attribute. **D** is not a migration.

---

## Q15. Sampling trade-off — **B**

Tail-based sampling defers the keep/drop decision until after all spans of a trace have been received, which means it can use trace-level signals (any error span? any high-latency span?) to decide. The OpenTelemetry `tail_sampling_processor` is the canonical implementation; the cost is buffering and complexity in the collector.

**A** (head-based) decides at trace start, before any error has happened — incompatible with "keep all error traces".
**C** (per-span) breaks trace coherence (you'd lose half a trace's spans).
**D** (no sampling) is rarely viable at 1 M traces/s.

The right design point in production is usually a tail sampler with rules like "keep 100% of error traces, 5% of slow-but-OK traces, 0.1% of fast OK traces".

---

## Scoring guidance

| Score | What it means |
|---|---|
| **14–15** | You've internalized both ClickHouse mechanics and OTel. You're ready to drive a customer migration. |
| **11–13** | Pass — but re-read the explanations for the questions you missed. Schema and data-model questions are the most predictive of customer-conversation success. |
| **8–10** | Borderline. Re-read [Part 2's solutions](../../part2/solutions/) — those exercises directly cover most of these concepts. |
| **≤ 7** | Re-do the relevant teaching points. You'll find the open-ended exercises hard otherwise. |
