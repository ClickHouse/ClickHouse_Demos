# Part 4 — Multiple-Choice Questions

15 questions. Pick one answer for each. Pass: **≥ 11 correct**.

Write your answer letter (`A` / `B` / `C` / `D`) on the line below each question. Do **not** consult `solutions/mcq-answers.md` until you've answered all 15.

---

## Q1. Sort-key prefix matching

A `MergeTree` table is `ORDER BY (ServiceName, SeverityText, TimestampTime)`. Which query benefits **most** from this primary key?

- **A.** `SELECT count() FROM logs WHERE TimestampTime > now() - INTERVAL 1 HOUR`
- **B.** `SELECT count() FROM logs WHERE ServiceName = 'api' AND SeverityText = 'ERROR' AND TimestampTime > now() - INTERVAL 1 HOUR`
- **C.** `SELECT count() FROM logs WHERE Body LIKE '%timeout%'`
- **D.** `SELECT count() FROM logs WHERE LogLevel = 'WARN'`

**Your answer:** `___`

---

## Q2. TTL drop granularity

A table has `TTL TimestampDate + INTERVAL 30 DAY DELETE SETTINGS ttl_only_drop_parts = 1`. When data ages past 30 days, what does ClickHouse physically remove?

- **A.** Individual rows, scanned and rewritten in place
- **B.** MergeTree parts (file-system files) whose entire date range has expired
- **C.** Whole tables, recreated empty
- **D.** Just the index entries; the data files stay on disk

**Your answer:** `___`

---

## Q3. LowCardinality applicability

For which column is `LowCardinality(String)` likely to give the **biggest** compression and query benefit?

- **A.** A randomly-generated UUID column (essentially unique per row)
- **B.** The free-text body of every log message
- **C.** A `ServiceName` column with ~30 distinct values across hundreds of millions of rows
- **D.** A `Timestamp` column

**Your answer:** `___`

---

## Q4. Null engine purpose

The lab's `otel_logs` table uses the `Null` engine, with a Materialized View routing transformed rows into `otel_logs_v2`. Why?

- **A.** To prevent disk corruption during ingestion
- **B.** Because the OTel Collector cannot write to MergeTree directly
- **C.** To avoid storing the same data twice — the OTel Collector writes the raw OTel schema once into the discarding `Null` table, and only the enriched, optimized rows land in `otel_logs_v2`
- **D.** To provide a separate replication target for high availability

**Your answer:** `___`

---

## Q5. Where attributes live in OTel

In the OpenTelemetry data model, `service.name` is most naturally represented as:

- **A.** A column on every signal record (one per log line, one per span, one per metric data point)
- **B.** A **resource attribute** — declared once per process, propagated with every signal that process emits
- **C.** A span-level attribute that exists only on traces
- **D.** A static value baked into the OTel Collector config

**Your answer:** `___`

---

## Q6. Skip indexes for non-prefix filters

A query filters by `TraceId = 'abc123'` against a 100M-row table that's `ORDER BY (ServiceName, SeverityText, TimestampTime)`. Which feature speeds up this point lookup without redesigning the primary key?

- **A.** Increasing `index_granularity` (default 8192) to 65536
- **B.** A `bloom_filter` skip index on `TraceId`
- **C.** Adding `TraceId` to the leading position of `ORDER BY` (no rebuild needed)
- **D.** Running `OPTIMIZE TABLE ... FINAL`

**Your answer:** `___`

---

## Q7. Why dual-write at the collector

During a parallel-run migration, why is **dual-write at the OTel Collector** a more common pattern than **dual-read at the dashboard layer**?

- **A.** The collector is faster than dashboards
- **B.** Dual-write produces identical inputs to both backends, so subsequent parity comparisons are like-for-like; dual-read pushes correlation logic into clients and obscures whether differences come from storage or query layers
- **C.** Dashboards literally cannot connect to two data sources at once
- **D.** Elasticsearch does not accept HTTP writes

**Your answer:** `___`

---

## Q8. AggregatingMergeTree purpose

What is the fundamental advantage of `AggregatingMergeTree` with `countState` / `avgState` aggregates and `countMerge` / `avgMerge` queries, compared to running `GROUP BY` over a raw events table on every dashboard load?

- **A.** It supports transactional inserts
- **B.** It exclusively uses one CPU core, reducing contention
- **C.** It accumulates partial aggregation states incrementally — every insert produces a tiny state, background merges combine states, and queries combine the partial states again. The raw rows are never re-read at query time
- **D.** It applies bloom filters automatically

**Your answer:** `___`

---

## Q9. Why IP_TRIE dictionaries are fast

`dictGet('country_dict', 'name', toIPv4(ip))` returns in microseconds even for billions of rows. The primary reason:

- **A.** Dictionaries bypass the query planner
- **B.** The `IP_TRIE` layout is a radix-tree-like structure purpose-built for longest-prefix CIDR lookups, and dictionaries are kept in process memory rather than on storage
- **C.** The dictionary fully fits in CPU L1 cache
- **D.** The IPv4 address is compared as a 32-bit integer

**Your answer:** `___`

---

## Q10. ILM equivalence

In Elasticsearch you ran a 3-phase ILM policy: hot (5 GB or 1 d rollover) → warm (shrink + forcemerge at 7 d) → delete (30 d). The closest faithful equivalent in ClickHouse Cloud is:

- **A.** Replicate to a secondary cluster with read-only replicas
- **B.** `ALTER TABLE ... MOVE PARTITION TO DISK 'cold'` on a cron schedule
- **C.** A single `TTL TimestampDate + INTERVAL 30 DAY DELETE SETTINGS ttl_only_drop_parts = 1` clause; rollover and forcemerge are handled automatically by background merges, and there are no node tiers to allocate to
- **D.** Schedule `OPTIMIZE TABLE ... FINAL` every hour and rely on `system.parts` cleanup

**Your answer:** `___`

---

## Q11. Why filter to `SpanKind = 'Server'`

When asking "what are the slowest user-facing requests?" against an OTel-instrumented application, why is `WHERE SpanKind = 'Server'` (or equivalent) essential?

- **A.** Server spans have higher priority in tracing
- **B.** Without it, long-lived bookkeeping spans — gRPC streaming RPCs, queue consumers, idle keep-alives — dominate the top of the duration distribution and have no associated business logs
- **C.** Client and Internal spans are not stored in `otel_traces`
- **D.** It's the only valid value for `SpanKind` in the OTel collector's ClickHouse exporter

**Your answer:** `___`

---

## Q12. Indexing for full-text search

You want a skip index that accelerates `WHERE Body LIKE '%timeout%'` (or `Body ILIKE '%timeout%'`) over a 1 B-row `String` column. Which index type is the **modern** ClickHouse recommendation?

- **A.** `minmax` — tracks the per-granule min/max
- **B.** `tokenbf_v1(...)` — tokenized bloom filter; splits the string into words and maintains a per-granule bloom (legacy approach, still works)
- **C.** `text(tokenizer = 'sparseGrams')` — the GA full-text index (`enable_full_text_index = 1` on CH < 26.2; native on 26.2+) with a configurable tokenizer
- **D.** `bloom_filter(0.01)` — untokenized bloom on the full string

**Your answer:** `___`

---

## Q13. Why ClickHouse doesn't ship an ES-style inverted index

A customer asks: "Why doesn't ClickHouse have inverted indexes like Elasticsearch?" The most accurate technical answer:

- **A.** It does — `tokenbf_v1` is the same internal data structure under a different name
- **B.** ClickHouse is column-oriented and optimized for analytical scans plus skip indexes; it supports text search via `text` indexes (GA in CH 26.2) and `tokenbf_v1` skip indexes, but the engine's design point is sequential reads with predicate push-down, not per-document inverted lookups
- **C.** Inverted indexes don't compress well in any engine
- **D.** ClickHouse lacks a `String` data type

**Your answer:** `___`

---

## Q14. Schema-evolution strategy

A customer wants schema flexibility (new attribute keys can appear at runtime without redeploys) **and** fast queries on common attributes. Which strategy best fits ClickHouse?

- **A.** Use a single `JSON` column for all attributes; never promote anything
- **B.** Default unknown keys into a `Map(LowCardinality(String), String)` column; periodically (e.g., per-sprint) promote frequently-accessed keys to dedicated columns via `ALTER TABLE ... ADD COLUMN x String MATERIALIZED LogAttributes['x']`
- **C.** Pre-create one nullable column for every possible attribute, even if NULL most of the time
- **D.** Don't migrate — keep using Elasticsearch's dynamic mapping

**Your answer:** `___`

---

## Q15. Sampling trade-off

You're advising a customer on sampling 1 M traces/sec. The customer wants to keep all traces that contain an error span. Which sampling strategy supports this requirement?

- **A.** Head-based sampling at the collector: e.g., `probabilistic_sampler` with rate 0.1 — drops 90% of traces uniformly at the start of each trace
- **B.** Tail-based sampling at the collector: e.g., `tail_sampling_processor` keeps 100% of traces marked `STATUS_CODE_ERROR` and samples successful traces at a lower rate, deciding only after all spans of a trace have been received
- **C.** Span-based sampling: drop spans whose `Duration < 10 ms`
- **D.** No sampling — store everything regardless of volume

**Your answer:** `___`

---

## Submission

Once you've answered all 15, count your `A/B/C/D` letters against [`solutions/mcq-answers.md`](../solutions/mcq-answers.md). Write your score below before moving on.

> **My MCQ score:** ____ / 15
