# Part 4 — Open-Ended Exercises

Five exercises, recommended timeboxes given. Each one tests whether you can apply the lab's reasoning to a customer scenario you haven't seen before. Write actual SQL / actual prose — not bullet lists of buzzwords. After completing all five, self-grade against [`solutions/open-answers.md`](../solutions/open-answers.md).

---

## Exercise 1 — Schema design (15 minutes)

A new workload arrives in your ClickHouse Cloud service: **e-commerce clickstream events**.

**Schema (raw input):**

| Field | Type | Cardinality / shape |
|---|---|---|
| `event_id` | UUID v4 | Globally unique per row |
| `event_time` | timestamp, millisecond precision | Monotonic-ish |
| `user_id` | UUID | ~50 M distinct values, long-tail |
| `session_id` | UUID | ~500 M / day |
| `product_id` | string | ~2 M distinct |
| `event_type` | enum | 12 values: `view`, `add_to_cart`, `remove_from_cart`, `purchase`, `wishlist_add`, `search`, `category_browse`, `checkout_start`, `checkout_complete`, `signup`, `login`, `logout` |
| `value_usd` | decimal | only set for purchase / refund events |
| `attributes` | object | up to ~30 keys, schema evolves over time (new product flags, A/B test buckets, …) |

**Volume:** 5 B events/day. **Retention:** 90 days hot, archive after.

**Top three queries the team runs daily:**

1. Funnel analysis — count of distinct users that fired `view` then `add_to_cart` then `purchase` for a given `product_id` over the last 7 days.
2. Per-user activity — all events for a single `user_id` over the last 7 days, ordered by time.
3. Revenue dashboard — `sum(value_usd) WHERE event_type = 'purchase'` grouped by minute, last 24 hours.

**Your task:** write the `CREATE TABLE` statement (or two tables, if you choose to denormalize). Justify each of these choices in 1–2 sentences each:

- The choice of types for `user_id`, `event_type`, `attributes`
- `ORDER BY` columns and ordering rationale
- `PARTITION BY` choice
- TTL clause
- Any skip indexes
- Whether you'd use `AggregatingMergeTree` for query #3

Write your answer in the `___` blocks below.

```sql
-- Your CREATE TABLE here:

```

**Rationale:**
- **Type choices:** ___
- **ORDER BY:** ___
- **PARTITION BY:** ___
- **TTL:** ___
- **Skip indexes:** ___
- **AggregatingMergeTree?:** ___

---

## Exercise 2 — Migration plan (20 minutes)

A retail customer's current observability stack:

- **12-node Elasticsearch cluster**, 50 TB hot data, 4 PB cold (S3 snapshots), 3-node coordinator tier.
- **Filebeat on every host** (~3,000 hosts).
- **Logstash cluster** in front of ES (5 nodes), running 60+ grok patterns to enrich incoming logs.
- **Kibana** for ops dashboards — 300+ saved searches, 20+ dashboards, 5 alerting rules.
- **ILM**: hot 7 d, warm 21 d, cold 90 d, snapshot to S3 thereafter, retain in S3 for 1 year (compliance).
- **Volume**: 200 K events/sec sustained, 1 M peak.
- **Annual cost**: ~$1.2 M ES infra + ingest, ~$400 K cold-storage S3.

**Your task:** outline a **6-month migration plan** to ClickHouse Cloud. Address each of the following in 2–4 sentences. Avoid generic platitudes — be specific about what tool, what config, what cutover signal.

1. **Phasing** — break the 6 months into named phases, each with an exit criterion. (Aim for 4–6 phases.)
2. **Schema strategy for the 60+ grok-extracted fields** — Map vs structured columns? Promotion criteria? What do you do at month 1 vs month 6?
3. **Logstash decision** — keep, replace with OTel Collector, replace with Vector, or hybrid? Justify.
4. **Compliance retention** — CH Cloud has no native S3-snapshot ILM. How do you meet the 1-year compliance requirement?
5. **Rollback plan** — if cutover fails 3 weeks in (already migrated dashboards, ingest dual-flowing), what's the recovery path?
6. **Top 3 risks and mitigations** — pick the 3 you'd flag at the kick-off meeting; for each, name the mitigation in one sentence.

Write your answer in prose below — bullet structure is fine, but each bullet should be a complete thought, not a tag.

> Your plan: ___

---

## Exercise 3 — Debugging scenario A (10 minutes)

A partner reports:

> *"After we migrated, my Top-N request-paths query returns the same numbers as ES — but the **p95 latency** column is wrong. ClickHouse shows ~120 ms, ES showed ~100 ms. Same time window, same filter."*

**Your task:** identify the **3 most likely root causes** and, for each one, describe the specific check you'd run to confirm or rule it out. Order them by likelihood.

> Cause 1: ___
> Check: ___
>
> Cause 2: ___
> Check: ___
>
> Cause 3: ___
> Check: ___

---

## Exercise 4 — Debugging scenario B (10 minutes)

A partner reports:

> *"I added a new attribute, `request_id`, to my application logs. It correctly shows up in `LogAttributes`, but my dashboard query `WHERE LogAttributes['request_id'] = 'abc123'` is taking 10 seconds against a 100 M-row table. The exact same query against `WHERE TraceId = 'def456'` returns in ~50 ms. What's wrong?"*

**Your task:**

1. **Diagnose the root cause** in 2–3 sentences.
2. **Propose two fixes — one quick, one durable.** For each, name the specific DDL or config change and explain its trade-offs.

> Diagnosis: ___
>
> Quick fix: ___
> Trade-off: ___
>
> Durable fix: ___
> Trade-off: ___

---

## Exercise 5 — Trade-off analysis (10 minutes)

You're advising a customer on **two alerting designs** for the same rule: "5xx error rate > 5% over a 5-minute window".

**Design A — query-time aggregation:**
An external poller queries ClickHouse every 60 seconds with:
```sql
SELECT countIf(StatusCode >= 500) / count()
FROM otel_logs_v2
WHERE TimestampTime > now() - INTERVAL 5 MINUTE
```
The poller compares the result to `0.05` and fires the alert in its own runtime.

**Design B — pre-aggregated MV:**
A `MATERIALIZED VIEW` writes one row per minute into `alert_error_rate (minute, error_rate)` as logs are ingested. The poller queries:
```sql
SELECT minute, error_rate
FROM alert_error_rate
WHERE minute > now() - INTERVAL 5 MINUTE
  AND error_rate > 0.05
```
and fires if any rows are returned.

**Your task:** fill in the comparison table. For each cell, write 1 sentence with concrete reasoning (not just "lower" / "higher").

| Property | Design A | Design B |
|---|---|---|
| Read cost per evaluation (CPU, IO) | ___ | ___ |
| Write cost per ingested row | ___ | ___ |
| Update lag (alert latency from real spike) | ___ | ___ |
| Behavior at 100 services × 50 endpoints (cardinality scaling) | ___ | ___ |

**Final recommendation at 1 M events/sec — which design wins, and why?**

> Recommendation: ___
> Rationale: ___

---

## When you're done

Open [`solutions/open-answers.md`](../solutions/open-answers.md) and self-grade. **Note specifically where you disagreed with the model answer** — those are the most valuable disagreements to discuss with a peer or the lab author.

> **Self-graded score** (out of 5 — count an exercise as "passed" if your answer reaches a similar conclusion via similar reasoning, even if the wording differs): ____ / 5
