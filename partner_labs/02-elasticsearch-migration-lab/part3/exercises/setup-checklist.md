# Exercise 3A — Setup Checklist

Work through each step in the README and check it off as you go. Fill in the blanks where indicated — these become your migration runbook.

---

## Step 1: Provision ClickHouse Cloud

- [ ] Created a ClickHouse Cloud service (Basic tier)
- [ ] Noted connection details:
  - Host: `_____________________________________________`
  - Port: `9440` (native TLS)
  - User: `default`
- [ ] Tested connectivity with `clickhouse client`:

  ```bash
  clickhouse client --host _____________ --port 9440 \
      --user default --password __________ --secure
  ```

  Result: `CONNECTED / ERROR` (circle one)

---

## Step 2: Create Target Tables

**All Part 3 objects live in the `otel` database** — created automatically by `dictionaries.sql` via `CREATE DATABASE IF NOT EXISTS otel; USE otel;`. Every collector config and validation script in this part is preset to point at it.

**Dictionaries first, then tables** — `otel_logs_v2` references `otel.geoip_country`/`otel.geoip_city` at CREATE time.

- [ ] Ran `clickhouse/dictionaries.sql` (creates the `otel` database, `geoip_data` table, and empty dictionaries)
- [ ] Loaded GeoIP CSV: `clickhouse client --database otel --query "INSERT INTO geoip_data FORMAT CSV" < clickhouse/geoip-sample-data.csv`
- [ ] Reloaded dictionaries: `SYSTEM RELOAD DICTIONARY otel.geoip_country` and `SYSTEM RELOAD DICTIONARY otel.geoip_city`
  - [ ] `geoip_country` dictionary status = `LOADED` (in database `otel`)
  - [ ] `geoip_city` dictionary status = `LOADED` (in database `otel`)

- [ ] Ran `clickhouse/schema.sql` — all 9 objects created:
  - [ ] `otel_logs` (Null engine)
  - [ ] `otel_logs_v2` (MergeTree with materialized columns)
  - [ ] `otel_logs_mv` (Materialized View)
  - [ ] `otel_traces` (MergeTree)
  - [ ] `otel_metrics_gauge` (MergeTree)
  - [ ] `otel_metrics_sum` (MergeTree)
  - [ ] `otel_metrics_histogram` (MergeTree)
  - [ ] `otel_metrics_exponentialhistogram` (MergeTree)
  - [ ] `otel_metrics_summary` (MergeTree)

**Question:** Why do we use a `Null` engine for `otel_logs` instead of writing directly into `otel_logs_v2`?

> Your answer: _______________________________________________

- [ ] Verified dictionary is working:
  ```sql
  SELECT dictGet('otel.geoip_country', 'country', toIPv4('8.8.8.8'));
  -- Expected: 'United States'
  ```
  Actual result: `_____________________________`

- [ ] Ran `clickhouse/alert-tables.sql`:
  - [ ] `alert_error_rate` + `alert_error_rate_mv`
  - [ ] `logs_summary_1min` + `logs_summary_1min_mv`

---

## Step 3: Deploy OTel Collector

- [ ] Stopped Filebeat (so the OTel Collector becomes the single producer for `logs-*-lab` ES data streams — both Filebeat and the collector would otherwise write to the same data streams, doubling ES doc counts):
  ```bash
  docker compose -f ../part1/docker/docker-compose.source.yml stop filebeat
  ```

- [ ] Started the OTel Collector as a Docker container (recommended on macOS):
  ```bash
  source ../common/env.sh
  docker run -d \
    --name otelcol-lab \
    --restart unless-stopped \
    --network docker_default \
    -v docker_log-data:/var/log/generators:ro \
    -v "$(pwd)/configs/otel-collector-config.parallel.yaml:/etc/otelcol-contrib/config.yaml:ro" \
    -e CH_HOST="${CH_HOST}" \
    -e CH_PASSWORD="${CH_PASSWORD}" \
    otel/opentelemetry-collector-contrib:0.146.1
  ```
  - `--restart unless-stopped` recovers from CH Cloud cold-start timeouts on first launch

- [ ] Confirmed logs are flowing — healthy startup in `docker logs otelcol-lab --tail=10`

- [ ] Swapped the OTel Demo collector to parallel-run config (dual-write to APM + ClickHouse):
  ```bash
  bash scripts/swap-otelcol-demo-config.sh parallel
  ```
  - [ ] `docker logs docker-otelcol-demo-1 --tail=10` shows "Everything is ready. Begin running and processing data."
  - [ ] No "Failed to start component" or "connection refused" errors

---

## Step 4: Validate the Parallel Run

- [ ] Waited at least **5 minutes** for data to accumulate

- [ ] Ran `scripts/validate_migration.sh`:
  ```bash
  source ../common/env.sh
  bash scripts/validate_migration.sh
  ```

  Result: `PASSED / PASSED WITH WARNINGS / FAILED` (circle one)

- [ ] Row count comparison (fill in):

  | Stream | ES Count | CH Count | Difference % |
  |--------|----------|----------|-------------|
  | Web access | ________ | ________ | ________% |
  | Application | ________ | ________ | ________% |
  | Infrastructure | ________ | ________ | ________% |

- [ ] GeoCountry enrichment coverage: `________%` (target: ≥ 20% with sample data; full MaxMind dataset gives >90%)

- [ ] Ran `scripts/validate_enrichment.sh` and reviewed output

**Sample enriched row** (paste one row from the spot-check query):

```
RemoteAddr: _____________  GeoCountry: _____________  GeoCity: _____________
BrowserFamily: _____________  OSFamily: _____________  IsBot: _
```

---

## Step 5: Explore Your Data in HyperDX (ClickStack UI)

Follow [`hyperdx-guide.md`](../hyperdx-guide.md) for the full step-by-step with screenshots.

- [ ] **A. Launched ClickStack** from the ClickHouse Cloud sidebar; HyperDX opens in a new tab
- [ ] **B. Created the `Traces` source** (Source Data Type: Trace, Database: `otel`, Table: `otel_traces`)
- [ ] **B. Created the `log` source** (Source Data Type: Log, Database: `otel`, Table: `otel_logs_v2`, Timestamp Column: `TimestampTime`)
- [ ] **B. Created the `otel_metrics` source** (Source Data Type: OTEL Metrics, Database: `otel`, Gauge/Histogram/Sum/Summary/ExponentialHistogram tables wired up, Correlated Log Source: `log`)
- [ ] **C. Search view** with the `log` source shows a populated histogram + table for "Last 1 hour"; clicking a service in the facets sidebar filters the results
- [ ] **D. AI Assistant** (Chart Explorer → AI Assistant [A] toggle) successfully translates `Error count by services for past 2 hours` into a working Line/Bar chart

**Observation:** Pick one HyperDX feature you used (full-text search, Service Map, AI Assistant, Notebooks) and explain why the equivalent would be impossible or impractical in Kibana.

> Feature: `_____________________________`
>
> Kibana limitation: _______________________________________________

---

## Step 6: Verify Data Lifecycle (TTL)

- [ ] Confirmed TTL is configured on all tables:
  ```sql
  SELECT name, extractAll(create_table_query, 'TTL[^\\n]+') AS ttl_clauses
  FROM system.tables
  WHERE database = 'otel'
    AND name IN ('otel_logs_v2', 'otel_traces',
                 'otel_metrics_gauge', 'otel_metrics_sum', 'otel_metrics_histogram',
                 'otel_metrics_exponentialhistogram', 'otel_metrics_summary');
  ```

  TTL expression for `otel_logs_v2`: `_______________________________________________`

**Question:** In ES, you had a 3-phase ILM policy (hot → warm → delete). In ClickHouse Cloud, what replaces it and why is the hot/warm distinction unnecessary?

> Your answer: _______________________________________________

---

## Step 7: Aggregation Summary Table

- [ ] Confirmed `logs_summary_1min_mv` is running (rows accumulating):
  ```sql
  SELECT count() FROM logs_summary_1min;
  ```
  Row count: `____________`

- [ ] Queried summary using `-Merge` combinators and saw results

**Question:** What does `countMerge()` do, and why must you use it instead of `count()` when querying an `AggregatingMergeTree` table?

> Your answer: _______________________________________________

---

## Step 8: Alerting Migration

Choose one:

**Option A: HyperDX Alerts** — follow the [official ClickStack Alerts docs](https://clickhouse.com/docs/use-cases/observability/clickstack/alerts) for the exact UI flow
- [ ] Saved search `web-5xx-errors` created on the `log` source — search criteria: `RequestType:* AND StatusCode:>=500`
- [ ] Alert 1 (High Error Rate) wired to that saved search, condition `count() > 0` over a 5-minute window — or a chart-backed alert on the ratio with threshold `> 0.05`
- [ ] At least one per-service heartbeat alert (e.g. `heartbeat-payment-service`) created — search criteria: `ServiceName:"<service>"`, condition `count() == 0` over 3 minutes
- [ ] Both alerts visible in the **Alerts** view with status `Normal` (not firing)

**Option B: Pre-computed alert table**
- [ ] `alert_error_rate` is populating (confirmed in Step 2)
- [ ] Wrote a manual poll query:
  ```sql
  -- Paste your alert poll query here:
  
  ```

---

## Step 9: SQL Exercises

Complete the 6 SQL exercises in `exercises/sql-exercises.md`.

- [ ] Exercise 1 (Cross-signal JOIN) — ran successfully
- [ ] Exercise 2 (LAG window function) — ran successfully
- [ ] Exercise 3 (Unbounded GROUP BY) — ran successfully
- [ ] Exercise 4 (sequenceMatch) — ran successfully
- [ ] Exercise 5 (-If combinators) — ran successfully
- [ ] Exercise 6 (CTE root cause) — ran successfully

**Most surprising result:** _______________________________________________

---

## Step 10: Decommission Elasticsearch

Only proceed once `validate_migration.sh` passes and you're confident in the data.

- [ ] Swapped the file-based collector from parallel-run to cutover config:
  ```bash
  docker stop otelcol-lab && docker rm otelcol-lab
  # then re-run the docker run from Step 3b but mount otel-collector-config.cutover.yaml instead
  ```
  - Reuses the existing `configs/otel-collector-config.cutover.yaml` (CH only); no in-place edits required.
- [ ] Ran `validate_migration.sh` again — all ClickHouse checks still pass
- [ ] Stopped ES/Kibana/APM containers:
  ```bash
  docker compose -f ../part1/docker/docker-compose.source.yml stop elasticsearch kibana elastic-apm-server filebeat
  ```
- [ ] Swapped OTel Demo collector to cutover config (ClickHouse only):
  ```bash
  bash scripts/swap-otelcol-demo-config.sh cutover
  ```
- [ ] Verified metrics still flowing: `SELECT table, count() FROM system.parts WHERE table LIKE 'otel_metrics%' AND active GROUP BY table`

---

## Final Sign-off

| Check | Status |
|-------|--------|
| Data flowing into ClickHouse (`otel_logs_v2`) | ✅ / ❌ |
| GeoIP dictionary loaded and enriching rows | ✅ / ❌ |
| 4 HyperDX dashboards populating | ✅ / ❌ |
| `validate_migration.sh` passing | ✅ / ❌ |
| TTL configured on all tables | ✅ / ❌ |
| `logs_summary_1min` accumulating | ✅ / ❌ |
| At least one alerting rule active | ✅ / ❌ |
| All 6 SQL exercises completed | ✅ / ❌ |
