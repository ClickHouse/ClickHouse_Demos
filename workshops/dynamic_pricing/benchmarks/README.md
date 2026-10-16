# Measure the pricing data path

Run from the lab directory after loading and reconciling a verified manifest:

```bash
.venv/bin/workshop benchmark --manifest "$PRICING_DATASET_MANIFEST" --repetitions 10
```

Supply the same manifest you loaded. The command verifies its contents against the active immutable import. It fixes store, SKU, physical table IDs, import-run ID, and a complete-hour cutoff. Every raw and rollup query must return identical row counts, net units, receipts, and sold costs. A mismatch stops the comparison.

The runner alternates raw and rollup order, performs two warmups each, and records 3–100 samples per query at concurrency one. The query result cache is disabled. Storage caches are not flushed, and the query-condition cache remains at the server default. This is a warm workload; do not label it a cold-cache benchmark.

Each sample records the actual query ID, client elapsed time, and available HTTP read counters. A bounded query-log lookup records server duration, read rows, read bytes, and memory. Cloud uses all replicas; missing access or eventual-log visibility is labeled. Missing counters remain null. See the [ClickHouse Cloud system-table guide](https://clickhouse.com/docs/cloud/monitoring/system-tables).

The manifest's total row count differs from the rows selected for one SKU/store. Report both. Import measurements cover table creation and manifested loads; verification time is separate. They exclude manifest download/checksum verification before the load starts. Older imports without measurements remain unmeasured; reload only if you need an import measurement.

Client elapsed time uses a monotonic clock. The runner reports median/minimum/maximum and sample count. Fewer than 30 observations have no p95 value. With 30 or more, the p95 is the observed nearest-rank statistic, not a product guarantee.

## Measure one live event

This option creates one synthetic source sale, waits for sale and stock revision visibility, captures a fresh snapshot, and runs the prepared simulator. It does not apply a price:

```bash
.venv/bin/workshop benchmark --manifest "$PRICING_DATASET_MANIFEST" \
  --repetitions 10 --probe-freshness
```

The source transaction, post-commit visibility wait, feature capture, recommendation, and total emission-to-candidate times use one process's monotonic clock. Source acknowledgement and visible-at timestamps use real wall time. The simulated scenario cutoff is business time and never supplies an elapsed duration. A ClickPipes sync interval is a configuration value, not an observed latency guarantee.

## Keep reproducible evidence

Results are saved under `benchmarks/results/private/`. They include exact queries, fixed parameters, dependency-lock hash, ClickHouse/Python versions, and configured ingestion mode. Unverified Cloud sizing and hardware stay null. Before sharing a Cloud benchmark, add the actual service sizing, region, replication cadence, and rehearsal evidence. Local fixture results do not establish Cloud ingestion performance or business uplift.

## Local showcase evidence

`results/local-showcase.json` records an actual 100M-row local Docker import with independently reconciled totals. The import took 880.18 seconds, excluding manifest checks. The selected SKU/store cohort contains 24,998,408 rows. Across 30 warm samples, raw-query median was 66.84 ms and rollup median was 3.37 ms. Observed nearest-rank p95 values were 84.31 ms and 4.74 ms. Query IDs and server read/memory counters are retained.

This uses the local file/client ingestion path, concurrency one, disabled query-result cache, and unflushed storage caches. It does not measure server-side S3 loading, managed CDC, or Cloud service capacity. Hardware and Cloud sizing were not measured. The benchmark barrier's `manifest_sha256` hashes normalized JSON content; the material-preparation report separately records the SHA-256 of the manifest file bytes.

## Retail chain measurements

Run `workshop retail-benchmark --district 1 --repetitions 20 --output reports/retail-benchmark.json` after the first complete live batch. The command captures one closed minute from `retail_analysis_context`. It compares district/product totals across history and the active run, excluding comparison-branch events, at that same exclusive cutoff.

Each query gets one warmup followed by 20 alternating measured samples. Every sample must match the same integer units, receipts and exact transaction counts. Result caching is disabled; storage caches are not flushed. Client elapsed seconds include transport. Server seconds come from `system.query_log.query_duration_ms / 1000`; missing logs remain null and make server evidence incomplete. p50/p95 use nearest-rank percentiles. No global log flush is required.

Repeat with the writer running and paused, using separate output files. Record actual Cloud service replicas, memory, CPU, region and storage-cache conditions alongside both reports before publishing a performance claim. The report records query IDs, rows/bytes read, query memory, actual recent live rate, active parts, dataset row count and EXPLAIN pruning. Service sizing remains explicitly unverified until supplied from the Cloud service. These measurements do not establish a guaranteed throughput or latency.
