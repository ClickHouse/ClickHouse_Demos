# Optional performance tooling

The learner workshop compares raw and aggregate queries directly in SQL console in Module 02. No benchmark report is required to complete the workshop.

The retained Python benchmark is available for maintainers and optional investigation:

```bash
.venv/bin/workshop retail-benchmark --district 1 --repetitions 20 --output reports/retail-benchmark.json
```

It captures a complete cutoff and alternates raw and rollup queries over the same history and active run, excluding comparison-branch events. Results must agree on units, receipts, and exact transaction counts. The exact-distinct aggregation adds work; this command measures a different query mix from the learner's sum-based performance comparison.

Reports record query IDs, timings, rows and bytes read, and available server metrics. Missing metrics remain missing. Result caching is disabled; storage caches are not flushed. Local or previous-run results are not a performance guarantee for a learner's Cloud service.

Generated files under `reports/` and `benchmarks/results/` are ignored by Git. Keep service sizing and workload conditions with any measurements you share. The older `workshop benchmark` command remains available for its retained CLI and regression coverage.
