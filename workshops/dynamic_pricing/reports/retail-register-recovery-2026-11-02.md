# Retail aggregate registration recovery — Nov 2, 2026

The district checks completed, both canonical materialized views were attached, and Postgres saved `rollup: true`. The later analytical-view installation failed with `UNKNOWN_TABLE` for `pricing.retail_batches_cdc`.

The existing private environment had retail mode disabled and a legacy four-table CDC connector using `pricing_workshop`. The current retail flow requires six operational tables and publication `pricing_retail`.

## Runtime fix

- The learner setup entry point rejects legacy mode before Cloud discovery or resource creation.
- Aggregate CLI commands check retail mode and all six CDC tables before expensive grouped verification.
- CLI output identifies aggregate verification, model installation, and analytical-view installation separately. Raw errors and credentials remain omitted.

Regression tests first reproduced the missing guards and stage feedback, then passed after the fix.

## Instance recovery

The existing ClickHouse and Managed Postgres services were reused. The old connector was paused and retained with a `-legacy` name for rollback. A new six-table connector was created under the canonical workshop name, and private settings/inventory were updated. A private backup of the previous environment and inventory is under `.run/register-debug/retail-cdc-upgrade` in the WorkshopHouse kit; it is not committed.

The raw sales table and both aggregate tables retained their UUIDs and row counts. No history reload or backfill was needed. The old paused connector belongs to the same owned ClickHouse service and is removed when that service is destroyed during workshop cleanup.

The actual command in the user's ClickHouse_Demos checkout completed successfully:

```text
Checking retail CDC setup...
Verifying district and store aggregates...
Installing the prepared pricing model...
Installing analytical views...
{
  "ready": true
}
```

The saved rollup flag skips the already-completed grouped validation on this retry. PostgreSQL timeout settings were all zero; this was a missing-CDC dependency, not a timeout.

Validation: 233 unit tests, five real rollup/model SQL integration tests, and 43 focused CLI/setup tests in the public kit passed. The four new regression tests were observed failing before the fixes.

The existing workbench backend was recreated with the corrected private environment. Retail mode is true, the backend is healthy, and `/api/health` responds with HTTP 200. Other application containers and Cloud data were retained.
