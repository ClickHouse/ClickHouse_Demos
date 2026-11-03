# Retail dynamic pricing with ClickHouse

Investigate a synthetic 5,000-store retail chain using 100 million historical sales lines and a live batched feed. Replicate operational inventory, price, and policy records from managed Postgres with ClickPipes. Measure raw versus aggregate queries, compare prices with SQL, and approve your own change in the workbench.

Follow the [learner workshop](https://dev-labs.demohouse.cloud/docs/dynamic-pricing/learner/00-setup). Complete setup before the three-hour session. Each attendee creates a dedicated Cloud stack; public S3 data and prepared model artifacts are shared.

Production teams can use ClickHouse transaction history and additional business features as training data for demand or revenue models built with appropriate ML tools. This workshop supplies a prepared model; production model training and deployment are outside its scope. After learner approval, a separate synthetic demand rule generates transactions for ClickHouse to ingest and analyze. See [pricing/model-card.md](pricing/model-card.md) for that flow and its assumptions.

Use the SQL-console steps in the learner workshop to load history and create aggregates. `bash scripts/setup-cloud.sh` creates your Cloud resources; the module instructions cover verification, the local workbench, and the sales writer. Optional Python import and aggregate commands remain available in the source.

The writer targets 100,000 transactions/minute for its first two business minutes, then 10,000/minute automatically, for a 180-business-minute run. Restarts preserve the clock and do not repeat a completed burst.

For another workshop session, stop the writer and run `.venv/bin/workshop retail-initialize --new-run`. This restores opening stock in a new live scenario while preserving historical data, aggregates, and earlier runs.

Module 02 focuses on ingestion and materialized-view performance. Manual aggregate registration uses metadata-only checks by default; the lesson then verifies a representative SQL result. Exhaustive grouped verification is an optional contributor check documented in `DEVELOPMENT.md`. Module 03 records the demand-slowdown start and target in the workbench and temporarily accelerates the writer to 20× speed while a full 15-business-minute window reaches ClickHouse. Progress is automatic and survives reloads; generation and delivery can exceed the approximately 45-second pacing target. It then finds excess stock and compares permitted prices with SQL. Run `sql/retail/learner_price_comparison.sql` in SQL console after choosing a ready product; change only the selected store and SKU.

Create **Retail Pricing Strategist** in managed ClickHouse Agents using the instruction and context files in `agent/`. The agent queries and recommends; you review and approve in the workbench. Inspect the receipt-scoped observed series after approval.

Stop local containers and use `bash scripts/setup-cloud.sh --destroy` to preview cleanup. Add `--execute` only after reviewing the owned resource IDs. This deletes the workshop Cloud data.

## Files you use

| Path | Purpose |
| --- | --- |
| `app/` | Environment template, Docker application, and sales writer. |
| `scripts/setup-cloud.sh` | Create, reuse, or delete your workshop Cloud resources. |
| `sql/retail/` | Pricing calculations and observed-outcome SQL. |
| `agent/retail-*.md` | Instructions, data dictionary, and policy for your managed pricing agent. |
| `pricing/artifacts/retail/` | Prepared model files loaded during initialization. |
| `infra/` | Cloud provisioning and CDC implementation used by setup. |

You do not need a `reports/` folder to complete the workshop. Measurement and export commands create local output when requested. `generator/`, model-preparation code, `tests/`, benchmark tooling, and developer Compose files support dataset preparation and maintenance; see [DEVELOPMENT.md](DEVELOPMENT.md).
