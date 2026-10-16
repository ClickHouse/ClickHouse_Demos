# Retail dynamic pricing with ClickHouse

Investigate a synthetic 5,000-store retail chain using 100 million historical sales lines and a live batched feed. Replicate operational inventory, price, and policy records from managed Postgres with ClickPipes. Measure raw versus aggregate queries, compare prices with SQL, and approve your own change in the workbench.

Follow the [learner workshop](https://dev-labs.demohouse.cloud/docs/dynamic-pricing/learner/00-setup). Complete setup before the three-hour session. Each attendee creates a dedicated Cloud stack; public S3 data and prepared model artifacts are shared.

The main commands, in order:

```bash
bash scripts/setup-cloud.sh
.venv/bin/workshop retail-import
.venv/bin/workshop retail-initialize
.venv/bin/workshop retail-create-rollup
```

The SQL-console lessons provide equivalent manual import and aggregate paths. `retail-register-history` and `retail-register-rollup` verify that work without inserting the same facts again. Start the Docker workbench and separate writer with the commands in Modules 01 and 02.

Create **Retail Pricing Strategist** in managed ClickHouse Agents using the instruction and context files in `agent/`. The agent queries and recommends; you review and approve in the workbench. Inspect the receipt-scoped observed series and ClickStack trace after approval.

Stop local containers and use `bash scripts/setup-cloud.sh --destroy` to preview cleanup. Add `--execute` only after reviewing the owned resource IDs. This deletes the workshop Cloud data.

See [DEVELOPMENT.md](DEVELOPMENT.md) for contributor checks and rehearsal evidence.
