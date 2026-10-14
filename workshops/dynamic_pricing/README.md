# Dynamic pricing with ClickHouse

A three-hour Use-Case Series workshop. Each learner owns ClickHouse Cloud, managed Postgres, a Postgres ClickPipe, and ClickStack access. The local React/FastAPI edge shows historical reconciliation, source freshness, projected candidates, learner review, receipt, and a next-sale/CDC price timeline.

The learner, instructor, and reference playbooks are maintained in WorkshopHouse and served at [the dynamic pricing workshop](https://labs.demohouse.cloud/docs/dynamic-pricing). Start with Module 00 in the playbook and [model common ground](pricing/model-card.md).

The kit contains the local workbench, data contracts, prepared model, and Cloud setup script.

## Learner setup

After installing dependencies, configuring `app/.env.workshop`, and logging in with your organization API key from this lab directory, run:

```bash
bash scripts/setup-cloud.sh
```

The script creates your owned Cloud services, analytical databases, managed Postgres schema/roles/publication, and four-table ClickPipe, then verifies readiness. Reruns reuse inventory and private credentials. Follow Module 00 for account and software prerequisites.

## Layout

- `app/`: local backend, frontend, writer, and Cloud Compose overlay
- `infra/`: owned inventory, provisioning, limited publication, and cleanup
- `generator/`: streaming Parquet generation and independently verified manifests
- `sql/`: source, history, rollup, canonical CDC, training, and evidence contracts
- `pricing/`: offline preparation, validation, and labeled artifacts
- `fixtures/`: golden facts and five generation-aware recovery checkpoints
- `agent/`, `obs/`: read-only evidence questions, bounded exception tools, and OTel forwarding
- `benchmarks/`, `reports/`: measured conditions and decision exports
- `tests/`, `scripts/`: local correctness and verification tools

Keep `app/.env.workshop`, inventories, exports, and private benchmark results out of Git. Stop/restart the local edge without recreating Cloud services. For deletion, preview `python -m infra.cleanup` before explicitly executing it for your owned inventory.
