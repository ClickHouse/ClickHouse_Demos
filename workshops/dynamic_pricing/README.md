# Dynamic pricing with ClickHouse

A three-hour Use-Case Series workshop. Each learner owns ClickHouse Cloud, managed Postgres, a Postgres ClickPipe, and ClickStack access. The local React/FastAPI edge shows historical reconciliation, source freshness, projected candidates, learner review, receipt, and a next-sale/CDC price timeline.

The learner, instructor, and reference playbooks are maintained in WorkshopHouse and served at [the dynamic pricing workshop](https://labs.demohouse.cloud/docs/dynamic-pricing). That route becomes available when the paired WorkshopHouse PR deploys. Start with [Cloud provisioning](infra/README.md) and [model common ground](pricing/model-card.md).

This public checkout contains the runnable kit. Site-only documentation checks stay in WorkshopHouse; the local kit checks run independently here.

## Release status

Implementation is under review. The 1M and 100M S3 tiers are published and verified, and the prepared model passes its synthetic validation gates. Complete managed Cloud rehearsal, Windows WSL2 rehearsal, and pacing must pass before a public session. The template selects the prepared model artifact; the development artifact remains available for local verification. Local SQL replay is not managed CDC evidence.

## Local verification

Use Python 3.12 and Node 22. Docker must be running. From this lab directory:

```bash
python3.12 -m venv .venv
.venv/bin/python -m pip install -r versions.lock -e app/backend
.venv/bin/python -m pytest tests -m 'not integration and not cloud' -q
bash scripts/test-integration.sh
# Full local kit (Node 22 and Docker required):
bash scripts/check-kit.sh
```

The integration wrapper starts only its isolated local project, executes actual Postgres and ClickHouse SQL, refuses skipped/no integration tests, and removes its own containers and volumes. It does not create Cloud resources. Frontend checks run in `app/frontend/` using its lockfile and Playwright fixtures.

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
