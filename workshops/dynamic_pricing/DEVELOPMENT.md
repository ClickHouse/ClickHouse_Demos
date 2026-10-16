# Contributor verification

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

## Retail analytics redesign

The default backend and frontend now use retail services. Legacy UI regression coverage uses `?legacy=1`; it is outside the learner flow. Do not copy private environment files, spools, virtual environments, or generated test output when syncing the public kit.

Focused checks: retail contract/generator/model unit tests; isolated SQL integration via `bash scripts/test-integration.sh`; frontend build and Playwright; WorkshopHouse docs checker and site type/link checks. The manual aggregate registration must preserve grouped raw facts and prevent a second backfill.

Release evidence requires the actual 100M immutable S3 dataset, model artifact, dedicated Cloud ingestion/CDC/approval/outcome/telemetry rehearsal, sustained observed throughput and sizing, actual managed-agent identity and six conversation cases, Windows WSL2, and pacing/cost observations. Local fixtures and mocked browser screenshots are not substitutes. Keep missing evidence explicit in maintainer reports, never as internal status prose in learner steps.
