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

Release evidence requires the actual 100M immutable S3 dataset, model artifact, dedicated Cloud ingestion/CDC/approval/outcome rehearsal, sustained observed throughput and sizing, actual managed-agent identity and six conversation cases, Windows WSL2, and pacing/cost observations. Local fixtures and mocked browser screenshots are not substitutes. Keep validation evidence privately with the release review, outside the published learner kit.

## Three-hour stock-capacity rehearsal

Run `PYTHONPATH="$PWD:$PWD/app/backend" .venv/bin/python scripts/rehearse-retail-capacity.py` from the kit directory. It uses the actual deterministic transaction generator, the published opening-stock formula, the two-minute 100,000/minute burst followed by 10,000/minute, and a district-1 demand reduction after minute 20. The 180-business-minute run verifies nonnegative stock, unit conservation, and remaining stocked products. It writes `reports/retail-three-hour-capacity.json`. This is generator capacity evidence; it does not claim three wall-clock hours of Cloud throughput.

## Generated output and source layout

Saved rehearsal runs, benchmark results, release checklists, and internal publication notes are not workshop inputs. The public kit does not bundle them. Scripts create `reports/` when needed; Git ignores it and `benchmarks/results/`. Backend images create a writable `reports/exports` directory without copying historical reports.

Diagram assets are served by the WorkshopHouse site and maintained in that repository. Keep tests, fixtures, local Compose definitions, optional CLI implementations, and model preparation code: they verify or support the runnable kit even though attendees do not edit them. Legacy modules still imported by the application must be removed through a separate dependency refactor, not by deleting their files.

## Optional exhaustive aggregate verification

`workshop retail-register-rollup` defaults to metadata checks: schema, engine, sort/partition keys, and populated active parts. It does not scan raw sales or merge all aggregate states, and it does not establish full grouped-data parity.

Before starting the sales writer, use `.venv/bin/workshop retail-register-rollup --full-check` for exhaustive verification. This compares all measures in both directions for both aggregate tables across 20 districts, including exact distinct transactions. It can take several minutes on the 100M-row dataset. The option also runs after a previous quick registration, provided live sales have not started. Failed validation does not upgrade registration status; a successful check records `rollup_validation=full`.
