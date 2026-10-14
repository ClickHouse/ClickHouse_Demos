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
