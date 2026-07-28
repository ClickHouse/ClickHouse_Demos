## Backend integration tests

These are **integration tests** (not unit tests). They call the running API and require
the ClickHouse Cloud service configured in `app/.env.workshop` to contain the seeded data.

### Run via Docker Compose (recommended)

From `workshops/build_workshop/app`:

- Start the API against ClickHouse Cloud:

```bash
docker compose --env-file .env.workshop -f docker-compose.workshop.yml up -d --build backend
```

- Run the integration tests:

```bash
API_BASE_URL=http://localhost:8000 python3 -m pytest -q backend/tests
```

### Run locally (host Python)

1) Start the cloud-connected API:

```bash
docker compose --env-file .env.workshop -f docker-compose.workshop.yml up -d --build backend
```

2) Install deps + run:

```bash
python3 -m venv .venv && source .venv/bin/activate
pip install -r backend/requirements-dev.txt
API_BASE_URL=http://localhost:8000 pytest -q backend/tests
```
