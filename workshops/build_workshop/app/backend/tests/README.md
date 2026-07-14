## Backend integration tests

These are **integration tests** (not unit tests). They call the running API and require ClickHouse to be up with the seeded sample data.

### Run via Docker Compose (recommended)

From repo root:

- Start the stack (ClickHouse + API):

```bash
docker compose up -d --build clickhouse backend
```

- Run the integration tests:

```bash
docker compose -f docker-compose.yml -f docker-compose.test.yml --profile test up --build --abort-on-container-exit backend-tests
```

### Run locally (host Python)

1) Start services:

```bash
docker compose up -d --build clickhouse backend
```

2) Install deps + run:

```bash
python3 -m venv .venv && source .venv/bin/activate
pip install -r backend/requirements-dev.txt
API_BASE_URL=http://localhost:8000 pytest -q backend/tests
```

