# BUILD Workshop foundation app

The NYC-taxi analytics app participants run locally during the ClickHouse BUILD
Workshop: a React ops dashboard (with an AI chat panel), a FastAPI analytics backend,
a local Postgres with a synthetic trip generator, and an optional ClickStack
OpenTelemetry overlay. ClickHouse itself is your own ClickHouse Cloud service; live
ingestion is Postgres CDC via ClickPipes.

Follow the playbook (`../playbook`, published at demohouse.cloud/workshop) from
module 00 — it walks through every step below in order.

## Requirements

- A Docker engine — Docker Desktop, OrbStack, or Colima — running, with **Compose
  v2** (the `docker compose` subcommand; the legacy `docker-compose` v1 is not
  enough) and at least **6 GB of memory** allocated to it (Docker Desktop:
  Settings > Resources). The stack may not come up healthy with less.
- Roughly **10 GB of free disk** for images and volumes.
- **macOS** (Apple Silicon or Intel), **Linux**, or **Windows via WSL2**. All
  images are multi-arch, so there is no platform-emulation warning on Apple
  Silicon.
- A **ClickHouse Cloud** service (its host and password go in `.env.workshop`),
  plus `curl` and `git`.

Host ports (override any of these in `.env.workshop` if it is already taken —
`preflight.sh` tells you which one and suggests a free port):

| Service | Default host port | Override var |
|---|---|---|
| Frontend (UI) | 8080 | `FRONTEND_HOST_PORT` |
| Backend API | 8000 | `BACKEND_HOST_PORT` |
| Postgres (local fallback) | 5432 | `POSTGRES_HOST_PORT` |
| OTel gRPC (otel overlay) | 4317 | `OTEL_GRPC_HOST_PORT` |
| OTel HTTP (otel overlay) | 4318 | `OTEL_HTTP_HOST_PORT` |

## Run

Run the preflight check first — it verifies Docker, the effective ports, your
`.env.workshop`, and Cloud connectivity, and must report `READY` before you start:

```bash
cp .env.workshop.example .env.workshop     # fill in your ClickHouse Cloud values
./preflight.sh                             # must print "Overall: READY" (exit 0)

docker compose --env-file .env.workshop -f docker-compose.workshop.yml up -d

# with the ClickStack observability overlay (module 05 onward):
docker compose --env-file .env.workshop \
  -f docker-compose.workshop.yml -f docker-compose.otel.yml up -d
```

Frontend: http://localhost:8080 - Backend API docs (FastAPI Swagger):
http://localhost:8000/docs. If you overrode `FRONTEND_HOST_PORT` or `BACKEND_HOST_PORT`,
use those ports instead of 8080 / 8000.

## Layout

| Path | What |
|---|---|
| `preflight.sh` | Participant readiness check — run before `docker compose up` |
| `frontend/` | React/Vite SPA: Ops + Historical dashboards, zone map, chat panel |
| `backend/` | FastAPI analytics API, guardrailed AI chat (`/api/chat`), OTel instrumentation |
| `loadgen/` | `pg_trip_writer.py` — synthetic trips into Postgres (throttled via env) |
| `db/cloud/001_cloud_schema.sql` | Idempotent base schema (tables + views) for your Cloud service; applies cleanly on a fresh service |
| `db/cloud/002_seed_historical.sql` | Optional runnable historical seed (taxi_zones + a yellow-taxi month) from public object storage; idempotent, run after 001 |
| `db/cloud/003_cdc_mv.sql` | ClickPipes CDC materialized view into taxi_trips; run after the pipe's initial snapshot (ships console + CLI destination variants) |
| `db/postgres/` | Local-fallback Postgres init (CDC source table, publication) |
| `otel-collector/` | Optional container-log scrape config for the ClickStack overlay |
| `.env.workshop.example` | The single env template — copy to `.env.workshop` |

Details: `WORKSHOP_CHANGES.md` (stack and Cloud wiring), `CHAT_FEATURE.md` (AI chat +
Langfuse), `OBSERVABILITY.md` (ClickStack/OTel).

This app is derived from an upstream NYC-taxi demo; the full original stack (local
ClickHouse, Kafka/Debezium CDC, dataset loaders) lives upstream and is not part of
the workshop tree.
