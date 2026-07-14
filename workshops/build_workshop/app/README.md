# BUILD Workshop foundation app

The NYC-taxi analytics app participants run locally during the ClickHouse BUILD
Workshop: a React ops dashboard (with an AI chat panel), a FastAPI analytics backend,
a local Postgres with a synthetic trip generator, and an optional ClickStack
OpenTelemetry overlay. ClickHouse itself is your own ClickHouse Cloud service; live
ingestion is Postgres CDC via ClickPipes.

Follow the playbook (`../playbook`, published at demohouse.cloud/workshop) from
module 00 — it walks through every step below in order.

## Run

```bash
cp .env.workshop.example .env.workshop     # fill in your ClickHouse Cloud values
docker compose --env-file .env.workshop -f docker-compose.workshop.yml up -d

# with the ClickStack observability overlay (module 05 onward):
docker compose --env-file .env.workshop \
  -f docker-compose.workshop.yml -f docker-compose.otel.yml up -d
```

Frontend: http://localhost:8080 - Backend API docs: http://localhost:8080/api/docs

## Layout

| Path | What |
|---|---|
| `frontend/` | React/Vite SPA: Ops + Historical dashboards, zone map, chat panel |
| `backend/` | FastAPI analytics API, guardrailed AI chat (`/api/chat`), OTel instrumentation |
| `loadgen/` | `pg_trip_writer.py` — synthetic trips into Postgres (throttled via env) |
| `db/cloud/001_cloud_schema.sql` | Idempotent schema for your Cloud service, including the ClickPipes CDC materialized view (run after the pipe's initial snapshot) |
| `db/postgres/` | Local-fallback Postgres init (CDC source table, publication) |
| `otel-collector/` | Optional container-log scrape config for the ClickStack overlay |
| `.env.workshop.example` | The single env template — copy to `.env.workshop` |

Details: `WORKSHOP_CHANGES.md` (stack and Cloud wiring), `CHAT_FEATURE.md` (AI chat +
Langfuse), `OBSERVABILITY.md` (ClickStack/OTel).

This app is derived from an upstream NYC-taxi demo; the full original stack (local
ClickHouse, Kafka/Debezium CDC, dataset loaders) lives upstream and is not part of
the workshop tree.
