# Observability (ClickStack / HyperDX)

This app is instrumented so that **traces** and **logs** flow into each workshop
participant's own ClickHouse Cloud service, where **Managed ClickStack (HyperDX)**
renders them. Instrumentation is opt-in: without `OTEL_ENABLED=true` the app runs
exactly as before, so local development needs no collector.

## Architecture

```
                          docker-compose.workshop.yml  +  docker-compose.otel.yml
  ┌────────────────────────────────────────────────────────────────────────────┐
  │  backend (FastAPI)                                                           │
  │    - opentelemetry-instrument auto-instruments FastAPI/ASGI  ── traces ─┐    │
  │    - app logs -> stdout  (structured) ──────────────── logs (OTLP) ─────┤    │
  │  loadgen (pg-trip-writer)                                               │    │
  │    - logs -> stdout (structured)                                        │    │
  └────────────────────────────────────────────────────────────────────────┼────┘
                                                                             │ OTLP
                                                              http://otel-collector:4318
                                                                             │
                                          ┌──────────────────────────────────▼─────┐
                                          │  otel-collector                         │
                                          │  clickhouse/clickstack-otel-collector   │
                                          │  (batch + clickhouse exporter)          │
                                          └──────────────────────────────────┬─────┘
                                                                             │ https:8443
                                          ┌──────────────────────────────────▼─────┐
                                          │  Participant's ClickHouse Cloud service │
                                          │  Managed ClickStack (HyperDX) UI        │
                                          └──────────────────────────────────────────┘
```

Two log paths:

1. **App logs over OTLP** – the instrumented backend exports its Python logs
   through the collector (`OTEL_LOGS_EXPORTER=otlp`), correlated with traces.
   Live testing initially showed **zero** app logs in HyperDX while traces worked
   perfectly. The cause was a bug in `configure_logging()`: it reassigned the root
   logger's handlers and **evicted the OTLP log handler** that
   `opentelemetry-instrument` attaches at startup, so app logs reached container
   stdout but never OTLP. That is now fixed — `configure_logging()` preserves any
   OpenTelemetry handler. **VERIFY-LIVE:** the app-side export is fixed and verified
   locally (the handler survives startup), but that `otel_logs` actually populates
   in HyperDX under traffic has not been re-confirmed on a live service.
2. **Raw container stdout scraping** – an *optional* second collector
   (`--profile container-logs`) tails Docker's json-file logs, capturing every
   service including the non-instrumented ones (loadgen, postgres). This is the
   reliable fallback, and the only path that covers non-instrumented services;
   enable it if OTLP logs still do not appear. See VERIFY-LIVE below.

Traces always flow over OTLP from the auto-instrumented backend (they do not
depend on the logging handlers, which is why they worked when logs did not).

## What changed in the app

- **`backend/app/observability.py`** (new) – `configure_logging()` (stdout,
  `LOG_LEVEL`, uvicorn loggers aligned; **preserves the auto-instrumentation OTLP
  log handler** on the root logger so app logs still export over OTLP -- a blind
  handler reassignment used to evict it) and `start_span()` (real OTel span when
  instrumented, cheap no-op otherwise; import-guarded so the app runs even if
  OpenTelemetry is not installed).
- **`backend/app/db.py`** – `run_query()` now wraps ClickHouse calls in a
  `clickhouse.query` span with attributes `db.system`, `db.statement` (truncated
  to 500 chars), `db.elapsed_ms`, `db.rows_returned`, and on failure
  `error.category` (`too_many_rows` / `timeout` / `query_failed`) plus a recorded
  exception. The same categorization is logged at `ERROR` with the SQL, and it
  drives the HTTP status (413/504/500) exactly as before.
- **`backend/app/main.py`** – calls `configure_logging()` at startup.
- **`backend/requirements.txt`** – the **vanilla OpenTelemetry distro**
  (`opentelemetry-distro`, `opentelemetry-exporter-otlp`,
  `opentelemetry-instrumentation-fastapi`, `opentelemetry-instrumentation-logging`),
  all pinned to one line (core `1.43.0` / instrumentation `0.64b0`).
- **`backend/Dockerfile`** – runs `opentelemetry-bootstrap -a install` at build
  and starts via `entrypoint.sh`.
- **`backend/entrypoint.sh`** (new) – launches uvicorn under
  `opentelemetry-instrument` when `OTEL_ENABLED=true`, else plain uvicorn.
- **`loadgen/pg_trip_writer.py`** – bare `print()` replaced with `logging` (same
  message text) so container logs are structured.

### Divergence from the ClickStack docs: vanilla OTel, not `hyperdx-opentelemetry`

The ClickStack Python docs recommend the convenience package
`hyperdx-opentelemetry`. We deliberately do **not** use it. Every published
version of `hyperdx-opentelemetry` (latest is `0.3.0`) **hard-pins
`opentelemetry-api==1.30.0`**, which is incompatible with the **Langfuse SDK v4**
used by the chat feature (Langfuse requires `opentelemetry-api>=1.33.1,<2`). The
two cannot coexist in one environment.

`hyperdx-opentelemetry` is only a bundle of the standard OTel API/SDK/OTLP
exporter with defaults pre-set; the ClickStack collector ingests **standard
OTLP**, so the vanilla distro behaves identically. We therefore install vanilla
OTel and set the same exporter env vars ourselves (see the backend env table).
The `opentelemetry-bootstrap` and `opentelemetry-instrument` commands come from
`opentelemetry-distro`, so the Dockerfile and entrypoint are unchanged.

## Run it

All env vars (ClickHouse Cloud creds plus the observability additions
`OTLP_AUTH_TOKEN`, `CLICKSTACK_DATABASE`, `OTEL_SERVICE_NAME`, `LOG_LEVEL`,
`OTEL_GRPC_HOST_PORT`, `OTEL_HTTP_HOST_PORT`) live in the single
`.env.workshop.example`.

```bash
# 1) Fill in your ClickHouse Cloud creds + OTel settings (set OTLP_AUTH_TOKEN):
cp .env.workshop.example .env.workshop

# 2) Bring up the stack with the overlay:
docker compose --env-file .env.workshop \
  -f docker-compose.workshop.yml -f docker-compose.otel.yml up -d

# Optional: also scrape raw container stdout (see VERIFY-LIVE):
docker compose --env-file .env.workshop \
  -f docker-compose.workshop.yml -f docker-compose.otel.yml \
  --profile container-logs up -d
```

## Environment variables

### Collector (`otel-collector`)

| Variable | Default | Purpose |
|---|---|---|
| `CLICKHOUSE_ENDPOINT` | `https://${CLICKHOUSE_HOST}:${CLICKHOUSE_PORT}` | Full HTTPS endpoint (protocol + `:8443`) of the Cloud service. Assembled from the workshop's `CLICKHOUSE_HOST`/`CLICKHOUSE_PORT`. |
| `CLICKHOUSE_USER` | `default` | Cloud SQL user. |
| `CLICKHOUSE_PASSWORD` | (empty) | Cloud SQL password. |
| `OTLP_AUTH_TOKEN` | (empty) | Shared secret securing the collector's OTLP ingest. Clients send it back. Empty disables auth. |
| `HYPERDX_OTEL_EXPORTER_CLICKHOUSE_DATABASE` | `otel` (via `CLICKSTACK_DATABASE`) | Database for ClickStack's `otel_*` tables. Separate from the app data DB (`nyc_tlc_data`). |
| `CUSTOM_OTELCOL_CONFIG_FILE` | `/etc/otelcol-contrib/custom.config.yaml` | Only set on the container-logs collector; points at the filelog config. |

Collector OTLP ports: `4317` (gRPC), `4318` (HTTP). If either host port is
already taken, `docker compose ... up` fails to bind the collector; set
`OTEL_GRPC_HOST_PORT` / `OTEL_HTTP_HOST_PORT` to free ports in `.env.workshop`
**before** starting the overlay (`preflight.sh` flags the clash and suggests
values). Those host mappings only matter for host-side OTLP senders — the backend
reaches the collector in-network at `http://otel-collector:4318`, so overriding
the host ports does NOT change the backend wiring.

### Backend

| Variable | Default | Purpose |
|---|---|---|
| `OTEL_ENABLED` | `false` | When `true`, `entrypoint.sh` launches uvicorn under `opentelemetry-instrument`. |
| `OTEL_SERVICE_NAME` | `nyc-taxi-backend` | Service name shown in HyperDX. |
| `OTEL_EXPORTER_OTLP_ENDPOINT` | `http://otel-collector:4318` | Collector OTLP HTTP endpoint. |
| `OTEL_EXPORTER_OTLP_PROTOCOL` | `http/protobuf` | Required because we target the 4318 HTTP port (OTLP defaults to gRPC). |
| `OTEL_EXPORTER_OTLP_HEADERS` | `authorization=${OTLP_AUTH_TOKEN}` | Sends the shared token back to the collector. **VERIFY-LIVE.** |
| `OTEL_LOGS_EXPORTER` | `otlp` | Export app logs over OTLP. Only works because `configure_logging()` preserves the auto-instrumentation OTLP log handler (it used to evict it). **VERIFY-LIVE** that `otel_logs` populates. |
| `LOG_LEVEL` | `INFO` | App + uvicorn log level (`DEBUG`/`INFO`/`WARNING`/`ERROR`). |

### Loadgen

| Variable | Default | Purpose |
|---|---|---|
| `LOG_LEVEL` | `INFO` | Loadgen log level. |

## uvicorn caveat (documented)

OpenTelemetry auto-instrumentation **does not work with `uvicorn --reload` or
multiple workers**. The instrumented launch (`entrypoint.sh`, `OTEL_ENABLED=true`)
therefore uses a **single worker with no reload**. The backend `Dockerfile` CMD
already ran a single worker without `--reload`, so no reload/worker flags had to
be removed — just be aware if you add `--reload`/`--workers` later that the
instrumented path must not use them.

## How to verify in HyperDX

After `up -d` with `OTEL_ENABLED=true` and traffic on the app (open the
dashboard, or `curl` the API), in the ClickHouse Cloud HyperDX UI:

1. **Service** – a service named `nyc-taxi-backend` (or your `OTEL_SERVICE_NAME`)
   appears in the service list.
2. **Traces** – requests to `/api/...` endpoints show as traces. Each has a child
   `clickhouse.query` span carrying `db.statement`, `db.elapsed_ms`,
   `db.rows_returned`.
3. **Error traces** – trigger a failure (e.g. a query that exceeds the safety
   limits) and the `clickhouse.query` span shows `error.category` and a recorded
   exception; the matching request span is marked error.
4. **Logs** – backend `ERROR` logs from failing ClickHouse queries appear (via
   OTLP and/or container scraping). With `--profile container-logs`, loadgen
   `[loadgen] inserted N trips ...` lines appear too.

Quick smoke check without the UI:

```bash
docker compose ... logs otel-collector          # collector started, exporting
docker compose ... logs backend | head           # "[entrypoint] OTEL_ENABLED=true ..."
```

## VERIFY-LIVE items

These follow documented patterns but were not confirmed against a live Managed
ClickStack service; confirm during the first live run:

1. **OTLP auth header** – `OTEL_EXPORTER_OTLP_HEADERS=authorization=<token>`. The
   ClickStack docs describe `OTLP_AUTH_TOKEN` on the collector but do not spell
   out the exact client header name/format. If auth fails, try `Bearer <token>`
   or drop the header (and unset `OTLP_AUTH_TOKEN`) to confirm the rest works.
2. **App logs over OTLP** – live testing found zero app logs while traces worked.
   Root cause: `configure_logging()` evicted the auto-instrumentation OTLP log
   handler; now fixed (the handler is preserved, verified locally that it survives
   startup). Re-confirm on a live service that `otel_logs` now populates under
   traffic. If logs are still missing, check the collector -> ClickStack log
   delivery next; the `--profile container-logs` path remains the fallback.
3. **Container-log scraping** (`otel-collector/custom.config.yaml`):
   - Host path `/var/lib/docker/containers/*/*-json.log` is inferred from the
     documented `/var/log/**/*.log` filelog example. On **Docker Desktop
     (macOS/Windows)** the daemon runs in a VM and this host path may not be
     mountable; it is expected to work on Linux hosts. Confirm the path exists
     and the daemon uses the `json-file` log driver.
   - Timestamp parsing is omitted (observed/ingestion time is used) to avoid a
     layout mismatch dropping logs. Add a `timestamp:` block to the json_parser
     to use the real event time.
4. **ClickStack database bootstrap** – `HYPERDX_OTEL_EXPORTER_CLICKHOUSE_DATABASE`
   (default `otel`); confirm the collector creates/uses that database on the
   Cloud service with the provided user's grants.
```
