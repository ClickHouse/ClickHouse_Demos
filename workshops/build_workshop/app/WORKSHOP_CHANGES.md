# Workshop changes: Cloud backend + standalone compose

This branch (`workshop/cloud-backend`) adapts the NYC Taxi Ops War Room app to
run in the ClickHouse BUILD workshop, where each participant:

- uses their own **ClickHouse Cloud** service instead of a local `clickhouse`
  container;
- relies on **ClickPipes** (Cloud-managed) to capture change data from a
  **shared managed Postgres**, instead of the local Kafka + Debezium + Kafka
  Connect pipeline;
- still keeps a **local Postgres** available as a fallback for offline dev.

The workshop tree ships only the workshop path; the original full local stack
(local ClickHouse, Kafka/Debezium CDC, dataset loaders) lives in the upstream
demo repo and is not included here.

## What changed and why

### 1. Backend targets ClickHouse Cloud over TLS

`backend/app/settings.py`, `backend/app/db.py`

- Added `CLICKHOUSE_SECURE` and `CLICKHOUSE_CONNECT_TIMEOUT` settings and wired
  `secure=` / `connect_timeout=` into the `clickhouse_connect.get_client(...)`
  factory.
- `CLICKHOUSE_SECURE` is a tri-state (`true` / `false` / unset). **When unset it
  is inferred from the port**: `8443`/`443` implies TLS, anything else (e.g. the
  local `8123`) implies plain HTTP. An explicit value always wins.
  - Why: the upstream demo's compose sets `CLICKHOUSE_PORT=8123` and never
    sets `CLICKHOUSE_SECURE`. A blind
    `secure=true` default would break local dev; the port-based inference lets
    local (`8123`) and Cloud (`8443`) both work with zero extra config, while
    still honoring an explicit `CLICKHOUSE_SECURE` when set.
- `CLICKHOUSE_CONNECT_TIMEOUT` defaults to `10s` (was a hard-coded `2s`). A
  ClickHouse Cloud service can idle-scale to zero and take a few seconds to wake
  on first connect; local connects still return instantly.

No other backend behavior changed. All existing env vars
(`CLICKHOUSE_HOST/PORT/USER/PASSWORD/DATABASE`, CORS, query-safety limits) keep
their names and defaults.

### 2. Standalone workshop compose

`docker-compose.workshop.yml` (self-contained -- does **not** extend the
upstream demo's compose)

Contains only:

- `postgres` -- local fallback only (kept `wal_level=logical` for parity);
- `backend` -- all ClickHouse params env-driven, defaulting to `8443` + TLS;
- `frontend` -- unchanged build, nginx still proxies `/api` to `backend:8000`;
- `pg-trip-writer` -- the loadgen, PG connection env-driven, throttled;
- `pgadmin` -- optional, only starts with `--profile tools`.

Deliberately **omitted**: `clickhouse`, `ch-ui`, `broker`, `connect`,
`kafka-ui`, `pg-cdc-init`, `clickhouse-cdc-init`, `connect-init`. ClickHouse is
the participant's Cloud service and CDC is ClickPipes.

Every ClickHouse and Postgres parameter uses `${VAR:-default}` interpolation, so
`docker compose -f docker-compose.workshop.yml config` succeeds even with no
`.env` file, and container names are suffixed `-workshop` to avoid colliding
with the local stack.

### 3. Loadgen (pg-trip-writer)

`loadgen/pg_trip_writer.py` -- **no code change needed**. It was already fully
env-driven for the Postgres connection (`PGHOST`, `PGPORT`, `PGDATABASE`,
`PGUSER`, `PGPASSWORD`) and already had a configurable write rate
(`RATE_PER_SEC`, `BATCH_SIZE`, effective delay = `BATCH_SIZE / RATE_PER_SEC`).

The workshop compose lowers the defaults to `RATE_PER_SEC=2`, `BATCH_SIZE=10`
(from `10`/`25` in local) because a single shared managed Postgres may be fed by
30+ concurrent generators. Participants can raise or lower this via env, or drop
the generator entirely with `docker compose ... up -d --scale pg-trip-writer=0`.

The compose also passes `PGSSLMODE` (default `prefer`) through to the loadgen.
psycopg3/libpq honors it natively, so no code change is needed; set
`PGSSLMODE=require` for the shared managed Postgres, which mandates TLS.

### 4. ClickHouse Cloud schema

`db/cloud/001_cloud_schema.sql` (new)

Idempotent DDL adapted from the upstream demo's local schema for a participant to run
once against their Cloud service (Cloud SQL console, `clickhousectl`, or agent):

- Same `taxi_zones`, `fhv_trips`, `taxi_trips` tables and `*_expanded` views.
- Plain `ENGINE = MergeTree` retained -- Cloud transparently backs these with
  SharedMergeTree; no `ON CLUSTER` / `Replicated*` needed.
- **No local `file()` reads.** The upstream demo's local seed reads
  `file('sample/...')` from `user_files`, which does not exist on Cloud. The
  reference/historical seed instead lives in a separate **runnable** file,
  `db/cloud/002_seed_historical.sql` (001 keeps only a pointer comment, so there
  is no commented/uncommented drift). It loads `taxi_zones` from the public TLC
  zone-lookup CSV and a one-month yellow-taxi subset from the public TLC parquet
  exports (`yellow_tripdata_2022-07.parquet`), with the column mapping and borough
  enrichment cribbed from the upstream demo's dataset loader. Both statements are
  idempotent (each guarded by a `count() = 0` check, so re-running cannot
  double-load) and run after the schema, e.g.
  `clickhouse client ... --multiquery < db/cloud/002_seed_historical.sql`. This
  gives the Historical dashboard real volume; copy the second statement with the
  next month's file name for more months.
- The CDC path into `taxi_trips` is kept as the original **materialized-view
  model** and is now an **enabled, standardized MV** (no placeholder):
  ClickPipes/PeerDB always creates the destination table as
  `ENGINE = ReplacingMergeTree(_peerdb_version)` with `_peerdb_synced_at`,
  `_peerdb_is_deleted`, `_peerdb_version` columns, and the workshop pins the
  destination to database `nyc_tlc_data`, table `realtime_trips` (participants
  select `nyc_tlc_data` and keep the source table name in the ClickPipes
  wizard). The MV selects from `nyc_tlc_data.realtime_trips` into `taxi_trips`
  with `WHERE _peerdb_is_deleted = 0` (an MV fires per inserted block, so it
  needs no `FINAL`; the loadgen is append-only so version churn does not occur).
  Because the MV references `realtime_trips`, the file's run-order note makes
  clear it must be run **after** the ClickPipe's initial snapshot creates that
  table (the ClickPipe itself only needs the `nyc_tlc_data` database that the
  `CREATE DATABASE` at the top provides), and to confirm column types live with
  `DESCRIBE` (expected PeerDB mapping `timestamptz -> DateTime64(6)`,
  `int2/4/8 -> Int16/32/64`).

`.env.workshop.example` (new) documents every variable a participant fills:
ClickHouse Cloud endpoint/port/user/password/database + secure flag, shared (or
local) Postgres host/db/credentials + `PGSSLMODE`, loadgen throttle, host-port
mappings, and the query-safety limits.

### 5. Workshop hardening (from the first live end-to-end bring-up)

The first live run of the full stack surfaced five setup/runtime rough edges.
Each is now addressed so any participant can get to a running app:

- **`preflight.sh` (new).** A no-root, bash 3.2-compatible readiness check to run
  before `docker compose up`. It PASS/WARN/FAILs (one fix hint per failure,
  non-zero exit on any FAIL) on: the Docker CLI + daemon; that the daemon can
  actually **start** a container (a `docker run` with a timeout - the wedge
  detector for a daemon that answers `docker info` but leaves containers stuck in
  `Created`); host-port collisions on the **effective** ports read from
  `.env.workshop` (last-value-wins, matching compose), telling apart "in use by
  this workshop stack" from a real clash and printing the exact override var and
  a suggested free port; the required `.env.workshop` values (`CLICKHOUSE_HOST`,
  `CLICKHOUSE_PASSWORD`; module-07 keys are warn-only); and TLS/TCP connectivity
  to ClickHouse Cloud (and the shared Postgres, when used). Why: the live run hit
  port collisions on 5432/8080/8000 and a wedged Docker daemon with no signal to
  the participant until `up` failed.

- **Backend idle-wake resilience** (`backend/app/db.py`, `backend/app/main.py`).
  A ClickHouse Cloud service idle-scales to zero and takes ~5-30s to wake, so the
  first request read-timed-out and surfaced a raw 500. `run_query` now retries a
  transport-level connect/read timeout (clickhouse-connect raises
  `OperationalError`) **once**, on a fresh client with a 30s read timeout; the
  server-side `max_execution_time` is unchanged, so a genuinely slow query still
  returns `TIMEOUT_EXCEEDED` -> 504 (the tradeoff is one wasted retry for a truly
  slow query). A repeated transport timeout maps to 503 ("waking/unreachable"),
  not 500. `/api/health` now probes with the same generous timeout and returns a
  structured hint instead of reporting the service down mid-wake. Covered by
  `backend/tests/test_db_retry.py`.

- **Runnable seed** (`db/cloud/002_seed_historical.sql`, new) - see section 4.

- **Multi-arch Postgres** (`docker-compose.workshop.yml`,
  `db/postgres/init/000_postgis.sql`). The local-fallback Postgres image was
  `postgis/postgis:16-3.4`, which is amd64-only and printed a platform-mismatch
  warning (and ran under emulation) on Apple Silicon. The workshop path uses no
  geo types (centroids are plain `double precision`; the CDC table has no geo
  columns), so the image is now the official multi-arch `postgres:16` (native on
  Apple Silicon, no warning). The postgis init is kept but made non-fatal (a
  `DO`/`EXCEPTION` block) so it is portable across a plain and a PostGIS image and
  never aborts container init.

- **Requirements documented** (`README.md`): a Requirements section (Docker engine
  with 6 GB + Compose v2, ~10 GB disk, macOS/Linux/WSL2 stance, and a host-port
  table with the override vars) plus a "run `./preflight.sh` first" step.

## How to run

### Local mode (unchanged)

```
docker compose up -d
```

Uses the local `clickhouse` container, local Postgres, and the Kafka/Debezium
CDC pipeline exactly as before. The backend connects to `clickhouse:8123` over
plain HTTP (secure inferred `false` from the port).

### Workshop mode (Cloud + ClickPipes)

1. Create a ClickHouse Cloud service and create the destination database so the
   ClickPipe has somewhere to land (`CREATE DATABASE IF NOT EXISTS nyc_tlc_data;`
   -- also the first line of the schema file).

2. Create a ClickPipe from the shared managed Postgres `public.realtime_trips`
   into your Cloud service: select destination database `nyc_tlc_data` and keep
   the source table name, which yields `nyc_tlc_data.realtime_trips`. After its
   initial snapshot creates that table, `DESCRIBE nyc_tlc_data.realtime_trips`
   to confirm the column types, then run the schema file once:

   ```
   # via the Cloud SQL console, clickhousectl, or your agent
   db/cloud/001_cloud_schema.sql
   ```

   It creates the base tables/views and the `realtime_trips_to_taxi_trips_mv`
   materialized view (which needs `realtime_trips` to already exist). The `url()`
   seeds for `taxi_zones` and historical trips now live in
   `db/cloud/002_seed_historical.sql`; run that file directly to load them.

3. Configure and start the local app stack:

   ```
   cp .env.workshop.example .env.workshop
   # fill in CLICKHOUSE_HOST + CLICKHOUSE_PASSWORD (and shared PG, if using it)
   docker compose --env-file .env.workshop -f docker-compose.workshop.yml up -d
   ```

   App on http://localhost:8080, backend API on http://localhost:8000.
   Add `--profile tools` to also start pgadmin on http://localhost:5050.

## Verification performed

- `docker compose -f docker-compose.workshop.yml config` passes with **exit 0
  and no warnings** with no `.env` file, and again with a filled `--env-file`.
  With the profile off, only `backend`, `frontend`, `postgres`,
  `pg-trip-writer` are present; `pgadmin` appears only with `--profile tools`.
  Env interpolation was confirmed (e.g. `CLICKHOUSE_SECURE: "true"`, filled
  `CLICKHOUSE_HOST`, `PGHOST`, `RATE_PER_SEC`, and `PGSSLMODE` defaulting to
  `prefer` and overriding to `require`).
- Backend imports cleanly in a Python 3.11 venv (matching the Dockerfile) with
  `requirements.txt` installed: `import app.main` and `import app.db` succeed,
  all 13 `/api/*` routes register, `clickhouse_connect.get_client` accepts the
  `secure=` kwarg, and `get_client` passes both `secure` and the configurable
  connect timeout.
- `clickhouse_secure_effective` was checked across 6 scenarios: local `8123`
  unset -> `False`; Cloud `8443` unset -> `True`; explicit `CLICKHOUSE_SECURE`
  overrides the port inference in both directions.
- `db/cloud/001_cloud_schema.sql` was executed with `clickhouse-local`: it
  applies idempotently (re-run is a no-op) and creates the 3 tables + 2 views.
  The concrete CDC MV was exercised end-to-end against a stub table matching the
  documented ClickPipes shape (`ReplacingMergeTree(_peerdb_version)` with the
  `_peerdb_*` columns and PeerDB-mapped types): a live row landed in `taxi_trips`
  with `filename='realtime_cdc'`, and a row with `_peerdb_is_deleted = 1` was
  correctly filtered out.
- The yellow-trips seed projection was validated against a synthetic Parquet with
  the real TLC column names (`VendorID`, `tpep_pickup_datetime`, `PULocationID`,
  ...): the borough-enrichment `multiIf`, the `store_and_fwd_flag` mapping, and
  the implicit type casts into `taxi_trips` all worked, and `taxi_trips_expanded`
  computed `reasonable_time_distance_fare` over the result.

### Not verified (no live services available)

- No connection to a real ClickHouse Cloud service or a real ClickPipe was made;
  TLS/8443 connectivity and the exact ClickPipes destination column names/types
  must be confirmed on real infrastructure (hence the `DESCRIBE`-first note). The
  MV casts assume the documented PeerDB type mapping.
- The `url()` seeds were validated with a local `file()` stand-in, not against
  the live TLC CDN endpoints.
- The backend integration tests (`backend/tests/`) require a running API +
  ClickHouse and were not run (they poll a live `/api/health`); they were not in
  scope and no stack was launched.

## Env vars for the chat feature (for the /api/chat teammate)

The chat feature was built in a separate worktree (`workshop/chat`); **no chat
code is added here.** When the branches are merged, the workshop `backend`
service env block in `docker-compose.workshop.yml` and `.env.workshop.example`
will need the vars that feature introduced:

- `OPENAI_API_KEY`, `LLM_MODEL`, and `LLM_BASE_URL` (the in-app chat uses
  OpenAI, not Anthropic);
- `LANGFUSE_PUBLIC_KEY`, `LANGFUSE_SECRET_KEY`, and `LANGFUSE_BASE_URL` for
  tracing (Langfuse SDK v4 renamed `LANGFUSE_HOST` to `LANGFUSE_BASE_URL`).

Two things already in place make this easy:

- `backend/app/settings.py` uses `extra="ignore"`, so new env vars won't break
  startup, and new typed settings fields can be added alongside the existing
  ones.
- The chat endpoint can reuse `app.db.get_client()` to run text-to-SQL against
  the same ClickHouse Cloud service the dashboards use -- no new connection
  config required.

I intentionally did not reserve these vars in the compose/env files to avoid
guessing the chat design; they should be wired in when the branches merge.
