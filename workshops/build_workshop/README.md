# Build Series: Solutions on ClickHouse Cloud

This site now carries two dedicated use cases:

- **AI SRE with NYC taxi data:** the existing three-hour application, managed Postgres
  CDC, Agents, ClickStack, incident diagnosis, and Langfuse journey under `app/`.
- **Polymarket real-time analytics:** a two-hour public market-data stream, typed
  ClickHouse model, one-minute aggregate, investigation, and Cloud dashboard under
  `polymarket/`.

Both tracks support macOS and Windows through Ubuntu on WSL 2. ClickHouse Cloud is the
only ClickHouse server; no local database substitute is part of either track.

Learners clone the repository, then switch to `build-workshop-v1`. Maintainers create a
feature branch, open a PR to protected `dev-build-workshop-v1`, verify
[dev-workshop.demohouse.cloud](https://dev-workshop.demohouse.cloud), then promote
`dev-build-workshop-v1` to protected `build-workshop-v1` for
[workshop.demohouse.cloud](https://workshop.demohouse.cloud).

## AI SRE architecture

The app edge runs on the participant's laptop. ClickHouse, Postgres, ClickPipes, and
ClickStack/HyperDX live in ClickHouse Cloud; OpenAI and Langfuse are separate hosted
services. No database or product UI is deployed locally.

```mermaid
flowchart LR
  subgraph LAPTOP["Participant laptop - the NYC taxi app runs here"]
    FE["frontend<br/>React SPA, nginx :8080"]
    BE["backend<br/>FastAPI :8000, /api/chat NL-to-SQL"]
    GEN["pg-trip-writer<br/>creates CDC table + publication"]
    OC["otel-collector<br/>module 05 overlay"]
    CA["coding agent + ClickHouse skills<br/>+ clickhousectl"]
  end

  subgraph CLOUD["ClickHouse Cloud - YOUR trial org"]
    CH[("ClickHouse service :8443<br/>nyc_tlc_data + otel db")]
    PIPE["ClickPipes<br/>Postgres CDC pipe"]
    PG[("Postgres managed by ClickHouse :5432<br/>created by you via clickhousectl")]
    HDX["Managed ClickStack / HyperDX"]
    MCP["Remote MCP<br/>/mcp + /clickstack"]
    AG["ClickHouse Agents<br/>ai.clickhouse.cloud"]
  end

  subgraph THIRD["Third-party"]
    OAI["OpenAI API<br/>chat completions"]
    LF["Langfuse Cloud<br/>chat traces"]
    TLC["NYC TLC public dataset<br/>download source only"]
  end

  FE -->|/api proxy| BE
  BE -->|SQL over TLS| CH
  GEN -->|INSERT trips, TLS| PG
  PG -->|logical replication| PIPE
  PIPE -->|CDC rows, about 60s| CH
  BE -->|OTLP :4318| OC
  OC -->|traces + logs| CH
  CA -->|MCP over OAuth| MCP
  CA -->|creates, module 03| PG
  BE --> OAI
  BE -.->|traces| LF
  TLC -.->|url seed, module 01| CH
  HDX -->|reads otel db| CH
  AG -->|RBAC-governed SQL| CH
```

The published diagrams (the ClickHouse Cloud platform stack, the workshop architecture, the
data flow, and the module flow) are generated as clean SVGs by `docs/diagrams/gen_diagrams.py`
— edit that script and re-run it (`python3 gen_diagrams.py all`) to regenerate them, then copy
the SVGs into `playbook/public/`.

## Workflows

### The workshop journey (about 2h30 hands-on)

```mermaid
flowchart LR
  M00["00 Setup<br/>25 min"] --> M01["01 ClickHouse Cloud<br/>schema + 3M-row seed<br/>15 min"]
  M01 --> M02["02 Base app<br/>tour the seeded app<br/>5 min"]
  M02 --> M03["03 Managed Postgres CDC<br/>managed Postgres + ClickPipe<br/>20 min"]
  M03 --> M04["04 ClickHouse Agents<br/>conversational BI<br/>10 min"]
  M04 --> M05["05 ClickStack<br/>traces to HyperDX<br/>15 min"]
  M05 --> M06["06 AI SRE<br/>agent builds dashboard + alert<br/>15 min"]
  M06 --> M07["07 Test, fail, and fix<br/>20 min"]
  M07 --> M08["08 Chat + Langfuse<br/>15 min"]
  M08 --> M09["09 Wrap-up<br/>take it home<br/>10 min"]
  M07 -.->|pick one or more| F["fault/01-map-not-loading<br/>fault/02-zone-stats-500<br/>fault/03-slow-dashboard"]
```

Every module except 07 stays on `build-workshop-v1`; the only branch switches are the
fault branches in module 07.

### How a trip row flows (live CDC path)

```mermaid
sequenceDiagram
  participant G as pg-trip-writer (laptop)
  participant P as Managed Postgres (your trial)
  participant CP as ClickPipes CDC
  participant CH as ClickHouse service
  participant UI as Dashboards / Chat / Agents

  G->>P: INSERT trips (throttled, TLS)
  Note over G,P: first run also creates the table<br/>and publication pub_taxi
  P->>CP: WAL changes via publication + slot
  CP->>CH: rows land in default.realtime_trips, about 60s
  CH->>CH: materialized view fans rows into nyc_tlc_data.taxi_trips
  UI->>CH: parameterized SQL / guarded NL-to-SQL / RBAC-governed BI
  CH-->>UI: live Ops dashboard + 3M-row Historical seed
```

### Observability and the break-and-fix incident lab

```mermaid
flowchart LR
  APP["backend spans + logs<br/>clickhouse.query: SQL, elapsed_ms,<br/>error.category"] -->|OTLP| COL["otel-collector"]
  COL --> OTEL[("otel db in your service")]
  OTEL --> HDX2["HyperDX UI<br/>search, traces, dashboards"]
  SRE["coding agent via ClickStack MCP<br/>clickstack_search, save_dashboard,<br/>save_alert"] -.-> OTEL
  FAULT["module 07: git checkout fault/*<br/>a realistic bug ships"] --> APP
  SRE -->|diagnose from telemetry| FIX["apply the fix,<br/>return to build-workshop-v1"]
```

## Layout

| Path | What |
|---|---|
| `app/` | The local application edge: React frontend, FastAPI backend, managed-Postgres data generator, and stateless ClickStack OTel forwarder. All databases and product UIs are cloud-hosted. Workshop entrypoint: `preflight.sh` + `docker-compose.workshop.yml` + `.env.workshop.example`. |
| `polymarket/` | Public Polymarket collector, ClickHouse schema/reference queries, fixture mode, Docker Compose entrypoint, and tests. The only Compose service is the stateless collector. |
| `playbook/` | The published workshop catalog and dedicated AI SRE / Polymarket learner and instructor tracks. Requires Node >= 22.12 to build. |
| `docs/` | `diagrams/` — the platform, architecture, data-flow, and module-flow SVGs, generated by `gen_diagrams.py`. |
| `infra/` | Instructor tooling via clickhousectl: the demo stack end-to-end run and a cloud-hosted managed-Postgres fallback pool for participants whose orgs cannot create one. |

## Fault branches (module 07, break and fix)

Kept current with `build-workshop-v1`, each differs from the base by one innocent-looking
change touching one file under `app/`:

- `fault/01-map-not-loading`
- `fault/02-zone-stats-500`
- `fault/03-slow-dashboard`

The learner playbook lists the branch names and the observable symptom of each fault;
diagnosis paths and fixes live in the playbook's instructor track (module 07) and are
deliberately not documented in this directory.

## AI SRE quick start

```bash
git clone <this-repo>
cd ClickHouse_Demos
git switch build-workshop-v1
cd workshops/build_workshop/app
cp .env.workshop.example .env.workshop   # fill in your ClickHouse Cloud values
./preflight.sh                           # must print "Overall: READY"
docker compose --env-file .env.workshop -f docker-compose.workshop.yml up -d
```

Then follow the playbook from module 00 (or the self-paced page if no instructor is
around).

## Polymarket quick start

```bash
git clone <this-repo>
cd ClickHouse_Demos
git switch build-workshop-v1
cd workshops/build_workshop/polymarket
cp .env.polymarket.example .env.polymarket
set -a; source ./.env.polymarket; set +a
./preflight.sh
```

Then start at `/docs/polymarket/learner/00-setup`. Learners create the schema from
copyable SQL on the site before starting the collector.
