# ClickHouse BUILD Workshop — Plan v2 (NYC Taxi foundation app)

Date: 2026-07-14. Supersedes WORKSHOP_PLAN.md (v1, OTel-demo/agentic-sre based) — v1's
prework and instructor research (INSTRUCTOR_SETUP.md, PARTICIPANT_PREWORK.md) remains
largely valid and is referenced below; the foundation app, data story, and product
surfaces are new.

Team split (from whiteboard): workshop lead — whole thing + playbook. QA owner — QA.
Foundation-app owner — slides + foundation app (internal dev repo; public URL TBD).

---

## 1. What changed vs v1

| Dimension | v1 | v2 (this plan) |
|---|---|---|
| Foundation app | OTel demo reskinned to payments (22 containers) | NYC Taxi Ops War Room: React + FastAPI + Postgres, real product-shaped app |
| Ingestion story | OTLP telemetry only | Postgres CDC via ClickPipes (transactional -> analytical), the flagship ClickHouse ingestion story |
| Conversational BI | Attendee builds a GenBI app | ClickHouse Agents (ai.clickhouse.cloud, beta, Claude-powered) — managed product, plus in-app chat |
| AI chat o11y | Dropped (Langfuse "not ClickHouse-native") | Langfuse Cloud — now IS ClickHouse (acquired Jan 2026, runs on ClickHouse). First-class module |
| SRE story | Agent investigates via ClickStack MCP | Same, plus agent BUILDS the SRE dashboard/alerts (clickstack_save_dashboard / clickstack_save_alert verified) and a fault-branch break/fix finale |
| Playbook | Lab markdown in repo | Published Fumadocs site at demohouse.cloud/workshop, structured like langfuse.com/workshop |

## 2. Target architecture (per participant)

```
PARTICIPANT LAPTOP                          SHARED (instructor-hosted)
+----------------------------------+        +--------------------------------+
| App (Docker, 3-4 containers):    |        | Managed Postgres (ONE instance)|
|   frontend (nginx)  backend      |--SQL-->|   DB per participant:          |
|   pg-trip-writer (data gen) -----+--------|   taxi_p01 ... taxi_p35        |
|   otel-collector (ClickStack)    |        |   publication + user per DB    |
| Coding agent + CH skills + MCP   |        +-------------+------------------+
+---------------+------------------+                      | logical replication
                | OTLP (traces/logs)                      v
                |            PARTICIPANT'S CLICKHOUSE CLOUD (trial)
                +----------> service: nyc_tlc_data tables (seeded via url()
                             + ClickPipes CDC destination) + ClickStack/HyperDX
                             + ClickHouse Agents (ai.clickhouse.cloud)
                                          |
        In-app AI chat --OpenAI API-->  answers    traces --> LANGFUSE CLOUD
                                                              (participant's free org)
```

Local footprint after the slim-down: 3-4 containers, ~1-1.5 GB images, <1 GB RAM
(vs today's 13 services / ~6 GB images / 4.5-6 GB RAM with the Kafka stack). The 3
JVM containers (broker, connect, kafka-ui) and local ClickHouse all die.

Accounts per participant: ClickHouse Cloud trial ($300/30d, no card) + org API key,
Langfuse Cloud free Hobby org (50k units/month — a workshop uses ~2-3k), OpenAI API
key (chat runtime), their coding agent. All keys live in one `.env`.

## 3. Key design decisions

**D1 — Shared managed Postgres, per-participant DATABASE.** One instructor-hosted
instance; script provisions `taxi_pNN` databases each with the `realtime_trips`
table, a scoped `clickpipes_user`, and a pre-created publication. Participants' local
generators write to their own DB; their ClickPipe decodes only their DB's WAL.
Requirements (verified): wal_level=logical, max_replication_slots and max_wal_senders
raised to ~50 (defaults are 10 — will not work), max_connections 300+ (30 concurrent
initial snapshots use ~4-6 connections each), modest max_slot_wal_keep_size (10-20 GB)
as the safety valve — a stalled participant pipe gets invalidated and resyncs instead
of filling the disk. No PgBouncer/pooler in the path (unsupported for CDC). No
documented limit on pipes-per-source; the only documented pipe-count threshold is
destination-side (irrelevant — each lands in its own service).

Provider: **Postgres managed by ClickHouse** is the on-brand first choice (beta,
public TLS endpoint, CDC ClickPipes included free) — GATE: verify max_replication_slots
is raisable and that participants' trial-org ClickPipes can point at it cross-org.
Fallback (proven, documented in ClickHouse's own CDC guides): **RDS** (custom
parameter group, slots to 50, ~$0.07/hr) or **Supabase** (CLI-settable slots, direct
connection not the pooler). Neon is ruled out (slots fixed at 10, inactive slots
deleted after ~40h).

**D2 — CDC is the ingestion star, with two escape hatches.** ClickPipes auto-creates
destination tables as ReplacingMergeTree(_peerdb_version) with _peerdb_* columns —
"CHC tables created auto through clickpipes" is verified true. The app's landing->MV
publish model is rebuilt on top of that (MV filters _peerdb_is_deleted = 0 into
taxi_trips). Escape hatches if the dry run shows contention: (a) instructor demos one
live CDC pipe, participants use an S3 ClickPipe on staged files (pseudo-streaming,
zero source limits); (b) generator writes directly to ClickHouse. Decision trigger: a
10-concurrent-pipe dry run.

**D3 — Conversational BI = ClickHouse Agents (managed), chat = in-app (built).** Two
complementary surfaces: ai.clickhouse.cloud for exploratory BI over their taxi data
(beta, available to all Cloud customers, Claude-powered, no published beta pricing),
and the app's own chat panel (OpenAI NL-to-SQL with guardrails) as the "how you build
this into YOUR product" story, traced end to end in Langfuse.

**D4 — Langfuse is now the AI-observability module and an acquisition story.**
ClickHouse acquired Langfuse (Jan 16, 2026); Langfuse v3+ runs entirely on ClickHouse.
Per-participant free Hobby org (no card, 50k units, 2 users — individual orgs, not
shared). SDK v4 (langfuse 4.14.0): langfuse.openai drop-in wrapper + @observe,
LANGFUSE_BASE_URL env (not the old LANGFUSE_HOST). Ten-minute UI tour: trace tree,
generations with token cost, sessions, prompt management, playground — all free-tier.

**D5 — AI SRE = ClickStack MCP with create-tools.** Verified tool surface includes
clickstack_save_dashboard, clickstack_query_tile, clickstack_save_alert,
clickstack_search, clickstack_trace_waterfall, clickstack_event_patterns. The lab:
participant's coding agent connects to https://mcp.clickhouse.cloud/clickstack (OAuth;
x-service-id only needed for multi-service orgs) and builds a dashboard + alert over
the app's real telemetry. Webhook creation has NO MCP tool — pre-create the alert
webhook in HyperDX UI as a lab step. AI Notebooks (beta, Managed ClickStack only) is
the in-product AI surface to show. App instrumentation: hyperdx-opentelemetry +
opentelemetry-instrument on the FastAPI backend (single worker, no --reload),
clickstack-otel-collector sidecar in compose, OTLP_AUTH_TOKEN via
OTEL_EXPORTER_OTLP_HEADERS.

**D6 — Fault injection via checkpoint-style git branches.** Fault branches cut from
the finished workshop baseline; four validated candidates from the repo analysis:
(1) frontend: taxi_zones.geojson fetch path broken -> map dies, rest lives (matches
whiteboard "something is not loading e.g. map"); (2) backend: wrong column in
zone_stats_sql -> 500s with ClickHouse error visible in traces/logs; (3) CDC stall:
generator stopped or publication revoked -> Ops dashboard flatlines while Historical
works; (4) config: wrong CLICKHOUSE_PASSWORD/port -> health endpoint red, every card
errors. Labs use (1) or (2) as primary (best ClickStack diagnosis story); (3) as
stretch.

**D7 — Docs MCP still does not exist publicly (rechecked 2026-07-14).** Official blog
confirms an internal docs MCP behind Ask AI; nothing public; llms-full.txt still 404.
The workshop uses: https://clickhouse.com/docs/llms.txt (docs index for agents),
ClickHouse agent skills (npx skills add clickhouse/agent-skills), and the in-console
Docs AI. ACTION (workshop lead): ask the docs team whether the kapa-backed docs MCP can be
exposed for the workshop; the whiteboard box stays aspirational until then.

**D8 — Playbook = Fumadocs site at demohouse.cloud/workshop.** langfuse.com/workshop
is Next.js + Fumadocs; we replicate its patterns: dual-track learner/instructor docs,
checkpoint branch per module, per-module contract (Starting point -> Why -> Goal ->
file-path-named steps -> How to verify -> End state), modules table as syllabus.

## 4. Run of show — 3 hours (whiteboard summed to 195 min; this is 180)

| Time | Module (playbook) | What happens |
|---|---|---|
| 0:00-0:10 | Intro | Cold open (the finished thing, 3 min) + framing. Slides: foundation-app owner |
| 0:10-0:40 | 00 Setup | Accounts verified (prework), CHC service create + API key + chctl login, Langfuse org, keys into .env, docker compose up, agent skills + MCP wired |
| 0:40-1:05 | 01 Base App + 02 ClickHouse Cloud | Tour app; schema to Cloud; seed historical data via url() (1-3 months yellow parquet); backend flips to Cloud; feel the dashboard speed |
| 1:05-1:30 | 03 Realtime CDC | Start your generator (writes to your DB on shared PG); create Postgres CDC ClickPipe (console wizard); snapshot then streaming; Ops dashboard goes live. Hard checkpoint |
| 1:30-1:35 | Break | Telemetry and CDC keep flowing |
| 1:35-1:50 | 04 ClickHouse Agents | Agent over your taxi data at ai.clickhouse.cloud; conversational exploration; share prompts that produce insights |
| 1:50-2:10 | 05 ClickStack | OTel overlay on; traces/logs in HyperDX; walk a trace from dashboard click to ClickHouse query |
| 2:10-2:30 | 06 AI SRE | Coding agent + ClickStack MCP builds the SRE dashboard + alert (webhook pre-created in UI) |
| 2:30-2:45 | 07 Chat + Langfuse | Use the in-app chat; watch traces/generations/cost in Langfuse; the acquisition story |
| 2:45-3:00 | 08 Break and fix + 09 Wrap | Checkout fault branch; symptom appears; AI SRE diagnoses via ClickStack; apply fix; wrap: what you keep (app, trial, playbook) |

Facilitation: the 90-minute whiteboard block is compressed to 70 by making 04-07
tight, single-outcome modules; every module has a checkpoint branch so nobody is
stranded; hard pivot rules at 1:30 and 2:45.

## 5. Build workstreams and status

| # | Workstream | Owner | Status |
|---|---|---|---|
| 1 | This plan | Workshop lead | Done (this doc) |
| 2 | App: workshop compose + Cloud backend (branch workshop/cloud-backend) | teammate taxi-cloudify | In progress |
| 3 | App: AI chat + Langfuse v4 (branch workshop/chat) | teammate taxi-chat | In progress |
| 4 | App: OTel/ClickStack instrumentation (branch workshop/otel) | teammate taxi-otel | In progress |
| 5 | App: fault branches | blocked until 2+3+4 merge into the workshop baseline | Pending |
| 6 | Playbook scaffold (Fumadocs, build_ai_with_ai/) | teammate playbook-scaffold | In progress |
| 7 | Shared PG provisioning script (N databases + users + publications) + provider decision gate (D1) | Workshop lead | Next |
| 8 | Merge 2+3+4 -> workshop baseline branch -> PR to the foundation-app repo; cut fault branches | Workshop lead + foundation-app owner | After 2-4 land |
| 9 | Slides + foundation-app ownership | Foundation-app owner | External |
| 10 | QA: full dry run on fresh accounts; 10-pipe CDC load test (D2 gate); v1 INSTRUCTOR_SETUP.md section 9 items that still apply | QA owner | After 8 |

## 6. Verification gates before the dry run

1. ClickHouse Managed Postgres: slots raisable to 50? cross-org participant pipes
   accepted? (D1 gate; else RDS.)
2. 10 concurrent ClickPipes CDC against the shared PG: snapshot contention, slot
   stability, decode CPU. (D2 gate; else S3-pipe fallback.)
3. ClickStack MCP live tool names match the clickstack_* list (from HyperDX repo, not
   docs) and OTEL_EXPORTER_OTLP_HEADERS auth works against the collector.
4. ClickHouse Agents on a fresh trial org: enablement, consent dialog, any quotas.
5. Langfuse v4 wrapper: graceful no-key behavior; LANGFUSE_BASE_URL.
6. ClickStack enable-on-existing-service (services created via chctl service create
   are not created through the Observability wizard).
7. Pipe deletion drops the source replication slot? (Undocumented — post-workshop
   sweep script regardless: pg_replication_slots check + pg_drop_replication_slot.)
8. Still-valid v1 items: trial signup card-free, credits visibility, MCP toggle path,
   chctl cwd-relative tokens, macOS PATH.

## 7. Costs (per participant, on their trial credits)

CDC pipe: ~$0.10-0.20/hr compute + cents of ingest — under $1 for the session; pipes
keep waking idled services, so module 09 includes "delete your pipe" cleanup. The
whole workshop consumes a few dollars of the $300 trial. Langfuse: $0 (Hobby). OpenAI:
well under $5. Shared PG: instructor-side, ~$5-25 for the day depending on provider.
