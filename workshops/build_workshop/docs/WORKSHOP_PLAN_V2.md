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
PARTICIPANT LAPTOP                          PARTICIPANT'S TRIAL ORG
+----------------------------------+        +--------------------------------+
| App (Docker, 3-4 containers):    |        | Own Postgres managed by CH     |
|   frontend (nginx)  backend      |--SQL-->|   (clickhousectl create):      |
|   pg-trip-writer (data gen) -----+--------|   realtime_trips + pub_taxi    |
|   otel-collector (ClickStack)    |        +-------------+------------------+
| Coding agent + CH skills + MCP   |                      | logical replication
+---------------+------------------+                      v
                | OTLP (traces/logs)   PARTICIPANT'S CLICKHOUSE CLOUD (same trial org)
                +----------> service: nyc_tlc_data tables (seeded via url()
                             + ClickPipes CDC destination) + ClickStack/HyperDX
                             + ClickHouse Agents (ai.clickhouse.cloud)
                                          |
        In-app AI chat --OpenAI API-->  answers    traces --> LANGFUSE CLOUD
                                                              (participant's free org)

FALLBACK (instructor-hosted, only if managed Postgres is unavailable in an org):
one shared managed Postgres with a per-participant database + role + publication
(taxi_p01..pNN), handed out on slips — see infra/.
```

Local footprint after the slim-down: 3-4 containers, ~1-1.5 GB images, <1 GB RAM
(vs today's 13 services / ~6 GB images / 4.5-6 GB RAM with the Kafka stack). The 3
JVM containers (broker, connect, kafka-ui) and local ClickHouse all die.

Accounts per participant: ClickHouse Cloud trial ($300/30d, no card) + org API key,
Langfuse Cloud free Hobby org (50k units/month — a workshop uses ~2-3k), OpenAI API
key (chat runtime), their coding agent. All keys live in one `.env`.

## 3. Key design decisions

**D1 — Per-participant Postgres managed by ClickHouse (shared instance = fallback).**
Each participant creates their OWN managed Postgres in their own trial org
(`clickhousectl cloud postgres create`, module 03); their local generator writes to it
and their ClickPipe reads its WAL — Postgres and pipe in the same org, no cross-org
setup. This dissolves the slot gate: one participant needs exactly one replication slot,
and every managed instance ships `wal_level=logical` with 10 slots (plus 10 wal_senders,
500 connections) by default, so there is nothing to raise. It also removes the shared
blast radius (no one participant's stalled slot touches another) and the per-slip handout.

Verified live (2026-07-14): `clickhousectl cloud postgres create --name <n> --region <r>
--size m8gd.large --pg-version 17 --ha-type none` works with API-key auth and prints a
one-time password + hostname (sizes are instance-type names — m8gd.large valid, m7i.*
not). The beta status APIs (`postgres get`/`list`) can return FORBIDDEN/empty, so
readiness = poll a real psql/TCP connect — the app's own generator is that probe — not
the API. Defaults observed: wal_level=logical, max_replication_slots=10,
max_wal_senders=10, max_connections=500.

FALLBACK — shared managed Postgres pool. For participants whose org cannot create a
managed Postgres (beta availability varies), the instructor runs ONE shared instance with
a per-participant database + scoped role + pre-created publication (`infra/`), handed out
on slips. THIS is where the slot gate lives: 30+ pipes on one instance needs
max_replication_slots/max_wal_senders raised to ~50 and max_connections to 300+ — and on
managed Postgres that patch is a known beta gap (FORBIDDEN; raise via console/support),
so the documented fallback-of-the-fallback is **RDS** (custom parameter group, slots to
50, ~$0.07/hr) or **Supabase** (CLI-settable slots, DIRECT connection not the pooler).
Neon is ruled out (slots fixed at 10, inactive slots deleted after ~40h). No
PgBouncer/pooler in the CDC path. The `max_slot_wal_keep_size` WAL safety valve is
best-effort only: it reads back as -1 (unlimited) on live managed instances because the
patch is blocked.

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
| 1:05-1:30 | 03 Realtime CDC | Create your own managed Postgres (clickhousectl); start your generator (writes to it, and the generator log is the readiness probe); create Postgres CDC ClickPipe (console wizard); snapshot then streaming; Ops dashboard goes live. Hard checkpoint |
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

1. PRIMARY (D1) gate: can a fresh TRIAL org create a managed Postgres instance
   (`clickhousectl cloud postgres create`) and reach it via psql? Verify at the dry run
   on a clean trial org — this is the gate that matters now. FALLBACK gate: on one
   shared instance, are slots raisable to 50 for 30+ pipes? (else RDS/Supabase.)
2. 10 concurrent ClickPipes CDC against the shared PG: snapshot contention, slot
   stability, decode CPU. (D2 gate; now relevant only to the shared FALLBACK pool —
   the primary path is one pipe per own instance — else S3-pipe fallback.)
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
well under $5. Own managed Postgres: cents of the same trial for the session. Shared PG
(fallback only): instructor-side, ~$5-25 for the day depending on provider.
