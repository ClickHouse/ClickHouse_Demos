# ClickHouse BUILD Workshop — Repo Analysis and Conversion Plan

Date: 2026-07-02 (revised same day after three-lens review: feasibility, coherence, product)
Source repo analyzed: `agentic-sre/` (2 commits, internal executive demo)
Target: a public, follow-along, 3-hour "build AI with AI" workshop repo in `build_ai_with_ai/`

---

## 1. Executive summary

The `agentic-sre` repo is a good base: it already implements the hardest part of the
workshop — a realistic microservices workload (the OpenTelemetry demo, reskinned as a
payments platform) streaming full-fidelity telemetry to ClickHouse Cloud via the
ClickStack collector, plus a validated incident-investigation scenario with a
non-obvious root cause and an answer-key query pack. All of that carries over.

What does NOT carry over is the delivery model. The repo was built for one presenter
running a 30-minute executive demo through LibreChat against one internal ClickHouse
service. The workshop needs 30+ attendees, each with their own ClickHouse Cloud trial,
each driving their own agentic coding tool, following written labs. That means:

1. Drop LibreChat entirely — each attendee's coding agent (Claude Code, Cursor, Codex,
   Windsurf) becomes the SRE agent, connected to the hosted ClickHouse MCP endpoints.
2. Scrub all internal/customer artifacts (bank narrative, real service hostnames,
   internal AWS account, fabricated Splunk content).
3. Fix the setup landmines that currently make the repo un-runnable from a fresh clone.
4. Build two net-new tracks: GenBI and Slack/Telegram notifications — neither exists
   in the repo today.
5. Wrap everything in follow-along lab docs, a mandatory prework doc, and an
   instructor runbook.

Estimated conversion effort: six phases of work (section 8), with a full dry run on a
fresh trial account as the gate before the first delivery.

---

## 2. What the repo contains today

### 2.1 Architecture (as built)

```
Payments app (OTel demo, 22 containers, local Docker)
    --OTLP--> clickstack-otel-collector (in compose)
    --HTTPS--> ClickHouse Cloud (otel_logs / otel_traces / otel_metrics_*)
                     ^
                     | hosted ClickStack MCP (mcp.clickhouse.cloud/clickstack, OAuth2)
                LibreChat (7 containers) + agent definition  <-- to be dropped
```

### 2.2 Assets worth keeping (the crown jewels)

| Asset | Where | Why it matters |
|---|---|---|
| Payments-reskinned OTel demo | `app/compose.yaml` | Realistic workload; reskin is env-var-only (`OTEL_SERVICE_NAME` per service), so stock upstream ghcr.io images work — nothing to build |
| ClickStack collector wiring | `app/compose.yaml:891-913` | Stock `clickhouse/clickstack-otel-collector` image, three env vars (`CLICKHOUSE_ENDPOINT/USER/PASSWORD`), writes straight to the attendee's Cloud service |
| Validated hero scenario | `SPEC.md` section 3, `app/src/flagd/demo.flagd.json` | `paymentFailure` at 75 percent produces real checkout 5xx at the gateway, root cause two hops down in `card-auth`, failures isolated to gold-tier customers — a genuinely non-obvious, multi-signal investigation |
| SRE agent definition -> SRE investigator skill | `agent/sre-agent-definition.md`, ported to `skills/sre-investigator/` | ~90 percent portable: persona, 7-step investigation methodology, schema knowledge, gotchas (StatusCode='Error', legacy `http.status_code` key, exception extraction via Events, Duration in ns) |
| Answer-key query pack | `queries/hero-investigation.sh` | 8 validated queries (symptom, localize, leaf, rule-out, exception, impact, span tree, loyalty-tier dimension) — becomes the lab answer key and instructor validation tool |
| MCP service-binding insight | `librechat/librechat.yaml:22-26` | The ClickStack MCP binds to the FIRST ClickStack service on the account unless `x-service-id` is set. For single-service trial orgs the default is correct (troubleshooting note only); for the organizer fallback pool, where attendees share one org, the header is REQUIRED — this goes in `instructor/provisioning.md` |
| Fault-injection UX | flagd + flagd-ui at `localhost:8080/feature` | Attendees flip their own faults in a UI; flagd hot-reloads, no restarts |

### 2.3 Landmines found (must fix before anyone clones this)

All confirmed directly against the tree (e.g. `paymentFailure` defaultVariant `"75%"`
at `app/src/flagd/demo.flagd.json:111`).

| # | Problem | Fix |
|---|---|---|
| L1 | `app/.env` is git-ignored and missing — `docker compose up` fails from a fresh clone; `make` targets additionally require `.env.override` | Commit a safe `app/.env` (upstream's is public: image names, ports, mock-LLM defaults) with only `CLICKHOUSE_ENDPOINT/USER/PASSWORD` left for attendees |
| L2 | `paymentFailure` defaultVariant checked in as `"75%"` — the fault fires on first boot | Reset to `"off"`; the lab flips it at the right moment |
| L3 | `make start` and `make start-minimal` layer `compose.observability.yaml`, which re-mounts collector configs and silently disconnects ClickHouse export (and references removed volumes) | Support `docker compose -f compose.yaml up` as the only path; fix or delete the Makefile targets and the full/observability/agent/tests/profiling overlays |
| L4 | Unpinned images: `clickstack-otel-collector:latest`, `DEMO_VERSION=latest`; upstream has dropped `product-reviews`/`llm` from main so their `latest-*` tags may go stale; ghcr tags are mutable | Pin by digest (`image@sha256:...`) at minimum; preferred: mirror the pinned images (~18; final list produced in Phase 1) to a registry we control |
| L5 | Root `.env.example` variable names (`CLICKHOUSE_HOST`, `CLICKHOUSE_HTTP_PORT`) do not match what compose consumes (`CLICKHOUSE_ENDPOINT`) | Reconcile to one shape |
| L6 | `product-reviews` crashes unless `LLM_*`/`OPENAI_API_KEY` env vars are set; README steers to a real OpenAI key | Default everyone to the bundled mock LLM container (`LLM_BASE_URL=http://llm:<port>/v1`, `OPENAI_API_KEY=dummy`) — no OpenAI keys needed for the app |
| L7 | Internal data exposure: real internal service hostnames (two, redacted here), an internal AWS account number and SSO profile, bank/stakeholder narrative, fabricated SPL section — all inside the private source repo | Scrub before anything goes public; replace `SPEC.md` entirely with workshop docs |
| L8 | Storefront UI is still the Astronomy Shop (telescopes) while telemetry says payments | Keep the reskin; add one narrative line ("the storefront is a demo shop; the services are a payments platform"). Re-theming the UI is not worth the fork risk |
| L9 | Upstream CI workflows in `app/.github/` (nightly releases, image pushes) | Delete in the workshop repo |

### 2.4 Attendee resource footprint (minimal compose path, measured)

- ~1.8 GB total image pull per attendee (load-generator alone is 536 MB)
- Docker needs >= 6 GB RAM allocated; ~3-4 GB steady-state usage; ~10 GB disk free.
  With IDE + coding agent + browser + a dev server alongside, the realistic machine
  minimum is 16 GB total RAM
- 2-4 min to healthy after `up`; allow ~4 more min of load-generator warmup before
  telemetry is rich
- Only one published host port (Envoy on 8080) — shop, flagd UI, and load-gen UI all
  behind it

Bandwidth implication: 30 attendees x 1.8 GB is 55-60 GB over venue wifi if nobody
pre-pulls — and pulls are not the only load. Live session traffic includes ~30 laptops
streaming compressed OTLP upstream continuously, coding-agent token streaming all
session, and (if not pre-installed) a synchronized ~10 GB npm burst when Lab 3 starts.
Pre-pull and pre-scaffold are therefore mandatory prework; the venue needs real
symmetric bandwidth and no captive portal (section 10).

---

## 3. Gaps vs the workshop brief

| Brief requirement | Repo today | Gap |
|---|---|---|
| Guided ClickHouse walkthrough | Nothing | Build slides + a fundamentals lab |
| Agentic SRE build-along | Presenter demo via LibreChat | Convert to per-attendee labs; replace LibreChat with the attendee's coding agent |
| GenBI build-along | Nothing (one unbuilt "text-to-SQL throwaway" line in SPEC.md) | Net-new track |
| Slack/Telegram | Nothing | Net-new track |
| ClickHouse Docs MCP | Not used | See D7 — no hosted docs MCP verified to exist; official agent skills fill the role; confirm with product |
| ClickHouse hosted MCP | Used (ClickStack MCP via LibreChat) | Re-express as per-tool config for 4 coding agents; one endpoint per lab that needs it (D9) |
| clickhousectl | Not used (personal `clickhouse client --connection demo`) | Adopt for what its read-only OAuth supports: auth, service listing, queries, skills install (D6) |
| Hosted / ClickHouse-native only | Holds for data plane (no local ClickHouse anywhere); violated by LibreChat's mongo/meili/pgvector | Dropping LibreChat resolves it; Langfuse (not ClickHouse-native) also drops out |
| Follow-along docs, prework, instructor runbook | None (README is operator-oriented; SPEC.md is a customer strategy doc) | Write them |
| Per-attendee provisioning | Assumes one presenter-owned service | Trial-per-attendee model + organizer fallback pool |

---

## 4. Core design decisions

**D1 — The attendee's coding agent IS the SRE agent (drop LibreChat).**
LibreChat costs 7 containers, 6 generated secrets, an unvendored upstream clone, account
registration, Agent Builder setup, two OAuth flows, and manual tool trimming — 30-60 min
of setup that teaches nothing and violates the ClickHouse-native constraint (mongo,
meilisearch, pgvector). Every attendee already brings a coding agent that speaks MCP.
This kills the shared Anthropic key, the app's OpenAI key (with L6), all Langfuse keys,
and all LibreChat secrets. Remaining per-attendee credentials: their ClickHouse Cloud
login and service password, their coding tool's own auth, and one LLM API key for the
GenBI app they build in Lab 3 (see D4 — this is the one credential the original version
of this plan missed).

**D2 — One ClickHouse Cloud trial per attendee.**
Each attendee creates one personal trial account, which is one Cloud organization
containing one service ($300 credits, 30 days). ClickStack is selected at service
creation; remote MCP is enabled per service via the console's Connect -> MCP toggle.
Attendees keep a working environment for 30 days after the workshop — exactly the
"take it back to your team" promise. Account signup happens in prework with an explicit
checkpoint ("you can see the $300 trial credits in the console" — not merely "account
created", which misses SSO-claimed domains and already-consumed trials); service
creation happens live at the top of the session. Organizer fallback: a workshop org
with a pool of pre-provisioned services (sized at 15-20 percent of attendance,
pre-warmed) created via `clickhousectl` with org API keys, distributed on paper slips.
Pool users share one org, so their MCP setup includes the `x-service-id` header
(section 2.2).

**D3 — Keep the payments app as the SRE workload; it is the only local component.**
Docker is already mandatory in the brief. The app pulls stock upstream images (the
reskin is env-var-only), publishes one port, and ships telemetry to the attendee's own
Cloud service. Slim the repo to the single supported compose path (L3) and evaluate
disabling Playwright browser traffic in the load generator (saves 0.5 GB download and
1.5 GB RAM) after verifying telemetry stays rich enough for the hero scenario.
Docker-less attendees are first-class, not spectators: see the shared incident service
in section 10.

**D4 — GenBI track: attendees prompt their agent to complete a pre-scaffolded BI app.**
Target: a Next.js app that takes a natural-language question, has an LLM generate
ClickHouse SQL grounded in the actual schema, executes it read-only via the official
client, and renders a chart (ECharts) — always showing the generated SQL. To fit the
time box and avoid a synchronized npm storm, the repo ships a pre-scaffolded skeleton
(`genbi/skeleton/`, dependencies installed by `prework.sh`); the agent's build job is
the interesting 20 percent — the NL-to-SQL endpoint, schema grounding, and chart
wiring — driven by `genbi/SPEC.md`. A Streamlit variant ships for attendees who prefer
Python (organizer announces the default track in the prework email; prework verifies
the matching runtime).

Runtime LLM key: the app calls an LLM at runtime, which the coding agent's own auth
does not cover. Attendees bring their own Anthropic or OpenAI API key (prework item;
the spec reads a provider-agnostic `LLM_API_KEY`/`LLM_BASE_URL` pair); the organizer
carries a small pool of spend-capped fallback keys, revoked after the session.

Datasets: UK property price paid (28M rows, loads into a trial in ~2 minutes via
`INSERT ... SELECT FROM url(...)`, natural business questions) as the primary; the
attendee's own live telemetry tables as the finale (see run of show — the GenBI app
answering questions about the incident they caused in Lab 2 is the moment that unifies
the two builds). Guardrails baked into the spec: dedicated read-only DB user,
`readonly=1` per query, `max_result_rows`/`max_execution_time`, schema introspection
plus few-shot NL-to-SQL pairs in the system prompt, LLM emits a chart spec as
structured JSON rather than free-form code.

**D5 — Notifications: Telegram is core; the shared Slack channel is the room-wide payoff.**
Telegram is fully self-serve: BotFather bot in ~2 minutes, one curl to send — no shared
secrets, no workspace admin, no single point of failure across 30 laptops. In Lab 2,
every attendee's notifier posts to their own Telegram AND to one pre-created shared
Slack channel on the projector: thirty independently-generated agent incident reports
scrolling on the big screen, each slightly different, all converging on `card-auth` and
gold-tier. That is the engineered peak of the session. The Slack webhook variant also
serves attendees on networks that block Telegram. Under time pressure, the headless
watch loop (the stretch) is cut — the notifier itself is not.

**D6 — clickhousectl for what its OAuth actually allows.**
`curl https://clickhouse.com/cli | sh`, installed during prework, then
`clickhousectl cloud auth login` (browser OAuth). Verified constraint: browser OAuth
tokens are READ-ONLY — create/modify operations require org API keys, which attendees
will not have. So attendee-facing use is: auth, service inspection, running queries,
and installing the official agent skills (`clickhousectl skills`, which installs the
`ClickHouse/agent-skills` content — same skills, one installer — into 15+ agents
including all four target tools). ClickPipes are created in the Cloud console UI, not
the CLI. Organizer-side provisioning (fallback pool) uses org API keys and is
unaffected. clickhousectl is beta: pin the version in prework.sh and re-validate at
each dry run; verify `cloud service query` works under read-only OAuth (open item 5).

**D7 — "ClickHouse Docs MCP": flag to resolve.**
No hosted docs MCP was verifiable as of 2026-07-02 (`mcp.clickhouse.com` does not
resolve; nothing in the MCP docs index). Note the distinction to avoid confusion: the
verified hosted endpoints at `mcp.clickhouse.cloud` (`/mcp` general, `/clickstack`
observability — see D9) are data/platform MCPs, not documentation MCPs. The
docs-knowledge role is filled today by the official `ClickHouse/agent-skills`
(best-practices, architecture-advisor) installed into each attendee's coding agent,
plus the Ask AI agent in the Cloud console. Action: confirm internally whether a hosted
docs MCP exists or is imminent; if yes, add it to the MCP setup step; if no, the
brief's intent is covered by skills.

**D8 — Everything is prompt-driven, with one open step per lab.**
Each lab step is: paste a provided prompt into your coding agent, watch it work, verify
a checkpoint. Copy-paste prompts keep outcomes consistent across four different tools.
But every lab also contains exactly one deliberately open step where the attendee
authors the prompt themselves — Lab 1: ask your own analytical question; Lab 2: ask
your own follow-up about the incident (blast radius, timeline, customer impact);
Lab 3: find a question your app answers wrongly, then fix it through the spec. Those
are the ownership moments. Every lab has a checkpoint ("you should now see X") defined
on data states, not agent behavior, and an escape hatch (reference implementation or
answer-key query) so nobody is stranded.

**D9 — One MCP endpoint per lab that needs it, not all endpoints up front.**
Two hosted endpoints exist: `mcp.clickhouse.cloud/mcp` (general: `run_select_query`,
`list_databases`, `list_tables`, org/service introspection — 13 read-only tools) and
`mcp.clickhouse.cloud/clickstack` (observability investigation primitives). Configuring
both in Lab 0 doubles the OAuth burden at the most fragile moment. Lab 0 configures
only the general `/mcp` endpoint (serves Lab 1 and the GenBI build); Lab 2 opens by
adding `/clickstack` (same identity, already signed in — fast). Per-tool docs include
the exact auto-approve/allowlist configuration for these MCP servers so the
investigation is not interrupted by a dozen permission prompts; the Lab 0 checkpoint is
explicitly "your agent ran a query WITHOUT asking for approval."

---

## 5. Target architecture (workshop)

```
ATTENDEE LAPTOP                                  CLICKHOUSE CLOUD (per attendee, trial)
+--------------------------------------+         +----------------------------------+
| Payments app (Docker, ~20 containers)|--OTLP-->| ClickStack collector -> service  |
|   flagd fault injection (localhost)  |  HTTPS  |   otel_logs/traces/metrics       |
|                                      |         |   + genbi database (UK property) |
| Coding agent (Claude Code / Cursor / |         |   HyperDX UI                     |
|  Codex / Windsurf)                   |<--MCP-->| Remote MCP (OAuth):              |
|   + official ClickHouse agent skills |         |   /mcp        (Labs 0,1,3)       |
|   + SRE investigator skill (ours)    |         |   /clickstack (Lab 2)            |
|                                      |         +----------------------------------+
| GenBI app (agent-completed skeleton) |----------> same service, read-only user
|   Next.js/Streamlit + official client|            (runtime LLM key: attendee's own)
|                                      |----------> Telegram bot API (own phone)
| clickhousectl (read-only OAuth:      |----------> shared Slack channel (projector)
|   auth, query, skills install)       |
+--------------------------------------+
```

Local components: the payments app (the system under observation — Docker is mandatory
in the brief for exactly this) and the app the attendee builds. Everything stateful is
hosted ClickHouse: Cloud service, ClickStack/HyperDX, ClickPipes, remote MCP. The
documented OAuth-failure fallback is the official `mcp-clickhouse` package run locally
over stdio against the attendee's service — same tool surface, no OAuth (also the
Docker-less path's access route, section 10).

---

## 6. Run of show — 3 hours

Design principles applied: fire-and-wait operations start before the talk (service
provisioning and container startup run underneath it); the first checkpoint lands by
0:35; Lab 3 has a hard pivot time — at 2:10 the room moves regardless, with the Lab 2
stretch as the pre-designated cut.

| Time | Module | What happens | Checkpoint |
|---|---|---|---|
| 0:00-0:05 | Cold open | 3 minutes of the endgame on the projector: the instructor's agent investigates the incident live, the Telegram ping arrives, the Slack channel fills. "In three hours, yours does this." | — |
| 0:05-0:15 | Everyone starts their stack | Create Cloud service (ClickStack selected); COPY THE SERVICE PASSWORD NOW (shown once); paste 3 vars into `app/.env`; `docker compose up` (images pre-pulled); flip the Connect -> MCP toggle. Then leave it all running. Roamers sweep the room | Containers starting; service provisioning |
| 0:15-0:35 | Talk: ClickHouse and the agentic data stack | What ClickHouse is; real-time analytics, observability, AI/ML, warehousing; why agents want full-fidelity data — delivered while services provision, containers start, and telemetry warms | — |
| 0:35-0:55 | Lab 0: Wire up your agent | `clickhousectl cloud auth login` (installed in prework); add the `/mcp` remote MCP to your coding agent (config text pre-staged in prework where the tool allows) + OAuth; apply the per-tool auto-approve config | Agent lists databases and queries `otel_traces` WITHOUT a permission prompt |
| 0:55-1:20 | Lab 1: Feel the fundamentals | Agent loads UK property (28M rows, ~2 min via `url()`); THE moment: the same aggregation as a full scan vs an ORDER BY-aligned query, `EXPLAIN` + rows-read shown — sub-second over 28M rows is the point; create one ClickPipe in the console UI from the public sample bucket; open step: ask your own analytical question | UK table queryable; full-scan vs primary-key contrast observed; ClickPipe running |
| 1:20-1:25 | Break | Telemetry baseline now 70+ minutes deep | — |
| 1:25-2:10 | Lab 2: Agentic SRE | 5-min interstitial: why agents + full-fidelity telemetry (moved from the talk — lands better here); add the `/clickstack` MCP; install the SRE investigator skill (per-tool path); flip `paymentFailure` to 75 percent at `localhost:8080/feature`; ask: "checkout is throwing 5xx for some users — root cause?"; agent pivots to `card-auth` + gold-tier isolation; open step: your own follow-up question; then the agent builds the Telegram notifier and posts its incident report to your Telegram AND the shared Slack channel on the projector. Stretch (first cut): headless watch loop (`claude -p`), or start your Lab 3 build early | Root cause + gold-tier "aha" reproduced; your incident report visible on the projector |
| 2:10-2:50 | Lab 3: GenBI (hard pivot at 2:10) | Feed `genbi/SPEC.md` to your agent against the pre-scaffolded skeleton; it builds NL-to-SQL + charts on UK property (read-only user, SQL always visible); finale checkpoint: point it at your telemetry tables and ask "which service had the highest error rate in the last hour?" — a question whose answer you personally caused; open step: find a wrong answer, fix it via the spec | App charts a UK property question AND answers the incident question over your own telemetry |
| 2:50-3:00 | Wrap | 2-3 volunteers ask their GenBI apps questions live on the projector; reset the fault flag; take-home: swap in YOUR telemetry and YOUR tables (`labs/99-take-home.md`), your trial runs 30 more days; pointers: ClickHouse Agents beta, managed ClickStack MCP | — |

Additional facilitation notes:
- Doors open 30 minutes early for a staffed setup clinic — this is official (in the
  runbook and PREWORK.md), not an informal buffer. Anyone whose `prework.sh` did not
  print PASS is told to arrive early.
- The fault is injected ~70 minutes after telemetry starts, so onset detection has a
  deep clean baseline even if the opening slips. The answer-key window is
  parameterized; the runbook keys it to actual compose-up times.
- Labs 1-3 each have a "done early?" stretch and an escape hatch, absorbing skew
  across 30 attendees.
- Marketing alignment: the blurb's "master the fundamentals" overpromises for a
  25-minute module — Lab 1 is built around experiencing what makes ClickHouse fast;
  recommend softening the blurb or pointing it at the take-home doc for depth.

---

## 7. Target repo structure (`build_ai_with_ai/`)

```
build_ai_with_ai/
  README.md                  Landing page: what you build, agenda, image list, quick links
  PREWORK.md                 Mandatory pre-arrival setup (section 9); platform stance
                             (macOS/Linux native, Windows via WSL2) stated on line one
  prework.sh                 Verifier + pre-puller + GenBI dependency install; prints
                             PASS/FAIL per check; cross-platform (bash + WSL2; checks
                             total RAM, Docker allocation, ports, versions)
  app/                       Payments demo, slimmed: compose.yaml only, committed .env,
                             digest-pinned images, flags reset, upstream CI removed
  labs/
    00-setup.md              Service + ClickStack + MCP toggle + agent wiring
    01-fundamentals.md       Dataset load, full-scan vs primary-key moment, ClickPipe
    02-agentic-sre.md        Fault injection, investigation, notifier
    03-genbi.md              Complete the GenBI skeleton from the spec
    99-take-home.md          Leads with "swap in your own telemetry" (collector config
                             pointed at your staging OTLP) and "swap in your own tables"
                             (GenBI); then extensions: headless monitoring, ClickHouse
                             Agents beta, managed ClickStack MCP deep dive
  agents/
    claude-code.md           Per-tool setup: exact MCP add commands, OAuth notes,
    cursor.md  codex.md      auto-approve/allowlist config, and the install path for
    windsurf.md              the SRE investigator skill in THAT tool's native format
  skills/
    sre-investigator/        Tool-neutral markdown (single canonical source), ported
      SKILL.md               from agent/sre-agent-definition.md: methodology, schema
      references/queries.md  knowledge, gotchas; answer-key queries as reference
  genbi/
    SPEC.md                  The spec/prompt attendees feed their agent
    skeleton/                Pre-scaffolded Next.js app (deps installed by prework.sh)
    skeleton-streamlit/      Python variant (requirements.txt)
    reference-app/           Instructor-built escape hatch (complete implementation)
  notify/
    telegram.md              BotFather -> token -> curl pattern (core path)
    slack.md                 Webhook variant + the shared projector channel setup
  instructor/
    RUNBOOK.md               Minute-by-minute run of show, talk track, clinic plan,
                             hard-pivot rules, projector demos
    provisioning.md          Fallback service pool via clickhousectl + org API keys;
                             x-service-id requirement for pool users; shared incident
                             service for Docker-less attendees
    fallbacks.md             Wifi/venue network spec, OAuth failures (mcp-clickhouse
                             stdio fallback), trial-failure playbook, corporate-laptop
                             playbook (Docker-less path, Telegram-blocked path)
    validation.sh            Parameterized hero-investigation answer key (from queries/)
```

The original `agentic-sre` repo stays private as-is; `build_ai_with_ai` is assembled
fresh from it so no internal history (hostnames, AWS accounts, customer narrative)
leaks into the public repo.

---

## 8. Build plan

**Phase 0 — Logistics track (runs alongside everything).**
Slide deck for the opening talk and Lab 2 interstitial (ClickHouse-brand HTML deck).
Shared Slack workspace + projector channel + webhook. Organizer LLM fallback key pool
(spend-capped). Registry mirror or USB image bundles. Venue network requirements
document (~100 Mbps symmetric, no captive portal, OAuth-friendly). Fallback service
pool plan (15-20 percent of attendance) plus the shared incident service for
Docker-less attendees (organizer-owned service with the app streaming and the fault
injectable by the instructor).

**Phase 1 — Repo surgery (foundation).**
Assemble `build_ai_with_ai/app` from `agentic-sre/app`: fix L1-L9 (commit safe `.env`,
reset `paymentFailure` to off, delete broken compose overlays and Makefile targets or
reduce to compose.yaml-only, pin by digest, reconcile env var names, default the LLM
feature to the bundled mock, strip upstream CI). Scrub every internal identifier.
Decide mirroring (recommended: mirror the pinned images to a ClickHouse-owned public
registry namespace so upstream tag churn cannot break the workshop); publish the final
image list in README.md. Evaluate disabling load-generator browser traffic (D3).

**Phase 2 — Agent assets.**
Port `agent/sre-agent-definition.md` to `skills/sre-investigator/` as tool-neutral
markdown (drop the LibreChat framing; keep methodology, schema knowledge, gotchas; fold
the 8-query pack into `references/`), with four documented install targets in
`agents/*.md` — Claude Code skill, Cursor rules, Codex AGENTS.md, Windsurf rules.
Write the four per-tool setup docs with exact commands (verified today:
`claude mcp add --transport http clickhouse-cloud https://mcp.clickhouse.cloud/mcp`;
`codex mcp add ...`; Windsurf via `mcp-remote` shim; Cursor via marketplace), OAuth
walkthroughs, and auto-approve/allowlist configuration per tool (D9). Wire
`clickhousectl skills` for the official skills. Validate the ClickStack MCP tool list
against the skill text and confirm the managed ClickStack schema matches
`default.otel_traces` as the queries assume. Document the `mcp-clickhouse` stdio
fallback.

**Phase 3 — New tracks.**
GenBI: write `genbi/SPEC.md` (NL input, schema grounding, read-only execution, ECharts,
SQL always visible, telemetry tables as a second source); build the skeleton (and
Streamlit variant) and the reference app; script the read-only user creation;
pre-validate the UK property `url()` load and the console ClickPipe from the public
sample bucket on a fresh trial. Notifications: write telegram.md and slack.md, the lab
prompt that has the agent wire its own incident report to both targets, and the
headless watch-loop stretch (`claude -p` + curl).

**Phase 4 — Docs.**
Write the five labs, PREWORK.md, prework.sh (cross-platform: macOS, Linux, WSL2),
README.md, and the instructor runbook (talk track, clinic plan, hard-pivot rules, all
fallback playbooks). Every lab step gets a copy-paste prompt, a checkpoint, an escape
hatch — and each lab its one open step (D8).

**Phase 5 — Validation (gate).**
Full dry run on a fresh ClickHouse Cloud trial + fresh laptop profile, including one
Windows/WSL2 machine: prework timing, Lab 0-3 timing against the run of show, all four
coding agents through the MCP OAuth flow AND each reproducing the hero investigation
via its own skill-install path, GenBI completed from the spec by an agent cold.
Measure real per-seat OTLP upstream bandwidth and record it in the runbook. Verify
`clickhousectl cloud service query` under read-only OAuth. Then a second timed
rehearsal with 2-3 colleagues as mock attendees. Fix, re-run, freeze.

---

## 9. Attendee prework (mandatory, sent 1 week + 1 day before)

Platform stance up front: macOS or Linux natively; Windows via WSL2 (with Docker
configured for WSL2). Company policy note: Docker Desktop requires a paid license at
companies over 250 employees — OrbStack, Colima, or Rancher Desktop all work with the
standard compose file.

1. Laptop with 16 GB+ total RAM and 10 GB disk free; Docker installed and allocated
   >= 6 GB.
2. Clone the workshop repo; run `prework.sh` — it pre-pulls all images (~1.8 GB),
   installs clickhousectl (pinned version), installs GenBI skeleton dependencies, and
   verifies Docker, RAM allocation, versions, and ports. It must print PASS. If it
   does not, arrive 30 minutes early for the setup clinic.
3. An agentic coding tool installed and working (Claude Code, Cursor, Codex, or
   Windsurf) with an ACTIVE plan or credits (a 3-hour agentic session is heavy), and
   the ability to add MCP servers (some enterprise deployments lock this down — test
   now, not on the day): the per-tool doc includes a 60-second smoke test — add a
   public MCP server and call one tool. Pre-stage the workshop MCP config text where
   your tool supports configuration before OAuth.
4. Create a ClickHouse Cloud account and CONFIRM you can see the $300 trial credits in
   the console (work emails already attached to an existing ClickHouse org may not get
   a trial — use a personal email if so). Account only; the service is created together
   in the session.
5. An LLM API key (Anthropic or OpenAI) for the app you will build in Lab 3. Spend
   expectation: well under $5.
6. Node.js 20+ (or Python 3.11+ if the organizer announced the Streamlit track).
7. A Telegram account on your phone (or plan to use the Slack fallback).
8. A working email address you can access during the session.

---

## 10. Risks and fallbacks

| Risk | Likelihood | Mitigation |
|---|---|---|
| Attendees skip prework; 55-60 GB of pulls plus a synchronized npm burst on venue wifi | High | Mandatory prework email with PASS-printing verifier; GenBI deps installed by prework.sh; on-site registry mirror or USB bundles; official 30-min setup clinic before doors |
| Venue network: thin uplink or captive portal breaks OAuth and OTLP streaming | Medium | Venue requirements doc in Phase 0 (~100 Mbps symmetric, no captive portal); per-seat OTLP bandwidth measured in dry run; mcp-clickhouse stdio fallback avoids OAuth entirely |
| Trial signup fails: card policy, work email in an existing org, anti-abuse throttling of 30 accounts from one venue IP | Medium | Prework checkpoint is "credits visible", surfacing failures a week early; personal-email guidance; fallback pool sized 15-20 percent, pre-warmed; escalate the card question (section 11) |
| Lab 0 overruns (multiple OAuth flows, lost service passwords, novice variance) | Medium | Fire-and-wait steps moved before the talk; one MCP endpoint only in Lab 0; clickhousectl installed in prework; "copy the password NOW" called out in bold; roamers; first checkpoint targeted at 0:35 |
| MCP OAuth flow flaky on some coding tools | Medium | Per-tool docs dry-run tested on all four; fallback: official `mcp-clickhouse` run locally over stdio against their service — same tools, no OAuth |
| Agent permission-prompting derails the investigation (a dozen approval clicks mid-flow) | High if unhandled | Per-tool auto-approve/allowlist config applied in Lab 0; checkpoint explicitly verifies promptless query execution |
| Remote MCP rate limits at 30 concurrent users | Unknown (undocumented) | Each attendee is their own trial org/service so load is distributed; validate in dry run; confirm with product |
| Corporate laptops: Docker forbidden, Telegram blocked, MCP additions locked down | Medium | Docker-less attendees run the FULL investigation against the organizer's shared incident service (read-only user via mcp-clickhouse, or org invite + x-service-id) — they keep the investigation, the skill, the notifier, and GenBI, losing only fault-flipping; Slack webhook covers Telegram blocks; MCP lockdowns surface in the prework smoke test |
| Attendee's coding tool runs out of credits mid-session | Medium | Prework requires an active plan/credits with a stated expectation; pairing as last resort |
| Venue/downstream outage of ghcr.io or Cloud console | Low | Mirrored images; pre-provisioned service pool; labs are independent enough to reorder |
| Agent variance across four tools | Certain | Copy-paste prompts tuned per lab; checkpoints on data states, not agent behavior; per-tool skill installs validated in Phase 5; instructors roam |
| Timing overrun threatens the second build (GenBI) | Medium | Hard pivot at 2:10 is a stated rule; the Lab 2 stretch (headless loop) is the pre-designated cut — the Telegram notifier is core and is NOT cut; early finishers may start their GenBI build during Lab 2 stretch time |
| Beta tooling churn (clickhousectl, ClickStack MCP — both 2026 betas) | Medium | Pin versions; re-validate both at every dry run; mcp-clickhouse and console UI as stable fallbacks |

---

## 11. Open items to verify before the dry run

1. Hosted Docs MCP: does one exist or is one imminent? (None verifiable today — D7.)
2. Trial signup: credit-card requirement (third-party sources say no card; unverified
   officially); typical service provisioning time; whether the MCP toggle and ClickPipes
   are available on trial without restriction (no restriction documented, unverified
   explicitly); anti-abuse behavior for ~30 signups/logins from one venue IP.
3. Remote MCP rate limits (none documented) — confirm with product for 30 concurrent
   first-time OAuth flows on venue wifi.
4. Managed ClickStack schema: confirm table/database names match `default.otel_traces`
   etc. as all queries and the SRE investigator skill assume.
5. clickhousectl (beta): pin a version; verify `cloud service query` works under
   read-only browser OAuth (create operations confirmed to require org API keys);
   confirm `clickhousectl skills` install paths on all four target tools.
6. Whether disabling load-generator browser traffic (saves 0.5 GB / 1.5 GB RAM) keeps
   the telemetry rich enough for the hero investigation.
7. Slack workspace policy: pre-create the shared workspace, confirm webhook creation is
   allowed under the org's Slack plan, and test the projector channel flow.
8. Marketing blurb alignment: "master the fundamentals" vs the 25-minute Lab 1
   (section 6 note) — agree the wording or extend the module.
