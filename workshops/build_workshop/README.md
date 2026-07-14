# ClickHouse BUILD Workshop ("Build AI with AI")

A three-hour, hands-on workshop: participants use their own agentic coding tool to take
an NYC-taxi ride-hailing analytics app end to end on ClickHouse Cloud — CDC ingestion
with ClickPipes, conversational BI with ClickHouse Agents, observability with ClickStack
(including an AI-built SRE dashboard), an in-app AI chat traced to Langfuse Cloud, and a
break-and-fix finale diagnosed by an AI SRE.

Work on this workshop happens on the `build-workshop-v1` branch.

## Layout

| Path | What |
|---|---|
| `app/` | The foundation app participants run locally: React frontend, FastAPI backend, Postgres + data generator, ClickStack OTel overlay. Workshop entrypoint: `docker-compose.workshop.yml` + `.env.workshop.example`. See `app/WORKSHOP_CHANGES.md`, `app/CHAT_FEATURE.md`, `app/OBSERVABILITY.md`. |
| `playbook/` | The published follow-along playbook (Next.js + Fumadocs; dual learner/instructor tracks; deploys to demohouse.cloud/workshop). Requires Node >= 22.12 to build. |
| `docs/` | `WORKSHOP_PLAN_V2.md` — the workshop plan: architecture, agenda, decisions, and pre-delivery verification gates. |
| `infra/` | Instructor provisioning via clickhousectl: shared Postgres + demo ClickHouse service + CDC pipe, end to end (`provision_workshop_stack.sh e2e`), plus per-participant slots and teardown. |

## Fault branches (module 08, break and fix)

Cut from `build-workshop-v1`, each a single innocent-looking commit touching one file
under `app/`:

- `fault/01-map-not-loading`
- `fault/02-zone-stats-500`
- `fault/03-slow-dashboard`

The learner playbook lists only the branch names; symptoms, diagnosis paths, and fixes
live in the playbook's instructor track (module 08) and are deliberately not documented
in this directory.

## Quick start (participant)

```bash
git clone -b build-workshop-v1 <this-repo>
cd ClickHouse_Demos/workshops/build_workshop/app
cp .env.workshop.example .env.workshop   # fill in your ClickHouse Cloud values
docker compose --env-file .env.workshop -f docker-compose.workshop.yml up -d
```

Then follow the playbook from module 00.
