# Retail release evidence

Local code and curriculum can be reviewed without calling every external gate passed. The authoritative results are `retail-rehearsal.json` and `retail-agent-evaluation.json`.

- [x] SQL/business arithmetic, stale approval and delivery fault tests.
- [x] Full local unit, SQL, browser, site and OTLP checks.
- [x] Actual container browser → API → databases → paired observed chart.
- [x] 100M retail dataset generated and independently checked.
- [x] Prepared model covers 80 groups and passes held-out checks.
- [x] Separate Cloud service, managed Postgres and operational ClickPipe provisioned.
- [x] Full 100M direct load, bounded backfill, and exact manual aggregate registration (S3 remains separate).
- [x] Actual Cloud CDC delete/restore and duplicate insert/MV checks.
- [ ] Immutable S3 publication and anonymous access.
- [ ] Exact learner S3 SQL load and load duration.
- [x] Sustained observed rate and raw/aggregate p50/p95 on the full Cloud workload.
- [ ] Actual managed-agent identity, limits and six conversations.
- [x] Actual Cloud browser approval, observed sales API, and correlated receipt log in telemetry tables.
- [ ] Managed ClickStack UI receipt search.
- [ ] Windows WSL2 execution.
- [ ] Full timed learner run and costs.
- [ ] Separate rehearsal resource cleanup.

Do not advertise a measured throughput, speedup, fully verified agent workflow, or release readiness while its required evidence is missing. Keep this checklist out of learner instructions.
