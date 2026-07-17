# Workshop infrastructure — instructor demo stack + FALLBACK pool

**Primary participant path: each participant creates their OWN Postgres managed by
ClickHouse in their own trial org (module 03), not a shared instance.** One participant
needs exactly one replication slot, and every managed instance ships `wal_level=logical`
with 10 replication slots by default, so the "raise `max_replication_slots`" gate that
dominates this script does NOT apply to participants. It applies only to the shared
fallback pool below (many pipes on one instance).

This script is therefore two things:

1. The **instructor demo stack** — an end-to-end `e2e` run (managed Postgres -> demo
   ClickHouse service -> CDC ClickPipe -> verified sync) that is also the pre-event dry
   run of the flow participants build by hand.
2. A **shared FALLBACK Postgres pool** — a per-participant database + role + publication,
   handed out on slips to anyone whose org cannot create a managed Postgres (beta
   availability varies).

`provision_workshop_stack.sh` stands up the chain with clickhousectl (chctl): the
**Postgres** the demo/fallback ClickPipes read from, a demo **ClickHouse** Cloud service,
and the **CDC ClickPipe** between them — then proves data is syncing end to end.

```
managed Postgres (demo + FALLBACK)          demo ClickHouse Cloud service
  taxi_p00 (demo slot)  --ClickPipes CDC-->  nyc_tlc_data.realtime_trips
  taxi_p01 ... taxi_pNN (fallback: one DB + role + publication per participant)
```

## Prerequisites

- `clickhousectl` 0.3.x (`curl https://clickhouse.com/cli | sh`) and an org API
  key (write operations require API keys; browser OAuth is read-only):

  ```bash
  export CLICKHOUSE_CLOUD_API_KEY=...
  export CLICKHOUSE_CLOUD_API_SECRET=...
  ```

- `psql` (`brew install libpq && brew link --force libpq`), `openssl`, `python3`.

## The short version

```bash
cd workshops/build_workshop/infra

./provision_workshop_stack.sh e2e
# The e2e run pauses at each point where a chctl create prints a one-time
# credential or an id you must export (ADMIN_PGHOST/ADMIN_PGPASSWORD +
# PG_SERVICE_ID, then CH_SERVICE_ID, then PIPE_ID). Re-run e2e after each
# export; completed steps are skipped or are idempotent.

# When e2e prints "E2E COMPLETE", provision the shared FALLBACK slots (only needed
# for participants who cannot create their own managed Postgres):
PARTICIPANTS=35 ./provision_workshop_stack.sh provision

# Need more capacity on the day? START_SLOT (default 1) adds slots without
# re-touching lower ones (existing passwords are kept either way):
START_SLOT=36 PARTICIPANTS=50 ./provision_workshop_stack.sh provision
```

## Subcommands

| Stage | Command | What it does |
|---|---|---|
| PG | `create-pg` | Create the Postgres service managed by ClickHouse (beta). Admin password prints once; the create response is the source of truth for id/host/password |
| PG | `wait-pg` | Poll real connectivity (psql SELECT 1) until the instance is up — do not rely on the beta status API |
| PG | `configure-pg` | Patch runtime config: slots/senders/connections/WAL cap (targets below). Tolerates the beta FORBIDDEN gap with console/support guidance |
| PG | `verify-pg` | SHOW the settings, fail if below targets, list live slots + retained WAL |
| Slots | `provision` | Fallback slots `START_SLOT`..N (default 1..N): database + login role (with REPLICATION) + CDC table + pre-created publication. Idempotent; existing passwords kept. Raise `START_SLOT` to add capacity later without re-touching lower slots (e.g. `START_SLOT=36 PARTICIPANTS=50`) |
| Slots | `provision-demo` | Slot 00, used by the demo pipe |
| Slots | `slips` | Regenerate the printable per-participant hand-outs from the CSV |
| CH | `create-ch` | Create the demo ClickHouse service (1 x 8 GB, idle-scaling) |
| CH | `schema` | Apply `app/db/cloud/001_cloud_schema.sql` (base tables + views; clickhouse client when CH_HOST/CH_PASSWORD are set, else the chctl Query API). Applies cleanly on a fresh service; the CDC MV is separate — `create-mv` after `wait-pipe`, or `app/db/cloud/003_cdc_mv.sql` by hand |
| CDC | `create-pipe` | Postgres CDC ClickPipe: demo slot -> demo service, using the pre-created publication. CLI pipes land the destination table in the `default` database |
| CDC | `wait-pipe` | Poll pipe state until running; report where the destination table landed |
| CDC | `create-mv` | Detect the pipe's actual destination table and create the MV from it into `nyc_tlc_data.taxi_trips` |
| CDC | `verify-sync` | Insert marker rows into Postgres, poll ClickHouse until they arrive, then check the MV fan-out into `taxi_trips` |
| All | `e2e` | The whole chain in order, pausing wherever an id or one-time credential must be exported |
| Down | `teardown` | Drop participant slots: leftover replication slots, databases, roles |
| Down | `delete-pipe` / `delete-ch` / `delete-pg` | Delete the demo pipe, demo service, shared Postgres |

## Postgres targets — shared FALLBACK pool only

These matter only for the shared fallback pool (many pipes on one instance). A
participant's own managed Postgres needs none of this beyond the `wal_level=logical`
default. `configure-pg` applies all five; `verify-pg` FAILs on only **three**
(`wal_level`, `max_replication_slots`, `max_wal_senders`) and merely displays the other
two.

| Setting | Value | `verify-pg` | Why |
|---|---|---|---|
| `wal_level` | logical | FAIL if not | required for CDC (the managed default is already logical) |
| `max_replication_slots` | 50 | FAIL if `<` | one slot per fallback pipe + resync headroom (a resync creates a new slot) |
| `max_wal_senders` | 50 | FAIL if `<` | must be >= slots |
| `max_connections` | 300 | shown only | ~30 concurrent ClickPipes initial snapshots use 4-6 connections each |
| `max_slot_wal_keep_size` | 20480 MB | shown only | intended WAL safety valve — but see the day-of caveat: on live managed instances the patch is blocked and this reads back as `-1` (unlimited), so the valve may not be active |

Raising slots/senders on a managed instance is a **known beta gap**: the config patch
returns FORBIDDEN and `ALTER SYSTEM` is blocked, so a default instance FAILs `verify-pg`
at 10 slots. That is fine for the demo/e2e run and for participants' own instances (one
slot each); for the shared fallback at 30+ participants, raise them via the Cloud console
Postgres Settings tab or ClickHouse support. `verify-pg` prints this same hint on failure.

## Outputs

`infra/out/` (git-ignored, mode 600):

- `participants.csv` — one row per slot: database, user, password, publication
- `slips.txt` — printable hand-outs: the `.env.workshop` PG block, the ClickPipe
  wizard values, and an optional chctl one-liner per participant

## Using another Postgres provider (RDS, ...)

Skip `create-pg`/`configure-pg`; create the instance there and apply the same
targets (RDS: custom parameter group + `rds.logical_replication=1`). Use the DIRECT
endpoint — poolers are not supported for CDC. Then export the `ADMIN_PG*`
variables and use `provision`/`verify-pg`/`create-pipe`/`verify-sync`/`teardown`
unchanged.

## Live e2e results (run 2026-07-14 against a real org)

The full chain was executed end to end: managed Postgres created via chctl,
demo slot provisioned, ClickHouse service created, schema applied, CDC pipe
created, and 5 marker rows inserted into Postgres arrived in ClickHouse within
one sync interval and were fanned into `taxi_trips` by the MV. Teardown
verified (pipe, slot, demo databases, service). Findings now baked into the
script:

1. Valid managed-Postgres sizes are instance-type names (m8gd.large default
   now; m7i.* is NOT valid — the API returns the allowed list on error).
2. `postgres get/list/config patch/delete` returned FORBIDDEN or empty on the
   test org (beta entitlement gap) even though `create` works. The create
   response is the source of truth for id/host/password; `wait-pg` polls real
   connectivity instead of the API. `ALTER SYSTEM` is blocked in the managed
   environment.
3. Instance defaults observed: `wal_level=logical` (CDC-ready out of the box),
   `max_connections=500`, but `max_replication_slots=10` and
   `max_wal_senders=10` — plenty for one participant's OWN instance (the primary
   path: one slot each), and enough for the demo/e2e, but NOT for a shared instance
   feeding 30+ pipes. Raising slots requires the console Settings tab or ClickHouse
   support; this gate now applies only to the shared FALLBACK pool (else RDS for
   the fallback).
4. The chctl Query API (`cloud service query`) fails on orgs not migrated to
   Custom Roles ("Use 'roles' instead of 'assignedRoleIds'"). The script
   prefers `clickhouse client` whenever CH_HOST + CH_PASSWORD are exported.
5. CLI-created pipes ALWAYS land the destination table in the `default`
   database; `--table-mapping` does not accept a database qualifier (a
   qualified name becomes a literal table name). Only the console wizard
   offers destination-database choice. `create-mv` therefore detects the
   actual location and wires the MV to it.
6. The destination engine observed was plain (Shared)MergeTree ORDER BY id
   with `_peerdb_synced_at DateTime64(9)`, `_peerdb_is_deleted UInt8`,
   `_peerdb_version UInt64` (docs describe ReplacingMergeTree — the console
   path may differ; the MV filters `_peerdb_is_deleted = 0` and works with
   both; no FINAL is used anywhere).
7. Pipe lifecycle: Provisioning -> Running in about 2-3 minutes for a tiny
   table; `clickpipe delete` can print "Internal error" yet still succeed
   (confirm with `clickpipe list`); deleting a pipe DOES drop its replication
   slot on the source.
8. A running ClickHouse service cannot be deleted (CONFLICT) — `delete-ch`
   now stops it, waits for `stopped`, then deletes.
9. End-to-end latency observed: marker rows visible in ClickHouse within
   ~60 seconds (the default sync interval).

## Day-of and teardown

- Morning of (fallback pool only): `verify-pg` (settings + live slots + retained WAL per
  slot).
- A fallback participant whose pipe stalls just resyncs it. CAVEAT: the
  `max_slot_wal_keep_size` safety valve may NOT be active — on live managed instances the
  config patch is blocked and the value reads back as `-1` (unlimited), so a badly stalled
  slot can retain WAL without a cap. Watch the retained-WAL column in `verify-pg` and drop
  invalidated slots manually if needed.
- After the event: participants on the primary path delete their own managed Postgres
  (module 09 / `clickhousectl cloud postgres delete`). For the instructor stack:
  participants delete their pipes, then `teardown`, then
  `delete-pipe`/`delete-ch`/`delete-pg` as desired. Destroy `infra/out/` and printed
  slips — they contain credentials.
