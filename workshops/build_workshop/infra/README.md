# Workshop infrastructure provisioning (instructor)

`provision_workshop_stack.sh` stands up the full chain with clickhousectl (chctl):
the shared **Postgres** every participant's CDC ClickPipe reads from, a demo
**ClickHouse** Cloud service, and the **CDC ClickPipe** between them — then proves
data is syncing end to end. The same chain participants build by hand during the
workshop, so running `e2e` before the event is also the dry run of the whole flow.

```
shared managed Postgres            demo ClickHouse Cloud service
  taxi_p00 (demo slot)  --ClickPipes CDC-->  nyc_tlc_data.realtime_trips
  taxi_p01 ... taxi_pNN (one DB + role + publication per participant)
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

# When e2e prints "E2E COMPLETE", provision the participant slots:
PARTICIPANTS=35 ./provision_workshop_stack.sh provision
```

## Subcommands

| Stage | Command | What it does |
|---|---|---|
| PG | `create-pg` | Create the Postgres service managed by ClickHouse (beta). Admin password prints once; the create response is the source of truth for id/host/password |
| PG | `wait-pg` | Poll real connectivity (psql SELECT 1) until the instance is up — do not rely on the beta status API |
| PG | `configure-pg` | Patch runtime config: slots/senders/connections/WAL cap (targets below). Tolerates the beta FORBIDDEN gap with console/support guidance |
| PG | `verify-pg` | SHOW the settings, fail if below targets, list live slots + retained WAL |
| Slots | `provision` | Participant slots 01..N: database + login role (with REPLICATION) + CDC table + pre-created publication. Idempotent; existing passwords kept |
| Slots | `provision-demo` | Slot 00, used by the demo pipe |
| Slots | `slips` | Regenerate the printable per-participant hand-outs from the CSV |
| CH | `create-ch` | Create the demo ClickHouse service (1 x 8 GB, idle-scaling) |
| CH | `schema` | Apply `app/db/cloud/001_cloud_schema.sql` (clickhouse client when CH_HOST/CH_PASSWORD are set, else the chctl Query API). Before the pipe exists, the trailing static MV is expected to be pending — run `create-mv` after `wait-pipe` |
| CDC | `create-pipe` | Postgres CDC ClickPipe: demo slot -> demo service, using the pre-created publication. CLI pipes land the destination table in the `default` database |
| CDC | `wait-pipe` | Poll pipe state until running; report where the destination table landed |
| CDC | `create-mv` | Detect the pipe's actual destination table and create the MV from it into `nyc_tlc_data.taxi_trips` |
| CDC | `verify-sync` | Insert marker rows into Postgres, poll ClickHouse until they arrive, then check the MV fan-out into `taxi_trips` |
| All | `e2e` | The whole chain in order, pausing wherever an id or one-time credential must be exported |
| Down | `teardown` | Drop participant slots: leftover replication slots, databases, roles |
| Down | `delete-pipe` / `delete-ch` / `delete-pg` | Delete the demo pipe, demo service, shared Postgres |

## Postgres targets (applied by `configure-pg`, enforced by `verify-pg`)

| Setting | Value | Why |
|---|---|---|
| `max_replication_slots` | 50 | one slot per participant pipe + resync headroom (a resync creates a new slot) |
| `max_wal_senders` | 50 | must be >= slots |
| `max_connections` | 300 | ~30 concurrent ClickPipes initial snapshots use 4-6 connections each |
| `max_slot_wal_keep_size` | 20480 MB | safety valve: a stalled pipe gets its slot invalidated (that participant resyncs) instead of filling the disk |
| `wal_level` | logical | verified only — required for CDC |

## Outputs

`infra/out/` (git-ignored, mode 600):

- `participants.csv` — one row per slot: database, user, password, publication
- `slips.txt` — printable hand-outs: the `.env.workshop` PG block, the ClickPipe
  wizard values, and an optional chctl one-liner per participant

## Using another Postgres provider (RDS, Supabase, ...)

Skip `create-pg`/`configure-pg`; create the instance there and apply the same
targets (RDS: custom parameter group + `rds.logical_replication=1`; Supabase:
`supabase postgres-config update ... --experimental`, and use the DIRECT
endpoint — poolers are not supported for CDC). Then export the `ADMIN_PG*`
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
   `max_wal_senders=10` — enough for a demo, NOT for 30+ participants. THE D1
   GATE: raising slots requires the console Settings tab or ClickHouse support;
   confirm before committing to managed Postgres for the full room, else RDS.
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

- Morning of: `verify-pg` (settings + live slots + retained WAL per slot).
- A participant whose pipe stalls past the WAL cap just resyncs their pipe; the
  instance is protected by `max_slot_wal_keep_size`.
- After the event: participants delete their pipes (module 09), then
  `teardown`, then `delete-pipe`/`delete-ch`/`delete-pg` as desired. Destroy
  `infra/out/` and printed slips — they contain credentials.
