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
| PG | `create-pg` | Create the Postgres service managed by ClickHouse (beta). Admin password prints once |
| PG | `configure-pg` | Patch runtime config: slots/senders/connections/WAL cap (targets below) |
| PG | `verify-pg` | SHOW the settings, fail if below targets, list live slots + retained WAL |
| Slots | `provision` | Participant slots 01..N: database + login role (with REPLICATION) + CDC table + pre-created publication. Idempotent; existing passwords kept |
| Slots | `provision-demo` | Slot 00, used by the demo pipe |
| Slots | `slips` | Regenerate the printable per-participant hand-outs from the CSV |
| CH | `create-ch` | Create the demo ClickHouse service (1 x 8 GB, idle-scaling) |
| CH | `schema` | Apply `app/db/cloud/001_cloud_schema.sql` via the Query API. Before the pipe exists, the trailing CDC materialized view is expected to be pending — re-run after `wait-pipe` |
| CDC | `create-pipe` | Postgres CDC ClickPipe: demo slot -> demo service, using the pre-created publication |
| CDC | `wait-pipe` | Poll pipe state until running; report which database the destination table landed in |
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

## VERIFY-LIVE notes (confirm on the first real run)

1. `postgres config patch` acceptance of all four parameters and whether a
   restart is needed (`postgres restart <id>`), since managed-Postgres editable
   parameters are still beta.
2. The destination DATABASE of a CLI-created pipe: the app and the MV expect
   `nyc_tlc_data.realtime_trips`; `wait-pipe` prints where the table actually
   landed. If it lands elsewhere, recreate the pipe in the console choosing the
   `nyc_tlc_data` destination database (the wizard has an explicit destination
   step; the CLI's `--table-mapping` does not take a database qualifier).
3. Pipe state strings polled by `wait-pipe` (expects Running/Completed;
   fails on Failed/Error).

## Day-of and teardown

- Morning of: `verify-pg` (settings + live slots + retained WAL per slot).
- A participant whose pipe stalls past the WAL cap just resyncs their pipe; the
  instance is protected by `max_slot_wal_keep_size`.
- After the event: participants delete their pipes (module 09), then
  `teardown`, then `delete-pipe`/`delete-ch`/`delete-pg` as desired. Destroy
  `infra/out/` and printed slips — they contain credentials.
