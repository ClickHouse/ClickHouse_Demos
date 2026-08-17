# ClickHouse Cloud schema change management demo (local Atlas CLI)

An end-to-end, runnable demo of schema-as-code against **ClickHouse Cloud**, with
the **Atlas CLI running locally on macOS or Windows**. Six scenarios, built to
show both what the tooling does well and where it stops helping.

Nothing is deployed. No agent runs in your account. No schema leaves your laptop
unless you explicitly run `migrate push` — and this demo never does.

> **Before you install anything.** Atlas is built and supported by **Ariga**, not
> by ClickHouse, Inc. It is a Partner-tier integration in the ClickHouse
> integrations directory, which carries a "no endorsement implied" notice. The
> ClickHouse driver is gated behind a **paid Atlas Pro entitlement**, so
> `atlas login` is step 2 of 9 and nothing in this demo runs without it.
> Licensing and cost are a conversation with Ariga. Support routing follows the
> same line: Atlas bugs go to Ariga, and ClickHouse support cannot debug an Atlas
> plan. See [Caveats worth knowing](#caveats-worth-knowing) for the rest.
>
> This demo exists to show how schema-as-code behaves *on ClickHouse* — including
> where it stops helping. It is not a recommendation of a vendor.

**Set up first: [`SETUP.md`](SETUP.md)** — nine ordered steps, about 25 minutes,
including the ClickHouse Cloud service, IP access list, scoped users and grants.
Then work through [`SCENARIOS.md`](SCENARIOS.md).

---

## Quick start

Full detail and troubleshooting in [`SETUP.md`](SETUP.md). The short version:

### macOS, Linux, WSL2, Git Bash

```bash
curl -sSf https://atlasgo.sh | sh        # 1. install Atlas
atlas login                              # 2. mandatory — ClickHouse is a Pro driver

# 3. create the ClickHouse Cloud service, note host / user / password
# 4. add your IP to the service's IP access list
# 5. in the Cloud SQL console, run setup/01-users-and-grants.sql.
#    This is what runs CREATE DATABASE adtech and creates the scoped users.
#    Nothing below except preflight.sh works until it has run — not even as `default`.
# 6. pick a dev database — SETUP.md step 6. A second Cloud service needs
#    CREATE DATABASE atlas_dev; run on it first; docker needs nothing.

cp .env.example .env                     # 7. host, user, password, dev URL
$EDITOR .env
set -a && source .env && set +a

./scripts/preflight.sh                   # 8. checks everything; safe to run early
./scripts/bootstrap.sh                   # 9. baseline schema into adtech (no Atlas)
./scripts/seed.sh                        #    5M ad events across ~7 monthly partitions
./scripts/use-step.sh 0                  #    desired-state file = baseline

# then baseline migrations/ — SETUP.md step 9. Without it the first
# atlas migrate diff regenerates the whole schema.
```

`bootstrap.sh` applies the baseline schema to a database that already exists. It
does not create `adtech` — step 5 does, once, as an admin. The `atlas_admin` user
this demo connects with deliberately has no `CREATE DATABASE` grant.

`preflight.sh` is safe to run before step 5. A missing database is a WARN with the
command that fixes it, not a failure.

### Windows PowerShell

```powershell
# 1. install Atlas — SETUP.md has the download + checksum steps
atlas login
# 3-6. as above: Cloud service, IP access list,
#      setup/01-users-and-grants.sql (creates the database), dev database

Copy-Item .env.example .env              # 7.
notepad .env

Set-ExecutionPolicy -Scope Process -ExecutionPolicy Bypass

.\scripts\win\preflight.ps1              # 8.
.\scripts\win\bootstrap.ps1              # 9.
.\scripts\win\seed.ps1
.\scripts\win\use-step.ps1 0
# then baseline migrations\ — SETUP.md step 9
```

The PowerShell scripts read the **same `.env`** as the bash ones and use the
ClickHouse HTTP interface via `Invoke-RestMethod`, so there is no dependency on
`curl` or `clickhouse-client`.

### Rehearse locally first

Start a local ClickHouse and run the whole thing against it before you point
anything at Cloud. All six scenarios work against `--env local`:

```bash
./scripts/local-up.sh                    # docker compose, fixed ports + password
./scripts/bootstrap.sh local && ./scripts/seed.sh local
# then the scenarios, with --env local instead of --env cloud
./scripts/local-down.sh
```

```powershell
.\scripts\win\local-up.ps1
.\scripts\win\bootstrap.ps1 -Target local
.\scripts\win\seed.ps1 -Target local
.\scripts\win\local-down.ps1
```

`local-up` drives `docker-compose.yml`, so 9000 and 8123 are published on the
same numbers and the password is fixed — the `CH_LOCAL_*` defaults in
`.env.example` already match and there is nothing to paste. It deliberately does
not use `atlas tool docker`: that publishes only the native port, on a random
host port, with a generated password, which is right for Atlas's own dev database
and useless for a target the helper scripts drive over HTTP.

---

## The scenarios

| # | Scenario | What it teaches |
|---|---|---|
| 1 | Add `device_type` to `ad_events` | The safe, additive, metadata-only 90% case. Lint clean. |
| 2 | Create an `advertisers` dimension table | The cheapest change there is — and the engine choice it locks in for good. |
| 3 | Drop that table again | Lint fails and the apply still works. `UNDROP TABLE`, and how long the window really is. |
| 4 | Narrow a type, drop a column, reorder a sort key | A differ tells you *what* changes, not what it *costs*. |
| 5 | Add a country dimension across the MV chain | Ordering, sort-key rebuilds, and the backfill no tool does for you. |
| 6 | A 2am console hotfix | Drift, and why detection belongs on a cron rather than at apply time. |

Before and after for each, with the exact commands, is in
[`SCENARIOS.md`](SCENARIOS.md). Switch between them with:

```bash
./scripts/use-step.sh 1                  # prints the diff it is about to make
```
```powershell
.\scripts\win\use-step.ps1 1
```

---

## Layout

```
SETUP.md                        9 ordered setup steps + troubleshooting table
SCENARIOS.md                    the four scenarios: before, commands, after
setup/01-users-and-grants.sql   CREATE DATABASE adtech + the two scoped users
atlas.hcl                       three envs: local, cloud, ci + the lint gate
.env.example                    connection URLs and demo knobs
docker-compose.yml              the local rehearsal container, driven by local-up

schema/sql/schema.sql           THE SOURCE OF TRUTH. Desired state, OSS engines.
schema/hcl/schema.hcl           HCL comparison, hand-maintained
schema/hcl/schema.generated.hcl written by gen-hcl.sh, removed by reset.sh
schema/sql/schema.inspected.sql written by gen-hcl.sh, removed by reset.sh

steps/00-baseline.sql           starting state
steps/01-additive/              scenario 1 — add a column
steps/02-new-table/             scenario 2 — create a table
steps/03-drop-table/            scenario 3 — drop a table
steps/04-dangerous/             scenario 4 — narrow, drop, reorder
steps/05-mv-chain/              scenario 5 — the MV chain
steps/06-drift/hotfix.sql       scenario 6 — the out-of-band change

scripts/lib.sh                  shared helpers, sourced by the rest
scripts/                        bash: preflight, local-up/down, bootstrap, seed,
                                use-step, inject-drift, gen-hcl, reset
scripts/win/                    PowerShell equivalents, full parity
scripts/backfill-country.sql    restartable, idempotent data migration

ci/schema-ci.yml                example CI: lint on PR + scheduled drift check.
                                Not wired up here - see the header in that file.
migrations/                     generated by atlas migrate diff
```

---

## What you need

**The standard Atlas binary, logged in.** The ClickHouse driver — along with
materialized views, `migrate lint` and drift detection — is gated behind an Atlas
Pro entitlement, so `atlas login` is a setup step, not a suggestion. Logged out,
nothing here works. CI uses an `ATLAS_TOKEN` instead of a browser login.

Licensing and cost are between the team and Ariga.

---

## Two rules that will save you an hour

**Write OSS engine names, always.** `MergeTree`, never `SharedMergeTree`.
ClickHouse Cloud promotes OSS engines automatically; a local OSS dev database
rejects `Shared*` outright. Inspecting Cloud gives you `Shared*` back — convert
before diffing against anything local.

**Pin your dev database version and know the gap.** Plans are validated against
`CH_DEV_URL`, then applied to Cloud. Preflight prints the difference when
`CH_DEV_URL` is a `docker://` image. For anything real, point `CH_DEV_URL` at a
second ClickHouse Cloud service — exact parity, and it idles when unused.

---

## Caveats worth knowing

Atlas is built and supported by **Ariga**, not by ClickHouse, Inc. It is a
Partner-tier integration in the ClickHouse integrations directory, which carries
a "no endorsement implied" notice. The practical consequence is support routing:
Atlas bugs go to Ariga, and ClickHouse support cannot debug an Atlas plan.

Two independent release trains means version parity needs active management. Run
the [round-trip parity test](SETUP.md#round-trip-parity-test) after every Atlas
upgrade and after any Cloud version change: anything Atlas does not round-trip is
something it does not model, and therefore something it will not defend against
drift. Codecs, projections and TTLs are the usual suspects.

Three limits to be clear about:

- **Atlas will not tell you a plan is expensive.** It reports structure, not cost.
- **It will not backfill or re-aggregate data.** That is always a separate script.
- **There is no transactional rollback.** ClickHouse has no transactional DDL, so
  a `--down` migration is a new forward change, not an undo, and a migration that
  fails halfway stays half-applied.

## Sources

- [Schema migration tools for ClickHouse](https://clickhouse.com/docs/knowledgebase/schema_migration_tools) — ClickHouse Docs
- [Atlas docs home + installation](https://atlasgo.io/docs)
- [Getting Started](https://atlasgo.io/getting-started) — `atlas tool docker`, workflows
- [Feature Compatibility](https://atlasgo.io/features) — which drivers need Atlas Pro
- [Managing ClickHouse Cloud via Atlas](https://atlasgo.io/guides/clickhouse/clickhouse-cloud-atlas) — engine mapping, connection URLs
- [ClickHouse Cloud — setting IP filters](https://clickhouse.com/docs/cloud/security/setting-ip-filters)
- [Automatic ClickHouse Schema Migrations with Atlas](https://atlasgo.io/guides/clickhouse)
- [Verifying Migration Safety](https://atlasgo.io/versioned/lint) — `migrate lint`, `force`
- [ClickHouse integrations directory](https://clickhouse.com/docs/integrations) — support-tier definitions
