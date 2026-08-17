# Scenarios

Six schema changes against ClickHouse Cloud, run end to end. Each one opens with
what we are doing and what to expect, then shows the schema before, the exact
commands, and the schema after.

Assumes [`SETUP.md`](SETUP.md) is done: Atlas installed, `atlas login` complete,
`.env` filled in, and the `adtech` database created.

| # | Scenario | `use-step` | Applied? |
|---|---|---|---|
| [1](#scenario-1--add-a-field) | Add `device_type` to `ad_events` | 1 | yes |
| [2](#scenario-2--create-a-table) | Create an `advertisers` dimension table | 2 | yes |
| [3](#scenario-3--drop-a-table) | Drop it again | 3 | yes, after lint fails |
| [4](#scenario-4--narrow-a-type-drop-a-column-reorder-a-sort-key) | Narrow a type, drop a column, reorder a sort key | 4 | no — rolled back |
| [5](#scenario-5--add-a-dimension-across-the-mv-chain) | Add a country dimension across the MV chain | 5 | yes |
| [6](#scenario-6--drift) | A 2am console hotfix | n/a | n/a — drift, then adopt |

Each scenario starts from the state the previous one left behind. Run them in
order, or [reset](#start-here) and jump in.

---

## Start here

**If you have just finished [`SETUP.md`](SETUP.md) step 9, skip to
[scenario 1](#scenario-1--add-a-field).** You are already in the right state.
`bootstrap.sh` and `seed.sh` are not idempotent — running them twice fails on
`TABLE_ALREADY_EXISTS` and doubles the seeded rows.

Otherwise, get there:

```bash
cd schema_change_management
set -a && source .env && set +a

./scripts/preflight.sh          # no FAIL lines; WARNs are expected, read them
./scripts/bootstrap.sh          # baseline schema into adtech, without Atlas
./scripts/seed.sh               # 5M rows over ~7 monthly partitions
./scripts/use-step.sh 0         # desired-state file = baseline
```

```powershell
Set-ExecutionPolicy -Scope Process -ExecutionPolicy Bypass
. .\scripts\win\lib.ps1 ; Import-DotEnv     # atlas reads CH_* from the environment
.\scripts\win\preflight.ps1
.\scripts\win\bootstrap.ps1
.\scripts\win\seed.ps1
.\scripts\win\use-step.ps1 0
```

The bash `source .env` and the PowerShell `Import-DotEnv` are both per-shell.
Every `atlas ... --env cloud` below resolves its URLs through `getenv()`, so
re-run the loader in any new terminal window.

Then bring the existing database under Atlas control. This step is not optional.
`atlas migrate diff` compares the **migration directory** against
`schema/sql/schema.sql` — it never looks at the target. `migrations/` is empty, so
the first plan is the entire schema. Generate it, then record it as already
applied rather than running it:

```bash
atlas migrate diff baseline --env cloud
ls migrations/                     # note the version, e.g. 20260813120000_baseline.sql
atlas migrate apply --env cloud --baseline 20260813120000
atlas migrate status --env cloud     # Current Version = that version, 0 pending
```

`apply --baseline` prints `No migration files to execute` and exits 0, which is
correct — the objects already exist. Check `migrate status` rather than the exit
code: on a service that has just been reset the revision table is being recreated,
and the record occasionally does not stick on the first attempt. If status says
"No migration applied yet", run the same `apply --baseline` again.

Skip this and scenario 1 produces the whole schema — two `CREATE TABLE` and one
`CREATE MATERIALIZED VIEW` — instead of one `ADD COLUMN`, and the apply fails
against a database that already has those objects.

### Baseline state

| Object | Engine | Key |
|---|---|---|
| `ad_events` | MergeTree | `ORDER BY (advertiser_id, campaign_id, event_time)`, TTL 13 months |
| `campaign_daily_stats` | SummingMergeTree | `ORDER BY (advertiser_id, campaign_id, event_date)` |
| `campaign_daily_mv` | MaterializedView | `ad_events` -> `campaign_daily_stats` |

`seed.sh` ends with a reconciliation between the raw table and the aggregate. The
two rows match, because the MV was in place before the data landed. Screenshot
it — scenario 5 compares against it.

Back to a clean slate at any point. `reset.sh` clears `migrations/` as well, so
the baselining has to be redone:

```bash
./scripts/reset.sh && ./scripts/bootstrap.sh && ./scripts/seed.sh
./scripts/use-step.sh 0
atlas migrate diff baseline --env cloud
atlas migrate apply --env cloud --baseline <version>
```

### Rehearsing against a local container

Everything below works against `--env local` as well — that is the point of
`./scripts/local-up.sh`. But **you cannot switch targets mid-flight.** All three
envs in `atlas.hcl` share one `migrations/` directory, and `atlas migrate` replays
that whole directory against the dev database on every command. Point it at a
different target with the other target's history still on disk and it fails, e.g.

```
Error: sql/migrate: read migration directory state: executing statement
"INSERT INTO `campaign_daily_stats_tmp` SELECT * FROM `campaign_daily_stats`;"
code: 20, message: Number of columns doesn't match (source: 7 and result: 6)
```

So do a full reset when you change target, which clears `migrations/` too:

```bash
./scripts/local-up.sh
./scripts/reset.sh local && ./scripts/bootstrap.sh local && ./scripts/seed.sh local
./scripts/use-step.sh 0
atlas migrate diff baseline --env local
atlas migrate apply --env local --baseline <version>
# then the scenarios with --env local
```

The revision table is per database, so each target tracks its own applied
versions. It is only the directory on disk that is shared.

---

## Scenario 1 — Add a field

Product wants a device breakdown on the impression funnel.

| At a glance | |
|---|---|
| **Change** | One new column on `ad_events`: `device_type LowCardinality(String) DEFAULT 'unknown'`. |
| **Atlas plans** | A single `ALTER TABLE ... ADD COLUMN`. Metadata only, no parts rewritten. |
| **Lint** | Clean. Exit 0, no diagnostics. |
| **Apply** | Yes. Leave it applied — every later scenario branches from here. |
| **Lands** | The column, queryable immediately. Existing rows read the default until their next merge. |

### Before

```sql
CREATE TABLE `ad_events` (
    ...
    `bid_price_usd` Decimal(10, 6),
    `revenue_usd`   Decimal(10, 6)
)
ENGINE = MergeTree
```

### Run

```bash
./scripts/use-step.sh 1                          # prints the diff it is about to make
atlas migrate diff add_device_type --env cloud
cat migrations/*_add_device_type.sql
atlas migrate lint --env cloud --latest 1
atlas migrate apply --env cloud
```

```powershell
.\scripts\win\use-step.ps1 1
atlas migrate diff add_device_type --env cloud
Get-Content migrations\*_add_device_type.sql
atlas migrate lint --env cloud --latest 1
atlas migrate apply --env cloud
```

### After

```sql
CREATE TABLE `ad_events` (
    ...
    `bid_price_usd` Decimal(10, 6),
    `revenue_usd`   Decimal(10, 6),
    `device_type`   LowCardinality(String) DEFAULT 'unknown'
)
ENGINE = MergeTree
```

One `ALTER TABLE ... ADD COLUMN`. Metadata only — no parts rewritten. Lint is
clean. Existing rows read the default until their next merge.

This is 90% of your changes. If the process makes *this* painful, people route
around it, which is how you get scenario 6.

**Leave it applied.** Everything after this branches from here.

---

## Scenario 2 — Create a table

Reporting needs advertiser names and account tier, not just IDs. A new dimension
table lands in the repo. The cheapest change there is.

| At a glance | |
|---|---|
| **Change** | One new standalone table, `advertisers`, `ReplacingMergeTree(updated_at)` keyed on `advertiser_id`. Nothing existing is touched. |
| **Atlas plans** | A single `CREATE TABLE`. No `ALTER`, no table copy, no MV in the plan. |
| **Lint** | Clean. Nothing is dropped or narrowed, so the destructive analyzer has nothing to say. |
| **Apply** | Yes. Instant, and the table is created empty. |
| **Lands** | An empty table. Creating a dimension is free; populating it and pointing readers at it are separate jobs no migration plans for you. |

### Before

State after scenario 1. Three objects: `ad_events`, `campaign_daily_stats`,
`campaign_daily_mv`.

### Run

```bash
./scripts/use-step.sh 2
atlas migrate diff add_advertisers --env cloud
cat migrations/*_add_advertisers.sql
atlas migrate lint --env cloud --latest 1
atlas migrate apply --env cloud
```

```powershell
.\scripts\win\use-step.ps1 2
atlas migrate diff add_advertisers --env cloud
Get-Content migrations\*_add_advertisers.sql
atlas migrate lint --env cloud --latest 1
atlas migrate apply --env cloud
```

### After

```sql
CREATE TABLE `advertisers` (
    `advertiser_id`   UInt32,
    `advertiser_name` String,
    `account_tier`    LowCardinality(String) DEFAULT 'standard',
    `is_active`       UInt8 DEFAULT 1,
    `updated_at`      DateTime
)
ENGINE = ReplacingMergeTree(`updated_at`)
ORDER BY `advertiser_id`
```

Four objects now. The interesting part is not the migration, it is the engine
choice the migration locks in.

An advertiser record is a slowly-changing dimension — the CRM re-sends the same
`advertiser_id` with a new tier or a flipped flag. `ReplacingMergeTree` collapses
rows sharing the sort key, keeping the highest `updated_at`, so inserts stay
append-only and you never issue an `UPDATE`. But **deduplication happens during
background merges**: it is asynchronous and never guaranteed to have finished, so
a plain `SELECT` can return both rows for an unbounded time. Read it with `FINAL`,
or aggregate with `argMax`:

```sql
SELECT * FROM advertisers FINAL WHERE advertiser_id = 1000;
SELECT advertiser_id, argMax(account_tier, updated_at) FROM advertisers GROUP BY advertiser_id;
```

The sort key *is* the deduplication key. Changing `ORDER BY` later changes what
"the same row" means, on top of the table rebuild a sort-key change already forces
(scenarios 4 and 5). Get it right on day one.

**Leave it applied.** Scenario 3 removes it.

---

## Scenario 3 — Drop a table

The dimension was a mistake. Remove it. The change that looks smallest in the diff
and is the only one here you cannot undo with another pull request.

| At a glance | |
|---|---|
| **Change** | Delete `advertisers` from the desired state. That is the entire diff. |
| **Atlas plans** | A single `DROP TABLE`. No rename, no export, no backup first — the plan is the drop. |
| **Lint** | **Fails.** Exit 1 with `DS102 Dropping table "advertisers"`. `atlas.hcl` sets `destructive { error = true }`, so this is a hard failure in CI. |
| **Apply** | Yes, deliberately, after the room has watched lint fail. `migrate apply` does not run `migrate lint` — the gate lives in CI, not in the apply. |
| **Lands** | Table and parts gone. Recoverable with `UNDROP TABLE` for a limited window — see below. |

### Before

State after scenario 2. `advertisers` exists.

### Run

```bash
./scripts/use-step.sh 3
atlas migrate diff drop_advertisers --env cloud
cat migrations/*_drop_advertisers.sql
atlas migrate lint --env cloud --latest 1     # exits 1: destructive
atlas migrate apply --env cloud               # deliberate, after review
```

```powershell
.\scripts\win\use-step.ps1 3
atlas migrate diff drop_advertisers --env cloud
Get-Content migrations\*_drop_advertisers.sql
atlas migrate lint --env cloud --latest 1
atlas migrate apply --env cloud
```

Measured on the demo service:

```
-- destructive changes detected:
  -- L2: Dropping table "advertisers" https://atlasgo.io/lint/analyzers#DS102
-- suggested fix:
  -> Add a pre-migration check to ensure table "advertisers" is empty before dropping it
```

Note the division of labour: lint exited 1 and `apply` still worked. Lint is not a
lock, it is a gate — and the gate lives in the pull request, where a human decides.
That is the entire argument for `migrate lint` in CI over a review checklist.

### After

The table is gone. **But a ClickHouse `DROP TABLE` is not instantly
unrecoverable**, and almost nobody knows it:

```sql
UNDROP TABLE advertisers;

-- dropped more than once? disambiguate by UUID
SELECT table, uuid FROM system.dropped_tables;
UNDROP TABLE advertisers UUID '<uuid>';
```

Verified on this service: dropped `advertisers` with a row in it, confirmed it was
gone from `system.tables`, found it in `system.dropped_tables`, ran `UNDROP TABLE`,
and the row came back.

On the Atomic database engine the table is detached and parked in a delete queue
rather than deleted. How long you have differs between Cloud and OSS, measured on
this service:

```sql
SELECT name, value FROM system.server_settings WHERE name LIKE '%drop_table%';
```

| Setting | Value here | Applies to |
|---|---|---|
| `database_shared_drop_table_delay_seconds` | 28800 (8 hours) | ClickHouse Cloud |
| `database_atomic_delay_before_drop_table_sec` | 480 (8 minutes) | self-managed OSS |

Cloud runs the shared database engine, so 8 hours is the number that applies
there. Do not quote the OSS default at a Cloud customer or vice versa — check the
service in front of you. Two ways to lose the window entirely: `DROP TABLE ... SYNC`
bypasses the queue, and waiting it out leaves only a backup restore.

**What re-adding the table to the repo does not do:** it generates a
`CREATE TABLE` and gives you the structure back with zero rows. Atlas has no idea
the data existed. Schema-as-code versions structure, never contents.

The safest destructive change is the one you staged — rename or detach first, wait
out a business cycle, delete later. Then the recovery window stops being the thing
standing between you and an incident review.

---

## Scenario 4 — Narrow a type, drop a column, reorder a sort key

Housekeeping PR. Three changes, all of which read as reasonable in a diff.

| At a glance | |
|---|---|
| **Change** | Three at once: `user_id` UInt64 -> UInt32, `DROP COLUMN placement_id`, and `campaign_daily_stats` `ORDER BY` reordered. |
| **Atlas plans** | `MODIFY COLUMN` (an async mutation over every part; anything above 4,294,967,295 is silently truncated), `DROP COLUMN`, and a full rebuild of `campaign_daily_stats` — `CREATE _tmp`, `INSERT SELECT`, `EXCHANGE TABLES`, `DROP`. Atlas never emits `MODIFY ORDER BY`. |
| **Lint** | Exit 1 with exactly one diagnostic: `DS103 Dropping non-virtual column "placement_id"`. Nothing about the truncation, nothing about the table copy. |
| **Apply** | **No.** Delete the generated file, `atlas migrate hash`, return the desired state to scenario 3. |
| **Lands** | Nothing. The lesson is the two expensive changes the gate never mentioned. |

### Before

State after scenario 3 — which is the same schema as after scenario 1, since
scenario 2 created `advertisers` and scenario 3 dropped it again.

```sql
-- ad_events
    `placement_id`  UInt32,
    `user_id`       UInt64,

-- campaign_daily_stats
ORDER BY (advertiser_id, campaign_id, event_date)
```

### Run

```bash
./scripts/use-step.sh 4
atlas migrate diff tighten_types --env cloud
cat migrations/*_tighten_types.sql
atlas migrate lint --env cloud --latest 1        # non-zero exit: destructive change
```

```powershell
.\scripts\win\use-step.ps1 4
atlas migrate diff tighten_types --env cloud
Get-Content migrations\*_tighten_types.sql
atlas migrate lint --env cloud --latest 1
```

Ask the room which of the three changes are safe before showing them the plan.

| Change | What the plan says | What it costs |
|---|---|---|
| `user_id` UInt64 -> UInt32 | `MODIFY COLUMN` | An asynchronous mutation over every part. Any value above 4,294,967,295 is **silently truncated**. Irreversible. Lint says nothing. |
| `DROP COLUMN placement_id` | `DROP COLUMN` | Lint catches this one. Note that it caught the obvious change, not the dangerous one. |
| `campaign_daily_stats` ORDER BY reordered | `CREATE ... _tmp` + `INSERT SELECT` + `EXCHANGE TABLES` + `DROP` | A sort key cannot be altered in place, so Atlas quietly plans a **full table copy**. On this 7,240-row demo table it is instant. On a real reporting table it is a full data rewrite and a cutover window. Lint says nothing about it. |

Prove the truncation live, if you want it to land harder than the table does:

```sql
CREATE TABLE t (id UInt64, v UInt32) ENGINE = MergeTree ORDER BY v;
INSERT INTO t SELECT 5000000000 + number, number FROM numbers(1000);
SELECT max(id) FROM t;                                    -- 5000000999
ALTER TABLE t MODIFY COLUMN id UInt32 SETTINGS mutations_sync = 2;
SELECT max(id) FROM t;                                    -- 705033703
DROP TABLE t;
```

Five billion became 705 million, and the `ALTER` reported success.

### After

Nothing. **Do not apply this one** — rolling it back is the point.

```bash
rm -f migrations/*_tighten_types.sql
atlas migrate hash --env cloud
./scripts/use-step.sh 3
```

```powershell
Remove-Item -Force migrations\*_tighten_types.sql -ErrorAction SilentlyContinue
atlas migrate hash --env cloud
.\scripts\win\use-step.ps1 3
```

`atlas migrate hash` is required: `migrations/atlas.sum` holds a checksum per
file, and every later `migrate` command fails until it is regenerated.

Verified against ClickHouse Cloud 26.2 with Atlas v1.3.1: lint exits 1 and reports
exactly one diagnostic, `DS103 Dropping non-virtual column "placement_id"`. It says
nothing about the silent truncation, and nothing about the table copy.

The headline: a declarative tool answers *what* will change. It does not answer
*what it costs*. Two of the three changes here are the expensive ones, and the
gate caught neither. Every generated plan needs a reviewer who knows ClickHouse.

---

## Scenario 5 — Add a dimension across the MV chain

Break campaign performance down by country. Three objects change together. This is
the scenario ClickHouse teams actually lose time to.

| At a glance | |
|---|---|
| **Change** | `country_code` added to all three objects at once: the source table, the target table's sort key, and the MV's SELECT/GROUP BY. |
| **Atlas plans** | Seven statements in one required order — `DROP VIEW`, `ALTER ad_events ADD COLUMN`, `CREATE _tmp`, `INSERT SELECT` (explicit column list, `country_code` omitted so it defaults), `EXCHANGE TABLES`, `DROP _tmp`, `CREATE MATERIALIZED VIEW`. Statements 3-6 are a full copy of the table. |
| **Lint** | No diagnostics for the whole seven-statement plan, table copy included. |
| **Apply** | Yes. It applies cleanly, no errors, no manual intervention. |
| **Lands** | Correct schema, worthless dimension: totals still reconcile at 4,750,000 / 250,000 / 319,000, and all 7,240 rows read `country_code = 'XX'`. The backfill is a separate, reviewed script. |

### Before

State after scenario 3.

```sql
-- ad_events
    `device_type`   LowCardinality(String) DEFAULT 'unknown'

-- campaign_daily_stats
    `campaign_id`   UInt32,
    `impressions`   UInt64,
ORDER BY (advertiser_id, campaign_id, event_date)

-- campaign_daily_mv
SELECT event_date, advertiser_id, campaign_id, ...
GROUP BY event_date, advertiser_id, campaign_id
```

### Run

Read the plan before you apply it.

```bash
./scripts/use-step.sh 5
atlas migrate diff add_country_dimension --env cloud
cat migrations/*_add_country_dimension.sql
atlas migrate lint --env cloud --latest 1     # clean, despite a full table copy
atlas migrate apply --env cloud
```

```powershell
.\scripts\win\use-step.ps1 5
atlas migrate diff add_country_dimension --env cloud
Get-Content migrations\*_add_country_dimension.sql
atlas migrate lint --env cloud --latest 1
atlas migrate apply --env cloud
```

Read the plan top to bottom with the room before applying. Three things to check:

1. **Ordering.** Atlas gets this right, and it is worth showing *why* it had to:
   the plan drops the MV first, alters `ad_events`, rebuilds the target table,
   then recreates the MV last. Seven statements in a specific order. A plan
   ordered any other way looks equally valid and fails halfway — which is the
   argument for versioned migrations you can open and read over declarative
   apply, where you never see the order at all.

2. **The sort key.** For `SummingMergeTree` the sort key *is* the grouping key.
   Leaving `country_code` out collapses all countries together and quietly returns
   wrong numbers. Including it means a table rebuild — all three in-place routes
   are refused:

   ```
   -- middle of the key
   Code: 36. Primary key must be a prefix of the sorting key, but the column
   in the position 2 is country_code, not event_date.

   -- appended, in a separate statement after ADD COLUMN
   Code: 36. Existing column country_code is used in the expression that was
   added to the sorting key. You can add expressions that use only the newly
   added columns.

   -- appended in the same statement, but the column has a DEFAULT
   Code: 36. Newly added column country_code has a default expression, so
   adding expressions that use it to the sorting key is forbidden.
   ```

   Atlas does not attempt any of them. It plans the rebuild — `CREATE _tmp`,
   `INSERT SELECT`, `EXCHANGE TABLES`, `DROP` — which is the correct answer and
   also a full copy of the table. `migrate lint` reports **no diagnostics** for
   the whole seven-statement plan. Structure, not cost, again.

3. **Backfill.** The MV is an insert trigger. Recreating it rewrites **zero** rows
   of history. Every existing row in `campaign_daily_stats` has no country and
   never will unless you replay it.

The backfill is now runnable — `country_code` exists on both tables — but it is a
separate, reviewed script, not part of the migration. Read it:

```bash
less scripts/backfill-country.sql          # Get-Content on Windows
```

Four properties a generated migration will never give you: **restartable** (one
partition at a time), **idempotent** (`DROP PARTITION` before rewriting, or you
double count), **observable** (verify sums per partition before moving on),
**throttled** (bounded `max_threads` so live queries are not starved).

The verification is the real deliverable — the same query `seed.sh` printed at
the start:

```sql
SELECT 'raw' AS src, countIf(event_type='impression') AS impressions,
       countIf(event_type='click') AS clicks, sum(revenue_usd) AS revenue
FROM ad_events
UNION ALL
SELECT 'agg', sum(impressions), sum(clicks), sum(revenue_usd)
FROM campaign_daily_stats;
```

After the backfill, row counts go **up** — one row per campaign-day becomes one
per campaign-day-country. The **sums** must match throughout. Note this is the
backfill's effect, not the migration's: the apply below leaves the row count
unchanged at 7,240.

### After

The plan applies cleanly. Now run the reconciliation again and put it next to the
screenshot from `seed.sh`:

```sql
SELECT 'raw' AS src, countIf(event_type='impression') AS impressions,
       countIf(event_type='click') AS clicks, sum(revenue_usd) AS revenue
FROM ad_events
UNION ALL
SELECT 'agg', sum(impressions), sum(clicks), sum(revenue_usd)
FROM campaign_daily_stats;

SELECT country_code, count() AS rows, sum(impressions)
FROM campaign_daily_stats GROUP BY country_code;
```

Measured on the demo service after applying:

```
 src   impressions   clicks   revenue        country_code   rows   impressions
 agg       4750000   250000    319000        XX             7240       4750000
 raw       4750000   250000    319000
```

**The totals still reconcile — and the new dimension is worthless.** Every
historical row is `XX`, because the `INSERT SELECT` in the plan copied the old
rows with the new column defaulted, and the recreated MV only sees inserts from
now on. Nothing errored. Nothing looks broken. The numbers add up. And you cannot
answer a single question about country for any data that already existed.

That is the whole scenario: the schema migration succeeded completely and the
data migration has not started. Only the second one is your problem, and no tool
does it for you.

The asymmetry to name out loud: at seed time nobody ran an aggregation job — the
MV populated `campaign_daily_stats` on insert. Recreate that MV and the same DDL
yields zero rows of history. Same objects, opposite outcome, purely because of
ordering in time. That is the gap no schema migration tool closes.

For anything touching an MV chain, use expand and contract: add the column to
source and target, deploy; create a second MV writing the new shape alongside the
old one, deploy; backfill partition by partition, verifying; cut readers over;
drop the old MV and column, deploy. Five boring deploys beat one clever one.

---

## Scenario 6 — Drift

2am incident. An on-call engineer opens the Cloud SQL console, adds a debug
column, shortens a TTL to reclaim storage, and adds a skipping index to chase the
bug. All three work. The incident closes. No PR.

| At a glance | |
|---|---|
| **Change** | Nothing in the repo. `inject-drift.sh` applies the hotfix straight to production: `debug_trace_id`, TTL 13 months -> 6, and the `idx_creative` skipping index. |
| **Atlas plans** | Use `atlas schema diff`, not `migrate diff` — only the declarative command reads the live target. `--from` is required, and `--exclude atlas_schema_revisions` is mandatory or the plan also drops Atlas's own bookkeeping table. |
| **Lint** | Not involved. A declarative diff writes no migration file, which is exactly why drift belongs on a cron and not at apply time. |
| **Apply** | **No.** Adopt instead: hand-edit `schema/sql/schema.sql` to include all three, re-run the same diff, and it returns to synced. |
| **Lands** | Repo and production agree again, with the hotfix kept rather than reverted. The 6-month TTL is not reversible — anything already aged out stays gone. |

### Before

Repo and production agree — `schema/sql/schema.sql` is at scenario 5, which is
what was applied. Prove it first; an empty diff here is what makes the next one
mean something.

```sql
-- ad_events, as the repo describes it
    `country_code`  LowCardinality(String) DEFAULT 'XX'
)
TTL event_date + toIntervalMonth(13)
-- no skipping indexes
```

### Run

```bash
# Atlas keeps its revision history in adtech.atlas_schema_revisions, which the
# schema file does not describe. Exclude it or every drift check proposes
# dropping Atlas's own bookkeeping table.
DRIFT=(--from "$CH_CLOUD_URL" --to "file://schema/sql/schema.sql"
       --dev-url "$CH_DEV_URL" --exclude atlas_schema_revisions)

atlas schema diff "${DRIFT[@]}"     # "Schemas are synced, no changes to be made."

./scripts/inject-drift.sh

atlas schema diff "${DRIFT[@]}"     # now one ALTER with three clauses
```

```powershell
$drift = @('--from', $env:CH_CLOUD_URL, '--to', 'file://schema/sql/schema.sql',
           '--dev-url', $env:CH_DEV_URL, '--exclude', 'atlas_schema_revisions')

atlas schema diff @drift
.\scripts\win\inject-drift.ps1
atlas schema diff @drift
```

### After

Production has three objects the repository has never heard of:

```sql
-- ad_events, as production actually is
    `device_type`    LowCardinality(String) DEFAULT 'unknown',
    `debug_trace_id` String DEFAULT '',
    INDEX idx_creative creative_id TYPE minmax GRANULARITY 4   -- added, not materialized
)
TTL event_date + toIntervalMonth(6)
```

Measured on the demo service, the plan is one `ALTER` with three clauses:

```sql
ALTER TABLE `ad_events`
  DROP COLUMN `debug_trace_id`,
  DROP INDEX `idx_creative`,
  MODIFY TTL event_date + toIntervalMonth(13);
```

All three drift items are caught, including the skipping index. Correct behaviour and dangerous behaviour at the same
time: correct because the repo is supposed to be the truth, dangerous because if
the first time anyone sees this is during a deploy, someone approves it at speed.

No engine noise, despite `--from` being Cloud (`SharedMergeTree`) and the file
saying `MergeTree`: Atlas normalises the promoted engines back to their OSS names
before diffing. That is worth pointing at — it is the reason a single schema file
works against both Cloud and a local OSS dev database.

Which is why drift detection belongs on a schedule, not at apply time:

```bash
less ci/schema-ci.yml
```

Two jobs, deliberately split. `lint` on every PR, never touches production, fails
the build on destructive changes. `drift` on a 06:00 cron with **read-only**
credentials against production, answering "does the repo still describe reality?"
every morning. There is deliberately no auto-apply-on-merge job.

Note the TTL detail: shortening a TTL is not reversible. Once parts age out under
the new rule the data is gone, and re-lengthening brings nothing back.

Then close the loop the way a real team would — adopt the hotfix rather than
revert it. Hand-edit `schema/sql/schema.sql` to add all three (`debug_trace_id`,
`TTL ... toIntervalMonth(6)`, and the `idx_creative` index), then re-run the same
command:

```bash
atlas schema diff "${DRIFT[@]}"
# prints "Schemas are synced, no changes to be made."
```

Verified end to end on the demo service: adding `debug_trace_id`, the 6-month TTL
and the `idx_creative` index to `schema/sql/schema.sql` returns the diff to synced.

Use `atlas schema diff`, not `atlas migrate diff` — only the declarative command
compares the repo against the live target. `migrate diff` compares the repo
against the migration directory and will report a change either way.

Drift is not resolved by reverting. It is resolved by deciding, deliberately,
which side was right.

---

## Cleanup

```bash
./scripts/reset.sh              # drops every object in adtech, restores the baseline
                                # file, and clears migrations/
./scripts/local-down.sh         # only if you rehearsed against a local container
```

```powershell
.\scripts\win\reset.ps1
.\scripts\win\local-down.ps1
```

Reset before every run. Scenario 6 leaves a 6-month TTL on `ad_events`, and the
seed spans 180 days, so an un-reset service starts shedding its oldest partition
within a day or two — which will break the reconciliation for a reason that has
nothing to do with the lesson.

---

## Notes on the two commands people get wrong

**`atlas migrate diff` vs `atlas schema diff`.** `migrate diff` compares the
migration directory against `schema/sql/schema.sql` and writes a versioned file.
It never reads the target database. `schema diff` compares two states you name
directly — `--from` is mandatory — which is why scenario 6 uses it against the
live service.

**`migrate lint` is a gate, not a lock.** `atlas migrate apply` does not run lint.
Scenario 3 proves it: lint exits 1 on the table drop and the apply still succeeds.
The enforcement lives in the CI job ([`ci/schema-ci.yml`](ci/schema-ci.yml)), which is
where you want a human in the loop.

**`--dev-url` is not your target.** Atlas wipes and rebuilds the dev database on
every command to validate a plan before it touches anything real. Plans are
validated against that version and then applied to Cloud, so the two should match.
`preflight.sh` prints the gap when the dev database is a `docker://` image.
