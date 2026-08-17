# Setup and configuration

Complete, ordered steps to get this demo running against ClickHouse Cloud with
the Atlas CLI on your own machine. Nine steps, about 25 minutes the first time.

**There is exactly one supported configuration for ClickHouse.** The standard
Atlas binary, logged in, with an Atlas Pro entitlement. The Apache-2.0 Community
Edition does not contain the ClickHouse driver at all, so it is not an option and
is not mentioned again in this repo.

| Step | What | Time |
|---|---|---|
| [1](#step-1--install-the-atlas-cli) | Install the Atlas CLI | 2 min |
| [2](#step-2--log-in) | Log in to Atlas | 2 min |
| [3](#step-3--create-the-clickhouse-cloud-service) | Create the ClickHouse Cloud service | 5 min |
| [4](#step-4--open-the-ip-access-list) | Open the IP access list | 2 min |
| [5](#step-5--create-the-database-and-scoped-users) | Create the database and scoped users | 3 min |
| [6](#step-6--choose-a-dev-database) | Choose a dev database | 2 min |
| [7](#step-7--configure-env) | Configure `.env` | 3 min |
| [8](#step-8--run-preflight) | Run preflight | 1 min |
| [9](#step-9--load-the-baseline-and-data) | Load the baseline and data | 5 min |

---

## Step 1 — Install the Atlas CLI

### macOS

```bash
curl -sSf https://atlasgo.sh | sh
atlas version
```

Homebrew also works. Take the exact formula name from the Homebrew tab on the
[installation docs](https://atlasgo.io/docs#installation) rather than from
memory, it has changed before.

On a locked-down machine where piping a script to `sh` is not allowed, download
and verify the binary directly:

```bash
curl -Lo atlas        https://atlasbinaries.com/atlas/atlas-darwin-arm64-latest
curl -Lo atlas.sha256 https://atlasbinaries.com/atlas/atlas-darwin-arm64-latest.sha256

shasum -a 256 atlas | awk '{print $1}'   # must match
cat atlas.sha256                          # this

chmod +x atlas && sudo mv atlas /usr/local/bin/atlas
atlas version
```

Intel Macs: swap `arm64` for `amd64` in both URLs.

### Windows

```powershell
$dir = "$env:LOCALAPPDATA\Atlas"
New-Item -ItemType Directory -Force -Path $dir | Out-Null

Invoke-WebRequest -Uri "https://atlasbinaries.com/atlas/atlas-windows-amd64-latest.exe" `
                  -OutFile "$dir\atlas.exe"
Invoke-WebRequest -Uri "https://atlasbinaries.com/atlas/atlas-windows-amd64-latest.exe.sha256" `
                  -OutFile "$dir\atlas.exe.sha256"

# These two must match.
(Get-FileHash "$dir\atlas.exe" -Algorithm SHA256).Hash.ToLower()
(Get-Content "$dir\atlas.exe.sha256").Trim().ToLower()

[Environment]::SetEnvironmentVariable("Path",
  [Environment]::GetEnvironmentVariable("Path","User") + ";$dir", "User")

# Reopen PowerShell, then:
atlas version
```

WSL2 also works and is lower friction if you already have it — install with the
macOS/Linux command above and use the bash scripts unchanged.

### Which script set to run

| Shell | Scripts |
|---|---|
| macOS, Linux, WSL2, Git Bash | `scripts/*.sh` |
| Windows PowerShell 5.1 or 7+ | `scripts\win\*.ps1` |

Both read the same `.env`. PowerShell may block local scripts on first run:

```powershell
Set-ExecutionPolicy -Scope Process -ExecutionPolicy Bypass
```

### Also install

- **Docker** — only if you use a container as your dev database (Step 6, Option B)
- **curl** — the bash scripts use it. Preinstalled on macOS. The PowerShell
  scripts use `Invoke-RestMethod` instead, so Windows needs nothing extra.

---

## Step 2 — Log in

```bash
atlas login          # opens a browser
atlas whoami         # confirm
```

**This is mandatory, not "recommended".** The ClickHouse driver is gated behind
an Atlas Pro entitlement. Logged out, `atlas schema inspect` against ClickHouse
will not work, and neither will anything else in this repo.

Everything the demo relies on sits behind that entitlement: the ClickHouse
driver, materialized views, `migrate lint`, drift detection, custom schema rules.

Headless machines and CI use a token instead of a browser:

```bash
export ATLAS_TOKEN="..."      # bash
$env:ATLAS_TOKEN = "..."      # PowerShell
```

Licensing and cost are between the team and Ariga.

---

## Step 3 — Create the ClickHouse Cloud service

1. Sign in at [console.clickhouse.cloud](https://console.clickhouse.cloud).
2. Create a service. Any tier and region is fine for this demo.
3. Save the generated `default` password when it is shown. It is not shown again.
4. Open the service, click **Connect**, and note:
   - **Host** — e.g. `abc123xyz.us-east-1.aws.clickhouse.cloud`
   - **Native port** — `9440`, what Atlas connects on
   - **HTTPS port** — `8443`, what the helper scripts use
   - **Username** — `default` unless you changed it

Two things that will bite you:

- **Atlas needs the native protocol on port 9440 with `?secure=true`.** Cloud
  rejects the native protocol without TLS. The HTTPS port 8443 is for the helper
  scripts only.
- **The service idles.** First command after a pause will be slow while it wakes.
  Run something trivial before you present so you are not staring at a hang.

---

## Step 4 — Open the IP access list

New ClickHouse Cloud services default to **Allow from anywhere**, which will let
you connect but is not what you want to leave in place. In many corporate
accounts it has already been locked down, and that is the single most common
cause of "Atlas can't connect" during a demo.

In the Cloud console: select the service, **Settings**, then **Security** →
**IP access list** → **Add IPs**. Add your current public IP, or your office or
VPN range in CIDR notation. Click **Save**.

Verify from the machine you will present from:

```bash
curl https://<YOUR_HOST>.clickhouse.cloud:8443
# "Ok."                              -> you are allowed
# "Connection reset by peer"         -> you are not
# "SSL_ERROR_SYSCALL"                -> you are not
```

Notes worth knowing:

- IP access lists apply only to connections from the public internet. They do not
  apply to PrivateLink traffic. If you want PrivateLink only, set `DenyAll`.
- IPv4 only, currently.
- If you present from a different network than you tested on, your IP changes and
  you are locked out. Add both, or check again on the day.

---

## Step 5 — Create the database and scoped users

Open [`setup/01-users-and-grants.sql`](setup/01-users-and-grants.sql), replace the
two placeholder passwords, and run it in the Cloud SQL console as `default`.

It creates:

| Object | Purpose | Scope |
|---|---|---|
| `adtech` database | what Atlas manages | `CREATE DATABASE IF NOT EXISTS adtech` |
| `atlas_admin` user | plans and applies migrations | `adtech.*` plus `SELECT ON system.*` |
| `atlas_drift` user | the scheduled drift check | read-only |

**This step is not skippable, whichever user you connect as.** It is the only
place in the repo that runs `CREATE DATABASE adtech`, and nothing downstream
works without it: `bootstrap.sh` refuses to run, and Atlas cannot even open a
connection, because the database name in a ClickHouse URL travels in the
native-protocol handshake.

You *can* connect as `default` and skip the two users if you are short on time —
but you still have to run section 1 of the file, the `CREATE DATABASE`. Creating
the users is itself part of the argument you are making: a tool that applies
schema changes should not hold a full-admin credential, and the read-only drift
job should be structurally unable to mutate anything.

The file ends with a test that the read-only user really is read-only. Run it. A
read-only credential you have not tested is a hope, not a control.

`SELECT ON system.*` is required, not optional — Atlas reads `system.tables` and
`system.columns` to determine current state. Without it every diff comes back as
though the database were empty.

---

## Step 6 — Choose a dev database

Atlas needs a scratch database it wipes on every command, to compute and validate
plans before touching your target. This choice matters more than it looks.

**Option A — a second ClickHouse Cloud service. Recommended for anything real.**

Exact engine and version parity with production, and it idles when unused so the
cost is small. Create a second service exactly as in Step 3, then create the dev
database on it, as `default`:

```sql
CREATE DATABASE atlas_dev;
```

```
CH_DEV_URL=clickhouse://default:PASSWORD@dev123.us-east-1.aws.clickhouse.cloud:9440/atlas_dev?secure=true
```

The `CREATE DATABASE` is required. Atlas creates and drops *objects inside* the
dev database on every command and expects to find it empty, but it does not
create the database itself — and the same handshake rule as Step 5 applies, so a
missing `atlas_dev` is a connection failure, not an empty schema.

Use `default` on that service: Atlas needs broader rights there than
`atlas_admin` has, and the instance holds no real data.

**Option B — a local container Atlas manages. Fine for a demo.**

```
CH_DEV_URL=docker://clickhouse/26.6/dev
```

Needs Docker. Free and offline, but it is ClickHouse **OSS**, so it does not know
`SharedMergeTree`, and its version will not match Cloud. Preflight prints that
gap deliberately — it is a real risk, not a cosmetic one.

Separately from the dev database, you can start a local ClickHouse to *rehearse*
the whole demo against. All six scenarios work with `--env local`:

```bash
./scripts/local-up.sh                    # docker compose, fixed ports + password
```
```powershell
.\scripts\win\local-up.ps1
```

That drives `docker-compose.yml`: 9000 and 8123 published on the same numbers,
password fixed at `localpass`, `adtech` created for you. The `CH_LOCAL_*` defaults
in `.env.example` already match, so there is nothing to paste.

It deliberately does **not** use `atlas tool docker`. That publishes only the
native protocol port, on a random host port, with a generated password — correct
for the dev database above, where Atlas is the only thing connecting, and useless
for a rehearsal target that the helper scripts drive over HTTP on 8123.

---

## Step 7 — Configure `.env`

```bash
cp .env.example .env
$EDITOR .env
set -a && source .env && set +a        # bash: export everything
```
```powershell
Copy-Item .env.example .env
notepad .env
# PowerShell scripts read .env directly. No export step needed.
```

Fill in:

| Variable | From | Notes |
|---|---|---|
| `CH_CLOUD_HOST` | Step 3 | hostname only, no scheme |
| `CH_CLOUD_USER` | Step 5 | `atlas_admin`, or `default` |
| `CH_CLOUD_PASSWORD` | Step 3 or 5 | |
| `CH_CLOUD_DB` | — | `adtech` |
| `CH_CLOUD_URL` | composed | native, **9440**, **`?secure=true`** |
| `CH_CLOUD_HTTP` | composed | HTTPS, **8443**, helper scripts |
| `CH_DEV_URL` | Step 6 | |
| `CH_LOCAL_URL` | Step 6 | only if rehearsing locally |
| `SEED_ROWS` | — | `5000000` default |

`.env` is gitignored. Keep it that way.

The `CH_CLOUD_URL` and `CH_CLOUD_HTTP` lines in `.env.example` are composed from
the parts above them using `${VAR}` expansion, so you normally only edit host,
user and password. Both the bash and PowerShell loaders expand those references.

---

## Step 8 — Run preflight

```bash
./scripts/preflight.sh
```
```powershell
.\scripts\win\preflight.ps1
```

It checks the `SELECT ON system.*` grant — and how many tables you can actually
see through it, which is the only check here that catches a silent,
plan-corrupting misconfiguration — plus the CLI, login state, Docker, HTTP
reachability, whether the database exists yet, native-protocol connectivity
through Atlas, and the dev database. When the dev database is a `docker://`
image it prints the version gap against Cloud on purpose.

Fix every `FAIL`. Then read every `WARN`: most are things to say out loud rather
than things to fix — the dev-versus-Cloud version gap is the main one — but a
missing `adtech` is a `WARN` you do have to act on.

That is deliberate. Preflight is safe to run at any point after Step 7, including
before Step 5, so a not-yet-created database carries the `CREATE DATABASE`
command rather than failing the run and blaming the port.

### Troubleshooting

| Symptom | Cause | Fix |
|---|---|---|
| `atlas: command not found` | not on PATH | Step 1; reopen your shell |
| ClickHouse commands fail although logged in | no Pro entitlement on this account | check `atlas whoami` — likely the wrong org |
| `Connection reset by peer` on 8443 | IP access list | Step 4 |
| Atlas connects on 8443 but not 9440 | wrong protocol | Atlas needs native 9440 with `?secure=true` |
| Every diff wants to create everything | missing `SELECT ON system.*` | Step 5 |
| `Database adtech does not exist` (code 81) | database never created | Step 5 — `setup/01-users-and-grants.sql` line 37 |
| Preflight says "probed against 'default'" and "cannot inspect adtech" | `adtech` does not exist, or your user cannot see it | Step 5; if it does exist, check the grants preflight printed |
| `Database atlas_dev does not exist` on the dev service | dev database never created | Step 6, Option A — `CREATE DATABASE atlas_dev;` |
| First `atlas migrate diff` emits `CREATE TABLE` for everything | `migrations/` has no baseline | Step 9, the baselining block |
| `SharedMergeTree` rejected on dev | Cloud engine against OSS dev | write `MergeTree`; see the note below |
| dev image tag not found | bad `docker://` version | pick a tag that exists on Docker Hub |
| PowerShell won't run `.ps1` | execution policy | `Set-ExecutionPolicy -Scope Process -ExecutionPolicy Bypass` |
| First command hangs ~30s | service was idle | run something trivial to wake it |

---

## Step 9 — Load the baseline and data

```bash
./scripts/bootstrap.sh          # baseline schema into the EXISTING adtech, without Atlas
./scripts/seed.sh               # 5M events across ~7 monthly partitions
./scripts/use-step.sh 0         # desired-state file = baseline
```
```powershell
.\scripts\win\bootstrap.ps1
.\scripts\win\seed.ps1
.\scripts\win\use-step.ps1 0
```

`bootstrap.sh` deliberately does not use Atlas, and deliberately does not create
the database — Step 5 did that. It simulates the realistic starting point: a
database that already exists and is not in version control. The first thing the
demo does is bring it under control.

`seed.sh` prints a reconciliation between the raw table and the aggregate. They
must match. Screenshot it — you compare against it after scenario 5.

### Baseline the migration directory

Do this before scenario 1. `atlas migrate diff` compares the **migration
directory** against `schema/sql/schema.sql`; it never reads the target database.
`migrations/` is empty, so without this the first plan is the whole schema rather
than the one-line change you meant to show.

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

`--baseline` records that version as already applied and executes nothing, which
is correct: the objects are already there from `bootstrap.sh`.

This also creates `adtech.atlas_schema_revisions`, where Atlas keeps its applied-
migration history. `schema/sql/schema.sql` does not describe it, so any
`atlas schema diff` against the live service will propose dropping it unless you
pass `--exclude atlas_schema_revisions`. `reset.sh` drops it along with
everything else, which is consistent: reset also clears `migrations/`.

You are ready. Work through [`SCENARIOS.md`](SCENARIOS.md).

Reset at any point with `./scripts/reset.sh` or `.\scripts\win\reset.ps1`. Reset
clears `migrations/` too, so re-run the baselining block afterwards.

---

## Round-trip parity test

Worth 30 seconds after every Atlas upgrade and after any Cloud version change.
Anything Atlas does not round-trip is something it does not model, and therefore
something it will not defend against drift.

```bash
./scripts/gen-hcl.sh cloud     # or: local
```

That writes `schema/hcl/schema.generated.hcl` and `schema/sql/schema.inspected.sql`
and then prints the comparison itself. It normalises before diffing: comments
stripped, each statement collapsed to one line. A raw
`diff <(sort schema.sql) <(sort schema.inspected.sql)` is not usable for this —
measured on this repo it was 107 lines against 41 with essentially every line
differing, because your file is formatted for humans and Atlas's is not, so a
genuinely dropped attribute is invisible in the noise.

**Measured against ClickHouse Cloud 26.2 with Atlas v1.3.1, nothing was dropped.**
The baseline schema round-tripped completely: TTL, `index_granularity`, defaults,
`LowCardinality`, the `Decimal` precisions and the skipping index all survived.
What you do see is five cosmetic renderings, and it is worth knowing them so you
can dismiss them at speed:

| You wrote | Atlas reads back | Why |
|---|---|---|
| `ENGINE = MergeTree` | `ENGINE = SharedMergeTree('/clickhouse/tables/{uuid}/{shard}', '{replica}')` | Cloud promoted the engine and filled in the replication path |
| `CODEC(Delta, ZSTD(1))` | `CODEC(Delta(4), ZSTD(1))` | ClickHouse filled in the Delta byte size |
| (implicit) | `PRIMARY KEY (...)` | made explicit from the `ORDER BY` prefix |
| `PARTITION BY toYYYYMM(x)` | `PARTITION BY (toYYYYMM(x))` | expression wrapped in parentheses |
| `INDEX i x TYPE minmax` | `INDEX i ((x)) TYPE minmax` | expression wrapped twice |

One distinction that matters, because the two look contradictory:

- **`atlas schema inspect` returns `Shared*`.** Raw inspect reports what Cloud
  actually has, replication arguments included.
- **`atlas schema diff` does not report an engine change.** Verified in scenario 6:
  diffing Cloud against the OSS-worded file produced no engine noise, because Atlas
  normalises both sides through the dev database first.

So the `Shared*` in the parity output is expected and is not drift. It is also why
the "write OSS engine names" rule works at all.

Anything missing from the right-hand side that is *not* in the table above is the
real finding. Codecs, projections and TTLs are the usual suspects on other schemas.

`gen-hcl.sh` also gives you the SQL-versus-HCL comparison: put
`schema.generated.hcl` next to `schema/sql/schema.sql` and ask which one the team
would rather review in a pull request. For a ClickHouse-only team, stay on SQL —
they already read it, every ClickHouse feature is expressible, and review happens
on the exact text the database will see.

## Sources

- [Atlas docs — installation](https://atlasgo.io/docs#installation)
- [Getting Started](https://atlasgo.io/getting-started) — `atlas tool docker`, workflows
- [Feature Compatibility](https://atlasgo.io/features) — which drivers need Atlas Pro
- [Managing ClickHouse Cloud via Atlas](https://atlasgo.io/guides/clickhouse/clickhouse-cloud-atlas) — connection URLs, engine mapping
- [ClickHouse Cloud — setting IP filters](https://clickhouse.com/docs/cloud/security/setting-ip-filters)
- [CLI data privacy](https://atlasgo.io/cli/data-privacy)
