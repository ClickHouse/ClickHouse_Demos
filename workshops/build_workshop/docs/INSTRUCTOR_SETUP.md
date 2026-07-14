# ClickHouse BUILD Workshop — Instructor Setup Guide

Date: 2026-07-03. Companion documents: `WORKSHOP_PLAN.md` (design and run of show),
`PARTICIPANT_PREWORK.md` (what attendees do before arriving).

Everything below was researched against official docs and live sources on 2026-07-02/03.
Steps marked **VERIFY** could not be confirmed without a live account and must be checked
during your dry run — they are consolidated in section 9.

---

## 0. Timeline at a glance

| When | What |
|---|---|
| T-4 weeks | Section 1 (organizer org, API keys), section 2 (Slack), section 3 (LLM key pool), start Phase 1 repo work per WORKSHOP_PLAN.md |
| T-2 weeks | Section 4 (fallback service pool), section 5 (shared incident service), section 6 (your own demo environment) |
| T-1 week | Send PARTICIPANT_PREWORK.md to attendees; full dry run; work section 9 verification list |
| T-1 day | Re-send prework reminder ("prework.sh must print PASS"); re-validate clickhousectl version and MCP endpoints; charge everything |
| Day of | Section 7 (day-of checklist); doors open 30 min early for setup clinic |
| T+1 day | Section 8 (teardown) |

---

## 1. ClickHouse Cloud organizer org and API keys

You need one ClickHouse Cloud organization you control, with billing able to carry a
handful of small services (fallback pool + shared incident service + your demo service).

### 1.1 Create the org (skip if you already have one)

1. Go to https://console.clickhouse.cloud/signUp
2. Sign up with your work email (or Google/Microsoft SSO). Verify the email within 24h.
3. An organization is created automatically on first login. New orgs default to the
   Scale tier.

### 1.2 Create an org API key (needed for pool provisioning and clickhousectl writes)

Source: https://clickhouse.com/docs/cloud/manage/openapi

1. In the console, open the **API Keys** page (left menu / organization section —
   **VERIFY** exact location; the console was rebuilt recently and docs still say
   "API Keys tab in the left menu").
2. Click **New API Key** (or the on-screen prompt if this is the first key).
3. Name it `build-workshop-provisioning`, give it **Admin** permissions, set an
   expiration shortly after the workshop date.
4. Click **Generate API Key**. The **Key ID** and **Key secret** are shown ONCE —
   store both in your password manager immediately.
5. Note: deleting an API key is permanent and immediately cuts off anything using it.

---

## 2. Slack: shared workspace, app, and the projector webhook

Decision from the plan (D5): one shared webhook URL that all participants POST to; the
channel lives on the projector. Participants do not need Slack accounts.

### 2.1 Create the workspace

Source: https://slack.com/help/articles/206845317-Create-a-Slack-workspace

1. Go to https://slack.com/get-started#/createnew
2. Enter your email, click **Continue**, enter the emailed confirmation code.
3. Click **Create a workspace**; name it `clickhouse-build-workshop`. Skip the invite
   step.
4. Create the channel: click **+** next to Channels, **Create channel**, name it
   `#workshop-alerts`, keep it Public.

### 2.2 Create the app and webhook

Source: https://docs.slack.dev/messaging/sending-messages-using-incoming-webhooks/

1. Go to https://api.slack.com/apps?new_app=1 while signed into the workshop workspace.
2. **Create New App** -> **From scratch**. Name: `Workshop Notifier`. Workspace: the
   workshop workspace. Click **Create App**.
3. In the app's left sidebar under Features, click **Incoming Webhooks**.
4. Toggle **Activate Incoming Webhooks** on.
5. Click **Add New Webhook to Workspace** (bottom of page).
6. Select `#workshop-alerts`, click **Authorize** (button may read **Allow** — VERIFY).
7. Copy the webhook URL from **Webhook URLs for Your Workspace**. Shape:
   `https://hooks.slack.com/services/T.../B.../XXXXXXXXXXXXXXXXXXXXXXXX`

Test it:

```bash
curl -X POST -H 'Content-type: application/json' \
  --data '{"text": "Projector channel is live"}' \
  https://hooks.slack.com/services/T.../B.../XXX...
```

Success returns HTTP 200 with body `ok`.

### 2.3 Operating rules for the shared webhook

- The URL is a bearer credential: anyone holding it can post. Distribute it day-of only
  (slide + room chat), never commit it to the repo.
- Rate limit is roughly 1 message/second with burst tolerance; 30 people posting a
  handful of alerts over an hour is fine, but a synchronized "everyone fire now" moment
  will drop some messages with HTTP 429 — stagger it ("post as you finish").
- All posts render as the app identity. Lab instructions must tell participants to
  prefix messages with their name.
- If the URL leaks or misbehaves: app settings -> Incoming Webhooks -> remove the
  webhook (or uninstall the app) — this invalidates the URL instantly. Keep this page
  open during the session.
- Free-plan limits (90-day history, 10 app installs) do not affect a 3-hour workshop.

---

## 3. Instructor LLM fallback key pool (Anthropic, spend-capped)

For participants who arrive without a working LLM API key for the Lab 3 GenBI app.
Mechanism: one Anthropic org, N workspaces, one key per workspace, monthly spend limit
per workspace (a hard cap — unlike OpenAI project budgets, which are soft alerts only).

Source: https://platform.claude.com/docs/en/manage-claude/workspaces and
https://support.claude.com/en/articles/9796807

Prep (once): sign in at https://platform.claude.com (console.anthropic.com redirects
there — VERIFY), ensure the account is set up as an Organization, and load enough
prepaid credits under **Settings > Billing** to cover the pool (10 slots x $10 = $100).

Repeat per pool slot (recommend 8-10 slots):

1. **Settings > Workspaces** -> **Create workspace** -> name `ws-pool-01` (color-code) ->
   **Create**.
2. Open the workspace -> **Limits** tab -> Spend limits -> **Set spend limit** -> $10.
   Note: spend limits cannot be set on the Default Workspace — that is why each pool key
   gets its own workspace.
3. Workspace -> **API Keys** tab -> **Create Key** -> name `pool-key-01` -> **Create
   Key** -> record the `sk-ant-...` value in your vault (shown once).
4. Print each key on a paper slip with: the key, `LLM_MODEL=claude-haiku-4-5`, and
   "return slip after the session".

Notes:
- Workspaces can be created via the Admin API, but API keys can only be created in the
  Console ("for security reasons" — official docs), so the loop above is manual.
- Model guidance for the GenBI app: `claude-haiku-4-5` ($1/$5 per MTok) is the right
  default for NL-to-SQL; `claude-sonnet-5` is the step-up if SQL quality disappoints.
- Revocation after the event is one action per slot: archive the workspace (section 8).

---

## 4. Fallback ClickHouse service pool (for failed trials)

Sized at 15-20 percent of attendance (5-6 services for 30 people), created in YOUR org.
Participants who cannot get a trial receive a paper slip with connection details.

### 4.1 Install and authenticate clickhousectl with your API key

```bash
curl https://clickhouse.com/cli | sh
export PATH="$HOME/.local/bin:$PATH"    # installer does NOT edit your shell profile
clickhousectl --version                  # guide written against v0.3.1 (beta)
```

Auth for WRITE operations requires the org API key (browser OAuth is read-only):

```bash
export CLICKHOUSE_CLOUD_API_KEY=<key-id>
export CLICKHOUSE_CLOUD_API_SECRET=<key-secret>
```

Exact env var names above are correct for v0.3.1; the getting-started blog's
`CLICKHOUSE_API_KEY` (without CLOUD) is outdated. Alternative:
`clickhousectl cloud auth login --api-key ... --api-secret ...` — but note credentials
save to `.clickhouse/credentials.json` in the CURRENT DIRECTORY (known beta quirk,
issue #277), so either use the env vars or always run from one fixed directory.

### 4.2 Create the pool services

```bash
for i in 01 02 03 04 05 06; do
  clickhousectl cloud service create --name "ws-pool-$i" \
    --provider aws --region <your-workshop-region> \
    --min-replica-memory-gb 8 --max-replica-memory-gb 8 \
    --num-replicas 1 \
    --idle-scaling true --idle-timeout-minutes 5 \
    --ip-allow "0.0.0.0/0"
done
```

- The create output includes the service ID, host, and the `default` user password —
  SHOWN ONCE. Record everything per service immediately.
- IMPORTANT: services created this way are plain analytics services. The ClickStack
  (HyperDX) experience is tied to the "Observability" use-case selection in the console
  wizard — **VERIFY** during the dry run whether ClickStack can be enabled on an
  API-created service, or whether pool services must instead be created through the
  console wizard with Observability selected. If console-only, create the pool by hand
  in the console (6 services, ~10 minutes) instead of the loop above.
- Enable MCP on each pool service: console -> service -> **Connect** -> **Connect with
  MCP** -> toggle on (one toggle gates both `/mcp` and `/clickstack` endpoints).

### 4.3 Prepare the slips

Each slip: service name, HTTPS endpoint (`https://<host>:8443`), native endpoint
(`<host>:9440`), `default` password, and the service ID (UUID — get it via
`clickhousectl cloud service list --json`).

Pool users share YOUR org, which has many services — so unlike trial users, their
ClickStack MCP config MUST include the `x-service-id: <their-service-uuid>` header
(without it, the MCP binds to the first ClickStack service on the account). The
per-tool header syntax is in PARTICIPANT_PREWORK.md appendices.

Also invite pool users to your org so MCP OAuth resolves: console -> click your org
name -> **Users and roles** -> **Invite members** -> their emails, role **Member** plus
service role **Service Admin** scoped to their pool service (**VERIFY** the minimum
role combination that lets remote MCP OAuth run queries — test with a scratch account).

### 4.4 Idle-wake caveat

Pool services idle after 5 minutes; the first query auto-wakes them (~1 minute).
Pre-warm all pool services during the setup clinic:
`clickhousectl cloud service query --name ws-pool-01 --query "SELECT 1"`.

---

## 5. Shared incident service (Docker-less participants)

Participants whose laptops cannot run Docker do the full Lab 2 investigation against a
service YOU own that already has the payments app streaming into it.

1. Create one more service in your org named `ws-shared-incident` — through the console
   wizard with the **Observability** use case so ClickStack is on. Smallest size,
   idle-scaling OFF for the workshop day (it must stay warm).
2. On your instructor machine (or a cheap cloud VM), run the workshop `app/` stack
   pointed at this service (`app/.env` -> this service's endpoint/password). Start it
   at least 60 minutes before Lab 2.
3. Create a read-only user for these participants (SQL console, as `default`):

```sql
CREATE USER workshop_reader IDENTIFIED WITH sha256_password BY '<Strong12+CharPassword!>'
SETTINGS readonly = 2, max_result_rows = 100000, result_overflow_mode = 'break', max_execution_time = 60;
GRANT SELECT ON default.* TO workshop_reader;
```

   (readonly = 2, not 1: some MCP clients and drivers send per-query settings, which
   readonly = 1 rejects. Cloud requires 12+ char passwords with upper/lower/number/
   special.)
4. Docker-less access route (no org invite needed): they run the official local MCP
   server `mcp-clickhouse` (stdio) against this service with the `workshop_reader`
   credentials — same tool surface as the hosted MCP, no OAuth. Config snippets are in
   PARTICIPANT_PREWORK.md appendix E. Alternative: invite them to the org and give them
   the hosted MCP with `x-service-id` (section 4.3 role caveat applies).
5. You flip the fault flag for this shared service from YOUR app instance
   (`localhost:8080/feature` on the machine running it) at the same moment participants
   flip theirs.

---

## 6. Your own demo environment (cold open + projector)

Replicate the participant experience end to end on your presenter machine — this IS the
dry run for the participant guide, and it powers the 0:00-0:05 cold open.

1. Do every step of PARTICIPANT_PREWORK.md yourself, on a fresh ClickHouse Cloud trial
   created with a personal email (so you see exactly what attendees see, including the
   onboarding wizard, the credits page, and the one-time password screen — capture
   screenshots for the slides as you go).
2. Stand up the payments app streaming to your trial service; verify in HyperDX:
   console -> service -> **ClickStack** (left menu) -> **Start Ingestion** -> skip the
   collector step (yours already runs) -> **Launch ClickStack** -> it auto-detects the
   otel tables -> **Start Exploring** -> switch source to Logs, time range Last 15
   minutes. **VERIFY**: the repo's collector writes to the `default` database, which
   matches the managed ClickStack default; confirm auto-discovery fires (the docs'
   external-collector example uses an `otel` database instead — if discovery fails,
   this is why, and the fix is setting `HYPERDX_OTEL_EXPORTER_CLICKHOUSE_DATABASE`
   consistently in the workshop repo).
3. Wire your coding agent per the appendices, install the SRE investigator skill, and
   rehearse the full hero investigation at least twice: flip `paymentFailure` to 75
   percent at `localhost:8080/feature`, ask the root-cause question, confirm the agent
   lands on `card-auth` + gold-tier, and the Telegram + Slack notifications fire.
4. Record a backup screen capture of a successful run — this is your fallback if live
   wifi dies during the cold open.
5. Sanity-check StatusCode values on your service before finalizing lab SQL:
   `SELECT DISTINCT StatusCode FROM default.otel_traces` — the repo's validated queries
   assume `'Error'`; the lab answer key uses `IN ('Error', 'STATUS_CODE_ERROR')` to be
   safe either way.

---

## 7. Day-of checklist

Venue (agree with the venue T-1 week):
- Symmetric bandwidth ~100 Mbps or better; NO captive portal (breaks every OAuth flow);
  no blocking of `*.clickhouse.cloud`, `*.slack.com`, `api.telegram.org`,
  `platform.claude.com` / `api.anthropic.com`, `ghcr.io`, `github.com`.
- Projector + HDMI/USB-C, power strips per table, a spare laptop.

Morning of:
- [ ] Doors open 30 minutes early; setup clinic staffed (you + roamers).
- [ ] Pre-warm all pool services (section 4.4); start the shared incident service app.
- [ ] Slack `#workshop-alerts` open on the projector; webhook tested.
- [ ] Your demo stack running; fault flag confirmed OFF (`app/src/flagd/demo.flagd.json`
      -> `paymentFailure.defaultVariant = "off"`); telemetry flowing.
- [ ] Paper slips ready: pool service credentials (section 4.3), LLM pool keys
      (section 3), the Slack webhook URL slide.
- [ ] USB sticks with the Docker images (`docker save` bundle) as the wifi fallback.
- [ ] clickhousectl version re-checked (`clickhousectl update --check`) — if a new
      version shipped this week, re-run the smoke tests before trusting it live.

During:
- The hard pivot rule: at 2:10 the room moves to Lab 3 regardless of Lab 2 state.
- Track which participants took pool slips / LLM keys (a simple sign-out sheet) so
  teardown is complete.

---

## 8. Teardown (T+1 day)

1. LLM key pool: https://platform.claude.com/settings/workspaces -> for each `ws-pool-*`
   workspace: **...** -> **Archive** -> confirm. Archiving immediately revokes all keys
   in the workspace and cannot be undone. Do NOT archive your Default or Claude Code
   workspaces. Audit spend per workspace afterwards via the Usage page or the Usage
   and Cost API.
2. Slack webhook: api.slack.com/apps -> Workshop Notifier -> Incoming Webhooks ->
   remove the webhook (or uninstall the app) unless you plan to reuse the workspace.
3. Pool services: `clickhousectl cloud service delete <service-id>` for each `ws-pool-*`
   and `ws-shared-incident` (or stop instead of delete if a rerun is scheduled).
4. Org API key: delete `build-workshop-provisioning` in the console if it has no
   further use.
5. Org members: remove pool participants from **Users and roles** if you invited any.

---

## 9. Master verification list (work through during the dry run)

ClickHouse Cloud (fresh trial account):
1. Signup: confirm no credit card is requested; confirm the onboarding use-case screen
   and whether "Observability" can be selected when creating a service LATER from an
   existing org (New service button) — this decides whether participants can create
   accounts in prework and services day-of, which is the plan.
2. Credits visibility: exact widget/path showing "$300 / 30 days" (plan says the
   prework checkpoint is "credits visible").
3. Service creation: exact one-time password screen wording; where the service ID/UUID
   is shown (expect in the URL and Settings page); provisioning wall-clock time.
4. MCP toggle: exact Connect -> Connect with MCP path; whether a non-admin org role can
   flip it; confirm one toggle enables both `/mcp` and `/clickstack`.
5. ClickStack: left-menu item present on trial service; HyperDX auto-discovery against
   the workshop app's `default`-database tables; Team Settings -> API & Agents
   connection strings page.
6. ClickPipes wizard: whether the S3 Credentials fields accept empty values for the
   public `github-2022-flat.ndjson.gz` object; NDJSON format label (expect JSONEachRow);
   gz handling. Fallback if blocked: `INSERT ... FROM s3(...)` in the SQL console.
7. Read-only user: `CREATE SETTINGS PROFILE` acceptance on trial; hosted MCP behavior
   against readonly = 1 vs readonly = 2 user.
8. uk_price_paid: actual row count (~29-30M expected) and load minutes on a trial
   service; EXPLAIN indexes=1 output shapes for the aligned vs misaligned lab queries.
9. Whether a trial org accepts `clickhousectl cloud service create` custom sizing
   (matters only if you script anything against trial orgs).

clickhousectl:
10. OAuth device flow on a fresh trial account (no feature-flag gate); `cloud service
    query` under OAuth (SELECT works, INSERT correctly refused).
11. Clean-Mac install: PATH export needed; no Gatekeeper quarantine issue expected but
    do one test (`xattr -d com.apple.quarantine ~/.local/bin/clickhousectl` is the fix).
12. `clickhousectl skills --agent claude,cursor,codex,windsurf` output paths on a real
    machine; needs live GitHub access (it pulls main at runtime — a wifi risk; consider
    vendoring the skills into the workshop repo as backup).

Coding tools (one pass per tool, fresh config):
13. Claude Code: `claude mcp add --transport http` + `/mcp` OAuth against both
    endpoints; settings.json `permissions.allow: ["mcp__clickhouse-cloud", "mcp__clickstack"]`
    suppresses prompts; SKILL.md `allowed-tools` MCP wildcard behavior.
14. Cursor: marketplace "Add to Cursor" vs `.cursor/mcp.json`; headers + OAuth together
    on `/clickstack`; `permissions.json` mcpAllowlist `server:*` entries.
15. Codex: `codex mcp add` + `codex mcp login` OAuth (dynamic client registration);
    `http_headers` in config.toml; `default_tools_approval_mode = "auto"`; participants
    must be on a current Codex build.
16. Windsurf: native `serverUrl` + OAuth vs the `mcp-remote` shim fallback; MCP tool
    auto-approval behavior in Turbo/Allowlist modes; current `.windsurf/` vs `.devin/`
    paths and product branding.
17. All four: OAuth flow when the user belongs to exactly one fresh trial org, and the
    skill discovery from `.agents/skills/` + the `.claude/skills` symlink.

Other:
18. Slack: Authorize vs Allow button label; add 3-4 webhooks to one channel to confirm
    per-participant webhooks are possible if you change strategy.
19. Telegram: reproduce the "message the bot first" failure mode and capture the exact
    error text for the troubleshooting box; confirm @userinfobot still works (unofficial).
20. Anthropic console: current labels for Buy credits / Create workspace / Set spend
    limit; behavior when a workspace hits its cap (expect hard rejection).
21. StatusCode literal written by the current clickstack-otel-collector build
    (`'Error'` per the repo's validated queries vs `'STATUS_CODE_ERROR'` in HyperDX
    seed code) — pin the lab SQL accordingly.
