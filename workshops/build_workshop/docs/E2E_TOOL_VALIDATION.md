# BUILD workshop - end-to-end tool validation

Real (not dry-run) end-to-end validation of the agent-centric modules of the BUILD
workshop, executed against live infrastructure with three locally-installed coding
tools: Claude Code, Codex CLI, and Cursor. This validates the learner-track playbook
(`playbook/content/docs/learner/`), modules 00, 02, 03, and 04.

- Date: 2026-07-15/16
- Branch: `build-workshop-v1`
- Executor: automated agent run with a human present for browser OAuth clicks

## Summary

| Tool | CLI version | Outcome | Notes |
|---|---|---|---|
| Claude Code | 2.1.211 | VERIFIED | Both MCP query tasks correct against the live service |
| Codex CLI | codex-cli 0.144.4 | VERIFIED | Both MCP query tasks correct; results identical to Claude |
| Cursor (cursor-agent) | 2025.09.18-7ae6800 | DEFERRED | Config + OAuth flow validated; interactive login not completed, deferred to the dry run |

The MCP endpoint was confirmed enabled on the service (console modal, screenshot-verified).
Both verified tools returned answers that match a direct `clickhouse client` query exactly.

## Infrastructure provisioned (once, shared by all tool legs)

- ClickHouse Cloud service `build-workshop-e2e-tools`, AWS `us-east-1`, single replica,
  8 GB, idle-scaling on (15-min idle timeout). Created with `clickhousectl cloud service create`.
- Schema applied from `app/db/cloud/001_cloud_schema.sql` (clean, exit 0).
- Historical seed from `app/db/cloud/002_seed_historical.sql`: `taxi_zones` = 265 rows,
  `taxi_trips` = 3,174,394 rows (one month, `yellow_tripdata_2022-07.parquet`).
- Postgres CDC ClickPipe created via the CLI path (`clickhousectl cloud clickpipe create
  postgres`) from the shared managed Postgres, participant slot 01
  (`taxi_p01` / publication `pub_taxi_p01`), table mapping
  `public.realtime_trips:realtime_trips`. Reached `Running`.
- CDC materialized view applied from `app/db/cloud/003_cdc_mv.sql`, VARIANT B
  (source `default.realtime_trips`, the CLI-path destination).
- App stack up via `docker-compose.workshop.yml` (frontend `:28080`, backend `:28000`,
  Postgres `:55432`; ports overridden because 8080/8000/5432 were taken).
- Live verification: backend `/api/health` ok; generator inserting into the shared PG
  slot; CDC rows flowing (seed 3,174,394 + `realtime_cdc` 6,680 = 3,181,074 total at
  snapshot); Ops timeseries endpoint (`/api/metrics/timeseries`) returning live
  per-minute buckets.

## Ground-truth answers (direct `clickhouse client`)

Used to check tool output for correctness.

- Databases: `INFORMATION_SCHEMA`, `default`, `information_schema`, `nyc_tlc_data`, `system`.
- `nyc_tlc_data.taxi_trips` row count: ~3.18M (grows live via CDC; 3,181,074 at the run).
- Top 5 pickup zones by trips, July 2022:
  1. JFK Airport - 167,478
  2. Midtown Center - 134,062
  3. Upper East Side South - 129,594
  4. Penn Station/Madison Sq West - 113,973
  5. Upper East Side North - 107,566

## Per-tool results

### Claude Code - VERIFIED

MCP setup (from the tool's own scratch dir `~/tmp/e2e-tools/claude`):

```
claude mcp add --transport http clickhouse-cloud https://mcp.clickhouse.cloud/mcp
claude mcp login clickhouse-cloud      # interactive browser OAuth
```

After login, `claude mcp list` showed `clickhouse-cloud ... Connected`.

Task 1 (module 00 verify) - list databases + row count:

```
claude -p "Using ONLY the clickhouse-cloud MCP tools, list the databases on my service
and return the exact row count of the table nyc_tlc_data.taxi_trips as a number."
--allowedTools "mcp__clickhouse-cloud"
```

Result: listed all five databases correctly; `taxi_trips` = 3,181,074. Correct.

Task 2 (module 04-style conversational BI) - top 5 pickup zones, July 2022: returned the
five zones in the exact order and counts above, with the generating SQL (a
`taxi_trips`-to-`taxi_zones` join filtered to July 2022). Matches ground truth exactly.

### Codex CLI - VERIFIED

MCP setup (from `~/tmp/e2e-tools/codex`):

```
codex mcp add clickhouse-cloud --url https://mcp.clickhouse.cloud/mcp
```

`codex mcp add` auto-started the OAuth flow and reported "Successfully logged in" by
reusing an existing browser session; `codex mcp list` showed `clickhouse-cloud ...
enabled OAuth`.

Real runs used `codex exec` non-interactively:

```
codex exec --dangerously-bypass-approvals-and-sandbox "<task prompt>"
```

Task 1: listed all five databases correctly; `taxi_trips` = 3,181,074. Correct.

Task 2: returned the five pickup zones in the exact order and counts above, plus the
SQL it ran (a `taxi_trips`-to-`taxi_zones` inner join, `pickup_datetime` bounded to
July 2022, `GROUP BY zone ORDER BY count DESC LIMIT 5`). Matches ground truth exactly.

### Cursor - DEFERRED (config validated, OAuth not completed)

- `cursor-agent` is installed (2025.09.18-7ae6800) and logged in to a Cursor account.
- Config written per the playbook: `~/tmp/e2e-tools/cursor/.cursor/mcp.json` (the
  `clickhouse-cloud` URL server) and `.cursor/permissions.json`
  (`"mcpAllowlist": ["clickhouse-cloud:*"]`).
- `cursor-agent mcp login clickhouse-cloud` correctly loads the server from
  `.cursor/mcp.json`, opens the browser OAuth, and listens for the callback - the flow
  is wired correctly.
- The interactive login was not completed (see failure mode below); per decision, the
  Cursor query run is DEFERRED to the workshop dry run. The MCP wiring and login
  invocation are validated; only the final token exchange is outstanding.

## Findings (playbook vs observed reality)

Severity is a rough participant-impact estimate.

1. LOW - MCP toggle label wording. The playbook (module 00 step 7) says
   "Connect -> Connect with MCP -> toggle". The actual console modal is titled
   "Connect to <service>", with a "Connect with:" dropdown set to MCP and a toggle
   labeled "Enable Model Context Protocol"; the endpoint
   `https://mcp.clickhouse.cloud/mcp` shows after enabling. Consider matching the exact
   toggle label.

2. LOW/MEDIUM - CLI-created ClickPipe destination engine. Module 03 documents the
   CLI-path destination as landing in `default` and notes the engine "has been observed
   to differ." Confirmed live: the CLI pipe created `default.realtime_trips` as
   `SharedMergeTree` (not `ReplacingMergeTree`), with the `_peerdb_*` bookkeeping
   columns and the documented PeerDB type mapping (`timestamptz -> DateTime64(6)`,
   `int2/4/8 -> Int16/32/64`, `double precision -> Float64`). VARIANT B of
   `003_cdc_mv.sql` applied cleanly against it. Playbook text is accurate; this
   confirms it.

3. MEDIUM - frontend container reported `unhealthy` forever (found during E2E, FIXED in
   commit `22ec8d9`). `docker compose ps` showed the frontend service as `unhealthy`
   indefinitely even though the site loaded fine at the mapped host port. Root cause:
   the healthcheck ran `wget http://localhost:8080/` inside the container, but nginx
   binds IPv4-only (`0.0.0.0:8080`) while `localhost` resolves to IPv6 `::1` inside the
   container, so the check got "connection refused" on every probe. This contradicted
   module 00's verification step ("the STATUS column reads `healthy`"). Fix (`22ec8d9`):
   both the frontend and backend healthchecks in `docker-compose.workshop.yml` now probe
   `http://127.0.0.1:8080/` (and `:8000`) instead of `localhost`, so module 00's "STATUS
   reads healthy" step holds again.

4. LOW (automation only, not participant-facing) - MCP login needs a real TTY.
   `claude mcp login` (and `cursor-agent mcp login`) require an interactive terminal
   to catch the browser OAuth callback; they error / fail when run from a non-TTY shell
   ("stdin isn't a terminal"). This does NOT affect participants, who always run these
   in their own interactive terminal. It matters only for headless automation/CI of the
   workshop. (For this run, a pseudo-TTY wrapper plus a cached browser session let the
   Claude login complete automatically.)

5. LOW - login must run from the MCP-configured directory. `clickhousectl`/agent MCP
   config is directory-scoped. `claude mcp login` (and the Cursor/Codex equivalents)
   must be run from the directory where the MCP server was added, or the login will not
   attach. Participants who add the server in the app directory but wander elsewhere
   before logging in will hit this. Worth a one-line note in module 00 step 7.

6. INFO - `cursor-agent mcp list` reports "No MCP servers configured" even when a valid
   `.cursor/mcp.json` is present. This is misleading: `cursor-agent mcp login` and
   `mcp list-tools` both read the same file correctly. Do not rely on `mcp list` to
   confirm Cursor MCP config.

7. INFO - Codex non-interactive flag. To run `codex exec` fully non-interactively for
   MCP tool calls, `--dangerously-bypass-approvals-and-sandbox` was required (there is
   no `-a never` on this codex version; `-s`/sandbox alone still prompts for MCP tool
   approval). Fine for read-only MCP; worth noting if the instructor track shows a
   codex non-interactive example.

## Troubleshooting note (recommended addition)

OAuth stale-tab / PKCE mismatch. If the browser shows "Authorized - you can close this
window" but the CLI still reports `Authentication callback failed` (with an empty
reason), the cause is a stale "Continue to client" OAuth tab left open from an earlier
attempt: clicking the old tab delivers its code to the current callback listener, but
the PKCE verifier no longer matches, so the token exchange fails. Fix: close ALL stale
OAuth/authorize tabs, run the login once, and approve only the freshly opened tab. This
was the exact failure mode hit on the Cursor leg.

## Cleanup / cost

- The CDC ClickPipe (`taxi-cdc-e2e`) is deleted at the end of the run so the service can
  idle-scale to zero and stop accruing continuous compute cost. After deletion the Ops
  dashboard stops receiving NEW rows; historical dashboards and all seeded data remain.
- The ClickHouse service `build-workshop-e2e-tools` and the local app stack are LEFT
  RUNNING for exploration. The service is an 8 GB single-replica instance with
  idle-scaling; with the pipe removed it scales to zero when idle, so standing cost is
  minimal, but it should be deleted when no longer needed
  (`clickhousectl cloud service delete <service-id>`).
</content>
</invoke>
