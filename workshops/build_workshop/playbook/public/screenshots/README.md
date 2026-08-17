# Console screenshots

Status: SCREENSHOTS-DEFERRED. No authenticated captures exist yet. This directory
holds the shot list and capture instructions so the screenshots can be taken during
the dry run against a logged-in ClickHouse Cloud account. The learner modules already
contain `{/* screenshot: name */}` placeholders (see mapping below); embeds are wired
in once the PNGs land here.

Why deferred: authenticated capture via the headless/automated browser could not be
made reliable in the build environment (details in the Capture notes section). The
fastest reliable path is to capture manually from a browser already logged in to
ClickHouse Cloud, or to run the automated flow from a machine where the cookie picker
can extract the session cookie.

## Target service

`build-workshop-e2e-tools` (us-east-1, MCP already enabled). Data may be loading into
it; that is fine for screenshots.

## Shot list

Save each as a 1280px-wide PNG in this directory, using these exact names.

| File | Surface | What it should show |
|------|---------|---------------------|
| `console-services.png` | console.clickhouse.cloud/services | Services list/home with the service tile |
| `service-overview.png` | the build-workshop-e2e-tools service page | Service overview |
| `connect-modal-mcp.png` | service > Connect > "Connect with: MCP" | The MCP view: "Enable Model Context Protocol" toggle (already ON) and the endpoint URL |
| `connect-modal-credentials.png` | same Connect modal, default view | Host/port credentials view. Verify the password stays masked before saving the image |
| `sql-console.png` | service > SQL console | A trivial read-only result (`SELECT count() FROM system.tables`) if data exists, else the empty editor |
| `clickpipes-tiles.png` | service > Data sources | The create-source tile grid (Postgres / S3 / Kafka, etc.). Do not configure anything |
| `clickpipes-postgres-step1.png` | Data sources > new Postgres CDC | The Postgres wizard FIRST screen, connection form EMPTY. Do not fill or submit |
| `api-keys-page.png` | console.clickhouse.cloud/organizations/keys | Org API Keys list view only. Do NOT create a key; only show existing rows if names are non-sensitive |
| `agents-ui.png` | ai.clickhouse.cloud | Landing / agent picker, view only |
| `hyperdx-home.png` | hyperdx.clickhouse.cloud (ClickStack) | The ClickStack/HyperDX entry, only if it opens without requiring "Start Ingestion" clicks. Otherwise capture the pre-click state |

Not captured:
- Langfuse (`langfuse-traces` placeholder) is a different product with a separate login. Skipped by design.
- ClickPipes wizard step 2 (`clickpipes-wizard-replication` placeholder) requires filling and submitting the connection form to advance, which violates the read-only rule for this task. Left for the dry run, where a real pipe is configured anyway.

## Placeholder mapping

When a PNG exists, replace the matching `{/* screenshot: name */}` JSX comment in the
module with an image embed plus a short italic caption (mini-workshop style). Paths are
relative to the playbook root.

| Placeholder (module:line) | Capture file |
|---------------------------|--------------|
| `content/docs/learner/00-setup.mdx` `{/* screenshot: connect-mcp-modal */}` | `connect-modal-mcp.png` |
| `content/docs/learner/00-setup.mdx` `{/* screenshot: connect-modal */}` | `connect-modal-credentials.png` |
| `content/docs/learner/00-setup.mdx` `{/* screenshot: api-keys-page */}` | `api-keys-page.png` |
| `content/docs/learner/03-realtime-cdc.mdx` `{/* screenshot: clickpipes-wizard-connection */}` | `clickpipes-postgres-step1.png` |
| `content/docs/learner/04-clickhouse-agents.mdx` `{/* screenshot: clickhouse-agents-ui */}` | `agents-ui.png` |
| `content/docs/learner/05-clickstack.mdx` `{/* screenshot: clickstack-hyperdx */}` | `hyperdx-home.png` |
| `content/docs/learner/03-realtime-cdc.mdx` `{/* screenshot: clickpipes-wizard-replication */}` | PENDING (wizard step 2, see Not captured) |
| `content/docs/learner/08-chat-langfuse.mdx` `{/* screenshot: langfuse-traces */}` | PENDING (Langfuse, skipped) |

`console-services.png`, `service-overview.png`, `sql-console.png`, and
`clickpipes-tiles.png` have no placeholder yet; they can go in a doc gallery or get
placeholders added later.

The old module-number filenames for the five module 07/08 screenshots are retained as
compatibility aliases so saved links continue to load after the module reorder. New content
must use the filenames that match the current module number.

## Redaction (before publishing)

These docs are published, so before committing any capture, check for and blur/crop:
- The user's email and organization name (often in the top-right account menu and org switcher).
- Any full API key value. The keys list should show names/prefixes only; never a full secret.
- The service password in the Connect > credentials view. The console masks it by
  default. Confirm it is masked (dots), not revealed, before saving `connect-modal-credentials.png`.
- ClickPipes connection strings if any host/credentials get pre-filled.

Flag any capture that still contains identifying info in a note next to it here, and do
not embed it in a published module until it is redacted.

## Capture notes (for whoever does the dry run)

Deep-link URL patterns for the target service (replace `<SVC>` with the service UUID
from the services page):
- Service overview: `console.clickhouse.cloud/services/<SVC>`
- SQL console: `console.clickhouse.cloud/services/<SVC>/console`
- Data sources / ClickPipes tiles: `console.clickhouse.cloud/services/<SVC>/dataSources`
- Postgres wizard step 1: `console.clickhouse.cloud/services/<SVC>/dataSources/new/postgres`
- Org API keys: `console.clickhouse.cloud/organizations/keys`

Manual capture is the reliable path: open each URL in a browser already logged in to
ClickHouse Cloud, size the window to about 1280px wide, and save with the exact file
name above.

If capturing via the gstack `browse` tool: the httpOnly session cookie is only
extractable through the interactive cookie picker (`cookie-import-browser` with no
args), not through `--domain` import. Keep the browse server alive across commands with
a background holder and route every command with a fixed `BROWSE_STATE_FILE`; do not use
`status`/`focus` against a headed session (they spawn a competing server and tear it
down). See the `gstack-browse-auth-capture` project memory for the full writeup.
