# Learner-owned Cloud setup

Each learner owns one ClickHouse Cloud service, one managed Postgres instance, one Postgres ClickPipe, and ClickStack access. The local Docker application uses those services. Shared materials are read-only datasets and model artifacts.

These commands target `clickhousectl 0.4.2`. Help and the tagged [CLI source](https://github.com/ClickHouse/clickhousectl/tree/v0.4.2) were inspected on Oct 10, 2026. Region availability, sizing, and actual Cloud delivery remain release rehearsal checks. `ap-southeast-1` and `c6gd.large` are configurable starting values, not measured workshop guarantees.

## One-command setup

After installing dependencies and logging in with your organization API key from the lab directory, copy the environment template and change the region or Postgres size if needed. Setup generates the installation ID and detects your organization and public IPv4 address. Run:

```bash
bash scripts/setup-cloud.sh
```

This creates or verifies the owned ClickHouse Cloud and managed Postgres services, downloads the source CA, creates `pricing` and the configured telemetry database, initializes the source schema, generates separate app/CDC credentials, applies grants and the four-table publication, and creates a Postgres ClickPipe through the Cloud API. The pipe targets `pricing` with ReplacingMergeTree, preserved NULLs, verified TLS, and a 10-second sync interval. Readiness checks actual table engines and metadata before enabling canonical views.

The command uses the API key pair saved by `clickhousectl cloud auth login` in this lab's `.clickhouse/credentials.json`, or `CLICKHOUSE_CLOUD_API_KEY` and `CLICKHOUSE_CLOUD_API_SECRET` from the shell or current `.env`. No source password or API secret enters process arguments. Generated credentials go to the private environment file with mode 0600. The runtime gets only the limited app DSN; the administrator DSN stays available for host schema operations.

## Destroy and recreate

Stop the local workbench before deleting its Cloud resources. Preview the resources recorded in this installation's inventory:

```bash
bash scripts/setup-cloud.sh --destroy
```

Review the resource IDs, then permanently delete the owned ClickPipe, Managed Postgres instance, and ClickHouse service and their data:

```bash
bash scripts/setup-cloud.sh --destroy --execute
```

Deletion verifies ownership before removing any resources and journals each completed deletion. If interrupted, repeat the command to resume. Only after all deletions succeed does it archive the inventory and clear the old database credentials, session secret, and ClickStack receiver credentials. CLI authentication, installation identity, and workshop preferences remain available.

Run `bash scripts/setup-cloud.sh` to create a fresh stack with one ClickHouse replica. Reload history in Module 01, reconnect ClickStack in Module 02, and restart the local workbench with the new connection settings.

## Resume setup

Rerun the same command after fixing a failed dependency. Saved IDs and generated passwords are reused. An ambiguous create response leaves `pending_creation` set and stops automatic retries. Reconcile the owned resource ID in the console, record it in the corresponding inventory field, and clear `pending_creation` only after identifying that exact resource. Setup rejects unverified administrator TLS, elevated role attributes or memberships, excess source privileges, and mismatched existing pipe source configuration. Correct those settings before retrying; the script does not silently broaden privileges.

A preexisting manually created pipe requires its current `pricing_cdc` password in `PRICING_POSTGRES_CDC_PASSWORD` before adoption; setup refuses to change an unknown replication password. The Cloud GET response omits source credentials, so confirm that a manually created pipe uses `pricing_cdc` in the console before adopting it. New pipes always use that role.

Creating the telemetry database does not start the local collector or enable the Managed ClickStack UI. Follow Module 02 to start telemetry ingestion and launch ClickStack. Historical bulk loading remains the Module 01 exercise.

The sections below retain the individual operations for inspection and recovery.

## Prepare configuration

1. Sign in to your ClickHouse Cloud account. Confirm your organization and service quota.
2. Authenticate the CLI with your own Cloud API key through its supported credential configuration. OAuth access is read-only. Keep credentials in the CLI credential store or environment; do not put secrets in command arguments or commit them.
3. Check `clickhousectl cloud auth status` and `clickhousectl cloud org list`. Setup detects the organization, with a numbered choice if your key has access to several.
4. Copy `app/.env.workshop.example` to `app/.env.workshop`. Change the region or Postgres size if needed. Setup detects your public IPv4 address and configures `/32` access. It saves the organization in the resource inventory for reruns and cleanup. The source's ClickPipes egress access is a separate requirement.
5. Protect the file with `chmod 600 app/.env.workshop`.

Run from the lab directory in macOS Terminal or Ubuntu under Windows WSL2:

```bash
.venv/bin/python -m infra.provision --module setup
```

The wrapper uses the documented `cloud service create` and `cloud postgres create` commands. It supplies explicit owner tags, organization, region, and the ClickHouse IP access list. It captures their output privately, records each ID before polling, and saves one-time credentials only to `app/.env.workshop`. Managed Postgres remains a separately owned resource; the primary Compose file does not start a local database.

Creation is asynchronous. The wrapper polls the saved IDs and requires `state=running`. A retry reuses those resources. If a creation response was interrupted, the inventory records an unresolved creation. Inspect the console, record the actual owned ID, clear `pending_creation`, and recover its password if necessary. The wrapper refuses to blindly create another resource.

## Initialize source data

```bash
.venv/bin/workshop initialize
.venv/bin/python scripts/check-resources.py
bash app/preflight.sh setup
```

Initialization creates the schema and first baseline checkpoint. Repeating it preserves the existing generation, sales, and ledger. It does not reset scenario state or reload history.

Create separate `pricing_app` and `pricing_cdc` login roles in your own managed Postgres instance. Keep their generated passwords private. With `PRICING_POSTGRES_ADMIN_DSN` configured, apply the grants and limited publication:

```bash
.venv/bin/python -m infra.configure_cdc
```

The replication role needs `REPLICATION`, schema access, and `SELECT` on the four source tables. The application role receives the source and ledger privileges required by the workshop. Provisioning saves the administrator URI in `PRICING_POSTGRES_ADMIN_DSN` and initially in `PRICING_POSTGRES_DSN`. Keep the administrator setting for initialization and grants. Change only the username and URL-encoded password in `PRICING_POSTGRES_DSN` to the `pricing_app` credentials afterward. Retain `sslmode=verify-full` and the downloaded instance CA path. Use the `pricing_cdc` credentials only for the connector. Check logical replication through managed Postgres settings before creating it.

Provisioning rewrites the private environment file as quoted key/value assignments and removes template comments. Keep `app/.env.workshop.example` for setting descriptions.

## Create the owned ClickPipe

Use the managed Postgres console's **Sync to ClickHouse** flow or the ClickPipes console. This follows the [managed Postgres replication guide](https://clickhouse.com/docs/cloud/managed-postgres/clickhouse-integration).

The pinned clickhousectl 0.4.2 Postgres command has no destination-database option and takes the source password in process arguments. The workshop uses the console so it can target `pricing` and keep source credentials out of CLI arguments. Do not put a database qualifier into a `--table-mapping` target; it can become a literal table name.

Set the integration name to `pricing-<owner>-cdc`, select your own ClickHouse service, and choose **Initial load + CDC**. Choose destination database `pricing`. Map only these tables:

| Postgres source | ClickHouse destination |
| --- | --- |
| `pricing.sales_transactions` | `sales_transactions_cdc` |
| `pricing.inventory_history` | `inventory_history_cdc` |
| `pricing.price_history` | `price_history_cdc` |
| `pricing.cost_history` | `cost_history_cdc` |

Use publication `pricing_workshop` where the connector asks for an existing publication. Preserve nullable source columns. Verify TLS with the instance CA. Allow the documented [regional ClickPipes egress IPs](https://clickhouse.com/docs/integrations/clickpipes/networking/static-ips) if source access is restricted. This differs from your learner `/32` access list.

Set the sync interval to at most 20 seconds, or set `PRICING_CONVERGENCE_TIMEOUT_SECONDS` to cover it (maximum 120). CDC readiness rejects longer or missing intervals. Confirm each destination uses ReplacingMergeTree and includes `_peerdb_version`, `_peerdb_is_deleted`, and `_peerdb_synced_at`. CDC provisioning validates the owned pipe name and database plus this schema contract before reporting ready. The kit also validates the schema before enabling canonical views. Record the owned ClickPipe ID in `infra/resources.json`, then check it:

```bash
.venv/bin/python -m infra.provision --module cdc
bash app/preflight.sh cdc
```

A **Running** connector does not prove convergence for a particular sale and inventory revision. The learner checks those IDs and selected values in the **Freshness** view.

## Start and restart the local edge

The certificate command writes `.run/managed-postgres-ca.pem`. The Cloud Compose overlay mounts it into the backend and writer. The runtime adapts the host connection's certificate path to that container mount while retaining certificate verification.

```bash
docker compose --env-file app/.env.workshop \
  -f app/docker-compose.workshop.yml -f app/docker-compose.cloud.yml \
  up --build --detach --wait
```

Open `http://localhost:8080`. Use **Start learner session**. Local services restart without recreating Cloud resources. Scenario resets create a new generation and retain earlier evidence and historical imports.

```bash
docker compose --env-file app/.env.workshop \
  -f app/docker-compose.workshop.yml -f app/docker-compose.cloud.yml --profile telemetry up --detach --wait
```

## Connect diagnostics and agents

Start the telemetry profile with `PRICING_OTEL_ENDPOINT=http://collector:4318`. The local ClickStack collector reuses the workshop's ClickHouse connection settings and writes directly to the telemetry database. Enable Managed ClickStack on that service and launch its UI to read the `otel` tables. Follow [obs/README.md](../obs/README.md). Verify a real snapshot ID in ClickStack. The installed CLI has no `cloud clickstack` command.

Connect ClickHouse Agents or your coding agent's MCP connection with a read-only ClickHouse user scoped to the named evidence views. Run one question from [agent/bi-prompts.md](../agent/bi-prompts.md), inspect its SQL, and compare IDs with the ledger. Optional live exception-provider and Langfuse access do not block earlier modules. Use the recorded episode when those optional services are unavailable.

## Recover a lost creation password

`get` does not recover a creation password. With the saved inventory and matching owner, explicitly request recovery:

```bash
.venv/bin/python -m infra.provision --recover-password service
.venv/bin/python -m infra.provision --recover-password postgres
```

These commands reset the owned service credential and save the result privately. Update any connector or application that uses the changed credential. They do not create replacement services. For a dedicated application role, recover that role's password in the Postgres console instead of replacing it with the administrator credential.

## Clean up owned resources

Stop the local edge, then preview the saved Cloud inventory:

```bash
docker compose --env-file app/.env.workshop \
  -f app/docker-compose.workshop.yml -f app/docker-compose.cloud.yml --profile telemetry --profile writer down
.venv/bin/python -m infra.cleanup
```

Inspect the IDs. To delete those owned Cloud resources and their data, run:

```bash
.venv/bin/python -m infra.cleanup --execute
```

The wrapper verifies the configured organization, owner tags, and exact ClickPipe membership before any deletion. It removes the pipe before managed Postgres and ClickHouse. It does not delete shared S3 objects or model artifacts. Keep the inventory for the cleanup audit; remove it only when starting a separate new installation.

Cleanup records each confirmed deletion in `deleted_resources` in `infra/resources.json`, retaining the original IDs. A retry skips journaled deletions. If a previous deletion lost its acknowledgement, cleanup requires a successful Cloud list response proving absence before recording it; authentication and network errors do not prove absence. Forced ClickHouse service deletion has a 15-minute timeout for stop polling. Retry a timed-out cleanup using the same inventory.

For existing checkouts created before the administrator setting was added, copy the retained administrator URI into `PRICING_POSTGRES_ADMIN_DSN`. Do not copy the application-role URI there. Postgres password recovery updates only the administrator setting; if initial provisioning lost its credentials before an app role exists, explicitly copy the recovered administrator URI into `PRICING_POSTGRES_DSN` to complete setup, then switch to the app role in Module 02.
