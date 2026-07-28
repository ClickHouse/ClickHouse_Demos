# Workshop runtime: cloud services, local application edge

The workshop intentionally separates application code from managed services.

## Runs locally

- React/nginx frontend
- FastAPI backend
- synthetic trip writer, enabled only in the `cdc` profile after Module 03
- stateless OpenTelemetry collector that forwards local app telemetry
- `clickhousectl`, `clickhouse client`, the coding agent, and other client tools

## Runs in the cloud

- ClickHouse Cloud service
- ClickHouse-managed Postgres
- ClickPipes
- Managed ClickStack and the HyperDX UI
- remote ClickHouse and ClickStack MCP endpoints
- ClickHouse Agents
- Langfuse Cloud and OpenAI
- the optional instructor-provided HTTPS LibreChat instance

No PostgreSQL, ClickHouse, MongoDB, LibreChat, HyperDX, Langfuse, or MCP server is
started on a learner machine.

## Runtime sequence

1. Module 00 creates ClickHouse Cloud and starts only the local frontend/backend.
2. Module 01 creates and seeds the Cloud schema.
3. Module 03 creates managed Postgres, validates it with
   `./preflight.sh --require-postgres`, then explicitly enables the `cdc` trip writer.
4. Module 05 enables Managed ClickStack and starts the stateless collector overlay.
5. Module 06b uses hosted LibreChat; Module 08 sends chat traces to Langfuse Cloud.

`.env.workshop.example` leaves managed Postgres credentials blank until Module 03 and
requires TLS. `preflight.sh` rejects loopback/local database hosts. CI policy checks in
`../scripts/check-docs.sh` prevent local managed-service definitions from returning.
