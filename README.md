# ClickHouse Demos

Collection of ClickHouse demo projects showcasing various features and patterns.

## Projects

| Project                                                             | Description                                                                                               |
| ------------------------------------------------------------------- | --------------------------------------------------------------------------------------------------------- |
| [Incremental Materialized Views](./incremental_materialized_views/) | Progressive tutorial from basic MVs to full Medallion Architecture                                        |
| [Telco Marketing Analytics](./agent_stack_builds/telco_marketing/)  | AI-powered telco analytics stack with LibreChat, ClickHouse MCP, LiteLLM, and Langfuse                    |
| [Schema Change Management](./schema_change_management/)             | Schema-as-code against ClickHouse Cloud with the Atlas CLI. Requires a paid Atlas Pro entitlement (Ariga) |

## Getting Started

Each project is self-contained with its own README, SQL scripts, and utilities. Navigate to the project folder and follow the instructions.

## Requirements

Per project, since they diverge:

**Incremental Materialized Views** — a ClickHouse server (local, hybrid or cloud),
`clickhouse-client`, Python 3.8+ for the data generators.

**Telco Marketing Analytics** — Docker and Docker Compose.

**Schema Change Management** — a ClickHouse Cloud service, Docker (for Atlas's dev
database and the local rehearsal container), `bash` and `curl`, and the
[Atlas CLI](https://atlasgo.io/docs) logged in with a **paid Atlas Pro entitlement**
from Ariga, without which the ClickHouse driver does not load. PowerShell 7+ only
if you want the Windows script set. No Python, no `clickhouse-client`.
