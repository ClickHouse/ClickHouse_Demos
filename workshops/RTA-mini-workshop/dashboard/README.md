# Forex Live Dashboard

A small dashboard that visualizes the `forex` table you loaded in the workshop,
served from your own ClickHouse Cloud service. Change a filter and watch the
query time — that's the point: ClickHouse answers in milliseconds.

- **Backend:** FastAPI + `clickhouse-connect`
- **Charts:** Apache ECharts (candlestick + volume, interactive zoom)
- **Runs in Docker** so your laptop's Python version doesn't matter.

## Prerequisites

- [Docker Desktop](https://www.docker.com/products/docker-desktop/) (includes `docker compose`).
- A running ClickHouse Cloud service with the `forex` table loaded (Steps 1–3 of the workshop).

## Setup

1. Clone the repo and go into this folder:

   ```bash
   git clone https://github.com/ClickHouse/ClickHouse_Demos.git
   cd ClickHouse_Demos
   git switch build-workshop-v1
   cd workshops/RTA-mini-workshop/dashboard
   ```

2. Create your `.env` from the template and fill in your connection details:

   ```bash
   cp .env.example .env
   ```

   Find the values in the Cloud console under **Connect → HTTPS/Native**:

   | Variable | Value |
   | --- | --- |
   | `CLICKHOUSE_HOST` | e.g. `abc123.ap-southeast-1.aws.clickhouse.cloud` (no `https://`, no port) |
   | `CLICKHOUSE_PORT` | `8443` |
   | `CLICKHOUSE_USER` | `default` |
   | `CLICKHOUSE_PASSWORD` | the password you set when creating the service |
   | `CLICKHOUSE_DATABASE` | `default` |
   | `CLICKHOUSE_SECURE` | `true` |

3. Build and run:

   ```bash
   docker compose up --build
   ```

4. Open <http://localhost:8000>.

Stop it with `Ctrl-C`, or `docker compose down` from another terminal.

## Run without Docker (optional)

Needs Python 3.9+:

```bash
pip install -r requirements.txt
cp .env.example .env   # then edit it
uvicorn app:app --reload --port 8000
```

## Troubleshooting

- **Red banner "Could not reach ClickHouse":** re-check the values in `.env`. The
  host must have no `https://` prefix and no port suffix; the port is `8443`.
- **"forex table not found":** load the data first (workshop Steps 2–3).
- **Port 8000 in use:** change the mapping in `docker-compose.yml`, e.g.
  `"8080:8000"`, then open <http://localhost:8080>.
