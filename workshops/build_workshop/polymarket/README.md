# Polymarket real-time analytics track

This directory backs the dedicated Polymarket learner and instructor tracks published
at `/docs/polymarket`. It reads public market data only. It does not authenticate to
Polymarket, place orders, create wallets, or provide financial advice.

## Data flow

```text
Gamma discovery ───────────┐
CLOB WebSocket + book REST ├─ collector ─ ClickHouse Cloud
Data API trade REST ───────┘                 ├─ price_ticks -> midpoint MV
                                            └─ trades -> saved queries/dashboard
```

The WebSocket provides low-latency quote updates. Data API polling reconciles public
trades with a five-second overlap. When the socket stalls, CLOB REST maintains BBO and
spread freshness. Deterministic event/trade IDs, a recent-ID hydrate on restart, and a
stable ClickHouse insert deduplication token keep retries safe before the incremental MV
sees rows.

The public Data API does not expose a stable per-fill ID. `trade_id` therefore hashes
the documented transaction, token, wallet, side, price, size, and timestamp fields.
Rows indistinguishable across all of those public fields are treated as one observation.

## Local runtime

Only `collector` runs locally. ClickHouse is always Cloud-hosted.

```bash
cp .env.polymarket.example .env.polymarket
# Fill the ClickHouse Cloud host and password.
set -a; source ./.env.polymarket; set +a
./preflight.sh
docker compose --env-file .env.polymarket up -d --build collector
curl --fail --silent http://localhost:8090/health | python3 -m json.tool
```

## Tests

```bash
cd collector
python3 -m pip install -r requirements-dev.txt
python3 -m pytest -q
cd ..
./test-clickhouse.sh
```

`test-clickhouse.sh` starts a disposable ClickHouse 26.3 container, exercises the real
Python storage adapter, applies the schema, and executes canonical plus learner-only SQL.

## Files

- `collector/collector/`: source API, normalization, retry/deduplication, storage, and health code.
- `collector/tests/`: deterministic unit and fake-source integration tests.
- `db/schema.sql`: executable maintainer/test copy of the learner Module 02 SQL.
- `db/queries.sql`: executable maintainer/test copy of the learner Module 05/06 SQL.
- `docker-compose.yml`: the single stateless collector service.
- `.env.polymarket.example`: Cloud and collector configuration contract.
- `preflight.sh`: Docker, ClickHouse Cloud, and public-source reachability checks.
