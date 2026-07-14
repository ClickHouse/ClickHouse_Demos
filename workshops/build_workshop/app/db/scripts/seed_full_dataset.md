## Full dataset seeding (optional)

This repo ships with a **tiny sample** so the stack boots quickly.

For the full NYC Taxi dataset, follow ClickHouse’s official guide:
- `https://clickhouse.com/docs/getting-started/example-datasets/nyc-taxi`

### Recommended workflow
- **Start the stack**: `docker compose up -d clickhouse`
- **Load zones**: download `taxi_zone_lookup.csv` and insert into `taxi.taxi_zones`
- **Load trips**: follow the ClickHouse guide to load a larger month/year range (Parquet/CSV via `s3()`/`url()` table functions)

### Notes
- Loading the full dataset can take time and disk space; keep it separate from container startup.
- Once loaded, all API endpoints should “just work” without code changes.

