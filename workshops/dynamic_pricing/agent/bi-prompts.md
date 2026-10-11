# BI evidence questions

Connect ClickHouse Agents with a read-only user. Grant access only to `input_snapshots`, `decision_events`, `application_events`, and `quarantine_events` in your pricing database. Use `FINAL` when a view does not already apply it. Filter every question to your sandbox and generation.

1. Which inventory revision and price version were selected for this snapshot ID?
2. Which policy reason explains the held candidate ID? Show the evidence ID, source ages, and lease expiry.
3. Did the reviewed decision receive an application receipt? Distinguish the requested price from the acknowledged price.
4. What must a real pilot measure to test a pricing hypothesis? Distinguish projected recovery from observed receipts.

Check the generated SQL and evidence IDs. A BI answer is an explanation; it does not approve a price. The application exception workflow uses separate named tools and never exposes arbitrary SQL or a publication tool.

## Read-only ClickHouse grants

Run as the administrator in your own ClickHouse service. Replace the password locally and keep it private. The user can read only the four evidence tables:

```sql
CREATE USER IF NOT EXISTS pricing_bi IDENTIFIED WITH sha256_password BY '<PRIVATE_PASSWORD>';
GRANT SELECT ON pricing.input_snapshots TO pricing_bi;
GRANT SELECT ON pricing.decision_events TO pricing_bi;
GRANT SELECT ON pricing.application_events TO pricing_bi;
GRANT SELECT ON pricing.quarantine_events TO pricing_bi;
```

Configure your agent connection with `pricing_bi`, the Cloud HTTPS endpoint, TLS, and this private password. Do not give it administrator credentials.
