-- Run as the learner's database administrator after creating the named roles.
-- Login secrets and replication privileges are configured in the Cloud console.
GRANT USAGE ON SCHEMA pricing TO pricing_app, pricing_cdc;
GRANT SELECT, INSERT, UPDATE ON ALL TABLES IN SCHEMA pricing TO pricing_app;
GRANT SELECT ON pricing.sales_transactions, pricing.inventory_history,
  pricing.price_history, pricing.cost_history TO pricing_cdc;
ALTER DEFAULT PRIVILEGES IN SCHEMA pricing GRANT SELECT, INSERT, UPDATE ON TABLES TO pricing_app;
-- The analytics/agent role has no Postgres login and cannot apply prices.
