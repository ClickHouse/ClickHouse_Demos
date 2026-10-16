CREATE TABLE IF NOT EXISTS pricing.retail_history_state (id boolean PRIMARY KEY DEFAULT true CHECK(id), data jsonb NOT NULL);
CREATE TABLE IF NOT EXISTS pricing.retail_runs (
 run_id uuid PRIMARY KEY, manifest_digest text NOT NULL, status text NOT NULL DEFAULT 'stopped',
 sequence bigint NOT NULL DEFAULT 0, logical_end timestamptz NOT NULL,
 seed bigint NOT NULL, control jsonb NOT NULL DEFAULT '{}', created_at timestamptz NOT NULL DEFAULT now()
);
CREATE TABLE IF NOT EXISTS pricing.retail_state (
 run_id uuid NOT NULL REFERENCES pricing.retail_runs, store_id integer NOT NULL, district_id integer NOT NULL,
 sku integer NOT NULL, stock_units integer NOT NULL CHECK(stock_units>=0), expires_at timestamptz NOT NULL,
 current_price_minor bigint NOT NULL CHECK(current_price_minor>0), unit_cost_minor bigint NOT NULL,
 selling_cost_minor bigint NOT NULL, disposal_cost_minor bigint NOT NULL,
 permitted_prices_minor bigint[] NOT NULL, inventory_version bigint NOT NULL,
 price_version bigint NOT NULL, rule_version bigint NOT NULL,
 PRIMARY KEY(run_id,store_id,sku)
);
CREATE TABLE IF NOT EXISTS pricing.retail_batches (
 batch_id uuid PRIMARY KEY, run_id uuid NOT NULL REFERENCES pricing.retail_runs,
 sequence bigint NOT NULL, payload_sha256 text NOT NULL, event_count bigint NOT NULL,
 transaction_count bigint NOT NULL, logical_end timestamptz NOT NULL, created_at timestamptz NOT NULL,
 status text NOT NULL DEFAULT 'pending', acknowledged_at timestamptz,
 UNIQUE(run_id,sequence)
);
CREATE TABLE IF NOT EXISTS pricing.retail_inventory_history (
 run_id uuid NOT NULL, store_id integer NOT NULL, sku integer NOT NULL, version bigint NOT NULL,
 stock_units integer NOT NULL, expires_at timestamptz NOT NULL, effective_sequence bigint NOT NULL,
 PRIMARY KEY(run_id,store_id,sku,version)
);
CREATE TABLE IF NOT EXISTS pricing.retail_price_history (
 run_id uuid NOT NULL, store_id integer NOT NULL, sku integer NOT NULL, version bigint NOT NULL,
 price_minor bigint NOT NULL, effective_sequence bigint NOT NULL,
 PRIMARY KEY(run_id,store_id,sku,version)
);
CREATE TABLE IF NOT EXISTS pricing.retail_policy_history (
 run_id uuid NOT NULL, store_id integer NOT NULL, sku integer NOT NULL, version bigint NOT NULL,
 permitted_prices_minor bigint[] NOT NULL, unit_cost_minor bigint NOT NULL,
 selling_cost_minor bigint NOT NULL, disposal_cost_minor bigint NOT NULL,
 PRIMARY KEY(run_id,store_id,sku,version)
);
CREATE TABLE IF NOT EXISTS pricing.retail_proposals (
 proposal_id uuid PRIMARY KEY, run_id uuid NOT NULL REFERENCES pricing.retail_runs,
 data jsonb NOT NULL, created_at timestamptz NOT NULL DEFAULT now()
);
CREATE TABLE IF NOT EXISTS pricing.retail_receipts (
 receipt_id uuid PRIMARY KEY, proposal_id uuid NOT NULL REFERENCES pricing.retail_proposals,
 run_id uuid NOT NULL REFERENCES pricing.retail_runs, idempotency_key uuid NOT NULL UNIQUE,
 data jsonb NOT NULL, acknowledged_at timestamptz NOT NULL DEFAULT now()
);
CREATE TABLE IF NOT EXISTS pricing.retail_return_claims (
 run_id uuid NOT NULL, original_event_id uuid NOT NULL, batch_id uuid NOT NULL,
 PRIMARY KEY(run_id, original_event_id)
);

ALTER TABLE pricing.retail_policy_history ADD COLUMN IF NOT EXISTS effective_sequence bigint NOT NULL DEFAULT 0;
CREATE TABLE IF NOT EXISTS pricing.retail_comparisons (
 receipt_id uuid PRIMARY KEY REFERENCES pricing.retail_receipts, run_id uuid NOT NULL REFERENCES pricing.retail_runs,
 store_id integer NOT NULL, sku integer NOT NULL, inputs jsonb NOT NULL, price_minor bigint NOT NULL,
 applied_stock integer NOT NULL, comparison_stock integer NOT NULL, seed bigint NOT NULL,
 logical_end timestamptz NOT NULL, sequence bigint NOT NULL DEFAULT 0, active boolean NOT NULL DEFAULT true
);
