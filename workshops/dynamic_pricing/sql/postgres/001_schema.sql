CREATE SCHEMA IF NOT EXISTS pricing;

CREATE TABLE IF NOT EXISTS pricing.scenarios (
  owner_id text NOT NULL,
  scenario_id text NOT NULL,
  generation integer NOT NULL CHECK (generation > 0),
  scenario_cutoff timestamptz NOT NULL,
  PRIMARY KEY (owner_id, scenario_id)
);

CREATE TABLE IF NOT EXISTS pricing.subject_state (
  owner_id text NOT NULL,
  scenario_id text NOT NULL,
  generation integer NOT NULL,
  sku integer NOT NULL,
  store_id integer NOT NULL,
  channel text NOT NULL CHECK (channel = 'store'),
  stock_units integer NOT NULL CHECK (stock_units >= 0),
  inventory_expires_at timestamptz NOT NULL,
  inventory_revision_id uuid NOT NULL,
  inventory_version bigint NOT NULL DEFAULT 1,
  current_price_minor bigint NOT NULL CHECK (current_price_minor > 0),
  reference_price_minor bigint NOT NULL CHECK (reference_price_minor > 0),
  applied_price_version bigint NOT NULL DEFAULT 1,
  unit_cost_minor bigint NOT NULL CHECK (unit_cost_minor >= 0),
  selling_cost_minor bigint NOT NULL CHECK (selling_cost_minor >= 0),
  disposal_cost_minor bigint NOT NULL CHECK (disposal_cost_minor >= 0),
  cost_version bigint NOT NULL DEFAULT 1,
  rule_version bigint NOT NULL DEFAULT 1,
  rules jsonb NOT NULL,
  PRIMARY KEY (owner_id, scenario_id, generation, sku, store_id, channel),
  FOREIGN KEY (owner_id, scenario_id) REFERENCES pricing.scenarios
);

CREATE TABLE IF NOT EXISTS pricing.inventory_history (
  revision_id uuid PRIMARY KEY,
  owner_id text NOT NULL,
  scenario_id text NOT NULL,
  generation integer NOT NULL,
  sku integer NOT NULL,
  store_id integer NOT NULL,
  channel text NOT NULL,
  source_version bigint NOT NULL,
  available_units integer NOT NULL CHECK (available_units >= 0),
  inventory_expires_at timestamptz NOT NULL,
  effective_at timestamptz NOT NULL,
  emitted_at timestamptz NOT NULL DEFAULT clock_timestamp(),
  UNIQUE (owner_id, scenario_id, generation, sku, store_id, channel, source_version)
);

CREATE TABLE IF NOT EXISTS pricing.price_history (
  revision_id uuid PRIMARY KEY,
  owner_id text NOT NULL, scenario_id text NOT NULL, generation integer NOT NULL,
  sku integer NOT NULL, store_id integer NOT NULL, channel text NOT NULL,
  source_version bigint NOT NULL, price_minor bigint NOT NULL CHECK (price_minor > 0),
  effective_at timestamptz NOT NULL, acknowledged_at timestamptz NOT NULL,
  UNIQUE (owner_id, scenario_id, generation, sku, store_id, channel, source_version)
);

CREATE TABLE IF NOT EXISTS pricing.cost_history (
  revision_id uuid PRIMARY KEY,
  owner_id text NOT NULL, scenario_id text NOT NULL, generation integer NOT NULL,
  sku integer NOT NULL, store_id integer NOT NULL, channel text NOT NULL,
  source_version bigint NOT NULL, unit_cost_minor bigint NOT NULL,
  selling_cost_minor bigint NOT NULL, disposal_cost_minor bigint NOT NULL,
  effective_at timestamptz NOT NULL, emitted_at timestamptz NOT NULL DEFAULT clock_timestamp(),
  UNIQUE (owner_id, scenario_id, generation, sku, store_id, channel, source_version)
);

CREATE TABLE IF NOT EXISTS pricing.sales_transactions (
  owner_id text NOT NULL, scenario_id text NOT NULL, generation integer NOT NULL,
  event_id uuid NOT NULL,
  sku integer NOT NULL, store_id integer NOT NULL, channel text NOT NULL,
  event_type text NOT NULL,
  quantity integer NOT NULL,
  unit_price_minor bigint NOT NULL,
  receipts_minor bigint NOT NULL,
  sold_cost_minor bigint NOT NULL,
  original_event_id uuid,
  applied_price_version bigint NOT NULL,
  inventory_revision_id uuid NOT NULL REFERENCES pricing.inventory_history,
  inventory_version bigint NOT NULL,
  event_time timestamptz NOT NULL,
  emitted_at timestamptz NOT NULL,
  commit_acknowledged_at timestamptz,
  content_hash text NOT NULL,
  PRIMARY KEY (owner_id, scenario_id, generation, event_id),
  CHECK ((event_type = 'sale' AND quantity > 0 AND original_event_id IS NULL)
      OR (event_type = 'return' AND quantity < 0 AND original_event_id IS NOT NULL)),
  CHECK (receipts_minor = quantity * unit_price_minor)
);

CREATE TABLE IF NOT EXISTS pricing.snapshots (
  snapshot_id uuid PRIMARY KEY, owner_id text NOT NULL, scenario_id text NOT NULL,
  generation integer NOT NULL, data jsonb NOT NULL, evidence jsonb NOT NULL
);
CREATE TABLE IF NOT EXISTS pricing.candidates (
  candidate_id uuid PRIMARY KEY, owner_id text NOT NULL, scenario_id text NOT NULL,
  generation integer NOT NULL, data jsonb NOT NULL
);
CREATE TABLE IF NOT EXISTS pricing.decisions (
  decision_id uuid PRIMARY KEY, candidate_id uuid NOT NULL UNIQUE REFERENCES pricing.candidates,
  owner_id text NOT NULL, scenario_id text NOT NULL, generation integer NOT NULL,
  reviewer_id text NOT NULL, reviewed_at timestamptz NOT NULL,
  status text NOT NULL CHECK (status IN ('approved', 'rejected')), data jsonb NOT NULL
);
CREATE TABLE IF NOT EXISTS pricing.application_attempts (
  attempt_id uuid PRIMARY KEY, candidate_id uuid NOT NULL REFERENCES pricing.candidates,
  owner_id text NOT NULL, idempotency_key uuid NOT NULL,
  status text NOT NULL, attempted_at timestamptz NOT NULL, data jsonb NOT NULL,
  UNIQUE (owner_id, idempotency_key)
);
CREATE TABLE IF NOT EXISTS pricing.receipts (
  receipt_id uuid PRIMARY KEY, candidate_id uuid NOT NULL UNIQUE REFERENCES pricing.candidates,
  decision_id uuid NOT NULL UNIQUE REFERENCES pricing.decisions,
  owner_id text NOT NULL, scenario_id text NOT NULL, generation integer NOT NULL,
  data jsonb NOT NULL
);
CREATE TABLE IF NOT EXISTS pricing.outbox (
  event_id uuid PRIMARY KEY, kind text NOT NULL, data jsonb NOT NULL,
  delivered_at timestamptz, attempts integer NOT NULL DEFAULT 0
);
CREATE TABLE IF NOT EXISTS pricing.import_runs (
  run_id uuid PRIMARY KEY, owner_id text NOT NULL, scenario_id text NOT NULL,
  status text NOT NULL, data jsonb NOT NULL, created_at timestamptz NOT NULL DEFAULT clock_timestamp()
);
CREATE TABLE IF NOT EXISTS pricing.history_active (
  owner_id text NOT NULL, scenario_id text NOT NULL,
  run_id uuid NOT NULL REFERENCES pricing.import_runs,
  PRIMARY KEY (owner_id, scenario_id)
);
CREATE TABLE IF NOT EXISTS pricing.resets (
  owner_id text NOT NULL, scenario_id text NOT NULL, idempotency_key uuid NOT NULL,
  from_generation integer NOT NULL, to_generation integer NOT NULL, checkpoint text NOT NULL,
  PRIMARY KEY (owner_id, scenario_id, idempotency_key)
);

CREATE TABLE IF NOT EXISTS pricing.scenario_events (
  owner_id text NOT NULL, scenario_id text NOT NULL, generation integer NOT NULL,
  event_id uuid NOT NULL, event_kind text NOT NULL, data jsonb NOT NULL,
  PRIMARY KEY (owner_id,scenario_id,generation,event_id)
);
