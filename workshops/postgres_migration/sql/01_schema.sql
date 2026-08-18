-- 01_schema.sql
-- Source-of-truth schema for the postgres-migration workshop's "shop" database.
--
-- Invoke with the two role passwords supplied as psql variables:
--
--   psql -h "$PGHOST" -U postgres -d shop \
--        -v writer_password="..." -v analytics_password="..." -f 01_schema.sql

CREATE TABLE customers (
  customer_id   bigserial PRIMARY KEY,
  email         text NOT NULL UNIQUE,
  region        text NOT NULL,
  created_at    timestamptz NOT NULL DEFAULT now()
);

CREATE TABLE products (
  product_id    bigserial PRIMARY KEY,
  sku           text NOT NULL UNIQUE,
  category      text NOT NULL,
  unit_price    numeric(10,2) NOT NULL
);

CREATE TABLE orders (
  order_id      bigserial PRIMARY KEY,
  customer_id   bigint NOT NULL REFERENCES customers(customer_id),
  status        text NOT NULL,
  placed_at     timestamptz NOT NULL,
  updated_at    timestamptz NOT NULL DEFAULT now()
);

CREATE TABLE order_items (
  order_item_id bigserial PRIMARY KEY,
  order_id      bigint NOT NULL REFERENCES orders(order_id),
  product_id    bigint NOT NULL REFERENCES products(product_id),
  quantity      int NOT NULL,
  line_total    numeric(12,2) NOT NULL,
  placed_at     timestamptz NOT NULL
);

CREATE INDEX orders_placed_at_idx ON orders (placed_at);
CREATE INDEX order_items_placed_at_idx ON order_items (placed_at);

-- Two roles, deliberately. This split is load-bearing for the whole workshop, not
-- housekeeping:
--
--   shop_writer     is the application. It inserts and updates orders and never reads
--                   anything analytical. Its search_path is NEVER touched.
--   shop_analytics  is the reporting/BI path. Later in the workshop
--                   (06_pg_clickhouse.sql) this role's search_path is shadowed so that
--                   an unqualified table name resolves to a ClickHouse-backed foreign
--                   table instead of the local Postgres table.
--
-- Because the shadowing is scoped to shop_analytics alone, the application's queries
-- keep resolving to the same local tables they always did. That is what makes the
-- claim "the application did not change" literally true rather than a figure of
-- speech: analytics moves to ClickHouse while the writer's resolution is untouched.
-- Collapsing these into one role would make that claim false, so keep them separate.
CREATE ROLE shop_writer LOGIN PASSWORD :'writer_password';
CREATE ROLE shop_analytics LOGIN PASSWORD :'analytics_password';

GRANT SELECT, INSERT, UPDATE ON ALL TABLES IN SCHEMA public TO shop_writer;
GRANT USAGE, SELECT ON ALL SEQUENCES IN SCHEMA public TO shop_writer;
GRANT SELECT ON ALL TABLES IN SCHEMA public TO shop_analytics;

-- AWS requires the rds_replication role to create and stream from logical replication slots.
-- The master user holds rds_superuser, which is what permits this grant, but rds_superuser
-- does not imply rds_replication. Guarded so this file still runs on a local Postgres, where
-- the role does not exist.
--
-- This is the third of three things RDS needs together, and the other two are in ../terraform:
-- the rds.logical_replication = 1 parameter (plus the reboot that makes it take effect) and
-- backup_retention_period >= 1. Miss any one and the publication still gets created while the
-- subscription silently never receives a row.
DO $$
BEGIN
  IF EXISTS (SELECT 1 FROM pg_roles WHERE rolname = 'rds_replication') THEN
    EXECUTE 'GRANT rds_replication TO ' || quote_ident(current_user);
  END IF;
END
$$;
