-- 06_pg_clickhouse.sql
-- Run on the ClickHouse Managed Postgres instance (the TARGET), as its master user, after
-- the ClickPipe has finished its initial load and CDC is caught up.
--
--   psql -h "$TARGET_HOST" -U postgres -d shop \
--        -v ch_host="abc123.us-east-1.aws.clickhouse.cloud" \
--        -v ch_database="shop" \
--        -v ch_user="workshop" \
--        -v ch_password="..." \
--        -f 06_pg_clickhouse.sql
--
-- This file is the mechanism behind the workshop's headline claim: the analytics workload
-- moves to ClickHouse without an application change. Read it as two halves.
--
--   Steps 1-5 change NOTHING an application can observe. CREATE EXTENSION, CREATE SERVER,
--   CREATE USER MAPPING and IMPORT FOREIGN SCHEMA add a new schema, `ch`, full of foreign
--   tables. No existing object is dropped, renamed, altered or shadowed. `public.orders`
--   is still `public.orders`; every query in flight keeps resolving exactly where it did.
--   You can run the whole first half in production during business hours and diff the
--   dashboard JSON afterwards: it is byte-identical, because nothing it names has moved.
--
--   Step 6 is the entire migration. One ALTER ROLE puts `ch` in front of `public` on the
--   search_path of ONE role, so an unqualified `order_items` in a dashboard panel resolves
--   to `ch.order_items` -- a ClickHouse table reached over the FDW -- instead of the local
--   heap table. The SQL text did not change. Name resolution did.
--
-- The writer role is deliberately absent from step 6. Read the note above it before
-- deciding to "tidy up" by setting the search_path database-wide.

-- ---------------------------------------------------------------------------
-- Step 1. Install the extension. Observable effect: none.
-- ---------------------------------------------------------------------------
-- On ClickHouse Managed Postgres the extension binary ships with the instance, so this one
-- statement is the whole install. Elsewhere it is not: on a self-managed Postgres you first
-- install pg_clickhouse itself (PGXN, source, or the project's Docker image), and if you
-- want it loaded for every connection rather than on first use, add it to
-- session_preload_libraries (no restart) or shared_preload_libraries (restart required).
-- If this line errors with: extension "pg_clickhouse" is not available -- then you are not on
-- Managed Postgres and the install step is missing, not this file.
CREATE EXTENSION IF NOT EXISTS pg_clickhouse;

-- ---------------------------------------------------------------------------
-- Step 2. Name the remote ClickHouse service. Observable effect: none.
-- ---------------------------------------------------------------------------
-- A foreign server is a catalog entry: a connection recipe with a name. Creating one
-- reaches out to nothing and exposes no table.
--
-- Three options are set and the rest are left at their defaults on purpose:
--
--   driver 'binary'  required. The native protocol; faster than 'http' for the column
--                    volumes these panels scan.
--   host             the ClickHouse Cloud endpoint hostname, no scheme and no port.
--   dbname           the ClickHouse database the ClickPipe writes into. Defaults to
--                    'default', which is not what this workshop uses.
--
-- `port` and `secure` are omitted BECAUSE the target is ClickHouse Cloud and not
-- localhost. pg_clickhouse defaults `secure` to 'auto', which means TLS when the host is
-- a ClickHouse Cloud host or the port is a secure port, and it defaults the port from the
-- driver and host type: binary to Cloud is 9440 (binary to non-Cloud is 9004; the HTTP
-- driver uses 8443 and 8123 respectively). So a Cloud endpoint needs neither option, and
-- pinning `port 9004` or `secure 'off'` out of habit is how you get a connection refused
-- or a plaintext attempt against a TLS-only port.
--
-- Against a self-managed, non-Cloud ClickHouse, be explicit instead:
--
--   OPTIONS (driver 'binary', host :'ch_host', port '9440', dbname :'ch_database',
--            secure 'on', min_tls_version 'TLSv1.2');
CREATE SERVER ch_srv
  FOREIGN DATA WRAPPER clickhouse_fdw
  OPTIONS (driver 'binary', host :'ch_host', dbname :'ch_database');

-- ---------------------------------------------------------------------------
-- Step 3. Give ONE Postgres role ClickHouse credentials. Observable effect: none.
-- ---------------------------------------------------------------------------
-- A user mapping is per Postgres role. This one is for shop_analytics only, so
-- shop_writer has no ClickHouse identity at all -- a second, independent reason its
-- queries cannot end up on the remote service even by accident.
--
-- `user` defaults to 'default' and `password` is optional; both are supplied here because
-- ClickHouse Cloud services do not accept an anonymous login.
CREATE USER MAPPING FOR shop_analytics
  SERVER ch_srv
  OPTIONS (user :'ch_user', password :'ch_password');

-- ---------------------------------------------------------------------------
-- Step 4. A schema to hold the foreign tables. Observable effect: none.
-- ---------------------------------------------------------------------------
-- The foreign tables land in `ch`, never in `public`. That separation is what makes the
-- first half of this file safe: `ch.order_items` and `public.order_items` coexist, and
-- which one an unqualified name means is decided per role in step 6 -- reversibly.
CREATE SCHEMA IF NOT EXISTS ch;
GRANT USAGE ON SCHEMA ch TO shop_analytics;

-- ---------------------------------------------------------------------------
-- Step 5. Import the ClickHouse tables. Observable effect: still none.
-- ---------------------------------------------------------------------------
-- The schema name in the FROM clause is the ClickHouse DATABASE name -- ClickHouse has no
-- schema level, so its database is what maps onto a Postgres schema. `:"ch_database"` is
-- the identifier form of the psql variable (double quotes), because this position is an
-- identifier, not a string literal; the same value is passed as a literal in step 2.
--
-- This creates foreign tables named after the ClickHouse tables the ClickPipe created:
-- customers, products, orders, order_items. Nothing in `public` is touched. Add
-- `LIMIT TO (...)` if the ClickHouse database holds more than the four.
-- The IMPORT runs as whichever role executes this file, and Postgres requires THAT role to
-- have its own user mapping -- the one created in step 3 is for shop_analytics and does not
-- satisfy it. Create one for the duration of the import and drop it immediately after, so the
-- only mapping that outlives this file is still shop_analytics's.
CREATE USER MAPPING FOR CURRENT_USER
  SERVER ch_srv
  OPTIONS (user :'ch_user', password :'ch_password');

IMPORT FOREIGN SCHEMA :"ch_database" FROM SERVER ch_srv INTO ch;

DROP USER MAPPING FOR CURRENT_USER SERVER ch_srv;

-- Grant AFTER the import, because the foreign tables did not exist until the line above
-- ran, and IMPORT FOREIGN SCHEMA leaves them owned by the master user. Without this the
-- reroute in step 6 turns every panel into "permission denied for foreign table
-- order_items" -- a failure that looks like a search_path bug and is not one. Re-run this
-- grant after any later IMPORT that adds a table.
GRANT SELECT ON ALL TABLES IN SCHEMA ch TO shop_analytics;

-- ---------------------------------------------------------------------------
-- Step 6. The one statement that migrates the workload.
-- ---------------------------------------------------------------------------
-- With `ch` ahead of `public`, an unqualified `order_items` in a shop_analytics session
-- resolves to ch.order_items, so the dashboard's existing SQL executes against ClickHouse.
-- The dashboard JSON, the panel queries and the connection string are all unchanged.
--
-- shop_writer's search_path is NOT set here, and must not be. It keeps the cluster default
-- -- "$user", public -- so the application's INSERTs and UPDATEs keep resolving to the
-- local heap tables in this same database. That is the whole point of the two-role split
-- in 01_schema.sql: OLTP stays on Postgres, OLAP moves to ClickHouse, and both are reading
-- and writing through the same host and port as before. Setting this database-wide (ALTER
-- DATABASE shop SET search_path) would point the writer at read-only foreign tables and
-- break the application -- exactly the change this workshop claims not to make.
--
-- Two things about scope, both of which have cost someone an afternoon:
--
--   * A role-level SET applies to NEW sessions. Existing connections keep the old
--     search_path, so a pooled reader (Grafana included) shows no change until its pool
--     recycles. Restart the reader, or `SELECT pg_terminate_backend(pid) FROM pg_stat_activity
--     WHERE usename = 'shop_analytics'`, before concluding this did not work.
--   * A client that sets search_path itself on connect overrides this. If the panels do
--     not move, check the reader's connection options for an explicit search_path before
--     touching anything here.
--
-- To roll the migration back, in full, instantly:
--   ALTER ROLE shop_analytics RESET search_path;
ALTER ROLE shop_analytics SET search_path = ch, public;

-- ---------------------------------------------------------------------------
-- Prove the routing per role, before trusting a dashboard panel.
-- ---------------------------------------------------------------------------
-- Module 06 runs exactly these two. Both use the q1_revenue_by_hour.sql text verbatim, and
-- the reconnect matters: `\c` starts a new session, which is when the role's search_path
-- from step 6 is applied.
--
-- As the analytics role -- expect a Foreign Scan on ch.order_items, and in VERBOSE the
-- remote ClickHouse query that the aggregate and the WHERE clause were pushed into:
--
-- \c shop shop_analytics
-- EXPLAIN (VERBOSE) SELECT date_trunc('hour', placed_at) AS hour, sum(line_total) AS revenue
-- FROM order_items
-- WHERE placed_at >= now() - interval '7 days'
-- GROUP BY 1
-- ORDER BY 1;
--
-- As the writer role, same query, same database, same connection -- expect a local plan:
-- an Index Scan using order_items_placed_at_idx, or a Seq Scan, on public.order_items, with
-- no Foreign Scan node anywhere:
--
-- \c shop shop_writer
-- EXPLAIN (VERBOSE) SELECT date_trunc('hour', placed_at) AS hour, sum(line_total) AS revenue
-- FROM order_items
-- WHERE placed_at >= now() - interval '7 days'
-- GROUP BY 1
-- ORDER BY 1;
