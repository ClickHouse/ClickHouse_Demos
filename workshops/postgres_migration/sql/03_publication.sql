-- 03_publication.sql
-- Run on the SOURCE. Two uses, and the workshop's main line only needs the second.
--
-- 1. THE MANUAL PATH. Module 03 creates a Postgres-to-Postgres ClickPipe, and the pipe
--    declares the publication itself from the table selection you make when you create it.
--    You only run the CREATE below if you are doing that leg by hand with logical
--    replication instead -- a documented alternative, and the right choice if you cannot
--    take a dependency on a public-beta console flow or you need every step in a script.
--
-- 2. THE INSPECTION. The two SELECTs are the point either way. Whatever created the
--    publication, these read the scope that is actually in force out of the source's own
--    catalog, rather than out of the form you filled in. Module 03 Step 5 runs them against
--    the publication the pipe created.
--
-- The table list is explicit rather than every table on purpose. Module 02 asks participants
-- to decide the replication scope, and an explicit list is what a scope decision looks like:
-- a table that is not named does not move, and adding one later is a visible, reviewable
-- change. Replicating everything silently enrols whatever happens to exist when the leg
-- starts, including tables nobody decided to migrate.

-- Manual path only. Skip this if the pipe is creating the publication.
CREATE PUBLICATION shop_pub FOR TABLE customers, products, orders, order_items;

-- Confirm the scope. puballtables must read 'f' -- if it reads 't', the scope covers every
-- table and the decision was bypassed. True of a hand-written statement and of a pipe whose
-- table selection was wider than intended.
SELECT pubname, puballtables FROM pg_publication;

-- The tables actually enrolled, for the record. Expect exactly four rows.
SELECT schemaname, tablename FROM pg_publication_tables ORDER BY tablename;
