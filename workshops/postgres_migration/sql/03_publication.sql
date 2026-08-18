-- 03_publication.sql
-- Run on the SOURCE. Declares what logical replication will carry to the target.
--
-- The table list is explicit rather than FOR ALL TABLES on purpose. Module 02 asks
-- participants to decide the replication scope, and an explicit list is what a scope
-- decision looks like: a table that is not named here does not move, and adding one
-- later is a visible, reviewable change. FOR ALL TABLES would silently enrol whatever
-- happens to exist at subscribe time, including tables nobody decided to migrate.
CREATE PUBLICATION shop_pub FOR TABLE customers, products, orders, order_items;

-- Confirm the scope. puballtables must read 'f' -- if it reads 't', the publication was
-- created FOR ALL TABLES and the scope decision was bypassed.
SELECT pubname, puballtables FROM pg_publication;

-- The tables actually enrolled, for the record.
SELECT schemaname, tablename FROM pg_publication_tables WHERE pubname = 'shop_pub' ORDER BY tablename;
