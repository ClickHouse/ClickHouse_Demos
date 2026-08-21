-- 05_reconcile.sql
-- Run identically on SOURCE and TARGET, capture both outputs, and diff them.
--
--   psql -h "$SOURCE_HOST" -At -F'|' -f 05_reconcile.sql > /tmp/source.txt
--   psql -h "$TARGET_HOST" -At -F'|' -f 05_reconcile.sql > /tmp/target.txt
--   diff /tmp/source.txt /tmp/target.txt
--
-- A clean diff is NOT the pass condition once the application has been repointed: the
-- target legitimately holds orders and order_items the source will never see. What must
-- hold is that customers and products match exactly on both count and checksum, and that
-- the target's counts for the two order tables are >= the source's. Module 04's reconcile
-- step has the check that asserts exactly that; `diff` alone reports a difference that is
-- expected and says nothing about whether it is acceptable.
--
-- A row count alone proves only that the right NUMBER of rows arrived. The per-table
-- checksum proves the right rows arrived: sum(hashtext(...)) over a business column is
-- order-independent, so it can be computed on two servers with different physical row
-- order and still compare equal. Deliberately cheap -- one sequential scan per table --
-- because this runs inside a cutover window.
--
-- Keep the column list and ordering identical on both sides; the whole design is that
-- the two outputs are byte-comparable.
\pset footer off

SELECT 'customers'   AS table_name, count(*) AS row_count, COALESCE(sum(hashtext(email)), 0)                            AS checksum FROM customers
UNION ALL
SELECT 'products',    count(*), COALESCE(sum(hashtext(sku)), 0)                                                                     FROM products
UNION ALL
SELECT 'orders',      count(*), COALESCE(sum(hashtext(order_id::text || status)), 0)                                                 FROM orders
UNION ALL
SELECT 'order_items', count(*), COALESCE(sum(hashtext(order_item_id::text || line_total::text)), 0)                                  FROM order_items
ORDER BY table_name;
