-- 04_sequences.sql
-- Run on the TARGET, inside the cutover window, after replication has caught up and
-- before the application is pointed at it.
--
-- THIS IS THE GOTCHA THE WORKSHOP IS BUILT AROUND.
--
-- Logical replication replicates table DATA. It does NOT replicate sequence values.
-- Every row of customers/products/orders/order_items arrives on the target with its
-- source primary key intact, but each target sequence is still sitting at 1, because
-- nothing ever advanced it -- replicated rows are applied with their literal key value
-- and never call nextval().
--
-- So the failure this prevents is not a slow query or a missing row. It is the very
-- first INSERT the application makes after cutover:
--
--   ERROR: duplicate key value violates unique constraint "orders_pkey"
--   DETAIL: Key (order_id)=(1) already exists.
--
-- and it repeats for every id up to max(order_id) -- millions of failed writes, on a
-- database that reconciled perfectly on row counts and checksums minutes earlier. A
-- cutover checklist that verifies data and forgets sequences passes every check and
-- still takes the application down.
--
-- setval with the two-argument form marks the value as already used, so the next
-- nextval() returns max+1. COALESCE covers an empty table, where max() is NULL.
SELECT setval('customers_customer_id_seq',     (SELECT COALESCE(max(customer_id), 1)     FROM customers));
SELECT setval('products_product_id_seq',       (SELECT COALESCE(max(product_id), 1)      FROM products));
SELECT setval('orders_order_id_seq',           (SELECT COALESCE(max(order_id), 1)        FROM orders));
SELECT setval('order_items_order_item_id_seq', (SELECT COALESCE(max(order_item_id), 1)   FROM order_items));

-- Verify: last_value must equal max(id) for each table before the application is let in.
SELECT sequencename, last_value FROM pg_sequences WHERE schemaname = 'public' ORDER BY sequencename;
