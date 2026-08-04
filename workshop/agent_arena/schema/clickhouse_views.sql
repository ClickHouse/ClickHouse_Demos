CREATE OR REPLACE VIEW v_customers DEFINER = CURRENT_USER SQL SECURITY DEFINER AS
  SELECT * EXCEPT (_peerdb_version, _peerdb_is_deleted) FROM customers FINAL
  WHERE _peerdb_is_deleted = 0;
CREATE OR REPLACE VIEW v_products DEFINER = CURRENT_USER SQL SECURITY DEFINER AS
  SELECT * EXCEPT (_peerdb_version, _peerdb_is_deleted) FROM products FINAL
  WHERE _peerdb_is_deleted = 0;
CREATE OR REPLACE VIEW v_orders DEFINER = CURRENT_USER SQL SECURITY DEFINER AS
  SELECT * EXCEPT (_peerdb_version, _peerdb_is_deleted) FROM orders FINAL
  WHERE _peerdb_is_deleted = 0;
CREATE OR REPLACE VIEW v_order_items DEFINER = CURRENT_USER SQL SECURITY DEFINER AS
  SELECT * EXCEPT (_peerdb_version, _peerdb_is_deleted) FROM order_items FINAL
  WHERE _peerdb_is_deleted = 0;
CREATE OR REPLACE VIEW v_events DEFINER = CURRENT_USER SQL SECURITY DEFINER AS
  SELECT * EXCEPT (_peerdb_version, _peerdb_is_deleted) FROM events FINAL
  WHERE _peerdb_is_deleted = 0;
