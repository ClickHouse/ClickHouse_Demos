-- Phase-1 seed tables. Column conventions mirror ClickPipes CDC output so the
-- v_* views are unchanged when we later repoint them at CDC-landed tables.
CREATE TABLE IF NOT EXISTS customers (
  customer_id UInt64, full_name String, email String, country String,
  segment String, signup_date Date, created_at DateTime64(3), updated_at DateTime64(3),
  _peerdb_version UInt64, _peerdb_is_deleted UInt8
) ENGINE = ReplacingMergeTree(_peerdb_version) ORDER BY customer_id;

CREATE TABLE IF NOT EXISTS products (
  product_id UInt64, name String, category String, brand String,
  unit_price Decimal(10,2), unit_cost Decimal(10,2),
  created_at DateTime64(3), updated_at DateTime64(3),
  _peerdb_version UInt64, _peerdb_is_deleted UInt8
) ENGINE = ReplacingMergeTree(_peerdb_version) ORDER BY product_id;

CREATE TABLE IF NOT EXISTS orders (
  order_id UInt64, customer_id UInt64, order_ts DateTime64(3), status String,
  channel String, created_at DateTime64(3), updated_at DateTime64(3),
  _peerdb_version UInt64, _peerdb_is_deleted UInt8
) ENGINE = ReplacingMergeTree(_peerdb_version) ORDER BY order_id;

CREATE TABLE IF NOT EXISTS order_items (
  order_item_id UInt64, order_id UInt64, product_id UInt64, quantity UInt32,
  unit_price Decimal(10,2), discount Decimal(10,2),
  created_at DateTime64(3), updated_at DateTime64(3),
  _peerdb_version UInt64, _peerdb_is_deleted UInt8
) ENGINE = ReplacingMergeTree(_peerdb_version) ORDER BY order_item_id;

CREATE TABLE IF NOT EXISTS events (
  event_id UInt64, customer_id Nullable(UInt64), session_id String,
  event_type String, product_id Nullable(UInt64), event_ts DateTime64(3),
  created_at DateTime64(3),
  _peerdb_version UInt64, _peerdb_is_deleted UInt8
) ENGINE = ReplacingMergeTree(_peerdb_version) ORDER BY event_id;
