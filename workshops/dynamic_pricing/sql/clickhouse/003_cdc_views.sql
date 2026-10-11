-- Connector DDL is inspected before these views are installed.
-- FINAL selects each source primary key's current version before the delete filter.
CREATE OR REPLACE VIEW __DATABASE__.sales_live_current AS
SELECT * FROM __DATABASE__.sales_transactions_cdc FINAL WHERE _peerdb_is_deleted = 0;
CREATE OR REPLACE VIEW __DATABASE__.inventory_live_current AS
SELECT * FROM __DATABASE__.inventory_history_cdc FINAL WHERE _peerdb_is_deleted = 0;
CREATE OR REPLACE VIEW __DATABASE__.price_live_current AS
SELECT * FROM __DATABASE__.price_history_cdc FINAL WHERE _peerdb_is_deleted = 0;
CREATE OR REPLACE VIEW __DATABASE__.cost_live_current AS
SELECT * FROM __DATABASE__.cost_history_cdc FINAL WHERE _peerdb_is_deleted = 0;
