CREATE TABLE IF NOT EXISTS __DATABASE__.retail_sales (
    event_id UUID, transaction_id UUID, line_number UInt16 DEFAULT 1,
    store_id UInt16, district_id UInt16, sku UInt32,
    quantity Int32, price_minor Int64, receipts_minor Int64, sold_cost_minor Int64,
    event_time DateTime64(3, 'UTC'), original_event_id UUID DEFAULT toUUID('00000000-0000-0000-0000-000000000000'),
    receipt_id UUID DEFAULT toUUID('00000000-0000-0000-0000-000000000000'),
    run_id UUID DEFAULT toUUID('00000000-0000-0000-0000-000000000000'), batch_id UUID DEFAULT toUUID('00000000-0000-0000-0000-000000000000'), sequence UInt64 DEFAULT 0,
    branch LowCardinality(String) DEFAULT 'applied', source LowCardinality(String) DEFAULT 'history',
    price_version UInt64 DEFAULT 1, emitted_at DateTime64(3, 'UTC') DEFAULT now64(3)
) ENGINE = MergeTree
PARTITION BY toYYYYMM(event_time)
ORDER BY (district_id, toDate(event_time), store_id, sku, event_time, event_id);
