CREATE TABLE __DATABASE__.__TARGET__ (
 district_id UInt16, __STORE_COLUMN__ sku UInt32, run_id UUID,
 branch LowCardinality(String), source LowCardinality(String), minute DateTime('UTC'),
 units_state AggregateFunction(sum, Int64), sold_units_state AggregateFunction(sum, Int64), receipts_state AggregateFunction(sum, Int64),
 cost_state AggregateFunction(sum, Int64), rows_state AggregateFunction(count),
 transactions_state AggregateFunction(uniqExact, UUID), sequence_state AggregateFunction(max, UInt64)
) ENGINE = AggregatingMergeTree PARTITION BY toYYYYMM(minute)
ORDER BY (district_id, __STORE_KEY__ sku, run_id, branch, source, minute);
