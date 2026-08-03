CREATE DATABASE IF NOT EXISTS polymarket;

CREATE TABLE IF NOT EXISTS polymarket.markets
(
    market_id UInt64,
    condition_id FixedString(66),
    token_id UInt256,
    outcome LowCardinality(String),
    question String,
    slug String,
    active Bool,
    accepting_orders Bool,
    volume_24h Decimal128(8),
    observed_at DateTime64(3, 'UTC')
)
ENGINE = ReplacingMergeTree(observed_at)
ORDER BY (condition_id, token_id);

CREATE TABLE IF NOT EXISTS polymarket.price_ticks
(
    event_id FixedString(64),
    condition_id FixedString(66),
    token_id UInt256,
    event_at DateTime64(3, 'UTC'),
    observed_at DateTime64(3, 'UTC'),
    event_kind Enum8(
        'book_snapshot' = 1,
        'price_change' = 2,
        'last_trade_price' = 3,
        'best_bid_ask' = 4,
        'rest_book' = 5
    ),
    source Enum8('WEBSOCKET' = 1, 'CLOB_REST' = 2, 'FIXTURE' = 3),
    price Decimal64(12),
    size Decimal128(8),
    side Enum8('UNKNOWN' = 0, 'BUY' = 1, 'SELL' = 2),
    best_bid Decimal64(12),
    best_ask Decimal64(12),
    midpoint Decimal64(12),
    source_hash String,
    raw_payload String
)
ENGINE = MergeTree
ORDER BY (toStartOfHour(event_at), token_id, event_at, event_id);

CREATE TABLE IF NOT EXISTS polymarket.trades
(
    trade_id FixedString(64),
    condition_id FixedString(66),
    token_id UInt256,
    event_at DateTime64(3, 'UTC'),
    observed_at DateTime64(3, 'UTC'),
    proxy_wallet FixedString(42),
    side Enum8('UNKNOWN' = 0, 'BUY' = 1, 'SELL' = 2),
    price Decimal64(12),
    size Decimal128(8),
    outcome LowCardinality(String),
    transaction_hash FixedString(66),
    title String
)
ENGINE = ReplacingMergeTree(observed_at)
ORDER BY (toStartOfHour(event_at), condition_id, event_at, trade_id);

CREATE OR REPLACE VIEW polymarket.trades_clean AS
SELECT *
FROM polymarket.trades FINAL;

CREATE TABLE IF NOT EXISTS polymarket.market_midpoints_1m
(
    token_id UInt256,
    minute DateTime('UTC'),
    open AggregateFunction(argMin, Decimal64(12), Tuple(DateTime64(3, 'UTC'), FixedString(64))),
    high AggregateFunction(max, Decimal64(12)),
    low AggregateFunction(min, Decimal64(12)),
    close AggregateFunction(argMax, Decimal64(12), Tuple(DateTime64(3, 'UTC'), FixedString(64))),
    updates AggregateFunction(count)
)
ENGINE = AggregatingMergeTree
ORDER BY (minute, token_id);

CREATE MATERIALIZED VIEW IF NOT EXISTS polymarket.market_midpoints_1m_mv
TO polymarket.market_midpoints_1m
AS
SELECT
    token_id,
    toStartOfMinute(event_at) AS minute,
    argMinState(midpoint, tuple(event_at, event_id)) AS open,
    maxState(midpoint) AS high,
    minState(midpoint) AS low,
    argMaxState(midpoint, tuple(event_at, event_id)) AS close,
    countState() AS updates
FROM polymarket.price_ticks
WHERE midpoint > 0
  AND event_kind IN ('book_snapshot', 'price_change', 'best_bid_ask', 'rest_book')
GROUP BY token_id, minute;
