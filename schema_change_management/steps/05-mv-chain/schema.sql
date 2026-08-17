-- =============================================================================
-- SCENARIO 5: evolving a materialized view chain. The one that actually bites
-- ClickHouse teams.
--
-- Ask: "break campaign performance down by country."
--
-- Three objects have to change together, in a specific order:
--
--   1. ad_events            + country_code                (source)
--   2. campaign_daily_stats + country_code IN THE SORT KEY (target)
--   3. campaign_daily_mv    + country_code in SELECT/GROUP BY (the trigger)
--
-- Three things a schema differ will get wrong or leave to you:
--
--   ORDERING. The target table must have the column before the MV that writes
--   it is recreated, and ad_events must have it before the MV that reads it.
--   A tool that plans objects in arbitrary order can produce a valid-looking
--   plan that fails halfway. Always read the generated file top to bottom.
--
--   THE SORT KEY. Adding country_code to campaign_daily_stats' ORDER BY is a
--   sort-key change. ClickHouse's rules here are narrow enough that a rebuild
--   is the only reliable route. All three of these were verified to fail:
--
--     a) putting it in the middle of the key
--        ALTER TABLE cds MODIFY ORDER BY (advertiser_id, campaign_id,
--                                         country_code, event_date);
--        -> Code 36: "Primary key must be a prefix of the sorting key, but the
--           column in the position 2 is country_code, not event_date."
--
--     b) appending it in a SEPARATE statement after ADD COLUMN
--        -> Code 36: "Existing column country_code is used in the expression
--           that was added to the sorting key. You can add expressions that use
--           only the newly added columns."
--
--     c) appending it in the SAME statement, but with a DEFAULT
--        ALTER TABLE cds ADD COLUMN country_code LowCardinality(String)
--                        DEFAULT 'XX',
--                        MODIFY ORDER BY (..., country_code);
--        -> Code 36: "Newly added column country_code has a default expression,
--           so adding expressions that use it to the sorting key is forbidden."
--
--   So the only in-place path is: same ALTER, appended at the very END of the
--   key, and no DEFAULT on the column. Which means no backfill value either.
--   Take the rebuild.
--
--   Atlas plans that rebuild itself -- CREATE _tmp, INSERT SELECT, EXCHANGE
--   TABLES, DROP -- so you never see a Code 36 in the generated plan. The three
--   errors above are what you hit hand-writing the ALTER, not what Atlas emits.
--   Measured: the generated plan is 7 statements and lint reports no
--   diagnostics for any of them, table copy included.
--
--   This is why aggregate tables deserve a deliberately future-proofed sort key
--   on day one. It is the design decision you cannot cheaply revisit.
--
--   BACKFILL. An MV is an insert trigger. Recreating it rewrites zero rows of
--   history. Every row already in campaign_daily_stats has no country and will
--   never get one unless YOU replay it. That replay is a data migration, not a
--   schema migration, and it belongs in its own reviewed, restartable,
--   partition-at-a-time script. See scripts/backfill-country.sql.
--
-- THE POINT: schema-as-code covers structure. Correctness of a materialized
-- view chain is structure PLUS ordering PLUS a backfill. Budget for all three.
--
-- BEST PRACTICE WORTH STATING OUT LOUD: prefer versioned migrations over
-- declarative apply for anything touching an MV chain, precisely because you
-- need to open the generated file, reorder it, and interleave backfill steps.
-- =============================================================================

CREATE TABLE `ad_events` (
    `event_time`    DateTime CODEC(Delta, ZSTD(1)),
    `event_date`    Date DEFAULT toDate(event_time),
    `event_type`    LowCardinality(String),
    `advertiser_id` UInt32,
    `campaign_id`   UInt32,
    `creative_id`   UInt32,
    `placement_id`  UInt32,
    `user_id`       UInt64,
    `bid_price_usd` Decimal(10, 6),
    `revenue_usd`   Decimal(10, 6),
    `device_type`   LowCardinality(String) DEFAULT 'unknown',
    -- >>> CHANGE 1 of 3: source column <<<
    `country_code`  LowCardinality(String) DEFAULT 'XX'
)
ENGINE = MergeTree
PARTITION BY toYYYYMM(event_date)
ORDER BY (advertiser_id, campaign_id, event_time)
TTL event_date + toIntervalMonth(13)
SETTINGS index_granularity = 8192;

CREATE TABLE `campaign_daily_stats` (
    `event_date`    Date,
    `advertiser_id` UInt32,
    `campaign_id`   UInt32,
    -- >>> CHANGE 2 of 3: new aggregation dimension <<<
    `country_code`  LowCardinality(String) DEFAULT 'XX',
    `impressions`   UInt64,
    `clicks`        UInt64,
    `revenue_usd`   Decimal(38, 6)
)
ENGINE = SummingMergeTree
PARTITION BY toYYYYMM(event_date)
-- Sort key gains country_code. For SummingMergeTree the sort key IS the
-- grouping key, so leaving it out would collapse all countries together and
-- quietly produce wrong numbers. It also means a table rebuild.
ORDER BY (advertiser_id, campaign_id, country_code, event_date)
SETTINGS index_granularity = 8192;

-- >>> CHANGE 3 of 3: the trigger <<<
-- Atlas will DROP and CREATE this view. That is fine, it stores nothing.
-- What it does NOT do is re-aggregate the history. That is on you.
CREATE MATERIALIZED VIEW `campaign_daily_mv` TO `campaign_daily_stats` AS
SELECT
    event_date,
    advertiser_id,
    campaign_id,
    country_code,
    countIf(event_type = 'impression') AS impressions,
    countIf(event_type = 'click')      AS clicks,
    sum(revenue_usd)                   AS revenue_usd
FROM `ad_events`
GROUP BY event_date, advertiser_id, campaign_id, country_code;
