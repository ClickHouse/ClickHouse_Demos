-- =============================================================================
-- SCENARIO 4: the innocent-looking PR that should never reach production.
--
-- Three changes, all of which a reviewer would nod through in a diff, and all
-- of which behave very differently in ClickHouse than in Postgres or MySQL.
--
--   (a) user_id: UInt64 -> UInt32
--       Reads like "tightening a type". Actually a MUTATION: ClickHouse
--       rewrites every part of the column across the whole table, and any
--       value above 4,294,967,295 is silently truncated. Irreversible.
--       Mutations are asynchronous, so the ALTER "succeeds" instantly and the
--       damage lands minutes or hours later in system.mutations.
--
--   (b) DROP COLUMN placement_id
--       Destructive. Atlas lint flags it. Worth showing that the tool catches
--       the obvious one and NOT the subtle ones above and below.
--
--   (c) campaign_daily_stats ORDER BY reordered
--       (advertiser_id, campaign_id, event_date)
--         -> (campaign_id, advertiser_id, event_date)
--       ClickHouse cannot ALTER an existing sort key like this. The only route
--       is create-new-table + INSERT SELECT + swap + drop. On a real reporting
--       table that is a full data copy with a cutover window.
--
--       Atlas plans that rebuild itself -- CREATE _tmp, INSERT SELECT,
--       EXCHANGE TABLES, DROP -- so you never see a Code 36 in the generated
--       plan. You see a plan that looks routine and copies the whole table.
--       Measured: lint reports only the DROP COLUMN, never the copy.
--
-- THE POINT OF THIS SCENARIO:
--   A declarative differ tells you WHAT will change. It does not tell you what
--   that change COSTS. Those are different questions, and only one of them is
--   automated. Every generated plan needs a human who knows ClickHouse.
-- =============================================================================

CREATE TABLE `ad_events` (
    `event_time`    DateTime CODEC(Delta, ZSTD(1)),
    `event_date`    Date DEFAULT toDate(event_time),
    `event_type`    LowCardinality(String),
    `advertiser_id` UInt32,
    `campaign_id`   UInt32,
    `creative_id`   UInt32,
    -- (b) placement_id deleted. Nothing else in the file records that it ever
    --     existed, which is exactly why lint exists.
    -- (a) narrowed from UInt64. Rewrites the column. Truncates silently.
    `user_id`       UInt32,
    `bid_price_usd` Decimal(10, 6),
    `revenue_usd`   Decimal(10, 6),
    `device_type`   LowCardinality(String) DEFAULT 'unknown'
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
    `impressions`   UInt64,
    `clicks`        UInt64,
    `revenue_usd`   Decimal(38, 6)
)
ENGINE = SummingMergeTree
PARTITION BY toYYYYMM(event_date)
-- (c) sort key reordered. Cannot be ALTERed. Forces a table rebuild.
ORDER BY (campaign_id, advertiser_id, event_date)
SETTINGS index_granularity = 8192;

CREATE MATERIALIZED VIEW `campaign_daily_mv` TO `campaign_daily_stats` AS
SELECT
    event_date,
    advertiser_id,
    campaign_id,
    countIf(event_type = 'impression') AS impressions,
    countIf(event_type = 'click')      AS clicks,
    sum(revenue_usd)                   AS revenue_usd
FROM `ad_events`
GROUP BY event_date, advertiser_id, campaign_id;
