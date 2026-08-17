-- =============================================================================
-- Desired state for the `adtech` database.  THIS FILE IS THE SOURCE OF TRUTH.
-- =============================================================================
-- Conventions that matter for ClickHouse Cloud + Atlas:
--
--   1. Engines are written in OSS form (MergeTree, SummingMergeTree).
--      ClickHouse Cloud silently promotes these to their Shared* equivalents.
--      Writing Shared* here would break any local OSS dev/shadow database,
--      because those engines do not exist in the open-source build.
--
--   2. ORDER BY is stated explicitly and PRIMARY KEY is left implicit
--      (it defaults to the ORDER BY prefix). Keep it that way: a divergent
--      PRIMARY KEY is one of the easiest ways to confuse a schema differ.
--
--   3. index_granularity is pinned. If you omit it, ClickHouse fills in 8192
--      and every subsequent inspect/diff shows a phantom SETTINGS change.
--      Pin the settings you care about, or expect noise in your plans.
-- =============================================================================

-- -----------------------------------------------------------------------------
-- Raw event stream. High cardinality, append only, TTL'd.
-- -----------------------------------------------------------------------------
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
    `revenue_usd`   Decimal(10, 6)
)
ENGINE = MergeTree
PARTITION BY toYYYYMM(event_date)
ORDER BY (advertiser_id, campaign_id, event_time)
TTL event_date + toIntervalMonth(13)
SETTINGS index_granularity = 8192;

-- -----------------------------------------------------------------------------
-- Pre-aggregated reporting table. This is what the BI layer actually reads.
-- -----------------------------------------------------------------------------
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
ORDER BY (advertiser_id, campaign_id, event_date)
SETTINGS index_granularity = 8192;

-- -----------------------------------------------------------------------------
-- Incremental materialized view: an insert trigger on ad_events that writes
-- into campaign_daily_stats. It holds no data of its own.
--
-- Consequence for change management: dropping and recreating this MV loses
-- nothing, but it also backfills nothing. Any historical data has to be
-- replayed by hand. No schema tool does that for you.
-- -----------------------------------------------------------------------------
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
