-- =============================================================================
-- SCENARIO 1: the boring, safe, additive change.
--
-- Ask: "product wants device breakdown in the impression funnel."
-- Change: one new low-cardinality column on ad_events.
--
-- What you want the room to notice:
--   * The plan is a single ALTER TABLE ... ADD COLUMN.
--   * In ClickHouse this is a metadata-only operation. No parts are rewritten.
--     Existing rows read back the DEFAULT until they are next merged.
--   * `atlas migrate lint` returns clean. Nothing to argue about.
--   * Total human effort: edit one file, open one PR.
--
-- This is the 90% case. If your change management process makes THIS painful,
-- people will route around it, and that is how you get drift.
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
    -- >>> THE CHANGE <<<
    -- LowCardinality + explicit DEFAULT. Cheap to store, cheap to add,
    -- and no NULL handling for downstream consumers to get wrong.
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
ORDER BY (advertiser_id, campaign_id, event_date)
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
