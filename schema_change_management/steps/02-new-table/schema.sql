-- =============================================================================
-- SCENARIO 2: adding a table is the cheapest change you will ever make. Use it
-- to talk about engine choice, because that is the part you cannot cheaply undo.
--
-- Before: the state after scenario 1 (ad_events already has device_type).
--
-- Ask: "reporting needs advertiser names and account tier, not just IDs."
-- Change: one new dimension table. Nothing existing is touched.
--
-- WHAT ATLAS DOES
--   * The plan is a single CREATE TABLE. No ALTER, no rebuild, no data moved.
--   * `atlas migrate lint --latest 1` is clean. CREATE TABLE is not destructive,
--     so the analyzer configured in atlas.hcl has nothing to say about it.
--   * Reverting this scenario is scenario 3, and that one is NOT free. Read the
--     header of steps/03-drop-table/schema.sql before you apply this in anger.
--
-- WHY ReplacingMergeTree, AND NOT MergeTree
--   An advertiser record is a slowly-changing dimension: the same advertiser_id
--   is re-sent every time the CRM syncs, with a newer tier or a flipped active
--   flag. Three options:
--
--     MergeTree          - every sync appends another row. The table grows
--                          without bound and every reader deduplicates by hand.
--     ReplacingMergeTree - ClickHouse collapses rows sharing the sort key,
--                          keeping the one with the highest version column.
--                          Inserts stay append-only, which is what ClickHouse
--                          is good at. No UPDATE, no mutation.
--     Real UPDATE        - a mutation. Rewrites parts. Wrong tool for a
--                          dimension that changes daily.
--
--   So: ReplacingMergeTree(`updated_at`), ORDER BY `advertiser_id`.
--
-- THE PART EVERYONE GETS WRONG
--   Deduplication happens during background merges. It is ASYNCHRONOUS and is
--   never guaranteed to have finished. Two rows for advertiser 1000 can sit in
--   two different parts for an unbounded amount of time, and a plain SELECT
--   returns both. No setting makes this synchronous.
--
--   You get correct reads one of two ways:
--
--     SELECT * FROM `advertisers` FINAL WHERE advertiser_id = 1000;
--       Merges at query time. Correct, and costs you something. Fine on a
--       dimension this size, dangerous on a fact table.
--
--     SELECT advertiser_id, argMax(account_tier, updated_at) AS account_tier
--     FROM `advertisers` GROUP BY advertiser_id;
--       Correct without FINAL. Verbose. Scales better.
--
--   The sort key IS the deduplication key. Change ORDER BY later and you have
--   changed what "the same row" means, on top of the table rebuild a sort-key
--   change already forces. Get it right on day one.
--
-- ENGINE NAMES, AGAIN
--   Written OSS-style. ClickHouse Cloud promotes this to
--   SharedReplacingMergeTree; Atlas normalises it back before diffing, exactly
--   as it does for MergeTree and SummingMergeTree.
--
-- No PARTITION BY. The seeded fact data carries 8 advertisers (advertiser_id
-- 1000-1007, see scripts/seed.sh). Partitioning a dimension this small buys
-- nothing and costs you parts.
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

-- -----------------------------------------------------------------------------
-- >>> THE CHANGE <<<
-- Advertiser dimension. Slowly changing, re-synced from the CRM, joined against
-- ad_events.advertiser_id at query time.
--
-- The version column is `updated_at`: on a merge, rows sharing advertiser_id
-- collapse to the one with the greatest updated_at. Ties resolve arbitrarily, so
-- never let two rows share both the key and the version.
-- -----------------------------------------------------------------------------
CREATE TABLE `advertisers` (
    `advertiser_id`   UInt32,
    `advertiser_name` String,
    `account_tier`    LowCardinality(String) DEFAULT 'standard',
    `is_active`       UInt8 DEFAULT 1,
    `updated_at`      DateTime
)
ENGINE = ReplacingMergeTree(`updated_at`)
ORDER BY `advertiser_id`
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
