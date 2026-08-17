-- =============================================================================
-- SCENARIO 3: deleting a table. The change that looks smallest in the diff and
-- is the only one in this demo you cannot undo with another pull request.
--
-- Before: the state after scenario 2 (advertisers exists).
-- Change: `advertisers` is deleted from this file. That is the entire diff.
--
-- WHAT ATLAS DOES
--   * The plan is one statement: DROP TABLE `advertisers`.
--   * `atlas migrate lint --latest 1` FAILS with exit 1 and one diagnostic,
--     measured on ClickHouse Cloud 26.2 with Atlas v1.3.1:
--
--       -- L2: Dropping table "advertisers"
--          https://atlasgo.io/lint/analyzers#DS102
--       -- suggested fix:
--          -> Add a pre-migration check to ensure table "advertisers" is empty
--             before dropping it
--
--     atlas.hcl sets lint.destructive.error = true, so this exits non-zero.
--     Compare with scenario 4, which trips DS103 on a dropped COLUMN.
--   * Note what lint does NOT do: it does not stop you. It fails the gate in CI.
--     A human still decides. That is the correct division of labour, and it is
--     the whole argument for putting `migrate lint` in the pull request instead
--     of trusting a review checklist.
--
-- WHY advertisers AND NOT SOMETHING ELSE
--   Deliberately the one table with no dependents. Nothing SELECTs from it and
--   no materialized view writes to it. Dropping a table an MV reads from is a
--   different and much worse failure: the MV survives as a broken object and
--   inserts start throwing. Keep that out of this scenario so the room can
--   concentrate on recoverability.
--
-- THE CLICKHOUSE-SPECIFIC BIT: A DROP IS NOT INSTANTLY UNRECOVERABLE
--   On the Atomic database engine (the default since 20.10, and what Cloud
--   builds on) DROP TABLE does not delete data immediately. The table is
--   detached, renamed, and parked in a delete queue. Until the queue entry
--   expires you can bring it back, data included:
--
--     UNDROP TABLE `advertisers`;
--
--   and, if a table of that name has been dropped more than once, disambiguate
--   by UUID:
--
--     SELECT table, uuid FROM system.dropped_tables;
--     UNDROP TABLE `advertisers` UUID '<uuid-from-that-query>';
--
--   HOW LONG YOU HAVE, MEASURED ON CLICKHOUSE CLOUD 26.2:
--
--     database_shared_drop_table_delay_seconds   28800   -- 8 hours, Cloud
--     database_atomic_delay_before_drop_table_sec  480   -- 8 minutes, OSS
--
--   Cloud runs the shared database engine, so the 8-hour value is the one that
--   applies there. Do not quote the 480-second OSS default at a Cloud customer,
--   and do not quote 8 hours at a self-managed one. Check the service in front
--   of you:
--
--     SELECT name, value FROM system.server_settings
--     WHERE name LIKE '%drop_table%';
--
--   Verified end to end on this demo service: create `advertisers`, insert a row,
--   DROP TABLE, confirm it is gone from system.tables, find it in
--   system.dropped_tables, UNDROP TABLE, and the row comes back.
--
--   Two ways to lose the window entirely:
--     * DROP TABLE `advertisers` SYNC;  -- bypasses the queue, deletes now
--     * waiting it out. After that, your only route back is a backup restore.
--
-- WHAT ROLLING BACK THE MIGRATION DOES, AND DOES NOT, DO
--   Re-adding this table to the desired state generates a CREATE TABLE. You get
--   the structure back and zero rows. Atlas has no idea the data existed.
--   Schema-as-code versions structure, never contents. UNDROP inside the window,
--   or a restore outside it, are the only things that recover data, and neither
--   is something your migration tool will do for you.
--
-- THE POINT: the safest destructive change is the one you staged. Drop in two
-- steps -- rename or detach first, wait out a full business cycle, delete later
-- -- and the recovery window stops being the thing standing between you and an
-- incident review.
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

-- >>> THE CHANGE <<<
-- `advertisers` was here. Nothing in this file records that it ever existed,
-- which is exactly why lint exists and why the migration file is the artifact
-- you review.

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
