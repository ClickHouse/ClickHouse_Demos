-- =============================================================================
-- The part no schema tool does for you: re-aggregating history after the
-- materialized view definition changed in scenario 5.
--
-- Properties a backfill script needs, and which a generated migration will
-- never have:
--
--   RESTARTABLE   one partition at a time, so a failure at month 7 does not
--                 mean redoing months 1 to 6
--   IDEMPOTENT    safe to re-run a partition without double counting
--   OBSERVABLE    you can see how far it got
--   THROTTLED     bounded memory and threads so it does not starve queries
--
-- Run it AFTER the scenario 5 migration has been applied. Atlas performs the
-- rebuild itself -- CREATE _tmp, INSERT SELECT, EXCHANGE TABLES, DROP -- so once
-- that apply succeeds, `country_code` exists on both tables and this script is
-- runnable. Before the apply it is not: the SELECT below references a column
-- that does not exist yet.
--
-- Run it partition by partition. Do not paste the whole file into a console.
-- =============================================================================

-- -----------------------------------------------------------------------------
-- 0. What are we backfilling? List partitions and their sizes first, so you can
--    estimate and sequence the work instead of discovering it live.
-- -----------------------------------------------------------------------------
SELECT
    partition,
    sum(rows)                              AS rows,
    formatReadableSize(sum(bytes_on_disk))  AS on_disk
FROM system.parts
WHERE database = 'adtech' AND table = 'ad_events' AND active
GROUP BY partition
ORDER BY partition;

-- -----------------------------------------------------------------------------
-- 1. Per partition, in a loop you drive from the list above.
--
--    Replace 202605 with each partition from the query above, one at a time.
--    Yes, manually. A backfill you have to step through deliberately is a
--    feature, not a limitation. `./scripts/seed.sh` prints the partition list
--    for exactly this reason; a 5M-row seed spread over 180 days gives you
--    about seven monthly partitions to work through.
--
--    Verified idempotent: running 1a + 1b twice against the same partition
--    leaves the totals unchanged and matching the raw table.
--
--    IDEMPOTENCY: drop the target partition before rewriting it. Without this,
--    re-running inflates every metric, and because SummingMergeTree merges in
--    the background you may not notice for hours.
-- -----------------------------------------------------------------------------

-- 1a. Clear the target partition.
ALTER TABLE adtech.campaign_daily_stats DROP PARTITION '202605';

-- 1b. Rewrite it with the new country dimension included.
INSERT INTO adtech.campaign_daily_stats
SELECT
    event_date,
    advertiser_id,
    campaign_id,
    country_code,
    countIf(event_type = 'impression') AS impressions,
    countIf(event_type = 'click')      AS clicks,
    sum(revenue_usd)                   AS revenue_usd
FROM adtech.ad_events
WHERE toYYYYMM(event_date) = 202605
GROUP BY event_date, advertiser_id, campaign_id, country_code
SETTINGS
    max_threads = 4,                       -- leave headroom for live queries
    max_execution_time = 900,              -- fail loudly rather than hang
    max_bytes_before_external_group_by = 4000000000;

-- 1c. Verify the partition before moving to the next one.
--     Totals must match. The row count will be HIGHER than before, because one
--     row per campaign-day has become one row per campaign-day-country. That is
--     expected. The SUMS are what must agree.
SELECT
    'raw' AS src,
    countIf(event_type = 'impression') AS impressions,
    countIf(event_type = 'click')      AS clicks,
    sum(revenue_usd)                   AS revenue
FROM adtech.ad_events
WHERE toYYYYMM(event_date) = 202605
UNION ALL
SELECT
    'agg' AS src,
    sum(impressions),
    sum(clicks),
    sum(revenue_usd)
FROM adtech.campaign_daily_stats
WHERE toYYYYMM(event_date) = 202605;

-- -----------------------------------------------------------------------------
-- 2. Watch for anything still running. Mutations in ClickHouse are async: the
--    ALTER returns immediately and the work happens afterwards. Any change
--    management process for ClickHouse that does not check this is incomplete,
--    because "the migration succeeded" and "the data is correct" are separated
--    by an unbounded amount of time.
-- -----------------------------------------------------------------------------
SELECT
    database,
    table,
    mutation_id,
    command,
    parts_to_do,
    is_done,
    latest_fail_reason
FROM system.mutations
WHERE NOT is_done
ORDER BY create_time DESC;

-- -----------------------------------------------------------------------------
-- 3. Only once every partition verifies, remove the double-write or dual-read
--    shim in the application, if you used one. See the expand-and-contract
--    sequence at the end of scenario 5 in SCENARIOS.md.
-- -----------------------------------------------------------------------------
