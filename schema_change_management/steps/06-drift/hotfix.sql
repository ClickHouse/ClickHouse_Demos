-- =============================================================================
-- SCENARIO 6: drift. This file is the villain.
--
-- Story: 2am incident. An on-call engineer opens the ClickHouse Cloud SQL
-- console, adds a debug column to chase a bug, shortens a TTL to reclaim
-- storage, and adds a skipping index to speed up the query they keep running.
-- All three work. The incident closes. Nobody opens a PR.
--
-- Nothing is broken yet. What is broken is the assumption that the repo
-- describes production. The next person who runs a declarative apply is now
-- holding a plan that will DROP the debug column, DROP the index and REVERT the
-- TTL, because from the tool's point of view none of it is supposed to exist.
-- Atlas does model skipping indexes, so all three are detected.
--
-- Run this against the demo target to manufacture drift, then run
-- `atlas schema diff` and let the room read the plan.
--
--   ./scripts/inject-drift.sh cloud
-- =============================================================================

ALTER TABLE `adtech`.`ad_events`
    ADD COLUMN IF NOT EXISTS `debug_trace_id` String DEFAULT '';

-- Shortening a TTL is not free and it is not reversible: once parts age out
-- under the new rule the data is gone. Re-lengthening the TTL afterwards
-- brings nothing back.
ALTER TABLE `adtech`.`ad_events`
    MODIFY TTL `event_date` + toIntervalMonth(6);

-- A "temporary" index that becomes permanent because nobody remembers it.
-- Note it is added but NOT materialized, so it only applies to new parts.
-- Half-applied objects like this are the hardest kind of drift to reason about.
ALTER TABLE `adtech`.`ad_events`
    ADD INDEX IF NOT EXISTS `idx_creative` `creative_id` TYPE minmax GRANULARITY 4;
