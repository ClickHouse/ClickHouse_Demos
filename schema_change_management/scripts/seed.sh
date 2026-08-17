#!/usr/bin/env bash
# =============================================================================
# Loads synthetic ad events. The materialized view already exists, so it fires
# on this insert and populates campaign_daily_stats automatically.
#
# That is the first half of the materialized-view mental model, and it is worth
# stating explicitly while the numbers are on screen:
#
#     An MV is an insert trigger. It sees every insert that happens AFTER it
#     was created, and none that happened before.
#
# Scenario 3 shows the other half: recreate the MV and history is untouched.
#
#     ./scripts/seed.sh          # cloud
#     ./scripts/seed.sh local
# =============================================================================
source "$(dirname "${BASH_SOURCE[0]}")/lib.sh"
load_env
resolve_target "${1:-cloud}"

ROWS="${SEED_ROWS:-5000000}"

say "Seeding ${ROWS} rows into ad_events on ${TARGET_LABEL}"
echo "  campaign_daily_stats will fill itself, via campaign_daily_mv."
ch_query "
INSERT INTO ad_events
  (event_time, event_type, advertiser_id, campaign_id, creative_id,
   placement_id, user_id, bid_price_usd, revenue_usd)
SELECT
  -- Spread events evenly over the last 180 days, INDEPENDENT of row count, so
  -- you always get ~7 monthly partitions to backfill regardless of SEED_ROWS.
  now() - toIntervalSecond(intDiv(number * 15552000, ${ROWS})) AS event_time,
  if(number % 20 = 0, 'click', 'impression')               AS event_type,
  1000 + (number % 8)                                      AS advertiser_id,
  50000 + (number % 40)                                     AS campaign_id,
  900000 + (number % 200)                                   AS creative_id,
  7000 + (number % 25)                                      AS placement_id,
  cityHash64(number)                                        AS user_id,
  toDecimal64(0.50 + (number % 100) / 1000, 6)              AS bid_price_usd,
  toDecimal64(if(number % 20 = 0, 1.20, 0.004), 6)          AS revenue_usd
FROM numbers(${ROWS})
SETTINGS max_insert_threads = 4
" >/dev/null
echo "  done"

say "Partitions you will be backfilling in scenario 5"
ch_query "
SELECT
  partition,
  sum(rows)                              AS rows,
  formatReadableSize(sum(bytes_on_disk)) AS on_disk
FROM system.parts
WHERE database = '${TARGET_DB}' AND table = 'ad_events' AND active
GROUP BY partition
ORDER BY partition
FORMAT PrettyCompactMonoBlock"
echo "  Note these partition values. scripts/backfill-country.sql works one at"
echo "  a time and you will substitute them in by hand, deliberately."

say "Row counts and on-disk size"
ch_query "
SELECT
  table,
  sum(rows)                              AS rows,
  formatReadableSize(sum(bytes_on_disk)) AS on_disk,
  count()                                AS parts
FROM system.parts
WHERE database = '${TARGET_DB}' AND active
GROUP BY table
ORDER BY table
FORMAT PrettyCompactMonoBlock"

say "BASELINE RECONCILIATION - screenshot this, you will re-run it after scenario 5"
ch_query "
SELECT
  'raw' AS src,
  countIf(event_type = 'impression') AS impressions,
  countIf(event_type = 'click')      AS clicks,
  sum(revenue_usd)                   AS revenue
FROM ad_events
UNION ALL
SELECT
  'agg' AS src,
  sum(impressions),
  sum(clicks),
  sum(revenue_usd)
FROM campaign_daily_stats
FORMAT PrettyCompactMonoBlock"

hr
echo "The two rows must match exactly. They do, because the MV was already in"
echo "place when the data landed."
echo
echo "Two things to say while this is on screen:"
echo
echo "  1. Nobody wrote an aggregation job. The MV did it on insert. That is"
echo "     the ClickHouse pattern, and it is also what makes schema change"
echo "     harder here than in a warehouse with batch transforms."
echo
echo "  2. If the MV had been created AFTER this insert, the agg row would read"
echo "     zero. Same DDL, completely different outcome. That asymmetry is what"
echo "     scenario 5 is about, and it is the one thing no schema migration"
echo "     tool will handle for you."
echo
echo "Next: ./scripts/use-step.sh 0, then baseline the migration directory"
echo "      (SETUP.md step 9) before scenario 1. Without that baseline the first"
echo "      atlas migrate diff regenerates the whole schema."
