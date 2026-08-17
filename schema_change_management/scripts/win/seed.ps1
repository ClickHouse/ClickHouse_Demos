# =============================================================================
# Loads synthetic ad events. Windows equivalent of scripts/seed.sh.
#
# The materialized view already exists, so it fires on this insert and populates
# campaign_daily_stats automatically. That is the first half of the MV mental
# model: an MV is an insert trigger, it sees every insert AFTER it was created
# and none that happened before. Scenario 3 shows the other half.
#
#     .\scripts\win\seed.ps1
#     .\scripts\win\seed.ps1 -Target local
# =============================================================================
param([string]$Target = 'cloud')

. (Join-Path $PSScriptRoot 'lib.ps1')
Import-DotEnv
Resolve-Target -Target $Target

$rows = if ($env:SEED_ROWS) { $env:SEED_ROWS } else { '5000000' }

Write-Say "Seeding $rows rows into ad_events on $script:TargetLabel"
Write-Host '  campaign_daily_stats will fill itself, via campaign_daily_mv.'

Invoke-ChQuery -Sql @"
INSERT INTO ad_events
  (event_time, event_type, advertiser_id, campaign_id, creative_id,
   placement_id, user_id, bid_price_usd, revenue_usd)
SELECT
  now() - toIntervalSecond(intDiv(number * 15552000, $rows))  AS event_time,
  if(number % 20 = 0, 'click', 'impression')                  AS event_type,
  1000 + (number % 8)                                         AS advertiser_id,
  50000 + (number % 40)                                       AS campaign_id,
  900000 + (number % 200)                                     AS creative_id,
  7000 + (number % 25)                                        AS placement_id,
  cityHash64(number)                                          AS user_id,
  toDecimal64(0.50 + (number % 100) / 1000, 6)                AS bid_price_usd,
  toDecimal64(if(number % 20 = 0, 1.20, 0.004), 6)            AS revenue_usd
FROM numbers($rows)
SETTINGS max_insert_threads = 4
"@ | Out-Null
Write-Host '  done'

Write-Say 'Partitions you will be backfilling in scenario 5'
Invoke-ChQuery -Sql @"
SELECT
  partition,
  sum(rows)                              AS rows,
  formatReadableSize(sum(bytes_on_disk)) AS on_disk
FROM system.parts
WHERE database = '$script:TargetDb' AND table = 'ad_events' AND active
GROUP BY partition
ORDER BY partition
FORMAT PrettyCompactMonoBlock
"@
Write-Host '  Note these partition values. scripts\backfill-country.sql works one'
Write-Host '  at a time and you will substitute them in by hand, deliberately.'

Write-Say 'Row counts and on-disk size'
Invoke-ChQuery -Sql @"
SELECT
  table,
  sum(rows)                              AS rows,
  formatReadableSize(sum(bytes_on_disk)) AS on_disk,
  count()                                AS parts
FROM system.parts
WHERE database = '$script:TargetDb' AND active
GROUP BY table
ORDER BY table
FORMAT PrettyCompactMonoBlock
"@

Write-Say 'BASELINE RECONCILIATION - screenshot this, you re-run it after scenario 5'
Invoke-ChQuery -Sql @"
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
FORMAT PrettyCompactMonoBlock
"@

Write-Hr
Write-Host 'The two rows must match exactly. They do, because the MV was already in'
Write-Host 'place when the data landed.'
Write-Host ''
Write-Host 'Two things to say while this is on screen:'
Write-Host ''
Write-Host '  1. Nobody wrote an aggregation job. The MV did it on insert. That is'
Write-Host '     the ClickHouse pattern, and it is also what makes schema change'
Write-Host '     harder here than in a warehouse with batch transforms.'
Write-Host ''
Write-Host '  2. If the MV had been created AFTER this insert, the agg row would read'
Write-Host '     zero. Same DDL, completely different outcome. That asymmetry is what'
Write-Host '     scenario 5 is about, and it is the one thing no schema migration'
Write-Host '     tool will handle for you.'
Write-Host ''
Write-Host 'Next: .\scripts\win\use-step.ps1 0, then baseline the migration directory'
Write-Host '      (SETUP.md step 9) before scenario 1. Without that baseline the first'
Write-Host '      atlas migrate diff regenerates the whole schema.'
