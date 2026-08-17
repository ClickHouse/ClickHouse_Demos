# =============================================================================
# Manufactures the 2am hotfix. Applies steps\06-drift\hotfix.sql directly to the
# target, bypassing Atlas and bypassing git, exactly as a real on-call engineer
# would from the ClickHouse Cloud SQL console.
#
#     .\scripts\win\inject-drift.ps1
#     .\scripts\win\inject-drift.ps1 -Target local
# =============================================================================
param([string]$Target = 'cloud')

. (Join-Path $PSScriptRoot 'lib.ps1')
Import-DotEnv
Resolve-Target -Target $Target

Write-Say "Applying out-of-band hotfix to $script:TargetLabel"
Write-Host 'No pull request. No migration file. No record anywhere but the database.'
Write-Host ''

Invoke-ChFile -Path (Join-Path $script:RepoRoot 'steps\06-drift\hotfix.sql')

Write-Say 'Production now has objects the repository has never heard of'
Invoke-ChQuery -Sql @"
SELECT name, type, default_expression
FROM system.columns
WHERE database = '$script:TargetDb' AND table = 'ad_events'
ORDER BY position
FORMAT PrettyCompactMonoBlock
"@

Write-Host ''
Write-Host '  TTL is now 6 months, not the 13 the repo asks for:'
# system.tables has no ttl_expression column, so pull it out of the DDL text.
Invoke-ChQuery -Sql @"
SELECT extractAll(create_table_query, 'TTL [a-zA-Z_]+ \+ [a-zA-Z0-9_()]+')[1] AS current_ttl
FROM system.tables
WHERE database = '$script:TargetDb' AND name = 'ad_events'
FORMAT PrettyCompactMonoBlock
"@

Write-Host ''
Write-Host '  And an undocumented skipping index nobody will remember adding:'
Invoke-ChQuery -Sql @"
SELECT table, name, type_full, expr, granularity
FROM system.data_skipping_indices
WHERE database = '$script:TargetDb'
FORMAT PrettyCompactMonoBlock
"@

Write-Hr
Write-Host 'Now ask Atlas to compare reality against the repo:'
Write-Host ''
Write-Host '    atlas schema diff `'
Write-Host '      --from "$env:CH_CLOUD_URL" `'
Write-Host '      --to file://schema/sql/schema.sql `'
Write-Host '      --dev-url "$env:CH_DEV_URL" `'
Write-Host '      --exclude atlas_schema_revisions'
Write-Host ''
Write-Host 'The --exclude matters: atlas migrate apply keeps its revision history in'
Write-Host 'adtech.atlas_schema_revisions, which schema.sql does not describe. Without'
Write-Host 'it, every drift check also proposes dropping Atlas own bookkeeping table.'
Write-Host ''
Write-Host 'Read the output carefully with the room. It proposes reverting all three:'
Write-Host 'DROP COLUMN debug_trace_id, DROP INDEX idx_creative, and MODIFY TTL back to'
Write-Host '13 months - because as far as the repo is concerned none of it should exist.'
Write-Host 'Atlas models skipping indexes, so the index is caught too.'
Write-Host ''
Write-Host 'That is the correct behaviour AND the dangerous behaviour. The lesson is'
Write-Host 'that drift detection has to run continuously, not at apply time, so the'
Write-Host 'conversation happens the next morning instead of during the next deploy.'
