# =============================================================================
# Applies the BASELINE schema to an EXISTING adtech database, directly, without
# Atlas. Windows equivalent of scripts/bootstrap.sh. It does not create the
# database - setup/01-users-and-grants.sql does that, once (SETUP.md step 5).
#
# Bypassing Atlas here is deliberate: it simulates the realistic starting point,
# an existing ClickHouse database nobody has under version control yet. The first
# thing the demo then does is bring it under control by inspecting it.
#
#     .\scripts\win\bootstrap.ps1
#     .\scripts\win\bootstrap.ps1 -Target local
# =============================================================================
param([string]$Target = 'cloud')

. (Join-Path $PSScriptRoot 'lib.ps1')
Import-DotEnv
Resolve-Target -Target $Target

Write-Say "Bootstrapping baseline schema on $script:TargetLabel"

# The database is NOT created here, on purpose.
#
# setup/01-users-and-grants.sql creates it, as an admin, once. The atlas_admin
# user this demo connects with deliberately has no CREATE DATABASE grant, because
# a schema migration tool should never be able to create or drop a database.
$dbCount = Invoke-ChQuery -NoDb -Soft -Sql "SELECT count() FROM system.databases WHERE name = '$script:TargetDb'"
if ("$dbCount".Trim() -ne '1') {
    Write-Host ''
    Write-Host "ERROR: database '$script:TargetDb' does not exist."  -ForegroundColor Red
    Write-Host ''
    Write-Host 'Create it once, as an admin user (SETUP.md step 5):'
    Write-Host "    CREATE DATABASE $script:TargetDb;"
    Write-Host ''
    Write-Host 'Running setup/01-users-and-grants.sql does this for you.'
    exit 1
}
Write-Host "  database $script:TargetDb present"

Invoke-ChFile -Path (Join-Path $script:RepoRoot 'steps\00-baseline.sql')

Write-Say 'Objects now present'
Invoke-ChQuery -Sql @"
SELECT name, engine FROM system.tables
WHERE database = '$script:TargetDb'
ORDER BY name
FORMAT PrettyCompactMonoBlock
"@

Write-Hr
Write-Host 'Note the engine names above.'
Write-Host 'On ClickHouse Cloud you asked for MergeTree and got SharedMergeTree.'
Write-Host 'That promotion is automatic, and it is why the schema file must be'
Write-Host 'written in OSS engine terms if you want a local dev database to work.'
Write-Host ''
Write-Host "Next: .\scripts\win\seed.ps1 -Target $Target"
