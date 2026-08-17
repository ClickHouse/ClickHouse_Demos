# =============================================================================
# Puts the demo back to its starting state so you can run it twice, or recover
# if something goes sideways mid-meeting.
#
#     .\scripts\win\reset.ps1
#     .\scripts\win\reset.ps1 -Target local
#
# Because migrations\ is cleared, re-run the baselining block in SETUP.md step 9
# after bootstrap + seed, or the next `atlas migrate diff` regenerates the whole
# schema instead of the one change you meant to show.
#
# Destructive. It only ever touches the `adtech` database.
# =============================================================================
param([string]$Target = 'cloud')

. (Join-Path $PSScriptRoot 'lib.ps1')
Import-DotEnv
Resolve-Target -Target $Target

# Wrap in $(...) — a bare $script:TargetLabel followed by '?' parses badly.
$ans = Read-Host "Drop every table and view in '$($script:TargetDb)' on $($script:TargetLabel)? [y/N]"
if ($ans -notmatch '^[Yy]$') { Write-Host 'aborted'; exit 0 }

# Objects are dropped individually rather than dropping the whole database, so
# this works with the least-privilege atlas_admin user from
# setup/01-users-and-grants.sql, which has DROP TABLE / DROP VIEW on adtech.* but
# deliberately no DROP DATABASE. The database itself survives.
#
# Materialized views go first: dropping a target table out from under a live MV
# leaves the MV pointing at nothing.
foreach ($kind in 'MaterializedView', 'View', '') {
    $filter = if ($kind) { "AND engine = '$kind'" }
              else       { "AND engine NOT IN ('MaterializedView','View')" }

    $objs = Invoke-ChQuery -Soft -Sql @"
SELECT name FROM system.tables
WHERE database = '$($script:TargetDb)' $filter
FORMAT TSV
"@
    if (-not $objs) { continue }

    foreach ($o in ("$objs".Trim() -split "`n" | Where-Object { $_.Trim() })) {
        $n = $o.Trim()
        if ($kind) {
            $r = Invoke-ChQuery -Soft -Sql "DROP VIEW IF EXISTS ``$($script:TargetDb)``.``$n``"
            if ($null -eq $r) {
                Invoke-ChQuery -Soft -Sql "DROP TABLE IF EXISTS ``$($script:TargetDb)``.``$n``" | Out-Null
            }
        } else {
            Invoke-ChQuery -Soft -Sql "DROP TABLE IF EXISTS ``$($script:TargetDb)``.``$n``" | Out-Null
        }
        Write-Host "  dropped $n"
    }
}

$remain = Invoke-ChQuery -Soft -Sql "SELECT count() FROM system.tables WHERE database = '$($script:TargetDb)'"
Write-Host "  objects remaining in $($script:TargetDb): $("$remain".Trim())"

Copy-Item -Force `
    (Join-Path $script:RepoRoot 'steps\00-baseline.sql') `
    (Join-Path $script:RepoRoot 'schema\sql\schema.sql')
Write-Host '  schema\sql\schema.sql restored to baseline'

# NOT `Get-ChildItem -Path <dir> -Include ...`: -Include is silently ignored unless
# the path ends in a wildcard or -Recurse is passed, so that form reported success
# and deleted nothing. Filter explicitly instead.
$migDir = Join-Path $script:RepoRoot 'migrations'
$cleared = 0
Get-ChildItem -LiteralPath $migDir -File -ErrorAction SilentlyContinue |
    Where-Object { $_.Extension -eq '.sql' -or $_.Name -eq 'atlas.sum' } |
    ForEach-Object { Remove-Item -Force -LiteralPath $_.FullName; $cleared++ }
Write-Host "  migrations\ cleared ($cleared file(s))"

foreach ($f in 'schema\hcl\schema.generated.hcl', 'schema\sql\schema.inspected.sql') {
    Remove-Item -Force (Join-Path $script:RepoRoot $f) -ErrorAction SilentlyContinue
}
Write-Host '  generated files removed'

Write-Hr
Write-Host "Clean. Next: .\scripts\win\bootstrap.ps1 -Target $Target ; .\scripts\win\seed.ps1 -Target $Target"
Write-Host "            .\scripts\win\use-step.ps1 0, then re-baseline migrations\ (SETUP.md step 9)"
