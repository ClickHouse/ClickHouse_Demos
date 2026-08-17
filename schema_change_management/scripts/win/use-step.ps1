# =============================================================================
# Swaps the desired-state file to a given scenario. Stands in for "an engineer
# edits schema.sql and opens a pull request".
#
#     .\scripts\win\use-step.ps1 0    # baseline
#     .\scripts\win\use-step.ps1 1    # scenario 1: additive column
#     .\scripts\win\use-step.ps1 2    # scenario 2: create a table
#     .\scripts\win\use-step.ps1 3    # scenario 3: drop a table
#     .\scripts\win\use-step.ps1 4    # scenario 4: dangerous change
#     .\scripts\win\use-step.ps1 5    # scenario 5: MV chain evolution
#
# Scenario 6 (drift) has no desired-state file: it is applied straight to the
# database by .\scripts\win\inject-drift.ps1, which is the whole point of it.
#
# It prints the diff itself, so there is nothing to remember and no dependency on
# this directory being a git working tree.
# =============================================================================
param([Parameter(Mandatory)][ValidateSet('0','1','2','3','4','5')][string]$Step)

. (Join-Path $PSScriptRoot 'lib.ps1')

$map = @{
    '0' = @{ Path = 'steps\00-baseline.sql';           Name = 'baseline' }
    '1' = @{ Path = 'steps\01-additive\schema.sql';    Name = 'additive column' }
    '2' = @{ Path = 'steps\02-new-table\schema.sql';   Name = 'create a table' }
    '3' = @{ Path = 'steps\03-drop-table\schema.sql';  Name = 'drop a table' }
    '4' = @{ Path = 'steps\04-dangerous\schema.sql';   Name = 'dangerous change' }
    '5' = @{ Path = 'steps\05-mv-chain\schema.sql';    Name = 'materialized view chain' }
}

$src = Join-Path $script:RepoRoot $map[$Step].Path
$dst = Join-Path $script:RepoRoot 'schema\sql\schema.sql'

# Show the change before making it, the way a reviewer would see it in a PR.
# Comments and blank lines are stripped: the step files carry long explanatory
# headers that differ completely and would bury the one line that matters.
function Get-Ddl { param([string]$Path) @(Get-Content $Path | Where-Object { $_ -notmatch '^\s*(--|$)' }) }

if (Test-Path $dst) {
    Write-Say 'What changes in the desired state (DDL only, comments stripped)'
    $delta = Compare-Object (Get-Ddl $dst) (Get-Ddl $src)
    if ($delta) {
        foreach ($d in $delta) {
            $mark = if ($d.SideIndicator -eq '=>') { '+' } else { '-' }
            $col  = if ($d.SideIndicator -eq '=>') { 'Green' } else { 'Red' }
            Write-Host "  $mark $($d.InputObject)" -ForegroundColor $col
        }
    } else {
        Write-Host "  (no change - the desired state was already step $Step)"
    }
}

Copy-Item -Force $src $dst

Write-Say "Desired state is now: step $Step ($($map[$Step].Name))"
Write-Host 'Read the full file with its reasoning:'
Write-Host ''
Write-Host "    Get-Content $($map[$Step].Path)"
Write-Host ''
Write-Host 'Then ask Atlas what it would do:'
Write-Host ''
Write-Host '    atlas schema diff --env cloud --from "$env:CH_CLOUD_URL" ``'
Write-Host '                      --to file://schema/sql/schema.sql'
