# =============================================================================
# Regenerates schema\hcl\schema.generated.hcl and schema\sql\schema.inspected.sql
# from a live database, then runs the round-trip parity test.
#
#     .\scripts\win\gen-hcl.ps1
#     .\scripts\win\gen-hcl.ps1 -Target local
#
# Both inspects go to temp files first and are only moved into place on success.
# Without that, a redirect truncates the previous good output before Atlas has said
# whether it can connect, and an idle ClickHouse Cloud service (i/o timeout on the
# first call) leaves an empty HCL file next to a stale .inspected.sql from a
# DIFFERENT database. Diffing those produces confident nonsense.
# =============================================================================
param([string]$Target = 'cloud')

. (Join-Path $PSScriptRoot 'lib.ps1')
Import-DotEnv

$url = if ($Target -eq 'local') { $env:CH_LOCAL_URL } else { $env:CH_CLOUD_URL }
if (-not $url) { Write-Error "connection URL for target '$Target' is empty. Check .env" }

$hclOut = Join-Path $script:RepoRoot 'schema\hcl\schema.generated.hcl'
$sqlOut = Join-Path $script:RepoRoot 'schema\sql\schema.inspected.sql'
$srcSql = Join-Path $script:RepoRoot 'schema\sql\schema.sql'
$tmpHcl = [IO.Path]::GetTempFileName()
$tmpSql = [IO.Path]::GetTempFileName()

function Invoke-Inspect {
    param([string]$Format, [string]$Dest, [string]$Label)
    $prev = $ErrorActionPreference; $ErrorActionPreference = 'Continue'
    $out = & atlas schema inspect --url $url --format $Format 2>&1
    $code = $LASTEXITCODE
    $ErrorActionPreference = $prev
    if ($code -ne 0 -or -not $out) {
        Write-Host "  ERROR: could not inspect $Target as $Label." -ForegroundColor Red
        ($out | Out-String).TrimEnd() -split "`n" | ForEach-Object { Write-Host "    $_" -ForegroundColor DarkGray }
        Write-Host ''
        Write-Host '  If that is an i/o timeout, the service was idle. Wake it and retry.' -ForegroundColor DarkGray
        Write-Host '  Existing generated files were left untouched, so you are not about'  -ForegroundColor DarkGray
        Write-Host '  to diff stale output against fresh output.'                          -ForegroundColor DarkGray
        Remove-Item -Force $tmpHcl, $tmpSql -ErrorAction SilentlyContinue
        exit 1
    }
    Set-Content -Path $Dest -Value ($out | Out-String) -NoNewline
}

Write-Say "Inspecting $Target as HCL"
Invoke-Inspect -Format '{{ hcl . }}' -Dest $tmpHcl -Label 'HCL'
Write-Say 'Same database, inspected as SQL'
Invoke-Inspect -Format '{{ sql . }}' -Dest $tmpSql -Label 'SQL'

Move-Item -Force $tmpHcl $hclOut
Move-Item -Force $tmpSql $sqlOut
Write-Host '  wrote schema\hcl\schema.generated.hcl'
Write-Host '  wrote schema\sql\schema.inspected.sql'

# Collapse each statement onto one line before sorting. Sorting raw lines compares
# a multi-line human-formatted CREATE against Atlas's single-line output and every
# line looks different.
function Get-NormalisedStatements {
    param([string]$Path)
    $text = (Get-Content $Path | Where-Object { $_ -notmatch '^\s*(--|$)' }) -join ' '
    return ($text -split ';' `
        | ForEach-Object { ($_ -replace '\s+', ' ').Trim() } `
        | Where-Object { $_ } `
        | Sort-Object)
}

Write-Hr
Write-Say 'Round-trip parity: schema\sql\schema.sql vs what Atlas read back'
$delta = Compare-Object (Get-NormalisedStatements $srcSql) (Get-NormalisedStatements $sqlOut)
if (-not $delta) {
    Write-Host '  identical after normalisation. Everything you wrote survived the round trip.'
} else {
    foreach ($d in $delta) {
        $mark = if ($d.SideIndicator -eq '=>') { 'read back' } else { 'yours    ' }
        Write-Host "    [$mark] $($d.InputObject)"
    }
    Write-Host ''
    Write-Host '  Read that as three separate things:'
    Write-Host '    1. Objects the database has and your file does not (atlas_schema_revisions,'
    Write-Host '       injected drift). Expected.'
    Write-Host '    2. Rendering differences. Cloud returns SharedMergeTree with a replication'
    Write-Host '       path, CODEC(Delta, ...) comes back as CODEC(Delta(4), ...), PRIMARY KEY'
    Write-Host '       is made explicit. Cosmetic. See SETUP.md for the full table.'
    Write-Host '    3. Anything ELSE missing from the read-back side is the finding.'
}
Write-Hr
Write-Host 'Put schema.generated.hcl next to schema\sql\schema.sql on screen and ask the'
Write-Host 'room which they would rather review in a pull request. For a ClickHouse-only'
Write-Host 'team the answer is usually SQL.'
