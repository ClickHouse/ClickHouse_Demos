# =============================================================================
# Stops and removes the local ClickHouse container started by local-up.ps1.
#
#     .\scripts\win\local-down.ps1
#     .\scripts\win\local-down.ps1 -Wipe    # also delete the data volume
# =============================================================================
param([string]$Name, [switch]$Wipe)

. (Join-Path $PSScriptRoot 'lib.ps1')
Import-DotEnv
if (-not $Name) { $Name = if ($env:LOCAL_CH_NAME) { $env:LOCAL_CH_NAME } else { 'atlas-demo-ch' } }
if (-not $env:LOCAL_CH_VERSION) { $env:LOCAL_CH_VERSION = '26.6' }
$env:LOCAL_CH_NAME = $Name

Write-Say "Stopping container '$Name'"
$args = @('compose', '-f', (Join-Path $script:RepoRoot 'docker-compose.yml'), 'down')
if ($Wipe) { $args += '-v'; Write-Host '  -Wipe: the data volume goes too' }

& docker @args
if ($LASTEXITCODE -ne 0) {
    & docker rm -f $Name 2>$null
    if ($LASTEXITCODE -ne 0) { Write-Host '  nothing to remove' }
}
Write-Host '  done'
