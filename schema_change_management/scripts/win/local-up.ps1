# =============================================================================
# Starts a local ClickHouse OSS container to rehearse against, using
# docker-compose.yml, and prints the exact .env values that match it.
#
#     .\scripts\win\local-up.ps1
#     .\scripts\win\local-down.ps1
#
# WHY COMPOSE AND NOT `atlas tool docker`
#
# This used to wrap `atlas tool docker`. It does not any more:
#
#   `atlas tool docker` exists to hand ATLAS a throwaway database. Atlas owns the
#   connection, so it publishes only the native protocol port, on a RANDOM host
#   port, with a RANDOM generated password. Verified on this repo: it bound 9000
#   to host port 32845 and never published 8123 at all.
#
#   Every helper script here (bootstrap, seed, inject-drift, reset) talks to
#   ClickHouse over HTTP on 8123. Against an Atlas-managed container there is no
#   HTTP port to reach and the password is not the one lib.ps1 expects.
#
# So `atlas tool docker` stays behind CH_DEV_URL, where Atlas is the only thing
# connecting. A rehearsal TARGET needs fixed ports and a known password.
# =============================================================================
param([string]$Version, [string]$Name)

. (Join-Path $PSScriptRoot 'lib.ps1')
Import-DotEnv
if (-not $Version) { $Version = if ($env:LOCAL_CH_VERSION) { $env:LOCAL_CH_VERSION } else { '26.6' } }
if (-not $Name)    { $Name    = if ($env:LOCAL_CH_NAME)    { $env:LOCAL_CH_NAME }    else { 'atlas-demo-ch' } }

if (-not (Test-Command 'docker' @('info'))) {
    Write-Error 'docker daemon is not reachable.'
}

Write-Say "Starting local ClickHouse $Version as container '$Name'"
Write-Host 'Pin this version close to your ClickHouse Cloud version. Mismatch here is'
Write-Host 'the parity risk SETUP.md step 6 talks about, so it is better to see it than'
Write-Host 'to let it hide.'
Write-Host ''

$env:LOCAL_CH_VERSION = $Version
$env:LOCAL_CH_NAME    = $Name
$compose = Join-Path $script:RepoRoot 'docker-compose.yml'
& docker compose -f $compose up -d

Write-Say 'Waiting for ClickHouse to accept queries'
$ready = $false
foreach ($i in 1..60) {
    try {
        $basic = [Convert]::ToBase64String([Text.Encoding]::ASCII.GetBytes('default:localpass'))
        Invoke-RestMethod -Method Post -Uri 'http://localhost:8123/' `
            -Headers @{ Authorization = "Basic $basic" } `
            -Body ([Text.Encoding]::UTF8.GetBytes('SELECT 1')) `
            -ContentType 'text/plain; charset=utf-8' -ErrorAction Stop | Out-Null
        Write-Host "  ready after ${i}s"; $ready = $true; break
    } catch { Start-Sleep -Seconds 1 }
}
if (-not $ready) {
    Write-Error "not accepting queries after 60s. Check: docker compose -f $compose logs"
}

Write-Hr
Write-Host 'These are already the defaults in .env.example, so there is normally'
Write-Host 'nothing to edit:'
Write-Host ''
Write-Host '    CH_LOCAL_URL=clickhouse://default:localpass@localhost:9000/adtech'
Write-Host '    CH_LOCAL_HTTP=http://localhost:8123'
Write-Host ''
Write-Host 'Both ports are published and the password is fixed, so the helper scripts'
Write-Host 'and Atlas agree on how to reach this container.'
Write-Hr
Write-Host 'Rehearse the whole demo here before you point anything at Cloud:'
Write-Host ''
Write-Host '    .\scripts\win\bootstrap.ps1 -Target local'
Write-Host '    .\scripts\win\seed.ps1 -Target local'
Write-Host ''
Write-Host 'Tear down when you are done:  .\scripts\win\local-down.ps1'
