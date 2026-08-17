# =============================================================================
# Run this BEFORE the meeting. Windows equivalent of scripts/preflight.sh.
#
#     .\scripts\win\preflight.ps1
#     .\scripts\win\preflight.ps1 -Target local
#
# Safe to run at any point in setup, including before the adtech database exists.
# A missing database is a WARN with the exact command that fixes it, not a FAIL.
#
# If PowerShell refuses to run it:
#     Set-ExecutionPolicy -Scope Process -ExecutionPolicy Bypass
# =============================================================================
param([string]$Target = 'cloud')

. (Join-Path $PSScriptRoot 'lib.ps1')
Import-DotEnv
Resolve-Target -Target $Target

$script:Failed = $false
function Ok   { param($m) Write-Host "  OK    $m"   -ForegroundColor Green }
function Bad  { param($m) Write-Host "  FAIL  $m"   -ForegroundColor Red;    $script:Failed = $true }
function Warn { param($m) Write-Host "  WARN  $m"   -ForegroundColor Yellow }

Write-Say '1. Tooling'

$hasAtlas = [bool](Get-Command atlas -ErrorAction SilentlyContinue)
if ($hasAtlas) {
    $v = (Get-CommandOutput 'atlas' @('version')) -split "`n" | Select-Object -First 1
    Ok "atlas CLI: $v"
} else {
    Bad 'atlas CLI not on PATH. See SETUP.md step 1 for the Windows install.'
}

$dockerOk = Test-Command 'docker' @('info')
if ($dockerOk) {
    Ok 'docker daemon reachable'
} elseif ($env:CH_DEV_URL -like 'docker://*') {
    Bad 'docker not reachable, but CH_DEV_URL is a docker:// URL'
} else {
    Warn 'docker not reachable (fine, CH_DEV_URL is not docker://)'
}

Write-Say '2. Atlas login  (the ClickHouse driver needs an Atlas Pro entitlement)'
if (-not $hasAtlas) {
    Bad 'skipped: atlas CLI not installed (see step 1 above)'
} elseif (Test-Command 'atlas' @('whoami')) {
    Ok "logged in: $((Get-CommandOutput 'atlas' @('whoami')) -replace "`n", ' ')"
} else {
    Bad 'not logged in. Run: atlas login    <- ClickHouse will not work without this'
    Write-Host '        The ClickHouse driver requires an Atlas Pro entitlement. See SETUP.md step 2.' -ForegroundColor DarkGray
}

Write-Say "3. Target database: $script:TargetLabel"
# -NoDb deliberately: the adtech database may not exist yet on a fresh service,
# and scoping this query to it would report UNKNOWN_DATABASE, which looks like a
# connectivity problem and wastes ten minutes.
$targetVer = $null
try {
    $targetVer = (Invoke-ChQuery -NoDb -Sql 'SELECT version()').ToString().Trim()
    Ok "reachable, server version $targetVer"
} catch {
    Bad "cannot reach $script:TargetHttp over HTTP. Check host, port 8443, credentials."
}

# Three states, not two. 'unknown' matters because the query below reads
# system.databases, which ClickHouse SILENTLY ROW-FILTERS for a user without
# SELECT ON system.* (see the long note in section 3b). Telling someone to
# CREATE DATABASE when the real problem is a missing grant is the same category
# of misdiagnosis this script exists to prevent.
$dbState = 'unknown'
if ($targetVer) {
    $raw = "$(Invoke-ChQuery -NoDb -Soft -Sql "SELECT count() FROM system.databases WHERE name = '$script:TargetDb'")".Trim()
    if     ($raw -eq '1') { $dbState = '1' }
    elseif ($raw -eq '0') { $dbState = '0' }

    if ($dbState -eq '1') {
        Ok "database '$script:TargetDb' exists"
    } elseif ($dbState -eq 'unknown') {
        Warn "could not determine whether '$script:TargetDb' exists. See the grant check below."
    } else {
        # Not a failure. This check is meant to be runnable before the database is
        # created. But name the command that actually creates it: bootstrap.ps1
        # does NOT, and sending people there costs a round trip.
        Warn "database '$script:TargetDb' does not exist yet. Nothing below can inspect it."
        if ($Target -eq 'local') {
            Write-Host '        The container should have been started with it. Recreate it:'      -ForegroundColor DarkGray
            Write-Host '            .\scripts\win\local-down.ps1 ; .\scripts\win\local-up.ps1'     -ForegroundColor DarkGray
            Write-Host '        Then: .\scripts\win\bootstrap.ps1 -Target local'                   -ForegroundColor DarkGray
        } else {
            Write-Host '        Create it once, as an admin user, in the Cloud SQL console:'       -ForegroundColor DarkGray
            Write-Host "            CREATE DATABASE $script:TargetDb;"                             -ForegroundColor DarkGray
            Write-Host '        setup/01-users-and-grants.sql does this and creates the scoped'    -ForegroundColor DarkGray
            Write-Host '        users (SETUP.md step 5). Then: .\scripts\win\bootstrap.ps1'         -ForegroundColor DarkGray
        }
    }
}
$dbExists = ($dbState -eq '1')

if ($targetVer) {
    Write-Say '3b. Permissions  (GRANT SELECT ON system.* - the one people forget)'
    #
    # ClickHouse treats these two groups differently, verified on ClickHouse 26.8:
    #
    #   HARD DENIED without the grant:
    #     system.parts, system.mutations, system.data_skipping_indices, system.clusters
    #
    #   SILENTLY ROW-FILTERED without the grant:
    #     system.tables, system.columns, system.databases
    #     A user missing the grant saw 1 table where an authorised user saw 127.
    #     The query SUCCEEDS and returns almost nothing.
    #
    # So probing system.tables proves nothing. Probe system.parts, which fails
    # loudly, then separately report how much the user can actually see.
    #
    # The row-filtered case is the nastiest failure mode in the whole setup: Atlas
    # reads a nearly-empty database, concludes nothing exists, and plans to create
    # everything from scratch. No error anywhere.
    #
    if ($null -ne (Invoke-ChQueryRetry -NoDb -Sql 'SELECT count() FROM system.parts')) {
        Ok 'SELECT ON system.* is present'
    } else {
        Bad 'missing the system grant. Run: GRANT SELECT ON system.* TO <your user>'
        Write-Host '        See SETUP.md step 5.'                                                  -ForegroundColor DarkGray
        Write-Host '        Symptom if you skip it: no error, but every diff proposes CREATE for'   -ForegroundColor DarkGray
        Write-Host '        objects that already exist, because Atlas cannot see them.'            -ForegroundColor DarkGray
    }

    if ($null -ne (Invoke-ChQueryRetry -NoDb -Sql 'SELECT count() FROM system.data_skipping_indices')) {
        Ok 'can read system.data_skipping_indices (scenario 6 needs this)'
    } else {
        Warn 'cannot read system.data_skipping_indices; scenario 6 will show less'
    }

    $who     = Invoke-ChQuery -NoDb -Soft -Sql 'SELECT currentUser()'
    $visible = Invoke-ChQuery -NoDb -Soft -Sql "SELECT count() FROM system.tables WHERE database = '$script:TargetDb'"
    $visible = if ($null -eq $visible) { '?' } else { "$visible".Trim() }
    Write-Host "  connected as: $("$who".Trim())"
    Write-Host "  tables visible in '$script:TargetDb': $visible"
    if ($visible -eq '0' -and $dbExists) {
        Warn 'database exists but you can see 0 tables in it.'
        Write-Host '        Either it is not bootstrapped yet (fine, run bootstrap), or your' -ForegroundColor DarkGray
        Write-Host '        grants hide it (not fine). Check the grants printed below.'       -ForegroundColor DarkGray
    }
    Write-Host '  grants:'
    $g = Invoke-ChQuery -NoDb -Soft -Sql 'SHOW GRANTS FORMAT TSVRaw'
    if ($g) { "$g".Trim() -split "`n" | ForEach-Object { Write-Host "    $_" } }
    else    { Write-Host '    (could not read own grants; not fatal)' }
}

Write-Say '4. Atlas connectivity over the native protocol'
#
# The database name in a ClickHouse URL travels in the native-protocol handshake,
# not per query, so connecting with a database that does not exist is refused at
# connect time with UNKNOWN_DATABASE (code 81). That is indistinguishable, from
# the exit code alone, from a wrong port or a blocked IP.
#
# So: whenever the scoped inspect fails, re-probe against `default` - always
# present on ClickHouse Cloud, and the database Atlas's own Cloud examples connect
# to. The re-probe uses the same credentials over the same port, so it cannot mask
# a real auth or network problem; it can only separate "cannot connect" from
# "connected fine, that one database is not reachable".
#
# It is deliberately NOT gated on $dbExists. $dbExists is derived from CH_CLOUD_DB,
# while the connection uses whatever database CH_CLOUD_URL happens to name, and
# those two can disagree.
#
$urlDb = Get-AtlasUrlDb $script:TargetAtlasUrl
if (-not $script:TargetAtlasUrl) {
    Bad 'connection URL for target is empty. Check .env'
} elseif (-not $hasAtlas) {
    Bad 'skipped: atlas CLI not installed (see step 1 above)'
} elseif (Test-Command 'atlas' @('schema', 'inspect', '--url', $script:TargetAtlasUrl)) {
    Ok 'atlas schema inspect succeeded (native protocol + TLS path is good)'
} elseif (Test-Command 'atlas' @('schema', 'inspect', '--url', (Get-AtlasUrlWithDb $script:TargetAtlasUrl 'default'))) {
    $shownDb = if ($urlDb) { $urlDb } else { $script:TargetDb }
    Ok "native protocol + TLS path is good (probed against 'default')"
    Warn "atlas connected, but cannot inspect '$shownDb'. Not a connection problem."
    if ($dbState -eq '0') {
        Write-Host '        That database does not exist yet - see step 3 above, then re-run this script.' -ForegroundColor DarkGray
    } elseif ($urlDb -and $urlDb -ne $script:TargetDb) {
        Write-Host "        Note the mismatch: CH_CLOUD_URL names '$urlDb', CH_CLOUD_DB is '$script:TargetDb'." -ForegroundColor DarkGray
        Write-Host '        Make them agree in .env.' -ForegroundColor DarkGray
    } else {
        Write-Host '        Either it does not exist, or your user cannot see it. Check the grants above.' -ForegroundColor DarkGray
    }
} else {
    Bad 'atlas cannot connect at all - this is not just a missing database.'
    Write-Host "        The probe against 'default' failed too, so the connection itself is bad." -ForegroundColor DarkGray
    Write-Host '        Three causes, in order of likelihood:'                                   -ForegroundColor DarkGray
    Write-Host '          1. IP access list does not include you (SETUP.md step 4).'            -ForegroundColor DarkGray
    Write-Host '          2. URL is not native 9440 with ?secure=true (Cloud rejects otherwise).' -ForegroundColor DarkGray
    Write-Host '          3. Not logged in, or no Atlas Pro entitlement (see step 2 above).'    -ForegroundColor DarkGray
    Write-Host '        Reproduce with the error text visible:'                                  -ForegroundColor DarkGray
    Write-Host '            atlas schema inspect --url "$env:CH_CLOUD_URL"'                      -ForegroundColor DarkGray
}

Write-Say '5. Dev database'
Write-Host "  CH_DEV_URL = $(Get-RedactedUrl $env:CH_DEV_URL)"
$devDb = Get-AtlasUrlDb $env:CH_DEV_URL
if (-not $env:CH_DEV_URL) {
    Bad 'CH_DEV_URL unset. Atlas needs a dev database to plan and validate.'
} elseif (-not $hasAtlas) {
    Bad 'skipped: atlas CLI not installed (see step 1 above)'
} elseif (Test-Command 'atlas' @('schema', 'inspect', '--url', $env:CH_DEV_URL)) {
    Ok 'dev database usable'
    if ($env:CH_DEV_URL -like 'docker://clickhouse/*') {
        $devVer = ($env:CH_DEV_URL -replace '^docker://clickhouse/', '') -replace '/.*$', ''
        if ($targetVer) {
            Warn "VERSION PARITY: dev is OSS $devVer, target is $targetVer."
            Write-Host "        Plans are validated against $devVer, then applied to $targetVer." -ForegroundColor DarkGray
            Write-Host '        Say this out loud in the demo. To remove the gap entirely, point'   -ForegroundColor DarkGray
            Write-Host '        CH_DEV_URL at a second ClickHouse Cloud service.'                  -ForegroundColor DarkGray
        }
    }
} elseif ($env:CH_DEV_URL -like 'docker://*') {
    Bad 'cannot use dev database. Check the image tag exists on Docker Hub, and that'
    Write-Host '        the docker daemon is reachable (see step 1 above).' -ForegroundColor DarkGray
} else {
    # Same handshake trap as section 4. Atlas wipes and rebuilds objects INSIDE the
    # dev database; it does not create the database itself.
    $shown = if ($devDb) { $devDb } else { '<none>' }
    Bad "cannot use dev database '$shown'."
    if ($devDb) {
        Write-Host '        Most likely it does not exist yet. Atlas creates and drops objects' -ForegroundColor DarkGray
        Write-Host '        inside the dev database, but the database itself must already be'   -ForegroundColor DarkGray
        Write-Host '        there. On the dev service, as an admin:'                            -ForegroundColor DarkGray
        Write-Host "            CREATE DATABASE $devDb;"                                        -ForegroundColor DarkGray
    }
    Write-Host '        Otherwise check credentials, port 9440 and ?secure=true.' -ForegroundColor DarkGray
}

Write-Hr
if ($script:Failed) {
    Write-Host 'Preflight failed. Fix the FAIL lines before the meeting.' -ForegroundColor Red
    exit 1
}
Write-Host 'Preflight passed. Read any WARN lines above before you present.' -ForegroundColor Green
Write-Host ''
if (-not $dbExists) {
    if ($Target -eq 'local') {
        Write-Host 'Next: start the container (.\scripts\win\local-up.ps1), re-run this script, then:'
    } else {
        Write-Host 'Next: create the database (SETUP.md step 5), re-run this script, then:'
    }
} else {
    Write-Host 'Next:'
}
Write-Host "    .\scripts\win\bootstrap.ps1 -Target $Target ; .\scripts\win\seed.ps1 -Target $Target"
Write-Host "    .\scripts\win\use-step.ps1 0"
Write-Host '    then baseline the migration directory - SETUP.md step 9. Skipping that'
Write-Host '    makes scenario 1 generate the whole schema instead of one ADD COLUMN.'
