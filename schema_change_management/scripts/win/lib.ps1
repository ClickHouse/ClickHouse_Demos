# =============================================================================
# Shared helpers for the Windows PowerShell scripts. Dot-sourced, not run.
#
# Deliberately reads the SAME .env file as the bash scripts, so there is one
# place to keep credentials and no chance of the two paths drifting apart.
# =============================================================================

$ErrorActionPreference = 'Stop'

$script:RepoRoot = (Resolve-Path (Join-Path $PSScriptRoot '..\..')).Path

# -----------------------------------------------------------------------------
# Parse .env (KEY=VALUE), expanding ${OTHER_VAR} references in order, the way
# `set -a && source .env` does in bash.
# -----------------------------------------------------------------------------
function Import-DotEnv {
    $envPath = Join-Path $script:RepoRoot '.env'
    if (-not (Test-Path $envPath)) {
        Write-Error "$envPath not found. Run: Copy-Item .env.example .env"
    }

    $vals = [ordered]@{}
    foreach ($line in Get-Content $envPath) {
        $t = $line.Trim()
        if ($t -eq '' -or $t.StartsWith('#')) { continue }

        $i = $t.IndexOf('=')
        if ($i -lt 1) { continue }

        $key = $t.Substring(0, $i).Trim()
        $val = $t.Substring($i + 1).Trim()

        # Strip one layer of surrounding quotes.
        if ($val.Length -ge 2 -and
           (($val.StartsWith('"') -and $val.EndsWith('"')) -or
            ($val.StartsWith("'") -and $val.EndsWith("'")))) {
            $val = $val.Substring(1, $val.Length - 2)
        }

        # Expand ${VAR} against values already seen, then the process env.
        $val = [regex]::Replace($val, '\$\{([A-Za-z_][A-Za-z0-9_]*)\}', {
            param($m)
            $n = $m.Groups[1].Value
            if ($vals.Contains($n)) { return [string]$vals[$n] }
            $fromEnv = [Environment]::GetEnvironmentVariable($n)
            if ($fromEnv) { return $fromEnv }
            return ''
        })

        $vals[$key] = $val
        Set-Item -Path "env:$key" -Value $val
    }
}

# -----------------------------------------------------------------------------
# Pick the target: cloud (default) or local. Sets script-scoped Target* vars.
# -----------------------------------------------------------------------------
function Resolve-Target {
    param([string]$Target = 'cloud')

    switch ($Target) {
        'cloud' {
            foreach ($v in 'CH_CLOUD_HTTP','CH_CLOUD_USER','CH_CLOUD_PASSWORD') {
                if (-not (Get-Item "env:$v" -ErrorAction SilentlyContinue)) {
                    Write-Error "$v is not set in .env"
                }
            }
            $script:TargetHttp  = $env:CH_CLOUD_HTTP
            $script:TargetUser  = $env:CH_CLOUD_USER
            $script:TargetPass  = $env:CH_CLOUD_PASSWORD
            $script:TargetDb    = if ($env:CH_CLOUD_DB) { $env:CH_CLOUD_DB } else { 'adtech' }
            $script:TargetLabel = "ClickHouse Cloud ($($env:CH_CLOUD_HOST))"
            $script:TargetAtlasUrl = $env:CH_CLOUD_URL
        }
        'local' {
            $script:TargetHttp  = if ($env:CH_LOCAL_HTTP) { $env:CH_LOCAL_HTTP } else { 'http://localhost:8123' }
            $script:TargetUser  = 'default'
            $script:TargetPass  = 'localpass'
            $script:TargetDb    = 'adtech'
            $script:TargetLabel = 'local ClickHouse OSS'
            $script:TargetAtlasUrl = $env:CH_LOCAL_URL
        }
        default { Write-Error "Unknown target '$Target'. Use 'cloud' or 'local'." }
    }
}

# -----------------------------------------------------------------------------
# Run one query over the ClickHouse HTTP interface. Uses Invoke-RestMethod so
# there is no dependency on clickhouse-client or curl being installed.
# -----------------------------------------------------------------------------
# -NoDb runs the query with no database scope, so it lands in `default`. Needed
# for anything that runs before the adtech database exists: server version,
# "does this database exist", etc. Scoping those to a database that has not been
# created yet returns UNKNOWN_DATABASE, which reads like a connectivity failure
# and sends you debugging the wrong thing.
function Invoke-ChQuery {
    param(
        [Parameter(Mandatory)][string]$Sql,
        [switch]$Soft,
        [switch]$NoDb
    )

    $pair  = "$($script:TargetUser):$($script:TargetPass)"
    $basic = [Convert]::ToBase64String([Text.Encoding]::ASCII.GetBytes($pair))
    $uri   = if ($NoDb) { "$($script:TargetHttp)/" }
             else       { "$($script:TargetHttp)/?database=$($script:TargetDb)" }

    try {
        return Invoke-RestMethod -Method Post -Uri $uri `
            -Headers @{ Authorization = "Basic $basic" } `
            -Body ([Text.Encoding]::UTF8.GetBytes($Sql)) `
            -ContentType 'text/plain; charset=utf-8'
    } catch {
        if ($Soft) {
            Write-Host "  (query failed, continuing) $($_.Exception.Message)" -ForegroundColor DarkYellow
            return $null
        }
        throw
    }
}

# -----------------------------------------------------------------------------
# Retry wrapper for a query whose failure would be reported as a hard FAIL.
# ClickHouse Cloud services idle, and the first query or two after a wake can
# fail transiently. Observed live: `SELECT count() FROM system.parts` failed once
# on a cold service and preflight reported a missing grant that was present.
# Mirrors ch_query_nodb_retry in scripts/lib.sh.
# -----------------------------------------------------------------------------
function Invoke-ChQueryRetry {
    param([Parameter(Mandatory)][string]$Sql, [switch]$NoDb)
    foreach ($attempt in 1..3) {
        $r = if ($NoDb) { Invoke-ChQuery -NoDb -Soft -Sql $Sql }
             else       { Invoke-ChQuery       -Soft -Sql $Sql }
        if ($null -ne $r) { return $r }
        Start-Sleep -Seconds 2
    }
    return $null
}

# -----------------------------------------------------------------------------
# Run a multi-statement .sql file, one statement per request. ClickHouse's HTTP
# interface takes a single statement at a time.
# -----------------------------------------------------------------------------
function Invoke-ChFile {
    param([Parameter(Mandatory)][string]$Path)

    $buf = New-Object Text.StringBuilder
    foreach ($line in Get-Content $Path) {
        if ($line -match '^\s*--') { continue }        # skip comment-only lines
        [void]$buf.AppendLine($line)
        if ($line -match ';\s*$') {
            $stmt = $buf.ToString()
            $stmt = $stmt.Substring(0, $stmt.LastIndexOf(';'))
            if ($stmt.Trim()) {
                $flat = ($stmt -replace '\s+', ' ').Trim()
                if ($flat.Length -gt 110) { $flat = $flat.Substring(0, 110) }
                Write-Host "  -> $flat"
                Invoke-ChQuery -Sql $stmt | Out-Null
            }
            [void]$buf.Clear()
        }
    }
}

# -----------------------------------------------------------------------------
# Run an external command, swallow its output, return $true on exit code 0.
#
# Needed because this library sets $ErrorActionPreference = 'Stop', which turns a
# missing executable into a TERMINATING error. Without this wrapper, preflight on
# a machine that has no `atlas` on PATH dies at the first atlas call instead of
# reporting all the other checks -- which is exactly the moment you most want the
# full report.
#
#   if (Test-Command 'atlas' @('whoami')) { ... }
# -----------------------------------------------------------------------------
function Test-Command {
    param(
        [Parameter(Mandatory)][string]$Exe,
        [string[]]$Arguments = @()
    )

    if (-not (Get-Command $Exe -ErrorAction SilentlyContinue)) { return $false }

    $prev = $ErrorActionPreference
    $ErrorActionPreference = 'Continue'
    try {
        & $Exe @Arguments 2>&1 | Out-Null
        return ($LASTEXITCODE -eq 0)
    } catch {
        return $false
    } finally {
        $ErrorActionPreference = $prev
    }
}

# Same, but returns the command's stdout as a single trimmed string (or '').
function Get-CommandOutput {
    param(
        [Parameter(Mandatory)][string]$Exe,
        [string[]]$Arguments = @()
    )

    if (-not (Get-Command $Exe -ErrorAction SilentlyContinue)) { return '' }

    $prev = $ErrorActionPreference
    $ErrorActionPreference = 'Continue'
    try {
        $out = & $Exe @Arguments 2>&1
        if ($LASTEXITCODE -ne 0) { return '' }
        return (($out | Out-String).Trim())
    } catch {
        return ''
    } finally {
        $ErrorActionPreference = $prev
    }
}

# -----------------------------------------------------------------------------
# Rewrite the database path segment of a ClickHouse URL.
#
#   Get-AtlasUrlWithDb 'clickhouse://u:p@h:9440/adtech?secure=true' 'default'
#   -> clickhouse://u:p@h:9440/default?secure=true
#
# The database name in a native-protocol URL is sent in the client handshake, not
# per query, so connecting with a database that does not exist is refused outright
# with UNKNOWN_DATABASE (code 81). That looks like a TLS or port problem and is
# not one. Probing against `default` - always present on ClickHouse Cloud -
# separates "cannot connect" from "database not created yet".
#
# docker:// URLs are returned unchanged: there the path is <image>/<version>/<db>
# and Atlas creates that database itself.
# -----------------------------------------------------------------------------
# The `(.*@)?` is greedy on purpose: it consumes up to the LAST '@', which is how
# RFC 3986 separates userinfo from host. That matters because ClickHouse Cloud
# passwords are pasted in raw and can contain '/', '?' and '@'. A non-greedy or
# character-class match splits such a URL at the wrong place and hands Atlas a
# different host.
function Get-AtlasUrlWithDb {
    param([string]$Url, [string]$Db = 'default')
    if (-not $Url) { return '' }
    if ($Url -like 'docker://*') { return $Url }
    return [regex]::Replace($Url, '^([A-Za-z0-9+.\-]+://(.*@)?[^/?#]*)(/[^?#]*)?', "`$1/$Db")
}

# The database named in a ClickHouse URL, '' if there is none or if it is docker://.
function Get-AtlasUrlDb {
    param([string]$Url)
    if (-not $Url -or $Url -like 'docker://*') { return '' }
    return [regex]::Replace($Url, '^[A-Za-z0-9+.\-]+://(.*@)?[^/?#]*/?([^?#]*).*', '$2')
}

# Password-redacted copy of a URL, safe to print on a shared screen. Greedy for
# the same reason as above: a password containing '@' must be redacted whole.
function Get-RedactedUrl {
    param([string]$Url)
    if (-not $Url) { return '' }
    return [regex]::Replace($Url, '://([^:/@]+):.*@', '://$1:***@')
}

function Write-Hr  { Write-Host '-------------------------------------------------------------------------' }
function Write-Say { param([string]$Msg) Write-Host ''; Write-Host $Msg -ForegroundColor Cyan }
