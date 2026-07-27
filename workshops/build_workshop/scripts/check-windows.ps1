$ErrorActionPreference = 'Stop'
Set-StrictMode -Version Latest

$WorkshopRoot = (Resolve-Path (Join-Path $PSScriptRoot '..')).Path
$ContentRoot = Join-Path $WorkshopRoot 'playbook/content/docs'
$Setup = Join-Path $ContentRoot 'learner/00-setup.mdx'
$Troubleshooting = Join-Path $ContentRoot 'learner/troubleshooting.mdx'
$PlatformComponent = Join-Path $WorkshopRoot 'playbook/src/components/platform.tsx'
$DocsPage = Join-Path $WorkshopRoot 'playbook/src/app/docs/[[...slug]]/page.tsx'

function Assert-Literal {
    param(
        [Parameter(Mandatory = $true)][string]$Description,
        [Parameter(Mandatory = $true)][string]$Literal,
        [Parameter(Mandatory = $true)][string]$Path
    )

    $content = Get-Content -LiteralPath $Path -Raw
    if (-not $content.Contains($Literal)) {
        throw "Missing Windows workshop contract: $Description ($Literal in $Path)"
    }
}

Assert-Literal 'persistent platform storage key' 'clickhouse-workshop-platform' $PlatformComponent
Assert-Literal 'accessible platform selector' 'role="radiogroup"' $PlatformComponent
Assert-Literal 'shell reminder on every docs page' '<PlatformShellNote />' $DocsPage
Assert-Literal 'Ubuntu WSL 2 installation' 'wsl --install -d Ubuntu' $Setup
Assert-Literal 'supported Windows prerequisite' 'Windows 10 version 2004 (build 19041)' $Setup
Assert-Literal 'WSL status verification' 'wsl --status' $Setup
Assert-Literal 'WSL version verification' 'wsl --version' $Setup
Assert-Literal 'WSL is mandatory' 'WSL 1 are not supported workshop shells' $Setup
Assert-Literal 'WSL 2 Docker engine guidance' 'Use the WSL 2 based engine' $Setup
Assert-Literal 'Ubuntu Docker integration guidance' 'WSL Integration -> Ubuntu' $Setup
Assert-Literal 'Linux-home checkout requirement' '`pwd` begins with `/home/`' $Setup
Assert-Literal 'coding agent uses WSL checkout' 'Your coding agent must use this same WSL checkout' $Setup
Assert-Literal 'desktop agent WSL terminal check' 'uname -s' $Setup
Assert-Literal 'Git LF checkout policy' 'git config --global core.autocrlf input' $Setup
Assert-Literal 'WSL memory recovery' '.wslconfig' $Troubleshooting
Assert-Literal 'ClickHouse client verification' 'clickhouse client --version' $Setup
Assert-Literal 'preflight path from repository root' 'workshops/build_workshop/app' $Setup
Assert-Literal 'wrong-shell recovery' 'not recognized" in PowerShell' $Troubleshooting
Assert-Literal 'CRLF recovery' "bash\r" $Troubleshooting
Assert-Literal 'OAuth browser recovery' 'paste it into the normal Windows' $Troubleshooting

$shellScripts = @(Get-ChildItem -Path $WorkshopRoot -Recurse -File -Include '*.sh', '*.bash')
foreach ($script in $shellScripts) {
    $bytes = [System.IO.File]::ReadAllBytes($script.FullName)
    for ($index = 0; $index -lt ($bytes.Length - 1); $index++) {
        if ($bytes[$index] -eq 13 -and $bytes[$index + 1] -eq 10) {
            throw "Shell script must use LF line endings: $($script.FullName)"
        }
    }
}

$powershellScripts = @(Get-ChildItem -Path $WorkshopRoot -Recurse -File -Filter '*.ps1')
foreach ($script in $powershellScripts) {
    $tokens = $null
    $errors = $null
    [void][System.Management.Automation.Language.Parser]::ParseFile(
        $script.FullName,
        [ref]$tokens,
        [ref]$errors
    )
    if ($errors.Count -gt 0) {
        $messages = ($errors | ForEach-Object Message) -join '; '
        throw "PowerShell parse failure in $($script.FullName): $messages"
    }
}

$mdxFiles = @(Get-ChildItem -Path $ContentRoot -Recurse -File -Filter '*.mdx')
$powershellBlockCount = 0
foreach ($file in $mdxFiles) {
    $content = Get-Content -LiteralPath $file.FullName -Raw
    $fences = @(Select-String -LiteralPath $file.FullName -Pattern '^```').Count
    if (($fences % 2) -ne 0) {
        throw "Unbalanced code fences in $($file.FullName)"
    }

    $blocks = [regex]::Matches($content, '(?ms)^[ \t]*```powershell[ \t]*\r?\n(.*?)^[ \t]*```')
    foreach ($block in $blocks) {
        $powershellBlockCount++
        $tokens = $null
        $errors = $null
        [void][System.Management.Automation.Language.Parser]::ParseInput(
            $block.Groups[1].Value,
            [ref]$tokens,
            [ref]$errors
        )
        if ($errors.Count -gt 0) {
            $messages = ($errors | ForEach-Object Message) -join '; '
            throw "PowerShell fence parse failure in $($file.FullName): $messages"
        }
    }
}

if ($powershellBlockCount -lt 3) {
    throw "Expected at least three tested PowerShell instruction blocks, found $powershellBlockCount"
}

Write-Host 'Windows workshop contract checks passed.'
Write-Host "Validated $($shellScripts.Count) Bash scripts, $($powershellScripts.Count) PowerShell scripts, $powershellBlockCount PowerShell instruction blocks, and $($mdxFiles.Count) MDX files."
