param(
    [string]$DatabaseDsn = $env:DATABASE_DSN,
    [string]$PythonExe = "",
    [string]$StageRoot = "runs",
    [int]$Concurrency = 3,
    [int]$MaxRetries = 2
)

$ErrorActionPreference = "Stop"

$repoRoot = Split-Path -Parent $PSScriptRoot
$workspaceRoot = Split-Path -Parent $repoRoot
$venvPythonCandidates = @(
    (Join-Path $repoRoot ".venv\Scripts\python.exe"),
    (Join-Path $workspaceRoot ".venv\Scripts\python.exe")
)
$venvScriptsCandidates = @(
    (Join-Path $repoRoot ".venv\Scripts"),
    (Join-Path $workspaceRoot ".venv\Scripts")
)
$pythonArgs = @()

if ([string]::IsNullOrWhiteSpace($PythonExe)) {
    foreach ($candidate in $venvPythonCandidates) {
        if (Test-Path $candidate) {
            $PythonExe = $candidate
            break
        }
    }

    if ([string]::IsNullOrWhiteSpace($PythonExe)) {
        $pyLauncher = Get-Command py -ErrorAction SilentlyContinue
        if ($null -ne $pyLauncher) {
            $PythonExe = $pyLauncher.Source
            $pythonArgs = @("-3")
        }
        else {
            $pythonCmd = Get-Command python -ErrorAction SilentlyContinue
            if ($null -ne $pythonCmd) {
                $PythonExe = $pythonCmd.Source
            }
        }
    }
}

if ([string]::IsNullOrWhiteSpace($PythonExe)) {
    throw "Python executable not found. Create .venv first or install Python and make 'py' or 'python' available in PATH."
}

foreach ($venvScripts in $venvScriptsCandidates) {
    if (Test-Path $venvScripts) {
        $env:Path = "$venvScripts;$env:Path"
        break
    }
}

$arguments = @(
    "-m", "coopscrape.run_all_stores",
    "--stage-root", $StageRoot,
    "--concurrency", $Concurrency,
    "--max-retries", $MaxRetries
)

if (-not [string]::IsNullOrWhiteSpace($DatabaseDsn)) {
    $arguments += @("--db-dsn", $DatabaseDsn)
}
else {
    Write-Host "DATABASE_DSN saknas. Kor staging-only utan ingest till Supabase/Postgres." -ForegroundColor Yellow
}

Push-Location $repoRoot
try {
    & $PythonExe @pythonArgs @arguments
    if ($LASTEXITCODE -ne 0) {
        exit $LASTEXITCODE
    }
}
finally {
    Pop-Location
}
