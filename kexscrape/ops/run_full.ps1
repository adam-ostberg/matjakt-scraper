param(
    [string]$DatabaseDsn = $env:DATABASE_DSN,
    [string]$PythonExe = "",
    [string]$StageRoot = "runs",
    [int]$Concurrency = 3,
    [int]$MaxRetries = 2
)

$ErrorActionPreference = "Stop"

$repoRoot = Split-Path -Parent $PSScriptRoot
$venvPython = Join-Path $repoRoot ".venv\Scripts\python.exe"
$venvScripts = Join-Path $repoRoot ".venv\Scripts"

if ([string]::IsNullOrWhiteSpace($PythonExe)) {
    $PythonExe = $venvPython
}

if (-not (Test-Path $PythonExe)) {
    throw "Python executable not found: $PythonExe"
}

if (Test-Path $venvScripts) {
    $env:Path = "$venvScripts;$env:Path"
}

$arguments = @(
    "-m", "kexscrape.run_all_stores",
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
    & $PythonExe @arguments
    if ($LASTEXITCODE -ne 0) {
        exit $LASTEXITCODE
    }
}
finally {
    Pop-Location
}
