param(
    [string]$DatabaseDsn = $env:DATABASE_DSN,
    [string]$PythonExe = "",
    [string]$StageRoot = "runs",
    [int]$MaxStores = 1,
    [int]$MaxCategories = 1,
    [int]$MaxPagesPerCategory = 1,
    [int]$MaxProducts = 20
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
    "--concurrency", 1,
    "--max-retries", 0,
    "--max-stores", $MaxStores,
    "--max-categories", $MaxCategories,
    "--max-pages-per-category", $MaxPagesPerCategory,
    "--max-products", $MaxProducts
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
