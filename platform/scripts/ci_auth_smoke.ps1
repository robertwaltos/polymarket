param(
    [string]$BaseUrl = "http://127.0.0.1:18080",
    [string]$ServerAddr = "127.0.0.1:18080",
    [string]$PostgresImage = "postgres:16-alpine",
    [string]$PostgresUser = "postgres",
    [string]$PostgresPassword = "postgres",
    [string]$PostgresDb = "trading_platform",
    [int]$PostgresPort = 55432
)

Set-StrictMode -Version Latest
$ErrorActionPreference = "Stop"

function Write-Step([string]$Message) {
    Write-Host "[STEP] $Message" -ForegroundColor Cyan
}

function Resolve-CommandPath([string[]]$Names) {
    foreach ($name in $Names) {
        $cmd = Get-Command $name -ErrorAction SilentlyContinue
        if ($cmd) { return $cmd.Source }
    }
    return $null
}

$root = (Resolve-Path (Join-Path $PSScriptRoot "..")).Path
$docker = Resolve-CommandPath @("docker")
$cargo = Resolve-CommandPath @("cargo", "cargo.exe")
if (-not $docker) {
    $dockerFallback = "C:\Program Files\Docker\Docker\resources\bin\docker.exe"
    if (Test-Path $dockerFallback) {
        $docker = $dockerFallback
    }
}
if (-not $cargo) {
    $cargoFallback = Join-Path $env:USERPROFILE ".cargo\bin\cargo.exe"
    if (Test-Path $cargoFallback) {
        $cargo = $cargoFallback
    }
}
if (-not $docker) { throw "docker command not found in PATH" }
if (-not $cargo) { throw "cargo command not found in PATH" }

$container = "trading-smoke-pg-$([DateTimeOffset]::UtcNow.ToUnixTimeSeconds())"
$serverOutLog = Join-Path $root "var\ci_trading_server.out.log"
$serverErrLog = Join-Path $root "var\ci_trading_server.err.log"
$null = New-Item -ItemType Directory -Force -Path (Split-Path $serverOutLog)
$serverProc = $null

try {
    Write-Step "Starting Postgres container $container"
    & $docker run -d --rm --name $container `
        -e "POSTGRES_USER=$PostgresUser" `
        -e "POSTGRES_PASSWORD=$PostgresPassword" `
        -e "POSTGRES_DB=$PostgresDb" `
        -p "${PostgresPort}:5432" `
        $PostgresImage | Out-Null

    Write-Step "Waiting for Postgres readiness"
    $pgReady = $false
    for ($i = 0; $i -lt 120; $i++) {
        & $docker exec $container pg_isready -U $PostgresUser -d $PostgresDb *> $null
        if ($LASTEXITCODE -eq 0) {
            Start-Sleep -Seconds 1
            & $docker exec $container pg_isready -U $PostgresUser -d $PostgresDb *> $null
            if ($LASTEXITCODE -eq 0) {
                $pgReady = $true
                break
            }
        }
        Start-Sleep -Seconds 1
    }
    if (-not $pgReady) {
        & $docker ps --filter "name=$container"
        & $docker logs $container --tail 120
        throw "Postgres did not become ready in time"
    }

    $env:DATABASE_URL = "postgres://$PostgresUser`:$PostgresPassword@127.0.0.1:$PostgresPort/$PostgresDb"
    $env:JWT_SECRET = "ci_jwt_secret_change_me"
    $env:DB_MAX_CONNECTIONS = "5"
    $env:DB_BOOTSTRAP_ORG_NAME = "ci-org"
    $env:DB_BOOTSTRAP_ADMIN_EMAIL = "admin@localhost"
    $env:DB_BOOTSTRAP_ADMIN_PASSWORD = "Admin123!Smoke"
    $env:DB_BOOTSTRAP_API_KEY = ""
    $env:DB_BOOTSTRAP_API_KEY_LABEL = "bootstrap-admin"
    $env:EXECUTION_MODE = "paper"
    $env:LIVE_TRADING_ENABLED = "false"
    $env:SHADOW_READS_ENABLED = "true"
    $env:SIM_STARTING_CASH_USD = "50000"
    $env:SIM_FEE_BPS = "2.0"
    $env:OMS_AUDIT_DIR = "var/oms_audit_ci"
    $env:ORG_RATE_LIMIT_PER_MIN = "600"
    $env:PASSWORD_RESET_TOKEN_TTL_SECS = "3600"
    $env:SERVER_ADDR = $ServerAddr
    $env:COINBASE_API_KEY = "ci-dummy-key"
    $env:COINBASE_API_SECRET = "ci-dummy-secret"
    $env:COINBASE_API_PASSPHRASE = "ci-dummy-passphrase"
    $env:COINBASE_BASE_URL = "https://api.coinbase.com"

    Write-Step "Starting trading server"
    $serverProc = Start-Process -FilePath $cargo `
        -ArgumentList @("run", "-p", "trading-server", "--", "serve") `
        -WorkingDirectory $root `
        -RedirectStandardOutput $serverOutLog `
        -RedirectStandardError $serverErrLog `
        -PassThru

    Write-Step "Waiting for trading server readiness"
    $serverReady = $false
    for ($i = 0; $i -lt 120; $i++) {
        try {
            $resp = Invoke-WebRequest -Uri "$BaseUrl/v1/health" -Method GET -UseBasicParsing -TimeoutSec 3
            if ($resp.StatusCode -ge 200 -and $resp.StatusCode -lt 300) {
                $serverReady = $true
                break
            }
        }
        catch {
            if ($serverProc.HasExited) {
                Get-Content $serverOutLog -ErrorAction SilentlyContinue
                Get-Content $serverErrLog -ErrorAction SilentlyContinue
                throw "trading-server exited before becoming ready"
            }
        }
        Start-Sleep -Seconds 1
    }
    if (-not $serverReady) {
        Get-Content $serverOutLog -ErrorAction SilentlyContinue
        Get-Content $serverErrLog -ErrorAction SilentlyContinue
        throw "trading-server did not become ready in time"
    }

    Write-Step "Running auth smoke flow"
    & (Join-Path $root "scripts\smoke_auth.ps1") `
        -BaseUrl $BaseUrl `
        -AdminEmail $env:DB_BOOTSTRAP_ADMIN_EMAIL `
        -AdminPassword $env:DB_BOOTSTRAP_ADMIN_PASSWORD

    Write-Host "[OK] CI auth smoke completed successfully" -ForegroundColor Green
}
finally {
    if ($serverProc -and -not $serverProc.HasExited) {
        Stop-Process -Id $serverProc.Id -Force -ErrorAction SilentlyContinue
    }
    if ($docker) {
        & $docker rm -f $container *> $null
    }
}
