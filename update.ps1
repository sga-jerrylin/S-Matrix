$ErrorActionPreference = 'Stop'

$scriptPath = Split-Path -Parent $MyInvocation.MyCommand.Path
Set-Location $scriptPath

$CanonicalBackendHost = "smatrix-be"
$CanonicalBackendPort = "9050"

function Wait-FeMysqlReady {
    param(
        [int]$Attempts = 60,
        [int]$SleepSeconds = 2
    )

    for ($i = 1; $i -le $Attempts; $i++) {
        $exitCode = 1
        try {
            docker compose exec -T smatrix-fe mysql -hsmatrix-fe -P9030 -uroot -e "SELECT 1;" 2>$null | Out-Null
            $exitCode = $LASTEXITCODE
        } catch {
            $exitCode = 1
        }
        if ($exitCode -eq 0) {
            return
        }
        Start-Sleep -Seconds $SleepSeconds
    }

    Write-Host "Timed out waiting for FE MySQL endpoint readiness (smatrix-fe:9030 inside smatrix-fe)" -ForegroundColor Red
    exit 1
}

function Wait-ContainerHealth {
    param(
        [Parameter(Mandatory = $true)][string]$Service,
        [int]$Attempts = 120,
        [int]$SleepSeconds = 5
    )

    for ($i = 1; $i -le $Attempts; $i++) {
        $status = docker inspect --format='{{if .State.Health}}{{.State.Health.Status}}{{else}}unknown{{end}}' $Service 2>$null
        if ($status -eq 'healthy') {
            return
        }
        if ($status -eq 'unhealthy') {
            Write-Host "Service is unhealthy: $Service" -ForegroundColor Red
            docker compose logs --no-color $Service
            exit 1
        }
        Start-Sleep -Seconds $SleepSeconds
    }

    Write-Host "Timed out waiting for healthy service: $Service" -ForegroundColor Red
    exit 1
}

function Get-BackendsRaw {
    try {
        $output = docker compose exec -T smatrix-fe mysql -hsmatrix-fe -P9030 -uroot --batch --skip-column-names -e "SHOW BACKENDS;" 2>$null
        if ($LASTEXITCODE -ne 0) {
            return $null
        }
        return $output
    } catch {
        return $null
    }
}

function Write-RecoveryHint {
    Write-Host "Recovery steps:" -ForegroundColor Yellow
    Write-Host "  1) .\init.ps1 -Reset -Yes" -ForegroundColor Yellow
    Write-Host "  2) .\init.ps1" -ForegroundColor Yellow
}

function Test-SingleCanonicalBackend {
    param([string[]]$BackendRows)

    $rows = @($BackendRows | Where-Object { $_ -and $_.Trim() })
    if ($rows.Count -gt 1) {
        Write-Host "Detected duplicate Doris backends. Expected exactly one backend ($CanonicalBackendHost`:$CanonicalBackendPort)." -ForegroundColor Red
        $rows | ForEach-Object { Write-Host $_ -ForegroundColor Red }
        Write-RecoveryHint
        return 1
    }

    if ($rows.Count -eq 0) {
        return 2
    }

    $columns = $rows[0] -split "`t"
    if ($columns.Count -lt 10) {
        Write-Host "Unable to parse SHOW BACKENDS output: $($rows[0])" -ForegroundColor Red
        Write-RecoveryHint
        return 1
    }

    $backendHost = $columns[1].Trim()
    $heartbeatPort = $columns[2].Trim()
    $alive = $columns[9].Trim().ToLowerInvariant()
    if ($backendHost -ne $CanonicalBackendHost -or $heartbeatPort -ne $CanonicalBackendPort) {
        Write-Host "Detected stale Doris backend: $backendHost`:$heartbeatPort. Expected $CanonicalBackendHost`:$CanonicalBackendPort." -ForegroundColor Red
        $rows | ForEach-Object { Write-Host $_ -ForegroundColor Red }
        Write-RecoveryHint
        return 1
    }
    if ($alive -ne "true") {
        Write-Host "Canonical Doris backend is not alive: $backendHost`:$heartbeatPort (Alive=$alive)." -ForegroundColor Red
        $rows | ForEach-Object { Write-Host $_ -ForegroundColor Red }
        Write-RecoveryHint
        return 1
    }

    return 0
}

function Ensure-BackendRegistered {
    param(
        [int]$Attempts = 20,
        [int]$SleepSeconds = 2
    )

    for ($i = 1; $i -le $Attempts; $i++) {
        $backendRaw = Get-BackendsRaw
        if ($null -eq $backendRaw) {
            Start-Sleep -Seconds $SleepSeconds
            continue
        }

        $backendRows = @($backendRaw -split "`r?`n" | Where-Object { $_.Trim() })
        $status = Test-SingleCanonicalBackend -BackendRows $backendRows
        if ($status -eq 0) {
            Write-Host "Backend already canonical: $CanonicalBackendHost`:$CanonicalBackendPort" -ForegroundColor Green
            return
        }
        if ($status -eq 1) {
            exit 1
        }

        $addExitCode = 1
        try {
            docker compose exec -T smatrix-fe mysql -hsmatrix-fe -P9030 -uroot -e "ALTER SYSTEM ADD BACKEND '$CanonicalBackendHost`:$CanonicalBackendPort';" 2>$null | Out-Null
            $addExitCode = $LASTEXITCODE
        } catch {
            $addExitCode = 1
        }

        if ($addExitCode -eq 0) {
            Write-Host "Registered $CanonicalBackendHost`:$CanonicalBackendPort" -ForegroundColor Green
            Start-Sleep -Seconds $SleepSeconds
            $backendRaw = Get-BackendsRaw
            if ($null -eq $backendRaw) {
                Write-Host "Backend registration succeeded but backend verification could not be fetched." -ForegroundColor Red
                Write-RecoveryHint
                exit 1
            }
            $backendRows = @($backendRaw -split "`r?`n" | Where-Object { $_.Trim() })
            $status = Test-SingleCanonicalBackend -BackendRows $backendRows
            if ($status -eq 0) {
                return
            }
            exit 1
        }

        Start-Sleep -Seconds $SleepSeconds
    }

    Write-Host "Failed to ensure backend $CanonicalBackendHost`:$CanonicalBackendPort within retry window" -ForegroundColor Red
    exit 1
}

Write-Host "========================================" -ForegroundColor Cyan
Write-Host "  S-Matrix runtime update" -ForegroundColor Cyan
Write-Host "========================================" -ForegroundColor Cyan
Write-Host ""

Write-Host "[1/4] Pulling latest code..." -ForegroundColor Cyan
try {
    git pull --ff-only
} catch {
    Write-Host "[warn] git pull failed, continuing with the current checkout" -ForegroundColor Yellow
}

Write-Host ""
Write-Host "[2/4] Rebuilding and starting services..." -ForegroundColor Cyan
docker compose up -d --build --remove-orphans smatrix-fe smatrix-be
$coreUpExitCode = $LASTEXITCODE
if ($coreUpExitCode -ne 0) {
    Write-Host "docker compose up for FE/BE returned exit code $coreUpExitCode; continuing with explicit health checks." -ForegroundColor Yellow
}

Write-Host ""
Write-Host "[3/4] Enforcing Doris single-backend canonical state..." -ForegroundColor Cyan
Wait-ContainerHealth -Service smatrix-fe
Wait-ContainerHealth -Service smatrix-be
Wait-FeMysqlReady
Ensure-BackendRegistered

Write-Host ""
Write-Host "[3.5/4] Starting API and frontend after Doris is stable..." -ForegroundColor Cyan
docker compose up -d smatrix-api smatrix-frontend
$appUpExitCode = $LASTEXITCODE
if ($appUpExitCode -ne 0) {
    Write-Host "docker compose up for API/frontend failed with exit code $appUpExitCode" -ForegroundColor Red
    exit $appUpExitCode
}
Wait-ContainerHealth -Service smatrix-api
Wait-ContainerHealth -Service smatrix-frontend

Write-Host ""
Write-Host "[4/4] Running runtime smoke checks..." -ForegroundColor Cyan
python doris-api/dc.py smoke

Write-Host ""
Write-Host "========================================" -ForegroundColor Cyan
Write-Host "  Update complete" -ForegroundColor Green
Write-Host "========================================" -ForegroundColor Cyan
Write-Host ""
docker compose ps
Write-Host ""
Write-Host "Access URLs:" -ForegroundColor White
Write-Host "  - Web UI:    http://localhost:35173" -ForegroundColor Cyan
Write-Host "  - API:       http://localhost:38018" -ForegroundColor Cyan
Write-Host "  - Health:    http://localhost:38018/api/health" -ForegroundColor Cyan
