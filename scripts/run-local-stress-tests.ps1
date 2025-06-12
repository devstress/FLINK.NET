#!/usr/bin/env pwsh
<#
.SYNOPSIS
    Local stress test verification that matches the GitHub Actions workflow exactly.

.DESCRIPTION
    This script replicates the stress test workflow logic to ensure local verification
    matches what runs in CI. It performs the complete stress test cycle:
    1. Port discovery and environment setup
    2. Solution builds with clean state
    3. AppHost startup with proper logging
    4. Health checks using IntegrationTestVerifier
    5. Full verification tests with performance validation
    6. Proper cleanup and reporting

.PARAMETER SkipCleanup
    If specified, leaves the AppHost running for debugging purposes.

.PARAMETER MessageCount
    Number of messages to process (default: 100 for local, 1000000 for CI simulation).

.PARAMETER MaxTimeMs
    Maximum allowed processing time in milliseconds (default: 10000).

.EXAMPLE
    ./scripts/run-local-stress-tests.ps1
    Runs local stress tests with default settings.

.EXAMPLE
    ./scripts/run-local-stress-tests.ps1 -MessageCount 1000000 -MaxTimeMs 10000
    Simulates CI environment with full message load.
#>

param(
    [switch]$SkipCleanup,
    [int]$MessageCount = 100,
    [int]$MaxTimeMs = 10000
)

$ErrorActionPreference = 'Stop'
$VerbosePreference = 'Continue'

Write-Host "=== FlinkDotNet Local Stress Test Verification ===" -ForegroundColor Cyan
Write-Host "Started at: $(Get-Date -Format 'yyyy-MM-dd HH:mm:ss') UTC" -ForegroundColor White
Write-Host "Parameters: MessageCount=$MessageCount, MaxTimeMs=$MaxTimeMs, SkipCleanup=$SkipCleanup" -ForegroundColor White

# Global variables for cleanup
$global:AppHostPid = $null
$global:BackgroundJobs = @()

function Cleanup-Resources {
    param([bool]$Force = $false)
    
    if ($SkipCleanup -and -not $Force) {
        Write-Host "⚠️ Skipping cleanup due to -SkipCleanup flag" -ForegroundColor Yellow
        return
    }
    
    Write-Host "`n=== Cleanup Starting ===" -ForegroundColor Yellow
    
    # Stop background jobs
    foreach ($job in $global:BackgroundJobs) {
        if ($job -and (Get-Job -Id $job.Id -ErrorAction SilentlyContinue)) {
            Write-Host "Stopping background job $($job.Id)..." -ForegroundColor Gray
            Stop-Job -Id $job.Id -ErrorAction SilentlyContinue
            Remove-Job -Id $job.Id -ErrorAction SilentlyContinue
        }
    }
    
    # Stop AppHost
    if ($global:AppHostPid) {
        Write-Host "Stopping AppHost process $global:AppHostPid..." -ForegroundColor Gray
        $process = Get-Process -Id $global:AppHostPid -ErrorAction SilentlyContinue
        if ($process) {
            Stop-Process -Id $global:AppHostPid -Force -ErrorAction SilentlyContinue
            Start-Sleep -Seconds 2
        }
    }
    
    # Clean up temp files
    $tempFiles = @('apphost.pid', 'apphost.out.log', 'apphost.err.log', 'apphost.output.job', 'apphost.error.job')
    foreach ($file in $tempFiles) {
        if (Test-Path $file) {
            Remove-Item $file -Force -ErrorAction SilentlyContinue
        }
    }
    
    Write-Host "=== Cleanup Complete ===" -ForegroundColor Yellow
}

# Set up cleanup trap
trap {
    Write-Host "`n❌ Script failed with error: $_" -ForegroundColor Red
    Cleanup-Resources -Force $true
    exit 1
}

# Register cleanup for normal exit
Register-EngineEvent -SourceIdentifier PowerShell.Exiting -Action { Cleanup-Resources -Force $true }

try {
    # Step 1: Environment Setup (matches workflow)
    Write-Host "`n=== Step 1: Environment Setup ===" -ForegroundColor Yellow
    
    # Set environment variables to match workflow
    $env:SIMULATOR_NUM_MESSAGES = $MessageCount.ToString()
    $env:MAX_ALLOWED_TIME_MS = $MaxTimeMs.ToString()
    $env:ASPIRE_ALLOW_UNSECURED_TRANSPORT = 'true'
    $env:DOTNET_ENVIRONMENT = 'Development'
    
    Write-Host "Environment variables set:" -ForegroundColor Gray
    Write-Host "  SIMULATOR_NUM_MESSAGES: $env:SIMULATOR_NUM_MESSAGES" -ForegroundColor Gray
    Write-Host "  MAX_ALLOWED_TIME_MS: $env:MAX_ALLOWED_TIME_MS" -ForegroundColor Gray
    Write-Host "  ASPIRE_ALLOW_UNSECURED_TRANSPORT: $env:ASPIRE_ALLOW_UNSECURED_TRANSPORT" -ForegroundColor Gray
    Write-Host "  DOTNET_ENVIRONMENT: $env:DOTNET_ENVIRONMENT" -ForegroundColor Gray

    # Step 2: Port Discovery (matches workflow)
    Write-Host "`n=== Step 2: Port Discovery ===" -ForegroundColor Yellow
    Write-Host "Discovering available ports for Redis and Kafka services..." -ForegroundColor White
    
    & ./scripts/find-available-ports.ps1
    if ($LASTEXITCODE -ne 0) {
        throw "Port discovery failed with exit code $LASTEXITCODE"
    }
    
    Write-Host "Port discovery results:" -ForegroundColor Gray
    Write-Host "  DOTNET_REDIS_URL: $env:DOTNET_REDIS_URL" -ForegroundColor Gray
    Write-Host "  DOTNET_KAFKA_BOOTSTRAP_SERVERS: $env:DOTNET_KAFKA_BOOTSTRAP_SERVERS" -ForegroundColor Gray

    # Step 3: Build Solutions (matches workflow) 
    Write-Host "`n=== Step 3: Build Solutions ===" -ForegroundColor Yellow
    
    Write-Host "Building FlinkDotNet solution (dependency)..." -ForegroundColor White
    dotnet build FlinkDotNet/FlinkDotNet.sln --configuration Release --verbosity minimal
    if ($LASTEXITCODE -ne 0) {
        throw "FlinkDotNet/FlinkDotNet.sln build failed with exit code $LASTEXITCODE"
    }
    
    Write-Host "Building FlinkDotNetAspire solution..." -ForegroundColor White
    dotnet build FlinkDotNetAspire/FlinkDotNetAspire.sln --configuration Release --verbosity minimal
    if ($LASTEXITCODE -ne 0) {
        throw "FlinkDotNetAspire/FlinkDotNetAspire.sln build failed with exit code $LASTEXITCODE"
    }
    
    Write-Host "✅ All solutions built successfully" -ForegroundColor Green

    # Step 4: Start Aspire AppHost (matches workflow exactly)
    Write-Host "`n=== Step 4: Start Aspire AppHost ===" -ForegroundColor Yellow
    
    # Create log files
    $outLogPath = "apphost.out.log"
    $errLogPath = "apphost.err.log"
    
    Write-Host "Starting AppHost with output logging to $outLogPath and $errLogPath" -ForegroundColor White
    
    # Use Start-Process with file redirection (matches workflow)
    $processArgs = @(
        'run',
        '--no-build',
        '--configuration', 'Release',
        '--project', 'FlinkDotNetAspire/FlinkDotNetAspire.AppHost.AppHost/FlinkDotNetAspire.AppHost.AppHost.csproj'
    )
    
    # Start the process with output redirection
    $proc = Start-Process -FilePath 'dotnet' -ArgumentList $processArgs -RedirectStandardOutput $outLogPath -RedirectStandardError $errLogPath -NoNewWindow -PassThru
    $global:AppHostPid = $proc.Id
    $proc.Id | Out-File apphost.pid -Encoding utf8
    
    Write-Host "Started AppHost with PID: $($proc.Id)" -ForegroundColor Green
    
    # Start background monitoring jobs (simplified version of workflow)
    $outputJob = Start-Job -ScriptBlock {
        param($logPath)
        $lastSize = 0
        $attempts = 0
        $maxAttempts = 300  # 5 minutes of checking
        
        while ($attempts -lt $maxAttempts) {
            try {
                if (Test-Path $logPath) {
                    $currentSize = (Get-Item $logPath).Length
                    if ($currentSize -gt $lastSize) {
                        $newContent = Get-Content $logPath -Tail ($currentSize - $lastSize) -Encoding utf8 -ErrorAction SilentlyContinue
                        if ($newContent) {
                            foreach ($line in $newContent) {
                                if ($line -and $line.Trim()) {
                                    Write-Host "[APPHOST-OUT] $line"
                                }
                            }
                        }
                        $lastSize = $currentSize
                    }
                }
                Start-Sleep -Seconds 1
                $attempts++
            } catch {
                Start-Sleep -Seconds 1
                $attempts++
            }
        }
    } -ArgumentList $outLogPath
    
    $global:BackgroundJobs += $outputJob
    Write-Host "Background monitor job started: $($outputJob.Id)" -ForegroundColor Gray
    
    Write-Host "AppHost started, waiting 30 seconds for initialization..." -ForegroundColor White
    Start-Sleep -Seconds 30

    # Step 5: Health Check (matches workflow)
    Write-Host "`n=== Step 5: Health Check ===" -ForegroundColor Yellow
    
    $maxAttempts = 2
    $delaySeconds = 5
    $verifierDll = "./FlinkDotNetAspire/IntegrationTestVerifier/bin/Release/net8.0/FlinkDotNet.IntegrationTestVerifier.dll"
    
    for ($attempt = 1; $attempt -le $maxAttempts; $attempt++) {
        Write-Host "Health check attempt $attempt/$maxAttempts..." -ForegroundColor White
        
        dotnet $verifierDll --health-check
        $healthExitCode = $LASTEXITCODE
        
        if ($healthExitCode -eq 0) {
            Write-Host "✅ Health check PASSED" -ForegroundColor Green
            break
        }
        
        Write-Host "❌ Health check FAILED. Waiting $delaySeconds seconds before retry..." -ForegroundColor Red
        Start-Sleep -Seconds $delaySeconds
        
        if ($attempt -eq $maxAttempts) {
            throw "Max health check attempts reached. Health checks failed."
        }
    }

    # Step 6: Verification Tests (matches workflow)
    Write-Host "`n=== Step 6: Verification Tests ===" -ForegroundColor Yellow
    
    # Check if AppHost is still running
    if (Test-Path apphost.pid) {
        $apphostPid = Get-Content apphost.pid
        $process = Get-Process -Id $apphostPid -ErrorAction SilentlyContinue
        if (-not $process) {
            throw "ERROR: AppHost process (PID $apphostPid) is not running!"
        }
        Write-Host "✅ AppHost process (PID $apphostPid) is running" -ForegroundColor Green
    } else {
        throw "ERROR: AppHost PID file not found!"
    }
    
    Write-Host "Running verification tests with SIMULATOR_NUM_MESSAGES=$MessageCount..." -ForegroundColor White
    Write-Host "Environment check:" -ForegroundColor Gray
    Write-Host "  DOTNET_REDIS_URL: $env:DOTNET_REDIS_URL" -ForegroundColor Gray
    Write-Host "  DOTNET_KAFKA_BOOTSTRAP_SERVERS: $env:DOTNET_KAFKA_BOOTSTRAP_SERVERS" -ForegroundColor Gray
    
    # Run the actual verification (matches workflow exactly)
    dotnet $verifierDll
    $verificationExitCode = $LASTEXITCODE
    
    if ($verificationExitCode -ne 0) {
        throw "Verification tests FAILED with exit code $verificationExitCode"
    }
    
    Write-Host "✅ Verification tests PASSED" -ForegroundColor Green

    # Step 7: Final Results
    Write-Host "`n=== Step 7: Final Results ===" -ForegroundColor Yellow
    Write-Host "✅ Local stress test verification PASSED" -ForegroundColor Green
    Write-Host "✅ All components working correctly:" -ForegroundColor Green
    Write-Host "  ✅ Port discovery successful" -ForegroundColor Green
    Write-Host "  ✅ Solutions built successfully" -ForegroundColor Green
    Write-Host "  ✅ AppHost started successfully" -ForegroundColor Green
    Write-Host "  ✅ Health checks passed" -ForegroundColor Green
    Write-Host "  ✅ Verification tests passed" -ForegroundColor Green
    Write-Host "`nLocal verification matches workflow requirements!" -ForegroundColor Green
    
} finally {
    # Always cleanup unless explicitly skipped
    Cleanup-Resources
}

Write-Host "`n=== Local Stress Test Verification Complete ===" -ForegroundColor Cyan
Write-Host "Completed at: $(Get-Date -Format 'yyyy-MM-dd HH:mm:ss') UTC" -ForegroundColor White
Write-Host "Result: ✅ SUCCESS - Local verification matches CI workflow" -ForegroundColor Green