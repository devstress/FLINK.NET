#!/usr/bin/env pwsh
<#
.SYNOPSIS
    Simple stress test following the documented approach in docs/wiki/Stress-Tests-Overview.md

.DESCRIPTION
    This script follows the exact documented process:
    1. Start FlinkDotNetAspire.sln (AppHost)
    2. Wait for all services to be running  
    3. Run produce-1-million-messages.ps1
    4. Run wait-for-flinkjobsimulator-completion.ps1

.PARAMETER MessageCount
    Number of messages to process (default: 1000000).

.PARAMETER SkipCleanup
    If specified, leaves the AppHost running for debugging.

.EXAMPLE
    ./scripts/run-simple-stress-test.ps1
    Runs stress test with 1 million messages.

.EXAMPLE  
    ./scripts/run-simple-stress-test.ps1 -MessageCount 10000
    Runs stress test with 10,000 messages.
#>

param(
    [int]$MessageCount = 1000000,
    [switch]$SkipCleanup
)

$ErrorActionPreference = 'Stop'

Write-Host "=== FLINK.NET Simple Stress Test ===" -ForegroundColor Cyan
Write-Host "Following documented approach from docs/wiki/Stress-Tests-Overview.md" -ForegroundColor White
Write-Host "Started at: $(Get-Date -Format 'yyyy-MM-dd HH:mm:ss') UTC" -ForegroundColor White
Write-Host "Message count: $MessageCount" -ForegroundColor White

# Global variables for cleanup
$global:AppHostPid = $null

function Cleanup-Resources {
    if ($SkipCleanup) {
        Write-Host "⚠️ Skipping cleanup due to -SkipCleanup flag" -ForegroundColor Yellow
        return
    }
    
    Write-Host "`n=== Cleanup Starting ===" -ForegroundColor Yellow
    
    # Stop AppHost
    if ($global:AppHostPid) {
        Write-Host "Stopping AppHost process $global:AppHostPid..." -ForegroundColor Gray
        $process = Get-Process -Id $global:AppHostPid -ErrorAction SilentlyContinue
        if ($process) {
            Stop-Process -Id $global:AppHostPid -Force -ErrorAction SilentlyContinue
            Start-Sleep -Seconds 5
        }
    }
    
    Write-Host "=== Cleanup Complete ===" -ForegroundColor Yellow
}

# Set up cleanup trap
trap {
    Write-Host "`n❌ Script failed with error: $_" -ForegroundColor Red
    Cleanup-Resources
    exit 1
}

# Register cleanup for normal exit
Register-EngineEvent -SourceIdentifier PowerShell.Exiting -Action { Cleanup-Resources }

try {
    # Step 1: Open FlinkDotNetAspire.sln > F5 (Start AppHost)
    Write-Host "`n=== Step 1: Start AppHost ===" -ForegroundColor Yellow
    Write-Host "Starting FlinkDotNetAspire AppHost..." -ForegroundColor White
    
    # Set environment variables for stress testing
    $env:ASPIRE_ALLOW_UNSECURED_TRANSPORT = 'true'
    $env:DOTNET_ENVIRONMENT = 'Development'
    $env:CI = 'true'  # Enable stress test mode
    $env:SIMULATOR_NUM_MESSAGES = $MessageCount.ToString()
    
    # Start AppHost
    $proc = Start-Process -FilePath 'dotnet' -ArgumentList @(
        'run', '--no-build', '--configuration', 'Release',
        '--project', 'FlinkDotNetAspire/FlinkDotNetAspire.AppHost.AppHost/FlinkDotNetAspire.AppHost.AppHost.csproj'
    ) -NoNewWindow -PassThru
    
    $global:AppHostPid = $proc.Id
    Write-Host "AppHost started with PID: $($proc.Id)" -ForegroundColor Green
    
    # Step 2: Wait all the services running
    Write-Host "`n=== Step 2: Wait for Services ===" -ForegroundColor Yellow
    Write-Host "Waiting 60 seconds for all services to start..." -ForegroundColor White
    Start-Sleep -Seconds 60
    
    # Discover ports
    Write-Host "Discovering Aspire container ports..." -ForegroundColor White
    & ./scripts/discover-aspire-ports.ps1
    if ($LASTEXITCODE -ne 0) {
        throw "Failed to discover Aspire container ports"
    }
    
    # Verify environment variables are set
    if (-not $env:DOTNET_REDIS_URL -or -not $env:DOTNET_KAFKA_BOOTSTRAP_SERVERS) {
        throw "Required environment variables not set after port discovery"
    }
    
    Write-Host "✅ Services are running:" -ForegroundColor Green
    Write-Host "  Redis: $env:DOTNET_REDIS_URL" -ForegroundColor Gray
    Write-Host "  Kafka: $env:DOTNET_KAFKA_BOOTSTRAP_SERVERS" -ForegroundColor Gray
    
    # Step 3: Run produce-1-million-messages.ps1
    Write-Host "`n=== Step 3: Produce Messages ===" -ForegroundColor Yellow
    Write-Host "cd scripts > Run .\produce-1-million-messages.ps1" -ForegroundColor White
    
    Push-Location "scripts"
    try {
        & "./produce-1-million-messages.ps1" -MessageCount $MessageCount
        if ($LASTEXITCODE -ne 0) {
            throw "Message producer failed with exit code: $LASTEXITCODE"
        }
        Write-Host "✅ Message production completed" -ForegroundColor Green
    }
    finally {
        Pop-Location
    }
    
    # Step 4: Run wait-for-flinkjobsimulator-completion.ps1
    Write-Host "`n=== Step 4: Wait for Completion ===" -ForegroundColor Yellow
    Write-Host "Run wait-for-flinkjobsimulator-completion.ps1" -ForegroundColor White
    
    Push-Location "scripts"
    try {
        & "./wait-for-flinkjobsimulator-completion.ps1" -ExpectedMessages $MessageCount
        if ($LASTEXITCODE -ne 0) {
            throw "FlinkJobSimulator completion check failed with exit code: $LASTEXITCODE"
        }
        Write-Host "✅ FlinkJobSimulator completed successfully!" -ForegroundColor Green
    }
    finally {
        Pop-Location
    }
    
    # Success!
    Write-Host "`n=== STRESS TEST RESULT: ✅ PASSED ===" -ForegroundColor Green
    Write-Host "All $MessageCount messages processed successfully following documented approach!" -ForegroundColor Green
    Write-Host "Completed at: $(Get-Date -Format 'yyyy-MM-dd HH:mm:ss') UTC" -ForegroundColor White
    
} finally {
    Cleanup-Resources
}

Write-Host "`n=== Simple Stress Test Complete ===" -ForegroundColor Cyan