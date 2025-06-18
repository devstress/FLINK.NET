#!/usr/bin/env pwsh

<#
.SYNOPSIS
    Runs verification tests for FlinkJobSimulator message processing

.DESCRIPTION
    This script runs the IntegrationTestVerifier to validate that FlinkJobSimulator
    correctly processed the expected number of messages and that all systems are
    functioning properly.

.PARAMETER ExpectedMessages
    The number of messages expected to be processed (default: from SIMULATOR_NUM_MESSAGES env var or 1000000)

.PARAMETER MaxAllowedTimeMs
    Maximum allowed processing time in milliseconds (default: from MAX_ALLOWED_TIME_MS env var or 300000)

.PARAMETER VerifierDllPath
    Path to the IntegrationTestVerifier DLL (default: "./FlinkDotNetAspire/IntegrationTestVerifier/bin/Release/net8.0/FlinkDotNet.IntegrationTestVerifier.dll")

.EXAMPLE
    ./run-verification-tests.ps1 -ExpectedMessages 1000000 -MaxAllowedTimeMs 300000
#>

param(
    [int]$ExpectedMessages = $env:SIMULATOR_NUM_MESSAGES -as [int] ?? 1000000,
    [int]$MaxAllowedTimeMs = $env:MAX_ALLOWED_TIME_MS -as [int] ?? 300000,
    [string]$VerifierDllPath = "./FlinkDotNetAspire/IntegrationTestVerifier/bin/Release/net8.0/FlinkDotNet.IntegrationTestVerifier.dll"
)

Write-Host "🔍 === RUNNING VERIFICATION TESTS ==="

# Check if AppHost is still running
if (Test-Path apphost.pid) {
    $apphostPid = Get-Content apphost.pid
    $process = Get-Process -Id $apphostPid -ErrorAction SilentlyContinue
    if (-not $process) {
        Write-Host "ERROR: AppHost process (PID $apphostPid) is not running!"
        exit 1
    }
    Write-Host "AppHost process (PID $apphostPid) is running."
} else {
    Write-Host "ERROR: AppHost PID file not found!"
    exit 1
}

Write-Host "Running verification tests with SIMULATOR_NUM_MESSAGES=$ExpectedMessages..."
Write-Host "Environment variables:"
Write-Host "  DOTNET_REDIS_URL: $env:DOTNET_REDIS_URL"
Write-Host "  DOTNET_KAFKA_BOOTSTRAP_SERVERS: $env:DOTNET_KAFKA_BOOTSTRAP_SERVERS"
Write-Host "  SIMULATOR_NUM_MESSAGES: $env:SIMULATOR_NUM_MESSAGES"
Write-Host "  SIMULATOR_REDIS_KEY_GLOBAL_SEQUENCE: $env:SIMULATOR_REDIS_KEY_GLOBAL_SEQUENCE"
Write-Host "  SIMULATOR_REDIS_KEY_SINK_COUNTER: $env:SIMULATOR_REDIS_KEY_SINK_COUNTER"
Write-Host "  SIMULATOR_KAFKA_TOPIC: $env:SIMULATOR_KAFKA_TOPIC"
Write-Host "  MAX_ALLOWED_TIME_MS: $env:MAX_ALLOWED_TIME_MS"

# Check if verifier DLL exists
if (-not (Test-Path $VerifierDllPath)) {
    Write-Host "ERROR: Verifier DLL not found at: $VerifierDllPath"
    Write-Host "Please ensure the IntegrationTestVerifier project has been built."
    exit 1
}

Write-Host "Using verifier DLL: $VerifierDllPath"

# Run the verification tests
Write-Host "🚀 Starting verification tests..."
& dotnet $VerifierDllPath

$exitCode = $LASTEXITCODE
if ($exitCode -ne 0) {
    Write-Host "❌ Verification tests FAILED with exit code: $exitCode" -ForegroundColor Red
    exit $exitCode
}

Write-Host "✅ Verification tests PASSED." -ForegroundColor Green