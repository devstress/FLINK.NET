#!/usr/bin/env pwsh
# Stress Tests Workflow - mirrors .github/workflows/stress-tests.yml

Write-Host "=== STRESS TESTS WORKFLOW STARTED ==="
$startTime = Get-Date

try {
    # Set environment variables
    $env:SIMULATOR_NUM_MESSAGES = if ($env:SIMULATOR_NUM_MESSAGES) { $env:SIMULATOR_NUM_MESSAGES } else { "1000000" }
    $env:ASPIRE_ALLOW_UNSECURED_TRANSPORT = "true"

    Write-Host "Stress Tests Configuration:"
    Write-Host "  SIMULATOR_NUM_MESSAGES: $env:SIMULATOR_NUM_MESSAGES"
    Write-Host "  MAX_ALLOWED_TIME_MS: $env:MAX_ALLOWED_TIME_MS"

    # Install .NET Aspire Workload
    Write-Host "Installing .NET Aspire Workload..."
    dotnet workload install aspire

    # Restore .NET Workloads
    dotnet workload restore FlinkDotNet/FlinkDotNet.sln
    dotnet workload restore FlinkDotNetAspire/FlinkDotNetAspire.sln
    dotnet workload restore FlinkDotNet.WebUI/FlinkDotNet.WebUI.sln

    # Build Solutions
    Write-Host "Building FlinkDotNet solution..."
    Push-Location FlinkDotNet
    dotnet build FlinkDotNet.sln --configuration Release
    if ($LASTEXITCODE -ne 0) { 
        Pop-Location
        throw "Build failed for FlinkDotNet solution" 
    }
    Pop-Location
    
    Write-Host "Building FlinkDotNetAspire solution..."
    Push-Location FlinkDotNetAspire
    dotnet build FlinkDotNetAspire.sln --configuration Release
    if ($LASTEXITCODE -ne 0) { 
        Pop-Location
        throw "Build failed for FlinkDotNetAspire solution" 
    }
    Pop-Location

    # Start Aspire AppHost
    Write-Host "Starting Aspire AppHost..."
    $processArgs = @(
        'run',
        '--no-build',
        '--configuration', 'Release',
        '--project', 'FlinkDotNetAspire/FlinkDotNetAspire.AppHost.AppHost/FlinkDotNetAspire.AppHost.AppHost.csproj'
    )
    
    $proc = Start-Process -FilePath 'dotnet' -ArgumentList $processArgs -RedirectStandardOutput 'apphost.out.log' -RedirectStandardError 'apphost.err.log' -NoNewWindow -PassThru
    $proc.Id | Out-File apphost.pid -Encoding utf8
    Write-Host "Started AppHost with PID: $($proc.Id)"
    
    # Wait for container initialization
    Write-Host "Waiting 60 seconds for Redis/Kafka container initialization..."
    Start-Sleep -Seconds 60

    # Discover Aspire Container Ports
    Write-Host "Discovering actual Aspire container ports..."
    & ./scripts/discover-aspire-ports.ps1

    # Health Check Loop
    Write-Host "Starting health checks..."
    $maxAttempts = 5
    $delaySeconds = 10
    $verifierDll = "./FlinkDotNetAspire/IntegrationTestVerifier/bin/Release/net8.0/FlinkDotNet.IntegrationTestVerifier.dll"

    for ($attempt = 1; $attempt -le $maxAttempts; $attempt++) {
        Write-Host "Health check attempt $attempt/$maxAttempts"
        & dotnet $verifierDll --health-check
        $healthExitCode = $LASTEXITCODE
        
        if ($healthExitCode -eq 0) {
            Write-Host "✅ Health check PASSED on attempt $attempt" -ForegroundColor Green
            break
        }
        
        Write-Host "❌ Health check FAILED on attempt $attempt" -ForegroundColor Red
        
        if ($attempt -lt $maxAttempts) {
            Write-Host "Waiting $delaySeconds seconds before retry..."
            Start-Sleep -Seconds $delaySeconds
        } else {
            throw "Max health check attempts ($maxAttempts) reached. Health checks failed."
        }
    }

    # Run Verification Tests
    Write-Host "Running verification tests..."
    & dotnet $verifierDll
    if ($LASTEXITCODE -ne 0) {
        throw "Verification tests failed"
    }

    Write-Host "Verification tests PASSED."
    $duration = (Get-Date) - $startTime
    Write-Host "=== STRESS TESTS WORKFLOW COMPLETED in $($duration.TotalMinutes.ToString('F1')) minutes ==="
    exit 0

} catch {
    Write-Host "❌ Stress Tests Workflow Failed: $_" -ForegroundColor Red
    $duration = (Get-Date) - $startTime
    Write-Host "=== STRESS TESTS WORKFLOW FAILED after $($duration.TotalMinutes.ToString('F1')) minutes ==="
    exit 1
} finally {
    # Stop AppHost
    Write-Host "Stopping AppHost..."
    if (Test-Path apphost.pid) {
        $apphostPid = Get-Content apphost.pid
        $process = Get-Process -Id $apphostPid -ErrorAction SilentlyContinue
        if ($process) {
            Stop-Process -Id $apphostPid -Force -ErrorAction SilentlyContinue
        }
    }
}
