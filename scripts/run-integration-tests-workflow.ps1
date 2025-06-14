#!/usr/bin/env pwsh
# Integration Tests Workflow - mirrors .github/workflows/integration-tests.yml

Write-Host "=== INTEGRATION TESTS WORKFLOW STARTED ==="
$startTime = Get-Date

try {
    $env:ASPIRE_ALLOW_UNSECURED_TRANSPORT = "true"

    # Install .NET Aspire Workload
    Write-Host "Installing .NET Aspire Workload..."
    dotnet workload install aspire

    # Restore .NET Workloads
    dotnet workload restore FlinkDotNet/FlinkDotNet.sln
    dotnet workload restore FlinkDotNetAspire/FlinkDotNetAspire.sln

    # Build Solutions
    Write-Host "Building FlinkDotNet solution..."
    dotnet build FlinkDotNet/FlinkDotNet.sln --configuration Release
    if ($LASTEXITCODE -ne 0) { throw "Build failed for FlinkDotNet solution" }
    
    Write-Host "Building FlinkDotNetAspire solution..."
    dotnet build FlinkDotNetAspire/FlinkDotNetAspire.sln --configuration Release
    if ($LASTEXITCODE -ne 0) { throw "Build failed for FlinkDotNetAspire solution" }

    # Run Integration Tests
    Write-Host "Running Aspire integration tests..."
    dotnet test FlinkDotNetAspire/FlinkDotNetAspire.IntegrationTests/FlinkDotNetAspire.IntegrationTests.csproj --configuration Release --logger trx --collect:"XPlat Code Coverage"
    if ($LASTEXITCODE -ne 0) { throw "Integration tests failed" }

    Write-Host "Integration tests completed successfully."
    $duration = (Get-Date) - $startTime
    Write-Host "=== INTEGRATION TESTS WORKFLOW COMPLETED in $($duration.TotalMinutes.ToString('F1')) minutes ==="
    exit 0

} catch {
    Write-Host "❌ Integration Tests Workflow Failed: $_" -ForegroundColor Red
    $duration = (Get-Date) - $startTime
    Write-Host "=== INTEGRATION TESTS WORKFLOW FAILED after $($duration.TotalMinutes.ToString('F1')) minutes ==="
    exit 1
}
