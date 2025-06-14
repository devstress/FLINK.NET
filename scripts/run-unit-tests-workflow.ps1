#!/usr/bin/env pwsh
# Unit Tests Workflow - mirrors .github/workflows/unit-tests.yml

Write-Host "=== UNIT TESTS WORKFLOW STARTED ==="
$startTime = Get-Date

try {
    # Navigate to repository root (handle both root and scripts folder execution)
    $rootPath = if (Test-Path "FlinkDotNet") { Get-Location } else { Split-Path -Parent (Get-Location) }
    Set-Location $rootPath
    Write-Host "Working directory: $rootPath"

    # Set up .NET Aspire Workload
    Write-Host "Installing .NET Aspire Workload..."
    dotnet workload install aspire
    if ($LASTEXITCODE -ne 0) { throw "Failed to install Aspire workload" }

    # Restore .NET Workloads for Solutions
    Write-Host "Restoring workloads for FlinkDotNet.sln..."
    dotnet workload restore FlinkDotNet/FlinkDotNet.sln
    if ($LASTEXITCODE -ne 0) { throw "Failed to restore FlinkDotNet workloads" }

    Write-Host "Restoring workloads for FlinkDotNetAspire.sln..."
    dotnet workload restore FlinkDotNetAspire/FlinkDotNetAspire.sln
    if ($LASTEXITCODE -ne 0) { throw "Failed to restore FlinkDotNetAspire workloads" }

    Write-Host "Restoring workloads for FlinkDotNet.WebUI.sln..."
    dotnet workload restore FlinkDotNet.WebUI/FlinkDotNet.WebUI.sln
    if ($LASTEXITCODE -ne 0) { throw "Failed to restore FlinkDotNet.WebUI workloads" }

    # Run tests for FlinkDotNet solution
    Write-Host "Running tests for FlinkDotNet/FlinkDotNet.sln with coverage collection..."
    Push-Location FlinkDotNet
    dotnet test FlinkDotNet.sln --configuration Release --logger "trx" --results-directory "TestResults" --collect:"XPlat Code Coverage" --settings ../coverlet.runsettings
    if ($LASTEXITCODE -ne 0) { 
        Pop-Location
        throw "Tests failed for FlinkDotNet solution" 
    }
    Pop-Location

    # Convert coverage to multiple formats
    Write-Host "Installing ReportGenerator..."
    dotnet tool install --global dotnet-reportgenerator-globaltool --version 5.2.0 2>$null
    
    Write-Host "Converting coverage files to multiple formats..."
    Get-ChildItem -Path "./FlinkDotNet/TestResults" -Recurse -Filter "coverage.cobertura.xml" | ForEach-Object {
        $coberturaFile = $_.FullName
        $targetDir = $_.DirectoryName
        Write-Host "Processing coverage file: $coberturaFile"
        
        # Convert to OpenCover format for SonarCloud
        $openCoverFile = Join-Path $targetDir "coverage.opencover.xml"
        Write-Host "Converting to OpenCover format: $openCoverFile"
        reportgenerator -reports:"$coberturaFile" -targetdir:"$targetDir" -reporttypes:"OpenCover" -verbosity:Info
        
        # Generate HTML report for debugging
        $htmlReportDir = Join-Path $targetDir "coverage-html"
        Write-Host "Generating HTML coverage report: $htmlReportDir"
        reportgenerator -reports:"$coberturaFile" -targetdir:"$htmlReportDir" -reporttypes:"Html" -verbosity:Info
    }

    # Build FlinkDotNetAspire solution (no tests)
    Write-Host "Building FlinkDotNetAspire/FlinkDotNetAspire.sln..."
    Push-Location FlinkDotNetAspire
    dotnet build FlinkDotNetAspire.sln --configuration Release
    if ($LASTEXITCODE -ne 0) { 
        Pop-Location
        throw "Build failed for FlinkDotNetAspire solution" 
    }
    Pop-Location

    # Build FlinkDotNet.WebUI solution (no tests)
    Write-Host "Building FlinkDotNet.WebUI/FlinkDotNet.WebUI.sln..."
    Push-Location FlinkDotNet.WebUI
    dotnet build FlinkDotNet.WebUI.sln --configuration Release
    if ($LASTEXITCODE -ne 0) { 
        Pop-Location
        throw "Build failed for FlinkDotNet.WebUI solution" 
    }
    Pop-Location

    # Verify Test Results
    Write-Host "Unit tests completed successfully."
    $duration = (Get-Date) - $startTime
    Write-Host "=== UNIT TESTS WORKFLOW COMPLETED in $($duration.TotalMinutes.ToString('F1')) minutes ==="
    exit 0

} catch {
    Write-Host "❌ Unit Tests Workflow Failed: $_" -ForegroundColor Red
    $duration = (Get-Date) - $startTime
    Write-Host "=== UNIT TESTS WORKFLOW FAILED after $($duration.TotalMinutes.ToString('F1')) minutes ==="
    exit 1
}
