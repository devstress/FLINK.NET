#!/usr/bin/env pwsh
<#
.SYNOPSIS
    Local unit test runner optimized for development environment.

.DESCRIPTION
    This script runs unit tests locally without the heavy CI-style installations.
    It assumes the development environment is already set up with:
    - .NET SDK 8.0+
    - .NET Aspire workload (if needed)
    - Required development tools

.PARAMETER Configuration
    Build configuration (default: Release).

.PARAMETER CollectCoverage
    If specified, collects code coverage data.

.PARAMETER ResultsDirectory
    Directory to store test results (default: TestResults).

.EXAMPLE
    ./scripts/run-local-unit-tests.ps1
    Runs unit tests with default settings.

.EXAMPLE
    ./scripts/run-local-unit-tests.ps1 -CollectCoverage
    Runs unit tests and collects code coverage.
#>

param(
    [string]$Configuration = "Release",
    [switch]$CollectCoverage,
    [string]$ResultsDirectory = "TestResults"
)

$ErrorActionPreference = 'Stop'

$startTime = Get-Date
Write-Host "=== FlinkDotNet Local Unit Tests ===" -ForegroundColor Cyan
Write-Host "Started at: $(Get-Date -Format 'yyyy-MM-dd HH:mm:ss')" -ForegroundColor White
Write-Host "Configuration: $Configuration" -ForegroundColor White
Write-Host "Collect Coverage: $CollectCoverage" -ForegroundColor White

try {
    # Navigate to repository root (handle both root and scripts folder execution)
    $rootPath = if (Test-Path "FlinkDotNet") { Get-Location } else { Split-Path -Parent (Get-Location) }
    Set-Location $rootPath
    Write-Host "Working directory: $rootPath" -ForegroundColor Gray

    # Check prerequisites (no installation, just verification)
    Write-Host "`n=== Prerequisites Check ===" -ForegroundColor Yellow
    
    # Check .NET SDK
    $dotnetVersion = dotnet --version
    if ($LASTEXITCODE -ne 0) {
        throw ".NET SDK not found. Please install .NET SDK 8.0 or later."
    }
    Write-Host "✅ .NET SDK version: $dotnetVersion" -ForegroundColor Green

    # Check if Aspire workload is needed and available
    $aspireWorkload = dotnet workload list | Where-Object { $_ -match "aspire" }
    if ($aspireWorkload) {
        Write-Host "✅ .NET Aspire workload detected" -ForegroundColor Green
    } else {
        Write-Host "⚠️ .NET Aspire workload not detected - will skip if needed" -ForegroundColor Yellow
    }

    # Clean previous test results
    Write-Host "`n=== Cleaning Previous Results ===" -ForegroundColor Yellow
    if (Test-Path "FlinkDotNet/$ResultsDirectory") {
        Remove-Item "FlinkDotNet/$ResultsDirectory" -Recurse -Force
        Write-Host "✅ Cleaned previous test results" -ForegroundColor Green
    }

    # Build FlinkDotNet solution (lightweight build)
    Write-Host "`n=== Building FlinkDotNet Solution ===" -ForegroundColor Yellow
    Push-Location FlinkDotNet
    
    Write-Host "Building FlinkDotNet.sln..." -ForegroundColor White
    dotnet build FlinkDotNet.sln --configuration $Configuration --verbosity minimal
    if ($LASTEXITCODE -ne 0) {
        Pop-Location
        throw "Build failed for FlinkDotNet solution"
    }
    Write-Host "✅ FlinkDotNet.sln built successfully" -ForegroundColor Green

    # Run unit tests
    Write-Host "`n=== Running Unit Tests ===" -ForegroundColor Yellow
    
    $testArgs = @(
        'test',
        'FlinkDotNet.sln',
        '--configuration', $Configuration,
        '--logger', 'trx',
        '--results-directory', $ResultsDirectory,
        '--verbosity', 'normal'
    )

    if ($CollectCoverage) {
        Write-Host "Code coverage collection enabled" -ForegroundColor White
        $testArgs += '--collect:"XPlat Code Coverage"'
        $testArgs += '--settings', '../coverlet.runsettings'
    }

    Write-Host "Running tests with args: $($testArgs -join ' ')" -ForegroundColor Gray
    & dotnet @testArgs
    $testExitCode = $LASTEXITCODE
    
    Pop-Location

    if ($testExitCode -ne 0) {
        throw "Unit tests failed with exit code $testExitCode"
    }
    
    Write-Host "✅ Unit tests PASSED" -ForegroundColor Green

    # Process coverage if collected
    if ($CollectCoverage) {
        Write-Host "`n=== Processing Coverage ===" -ForegroundColor Yellow
        
        # Check if ReportGenerator is available
        $reportGenerator = Get-Command reportgenerator -ErrorAction SilentlyContinue
        if (-not $reportGenerator) {
            Write-Host "Installing ReportGenerator..." -ForegroundColor White
            dotnet tool install --global dotnet-reportgenerator-globaltool --version 5.2.0
        }

        # Convert coverage files
        $coverageFiles = Get-ChildItem -Path "./FlinkDotNet/$ResultsDirectory" -Recurse -Filter "coverage.cobertura.xml"
        if ($coverageFiles) {
            Write-Host "Found $($coverageFiles.Count) coverage file(s)" -ForegroundColor White
            foreach ($coberturaFile in $coverageFiles) {
                $targetDir = $coberturaFile.DirectoryName
                Write-Host "Processing: $($coberturaFile.Name)" -ForegroundColor Gray
                
                reportgenerator -reports:$($coberturaFile.FullName) -targetdir:$targetDir -reporttypes:lcov
                if ($LASTEXITCODE -eq 0) {
                    Write-Host "✅ Generated LCOV coverage report in $targetDir" -ForegroundColor Green
                } else {
                    Write-Host "⚠️ Failed to generate LCOV report" -ForegroundColor Yellow
                }
            }
        } else {
            Write-Host "⚠️ No coverage files found" -ForegroundColor Yellow
        }
    }

    # Summary
    Write-Host "`n=== Summary ===" -ForegroundColor Yellow
    Write-Host "✅ Unit tests completed successfully" -ForegroundColor Green
    Write-Host "✅ Test results available in: FlinkDotNet/$ResultsDirectory" -ForegroundColor Green
    if ($CollectCoverage) {
        Write-Host "✅ Code coverage reports generated" -ForegroundColor Green
    }
    
    $duration = (Get-Date) - $startTime
    Write-Host "Total duration: $($duration.TotalSeconds.ToString('F1')) seconds" -ForegroundColor White

} catch {
    Write-Host "`n❌ Unit tests failed: $_" -ForegroundColor Red
    exit 1
}

Write-Host "`n=== Local Unit Tests Complete ===" -ForegroundColor Cyan
Write-Host "Result: ✅ SUCCESS" -ForegroundColor Green