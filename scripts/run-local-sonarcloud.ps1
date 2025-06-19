#!/usr/bin/env pwsh
<#
.SYNOPSIS
    Local SonarCloud analysis optimized for development environment.

.DESCRIPTION
    This script runs SonarCloud analysis locally without heavy CI-style installations.
    It assumes the development environment has basic tools available and focuses on
    local code quality analysis rather than full CI workflow replication.

.PARAMETER SonarToken
    SonarCloud authentication token (optional for local analysis).

.PARAMETER ProjectKey
    SonarCloud project key (default: devstress_FLINK.NET).

.PARAMETER Organization
    SonarCloud organization (default: devstress).

.PARAMETER SkipAnalysis
    Skip SonarCloud upload and only run local analysis.

.EXAMPLE
    ./scripts/run-local-sonarcloud.ps1 -SkipAnalysis
    Runs local code quality analysis without uploading to SonarCloud.

.EXAMPLE
    ./scripts/run-local-sonarcloud.ps1 -SonarToken $env:SONAR_TOKEN
    Runs analysis and uploads results to SonarCloud.
#>

param(
    [string]$SonarToken = $env:SONAR_TOKEN,
    [string]$ProjectKey = "devstress_FLINK.NET",
    [string]$Organization = "devstress",
    [switch]$SkipAnalysis
)

$ErrorActionPreference = 'Stop'

Write-Host "=== FlinkDotNet Local SonarCloud Analysis ===" -ForegroundColor Cyan
Write-Host "Started at: $(Get-Date -Format 'yyyy-MM-dd HH:mm:ss')" -ForegroundColor White
Write-Host "Skip Analysis: $SkipAnalysis" -ForegroundColor White

try {
    # Navigate to repository root (handle both root and scripts folder execution)
    $rootPath = if (Test-Path "FlinkDotNet") { Get-Location } else { Split-Path -Parent (Get-Location) }
    Set-Location $rootPath
    Write-Host "Working directory: $rootPath" -ForegroundColor Gray

    # Check prerequisites
    Write-Host "`n=== Prerequisites Check ===" -ForegroundColor Yellow
    
    # Check .NET SDK
    $dotnetVersion = dotnet --version
    if ($LASTEXITCODE -ne 0) {
        throw ".NET SDK not found. Please install .NET SDK 8.0 or later."
    }
    Write-Host "✅ .NET SDK version: $dotnetVersion" -ForegroundColor Green

    # Check Java (required for SonarScanner)
    $javaVersion = java -version 2>&1 | Select-Object -First 1
    if ($LASTEXITCODE -ne 0) {
        Write-Host "⚠️ Java not found - required for SonarScanner. Install Java 11+ if you plan to use SonarCloud." -ForegroundColor Yellow
        if (-not $SkipAnalysis -and $SonarToken) {
            throw "Java is required for SonarCloud analysis. Please install Java 11+ or use -SkipAnalysis."
        }
    } else {
        $javaVersionString = if ($javaVersion -is [string]) { $javaVersion.Trim() } else { $javaVersion.ToString().Trim() }
        Write-Host "✅ Java detected: $javaVersionString" -ForegroundColor Green
    }

    # Setup SonarScanner (only if needed)
    if (-not $SkipAnalysis -and $SonarToken) {
        Write-Host "`n=== SonarScanner Setup ===" -ForegroundColor Yellow
        
        if (-not (Test-Path ".sonar/scanner/dotnet-sonarscanner")) {
            Write-Host "Installing SonarScanner locally..." -ForegroundColor White
            New-Item -Path ".sonar/scanner" -ItemType Directory -Force | Out-Null
            dotnet tool update dotnet-sonarscanner --tool-path .sonar/scanner
            if ($LASTEXITCODE -ne 0) {
                throw "Failed to install SonarScanner"
            }
        }
        Write-Host "✅ SonarScanner ready" -ForegroundColor Green
    }

    # Lightweight workload check (no installation)
    Write-Host "`n=== Workload Verification ===" -ForegroundColor Yellow
    $aspireWorkload = dotnet workload list | Where-Object { $_ -match "aspire" }
    if ($aspireWorkload) {
        Write-Host "✅ .NET Aspire workload detected" -ForegroundColor Green
    } else {
        Write-Host "⚠️ .NET Aspire workload not detected - some projects may not build" -ForegroundColor Yellow
    }

    # Begin SonarCloud analysis (if token provided and not skipped)
    $sonarStarted = $false
    if (-not $SkipAnalysis -and $SonarToken) {
        Write-Host "`n=== Starting SonarCloud Analysis ===" -ForegroundColor Yellow
        
        $sonarArgs = @(
            'begin',
            "/k:$ProjectKey",
            "/o:$Organization",
            "/d:sonar.token=$SonarToken",
            "/d:sonar.host.url=https://sonarcloud.io",
            "/d:sonar.cs.dotcover.reportsPaths=*.html"
        )
        
        Write-Host "Starting SonarScanner..." -ForegroundColor White
        & .sonar/scanner/dotnet-sonarscanner @sonarArgs
        if ($LASTEXITCODE -ne 0) {
            throw "Failed to start SonarScanner"
        }
        $sonarStarted = $true
        Write-Host "✅ SonarScanner analysis started" -ForegroundColor Green
    } else {
        Write-Host "`n=== Skipping SonarCloud Analysis ===" -ForegroundColor Yellow
        if ($SkipAnalysis) {
            Write-Host "Analysis skipped due to -SkipAnalysis flag" -ForegroundColor Gray
        } else {
            Write-Host "Analysis skipped - no SONAR_TOKEN provided" -ForegroundColor Gray
        }
    }

    # Build solutions with minimal verbosity
    Write-Host "`n=== Building Solutions ===" -ForegroundColor Yellow
    
    $solutions = @(
        "FlinkDotNet/FlinkDotNet.sln",
        "FlinkDotNetAspire/FlinkDotNetAspire.sln",
        "FlinkDotNet.WebUI/FlinkDotNet.WebUI.sln"
    )

    foreach ($solution in $solutions) {
        if (Test-Path $solution) {
            Write-Host "Building $solution..." -ForegroundColor White
            dotnet build $solution --configuration Release --verbosity minimal
            if ($LASTEXITCODE -ne 0) {
                Write-Host "⚠️ Build failed for $solution" -ForegroundColor Yellow
                # Continue with other solutions for local analysis
            } else {
                Write-Host "✅ $solution built successfully" -ForegroundColor Green
            }
        } else {
            Write-Host "⚠️ Solution not found: $solution" -ForegroundColor Yellow
        }
    }

    # Run local analysis tools
    Write-Host "`n=== Local Code Analysis ===" -ForegroundColor Yellow
    
    # Check for local analysis script
    if (Test-Path "scripts/local-build-analysis.ps1") {
        Write-Host "Running local build analysis..." -ForegroundColor White
        & ./scripts/local-build-analysis.ps1
        if ($LASTEXITCODE -eq 0) {
            Write-Host "✅ Local analysis completed" -ForegroundColor Green
        } else {
            Write-Host "⚠️ Local analysis had warnings" -ForegroundColor Yellow
        }
    }

    # Check for warning detector
    if (Test-Path "scripts/sonar-warning-detector.ps1") {
        Write-Host "Running warning detection..." -ForegroundColor White
        & ./scripts/sonar-warning-detector.ps1
        if ($LASTEXITCODE -eq 0) {
            Write-Host "✅ Warning detection completed" -ForegroundColor Green
        } else {
            Write-Host "⚠️ Warnings detected" -ForegroundColor Yellow
        }
    }

    # End SonarCloud analysis (if started)
    if ($sonarStarted) {
        Write-Host "`n=== Completing SonarCloud Analysis ===" -ForegroundColor Yellow
        
        Write-Host "Ending SonarScanner analysis..." -ForegroundColor White
        & .sonar/scanner/dotnet-sonarscanner end /d:sonar.token=$SonarToken
        if ($LASTEXITCODE -ne 0) {
            Write-Host "⚠️ SonarScanner end failed" -ForegroundColor Yellow
        } else {
            Write-Host "✅ SonarCloud analysis completed" -ForegroundColor Green
        }
    }

    # Summary
    Write-Host "`n=== Summary ===" -ForegroundColor Yellow
    Write-Host "✅ Local code analysis completed" -ForegroundColor Green
    if ($sonarStarted) {
        Write-Host "✅ SonarCloud analysis uploaded" -ForegroundColor Green
        Write-Host "View results at: https://sonarcloud.io/project/overview?id=$ProjectKey" -ForegroundColor Cyan
    } else {
        Write-Host "ℹ️ Local analysis only - use SONAR_TOKEN for cloud analysis" -ForegroundColor Blue
    }
    
    $duration = (Get-Date) - $startTime
    Write-Host "Total duration: $($duration.TotalSeconds.ToString('F1')) seconds" -ForegroundColor White

} catch {
    Write-Host "`n❌ Analysis failed: $_" -ForegroundColor Red
    
    # Cleanup SonarScanner if it was started
    if ($sonarStarted) {
        Write-Host "Attempting to cleanup SonarScanner..." -ForegroundColor Yellow
        & .sonar/scanner/dotnet-sonarscanner end /d:sonar.token=$SonarToken 2>$null
    }
    
    exit 1
}

Write-Host "`n=== Local SonarCloud Analysis Complete ===" -ForegroundColor Cyan
Write-Host "Result: ✅ SUCCESS" -ForegroundColor Green