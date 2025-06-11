#!/usr/bin/env pwsh
# local-build-analysis.ps1 - Local build script that mirrors the CI Build and Analysis workflow
# This script allows developers to see warnings locally before committing, matching the CI environment

param(
    [switch]$SkipSonar,
    [switch]$Help
)

if ($Help) {
    Write-Host @"
Local Build and Analysis Script

Usage: ./local-build-analysis.ps1 [options]

Options:
  -SkipSonar    Skip SonarCloud analysis (useful for quick local builds)
  -Help         Show this help message

This script mirrors the CI Build and Analysis workflow to ensure no warnings
are present before committing. It performs the same warning detection and
build validation as the GitHub Actions workflow.

Environment Variables:
  SONAR_TOKEN   SonarCloud token (optional for local analysis)

Examples:
  ./local-build-analysis.ps1                    # Full build with analysis
  ./local-build-analysis.ps1 -SkipSonar         # Build only, skip SonarCloud
"@
    exit 0
}

function Write-Section($title) {
    Write-Host ""
    Write-Host "=" * 60
    Write-Host "  $title"
    Write-Host "=" * 60
}

function RunCommandWithWarningCheck($command, $description) {
    Write-Host "Running $description"
    
    # Capture both stdout and stderr
    $output = & $command 2>&1
    
    # Display output in real-time
    $output | ForEach-Object { Write-Host $_ }
    
    if ($LASTEXITCODE -ne 0) {
        Write-Host "[FAIL] $description failed with exit code $LASTEXITCODE" -ForegroundColor Red
        return -1
    }
    
    # Check for warnings (case-insensitive)
    $warnings = $output | Where-Object { $_ -match "(?i)warning " }
    if ($warnings.Count -gt 0) {
        Write-Host "[WARN] Found warning(s) in ${description}:" -ForegroundColor Yellow
        $warnings | ForEach-Object { Write-Host "  $_" -ForegroundColor Yellow }
    } else {
        Write-Host "[OK] $description completed successfully with no warnings." -ForegroundColor Green
    }
    
    return $warnings.Count
}

function Test-Prerequisites {
    Write-Section "Checking Prerequisites"
    
    # Check .NET CLI
    try {
        $dotnetVersion = dotnet --version
        Write-Host "[OK] .NET CLI found: $dotnetVersion" -ForegroundColor Green
    } catch {
        Write-Host "[FAIL] .NET CLI not found. Please install .NET 8.0 or later." -ForegroundColor Red
        exit 1
    }
    
    # Check Java for SonarCloud (if not skipping)
    if (-not $SkipSonar) {
        try {
            $javaVersion = java -version 2>&1 | Select-Object -First 1
            Write-Host "[OK] Java found: $javaVersion" -ForegroundColor Green
        } catch {
            Write-Host "[WARN] Java not found. SonarCloud analysis will be skipped." -ForegroundColor Yellow
            $script:SkipSonar = $true
        }
    }
    
    # Check if we're in the right directory
    if (-not (Test-Path "FlinkDotNet/FlinkDotNet.sln")) {
        Write-Host "[FAIL] Not in repository root. Please run from FLINK.NET directory." -ForegroundColor Red
        exit 1
    }
    
    Write-Host "[OK] All prerequisites met." -ForegroundColor Green
}

function Install-Workloads {
    Write-Section "Installing .NET Workloads"
    
    $exitCode = RunCommandWithWarningCheck { dotnet workload install aspire } "Install .NET Aspire Workload"
    if ($exitCode -lt 0) { exit 1 }
    
    $exitCode = RunCommandWithWarningCheck { dotnet workload restore FlinkDotNet/FlinkDotNet.sln } "Restore workloads for FlinkDotNet"
    if ($exitCode -lt 0) { exit 1 }
    
    $exitCode = RunCommandWithWarningCheck { dotnet workload restore FlinkDotNetAspire/FlinkDotNetAspire.sln } "Restore workloads for FlinkDotNetAspire"
    if ($exitCode -lt 0) { exit 1 }
    
    $exitCode = RunCommandWithWarningCheck { dotnet workload restore FlinkDotNet.WebUI/FlinkDotNet.WebUI.sln } "Restore workloads for FlinkDotNet.WebUI"
    if ($exitCode -lt 0) { exit 1 }
}

function Start-SonarAnalysis {
    if ($SkipSonar) {
        Write-Host "[SKIP] SonarCloud analysis skipped (-SkipSonar flag)" -ForegroundColor Yellow
        return 0
    }
    
    Write-Section "Starting SonarCloud Analysis"
    
    # Check for SonarCloud scanner
    if (-not (Test-Path ".sonar/scanner/dotnet-sonarscanner.exe") -and -not (Test-Path ".sonar/scanner/dotnet-sonarscanner")) {
        Write-Host "Installing SonarCloud scanner..."
        New-Item -Path ".sonar/scanner" -ItemType Directory -Force | Out-Null
        $exitCode = RunCommandWithWarningCheck { dotnet tool update dotnet-sonarscanner --tool-path .sonar/scanner } "Install SonarCloud scanner"
        if ($exitCode -lt 0) { return -1 }
    }
    
    # Start SonarCloud analysis
    $sonarToken = $env:SONAR_TOKEN
    if (-not $sonarToken) {
        Write-Host "[WARN] SONAR_TOKEN not set. SonarCloud analysis will be skipped." -ForegroundColor Yellow
        return 0
    }
    
    $scannerPath = if (Test-Path ".sonar/scanner/dotnet-sonarscanner.exe") { ".sonar/scanner/dotnet-sonarscanner.exe" } else { ".sonar/scanner/dotnet-sonarscanner" }
    
    $beginCmd = { & $scannerPath begin /k:"devstress_FLINK.NET" /o:"devstress" /d:sonar.token="$sonarToken" /d:sonar.host.url="https://sonarcloud.io" /d:sonar.scanner.skipJreProvisioning=true /d:sonar.scanner.scanAll=false /d:sonar.coverage.exclusions="**/Program.cs,**/Migrations/**" }
    $exitCode = RunCommandWithWarningCheck $beginCmd "Sonar begin"
    if ($exitCode -lt 0) { return -1 }
    
    return $exitCode
}

function Build-Solutions {
    Write-Section "Building Solutions"
    
    $totalWarnings = 0
    
    $exit1 = RunCommandWithWarningCheck { dotnet build FlinkDotNet/FlinkDotNet.sln } "Build FlinkDotNet"
    if ($exit1 -lt 0) { return -1 }
    $totalWarnings += [Math]::Max(0, $exit1)
    
    $exit2 = RunCommandWithWarningCheck { dotnet build FlinkDotNetAspire/FlinkDotNetAspire.sln } "Build FlinkDotNetAspire"
    if ($exit2 -lt 0) { return -1 }
    $totalWarnings += [Math]::Max(0, $exit2)
    
    $exit3 = RunCommandWithWarningCheck { dotnet build FlinkDotNet.WebUI/FlinkDotNet.WebUI.sln } "Build FlinkDotNet.WebUI"
    if ($exit3 -lt 0) { return -1 }
    $totalWarnings += [Math]::Max(0, $exit3)
    
    return $totalWarnings
}

function Complete-SonarAnalysis {
    if ($SkipSonar -or -not $env:SONAR_TOKEN) {
        return 0
    }
    
    Write-Section "Completing SonarCloud Analysis"
    
    $scannerPath = if (Test-Path ".sonar/scanner/dotnet-sonarscanner.exe") { ".sonar/scanner/dotnet-sonarscanner.exe" } else { ".sonar/scanner/dotnet-sonarscanner" }
    
    $endCmd = { & $scannerPath end /d:sonar.token="$env:SONAR_TOKEN" }
    $exitCode = RunCommandWithWarningCheck $endCmd "Sonar end"
    if ($exitCode -lt 0) { return -1 }
    
    return $exitCode
}

function Main {
    Write-Section "Local Build and Analysis - Matching CI Workflow"
    Write-Host "This script mirrors the GitHub Actions 'Build and Analysis' workflow"
    Write-Host "Use this to catch warnings locally before committing."
    
    Test-Prerequisites
    Install-Workloads
    
    $sonarBeginWarnings = Start-SonarAnalysis
    if ($sonarBeginWarnings -lt 0) {
        Write-Host "[ERROR] SonarCloud begin failed." -ForegroundColor Red
        exit 1
    }
    
    $buildWarnings = Build-Solutions
    if ($buildWarnings -lt 0) {
        Write-Host "[ERROR] Build failed." -ForegroundColor Red
        exit 1
    }
    
    $sonarEndWarnings = Complete-SonarAnalysis
    if ($sonarEndWarnings -lt 0) {
        Write-Host "[ERROR] SonarCloud end failed." -ForegroundColor Red
        exit 1
    }
    
    Write-Section "Build Results Summary"
    
    $totalWarnings = [Math]::Max(0, $sonarBeginWarnings) + $buildWarnings + [Math]::Max(0, $sonarEndWarnings)
    
    if ($totalWarnings -gt 0) {
        Write-Host "[ERROR] Build completed but found $totalWarnings warning(s)" -ForegroundColor Red
        Write-Host "Please fix all warnings before committing." -ForegroundColor Red
        exit 1
    } else {
        Write-Host "[SUCCESS] All builds passed with no warnings!" -ForegroundColor Green
        Write-Host "Your code is ready for commit." -ForegroundColor Green
    }
}

# Run the main function
Main