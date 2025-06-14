#!/usr/bin/env pwsh
# SonarCloud Workflow - mirrors .github/workflows/sonarcloud.yml

Write-Host "=== SONARCLOUD WORKFLOW STARTED ==="
$startTime = Get-Date

try {
    # Navigate to repository root (handle both root and scripts folder execution)
    $rootPath = if (Test-Path "FlinkDotNet") { Get-Location } else { Split-Path -Parent (Get-Location) }
    Set-Location $rootPath
    Write-Host "Working directory: $rootPath"
    function RunCommandWithWarningCheck($command, $description) {
        Write-Host "Running $description"
        $output = & $command 2>&1
        $output | ForEach-Object { Write-Host $_ }

        if ($LASTEXITCODE -ne 0) {
            Write-Host "[FAIL] $description failed with exit code $LASTEXITCODE"
            return -1
        }
        $warnings = $output | Where-Object { $_ -match "(?i)warning " }
        if ($warnings.Count -gt 0) {
            Write-Host "[WARN] Found warning(s) in ${description}:"
            $warnings | ForEach-Object { Write-Host "  $_" }
        } else {
            Write-Host "[OK] $description completed successfully with no warnings."
        }
        return $warnings.Count
    }

    # Install SonarCloud scanner
    if (-not (Test-Path ".sonar/scanner")) {
        Write-Host "Installing SonarCloud scanner..."
        New-Item -Path ".sonar/scanner" -ItemType Directory -Force | Out-Null
        dotnet tool update dotnet-sonarscanner --tool-path .sonar/scanner
    }

    # Install .NET Aspire workload
    Write-Host "Installing .NET Aspire Workload..."
    dotnet workload install aspire

    # Restore .NET Workloads for Solutions
    dotnet workload restore FlinkDotNet/FlinkDotNet.sln
    dotnet workload restore FlinkDotNetAspire/FlinkDotNetAspire.sln
    dotnet workload restore FlinkDotNet.WebUI/FlinkDotNet.WebUI.sln

    # Begin SonarCloud analysis (only if SONAR_TOKEN is available)
    if ($env:SONAR_TOKEN) {
        Write-Host "Starting SonarCloud analysis..."
        $beginCmd = { .\.sonar\scanner\dotnet-sonarscanner begin /k:"devstress_FLINK.NET" /o:"devstress" /d:sonar.token="$env:SONAR_TOKEN" /d:sonar.host.url="https://sonarcloud.io" /d:sonar.scanner.skipJreProvisioning=true /d:sonar.scanner.scanAll=false /d:sonar.coverage.exclusions="**/Program.cs,**/Migrations/**" /d:sonar.cs.opencover.reportsPaths="FlinkDotNet/TestResults/**/*.opencover.xml" }
        $exitBegin = RunCommandWithWarningCheck $beginCmd "Sonar begin"
        if ($exitBegin -lt 0) { throw "SonarCloud begin failed" }
    } else {
        Write-Host "SONAR_TOKEN not set - skipping SonarCloud analysis"
    }

    # Build all solutions
    $exit1 = RunCommandWithWarningCheck { dotnet build FlinkDotNet/FlinkDotNet.sln } "Build FlinkDotNet"
    $exit2 = RunCommandWithWarningCheck { dotnet build FlinkDotNetAspire/FlinkDotNetAspire.sln } "Build FlinkDotNetAspire"
    $exit3 = RunCommandWithWarningCheck { dotnet build FlinkDotNet.WebUI/FlinkDotNet.WebUI.sln } "Build FlinkDotNet.WebUI"

    $totalWarnings = [Math]::Max(0, $exit1) + [Math]::Max(0, $exit2) + [Math]::Max(0, $exit3)
    $buildFailed = ($exit1 -lt 0) -or ($exit2 -lt 0) -or ($exit3 -lt 0)

    if ($buildFailed) {
        throw "One or more builds failed"
    }

    if ($totalWarnings -gt 0) {
        throw "Found $totalWarnings warning(s) - builds should have no warnings"
    }

    # Check for test artifacts and run tests if needed
    $artifactsAvailable = $false
    if (Test-Path "./FlinkDotNet/TestResults") {
        $openCoverFiles = Get-ChildItem -Path "./FlinkDotNet/TestResults" -Recurse -Filter "*.opencover.xml" -ErrorAction SilentlyContinue
        if ($openCoverFiles.Count -gt 0) {
            Write-Host "Found existing test artifacts from unit-tests workflow"
            $artifactsAvailable = $true
        }
    }
    
    if (-not $artifactsAvailable) {
        Write-Host "No test artifacts available - running tests locally for coverage..."
        Push-Location FlinkDotNet
        dotnet test FlinkDotNet.sln --configuration Release --logger "trx" --results-directory "TestResults" --collect:"XPlat Code Coverage" --settings ../coverlet.runsettings
        Pop-Location
        
        # Convert coverage
        dotnet tool install --global dotnet-reportgenerator-globaltool --version 5.2.0 2>$null
        Get-ChildItem -Path "./FlinkDotNet/TestResults" -Recurse -Filter "coverage.cobertura.xml" | ForEach-Object {
            $coberturaFile = $_.FullName
            $targetDir = $_.DirectoryName
            $openCoverFile = Join-Path $targetDir "coverage.opencover.xml"
            reportgenerator -reports:"$coberturaFile" -targetdir:"$targetDir" -reporttypes:"OpenCover" -verbosity:Info
        }
    }

    # Submit to SonarCloud
    if ($env:SONAR_TOKEN) {
        Write-Host "Submitting to SonarCloud..."
        $endCmd = { .\.sonar\scanner\dotnet-sonarscanner end /d:sonar.token="$env:SONAR_TOKEN" }
        $exitEnd = RunCommandWithWarningCheck $endCmd "Sonar end with coverage"
        if ($exitEnd -ne 0) { throw "SonarCloud submission failed" }
    }

    $duration = (Get-Date) - $startTime
    Write-Host "=== SONARCLOUD WORKFLOW COMPLETED in $($duration.TotalMinutes.ToString('F1')) minutes ==="
    exit 0

} catch {
    Write-Host "❌ SonarCloud Workflow Failed: $_" -ForegroundColor Red
    $duration = (Get-Date) - $startTime
    Write-Host "=== SONARCLOUD WORKFLOW FAILED after $($duration.TotalMinutes.ToString('F1')) minutes ==="
    exit 1
}
