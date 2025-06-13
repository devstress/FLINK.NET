#!/bin/bash
# run-all-workflows.sh - Run all GitHub workflows locally in parallel
# This script mirrors the GitHub Actions workflows to run locally for development validation
# 
# Workflows executed in parallel:
# 1. Unit Tests          - Runs .NET unit tests with coverage collection
# 2. SonarCloud Analysis - Builds solutions and performs code analysis  
# 3. Stress Tests        - Runs Aspire stress tests with Redis/Kafka
# 4. Integration Tests   - Runs Aspire integration tests
#
# Usage: ./run-all-workflows.sh [options]
# Options:
#   --skip-sonar     Skip SonarCloud analysis (if SONAR_TOKEN not available)
#   --skip-stress    Skip stress tests (if Docker not available)
#   --help           Show this help message

set -e  # Exit on error

# Initialize variables
SKIP_SONAR=0
SKIP_STRESS=0
SHOW_HELP=0

# Parse command line arguments
while [[ $# -gt 0 ]]; do
    case $1 in
        --skip-sonar)
            SKIP_SONAR=1
            shift
            ;;
        --skip-stress)
            SKIP_STRESS=1
            shift
            ;;
        --help)
            SHOW_HELP=1
            shift
            ;;
        *)
            echo "Unknown option: $1"
            echo "Use --help for usage information"
            exit 1
            ;;
    esac
done

if [[ $SHOW_HELP -eq 1 ]]; then
    cat << 'EOF'

Local GitHub Workflows Runner

This script runs all GitHub Actions workflows locally in parallel:
  1. Unit Tests          - .NET unit tests with coverage
  2. SonarCloud Analysis - Code analysis and build validation
  3. Stress Tests        - Aspire stress tests with containers
  4. Integration Tests   - Aspire integration tests

Usage: ./run-all-workflows.sh [options]

Options:
  --skip-sonar     Skip SonarCloud analysis (useful if no SONAR_TOKEN)
  --skip-stress    Skip stress tests (useful if Docker unavailable)
  --help           Show this help message

Prerequisites:
  - .NET 8.0 SDK
  - Java 17+ (for SonarCloud)
  - Docker (for stress/integration tests)
  - PowerShell Core (pwsh)

Environment Variables:
  SONAR_TOKEN              SonarCloud authentication token
  SIMULATOR_NUM_MESSAGES   Number of messages for stress tests (default: 1000000)

EOF
    exit 0
fi

# Navigate to repository root
cd "$(dirname "$0")"
ROOT="$(pwd)"

echo "================================================================"
echo "   Local GitHub Workflows Runner - Parallel Execution"
echo "================================================================"
echo "Repository: $ROOT"
echo "Timestamp: $(date)"
echo ""

# Helper functions
check_command() {
    local cmd="$1"
    local name="$2"
    local url="$3"
    
    if command -v "$cmd" &> /dev/null; then
        local version
        case "$cmd" in
            dotnet)
                version=$(dotnet --version 2>/dev/null || echo "unknown")
                ;;
            java)
                version=$(java -version 2>&1 | head -1 | awk -F '"' '{print $2}' || echo "unknown")
                ;;
            docker)
                version=$(docker --version 2>/dev/null | awk '{print $3}' | sed 's/,//' || echo "unknown")
                ;;
            pwsh)
                version=$(pwsh -Command '$PSVersionTable.PSVersion.ToString()' 2>/dev/null || echo "unknown")
                ;;
            *)
                version="found"
                ;;
        esac
        echo "✅ $name: $version"
        return 0
    else
        echo "❌ $name not found."
        if [[ -n "$url" ]]; then
            echo "   Please install from: $url"
        fi
        return 1
    fi
}

check_docker_running() {
    if docker info &> /dev/null; then
        echo "✅ Docker is running"
        return 0
    else
        echo "❌ Docker is not running or not accessible"
        return 1
    fi
}

# Check prerequisites
echo "=== Checking Prerequisites ==="

check_command dotnet ".NET SDK" "https://dotnet.microsoft.com/download"
if [[ $? -ne 0 ]]; then
    exit 1
fi

if [[ $SKIP_SONAR -eq 0 ]]; then
    if ! check_command java "Java" "https://adoptopenjdk.net/"; then
        echo "WARNING: Java not found. SonarCloud analysis will be skipped."
        SKIP_SONAR=1
    fi
fi

if [[ $SKIP_STRESS -eq 0 ]]; then
    if ! check_command docker "Docker" "https://docker.com/" || ! check_docker_running; then
        echo "WARNING: Docker not available. Stress tests will be skipped."
        SKIP_STRESS=1
    fi
fi

check_command pwsh "PowerShell Core" "https://github.com/PowerShell/PowerShell"
if [[ $? -ne 0 ]]; then
    exit 1
fi

echo "Prerequisites check completed."
echo ""

# Create output directories for parallel execution
mkdir -p "$ROOT/workflow-logs"

# Set environment variables
export SIMULATOR_NUM_MESSAGES=${SIMULATOR_NUM_MESSAGES:-1000000}
export MAX_ALLOWED_TIME_MS=${MAX_ALLOWED_TIME_MS:-60000}
export ASPIRE_ALLOW_UNSECURED_TRANSPORT=true

echo "=== Starting Workflows in Parallel ==="
echo "Unit Tests: workflow-logs/unit-tests.log"
echo "SonarCloud: workflow-logs/sonarcloud.log"
if [[ $SKIP_STRESS -eq 0 ]]; then
    echo "Stress Tests: workflow-logs/stress-tests.log"
else
    echo "Stress Tests: SKIPPED (--skip-stress or Docker unavailable)"
fi
echo "Integration Tests: workflow-logs/integration-tests.log"
echo ""

# Create individual workflow scripts to run
create_unit_tests_workflow() {
    cat > "$ROOT/run-unit-tests-workflow.ps1" << 'EOF'
#!/usr/bin/env pwsh
# Unit Tests Workflow - mirrors .github/workflows/unit-tests.yml

Write-Host "=== UNIT TESTS WORKFLOW STARTED ==="
$startTime = Get-Date

try {
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
EOF
}

create_sonarcloud_workflow() {
    cat > "$ROOT/run-sonarcloud-workflow.ps1" << 'EOF'
#!/usr/bin/env pwsh
# SonarCloud Workflow - mirrors .github/workflows/sonarcloud.yml

Write-Host "=== SONARCLOUD WORKFLOW STARTED ==="
$startTime = Get-Date

try {
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
EOF
}

create_stress_tests_workflow() {
    cat > "$ROOT/run-stress-tests-workflow.ps1" << 'EOF'
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
    Write-Host "  MAX_ALLOWED_TIME_MS: $(if ($env:MAX_ALLOWED_TIME_MS) { $env:MAX_ALLOWED_TIME_MS } else { '60000' })"

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
EOF
}

create_integration_tests_workflow() {
    cat > "$ROOT/run-integration-tests-workflow.ps1" << 'EOF'
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
EOF
}

# Create individual workflow scripts
create_unit_tests_workflow
create_sonarcloud_workflow
if [[ $SKIP_STRESS -eq 0 ]]; then
    create_stress_tests_workflow
fi
create_integration_tests_workflow

# Start workflows in parallel
declare -a PIDS=()
declare -a WORKFLOW_NAMES=()

# Start Unit Tests
echo "Starting Unit Tests workflow..."
pwsh -File "$ROOT/run-unit-tests-workflow.ps1" > "$ROOT/workflow-logs/unit-tests.log" 2>&1 &
PIDS+=($!)
WORKFLOW_NAMES+=("Unit Tests")

# Start SonarCloud (if not skipped)
if [[ $SKIP_SONAR -eq 0 ]]; then
    echo "Starting SonarCloud workflow..."
    pwsh -File "$ROOT/run-sonarcloud-workflow.ps1" > "$ROOT/workflow-logs/sonarcloud.log" 2>&1 &
    PIDS+=($!)
    WORKFLOW_NAMES+=("SonarCloud")
fi

# Start Stress Tests (if not skipped)
if [[ $SKIP_STRESS -eq 0 ]]; then
    echo "Starting Stress Tests workflow..."
    pwsh -File "$ROOT/run-stress-tests-workflow.ps1" > "$ROOT/workflow-logs/stress-tests.log" 2>&1 &
    PIDS+=($!)
    WORKFLOW_NAMES+=("Stress Tests")
fi

# Start Integration Tests
echo "Starting Integration Tests workflow..."
pwsh -File "$ROOT/run-integration-tests-workflow.ps1" > "$ROOT/workflow-logs/integration-tests.log" 2>&1 &
PIDS+=($!)
WORKFLOW_NAMES+=("Integration Tests")

echo ""
echo "All workflows started in parallel. Monitoring progress..."
echo "Use 'tail -f workflow-logs/*.log' to follow progress in real-time."
echo ""

# Monitor workflow progress
FAILED_WORKFLOWS=()
COMPLETED_WORKFLOWS=()

while [[ ${#PIDS[@]} -gt 0 ]]; do
    for i in "${!PIDS[@]}"; do
        pid="${PIDS[$i]}"
        name="${WORKFLOW_NAMES[$i]}"
        
        if ! kill -0 "$pid" 2>/dev/null; then
            # Process finished
            wait "$pid"
            exit_code=$?
            
            if [[ $exit_code -eq 0 ]]; then
                echo "[COMPLETED] $name workflow finished successfully"
                COMPLETED_WORKFLOWS+=("$name")
            else
                echo "[FAILED] $name workflow failed with exit code $exit_code"
                FAILED_WORKFLOWS+=("$name")
            fi
            
            # Remove from arrays
            unset PIDS[$i]
            unset WORKFLOW_NAMES[$i]
            PIDS=("${PIDS[@]}")
            WORKFLOW_NAMES=("${WORKFLOW_NAMES[@]}")
            break
        fi
    done
    
    if [[ ${#PIDS[@]} -gt 0 ]]; then
        sleep 2
    fi
done

# Clean up temporary workflow scripts
rm -f "$ROOT/run-unit-tests-workflow.ps1"
rm -f "$ROOT/run-sonarcloud-workflow.ps1"
rm -f "$ROOT/run-stress-tests-workflow.ps1"  
rm -f "$ROOT/run-integration-tests-workflow.ps1"

# Report final results
echo ""
echo "=== Workflow Execution Summary ==="
echo "Completed: ${#COMPLETED_WORKFLOWS[@]}"
echo "Failed: ${#FAILED_WORKFLOWS[@]}"

if [[ ${#FAILED_WORKFLOWS[@]} -gt 0 ]]; then
    echo "Failed workflows: ${FAILED_WORKFLOWS[*]}"
    echo ""
    echo "❌ One or more workflows failed."
    echo "Check workflow-logs/ directory for error details:"
    for workflow in "${FAILED_WORKFLOWS[@]}"; do
        case "$workflow" in
            "Unit Tests")
                echo "  - workflow-logs/unit-tests.log"
                ;;
            "SonarCloud")
                echo "  - workflow-logs/sonarcloud.log"
                ;;
            "Stress Tests")
                echo "  - workflow-logs/stress-tests.log"
                ;;
            "Integration Tests")
                echo "  - workflow-logs/integration-tests.log"
                ;;
        esac
    done
    exit 1
else
    echo ""
    echo "✅ All workflows completed successfully!"
    echo "Check workflow-logs/ directory for detailed output."
    exit 0
fi