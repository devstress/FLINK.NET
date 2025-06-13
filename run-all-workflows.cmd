@echo off
REM run-all-workflows.cmd - Run all GitHub workflows locally in parallel
REM This script mirrors the GitHub Actions workflows to run locally for development validation
REM 
REM Workflows executed in parallel:
REM 1. Unit Tests          - Runs .NET unit tests with coverage collection
REM 2. SonarCloud Analysis - Builds solutions and performs code analysis  
REM 3. Stress Tests        - Runs Aspire stress tests with Redis/Kafka
REM 4. Integration Tests   - Runs Aspire integration tests
REM
REM Usage: run-all-workflows.cmd [options]
REM Options:
REM   --skip-sonar     Skip SonarCloud analysis (if SONAR_TOKEN not available)
REM   --skip-stress    Skip stress tests (if Docker not available)
REM   --help           Show this help message

setlocal enabledelayedexpansion

REM Parse command line arguments
set SKIP_SONAR=0
set SKIP_STRESS=0
set SHOW_HELP=0

:parse_args
if "%~1"=="--skip-sonar" (
    set SKIP_SONAR=1
    shift
    goto parse_args
)
if "%~1"=="--skip-stress" (
    set SKIP_STRESS=1
    shift
    goto parse_args
)
if "%~1"=="--help" (
    set SHOW_HELP=1
    shift
    goto parse_args
)
if not "%~1"=="" (
    echo Unknown option: %~1
    echo Use --help for usage information
    exit /b 1
)

if %SHOW_HELP%==1 (
    echo.
    echo Local GitHub Workflows Runner
    echo.
    echo This script runs all GitHub Actions workflows locally in parallel:
    echo   1. Unit Tests          - .NET unit tests with coverage
    echo   2. SonarCloud Analysis - Code analysis and build validation
    echo   3. Stress Tests        - Aspire stress tests with containers
    echo   4. Integration Tests   - Aspire integration tests
    echo.
    echo Usage: run-all-workflows.cmd [options]
    echo.
    echo Options:
    echo   --skip-sonar     Skip SonarCloud analysis ^(useful if no SONAR_TOKEN^)
    echo   --skip-stress    Skip stress tests ^(useful if Docker unavailable^)
    echo   --help           Show this help message
    echo.
    echo Prerequisites:
    echo   - .NET 8.0 SDK
    echo   - Java 17+ ^(for SonarCloud^)
    echo   - Docker ^(for stress/integration tests^)
    echo   - PowerShell Core ^(pwsh^)
    echo.
    echo Environment Variables:
    echo   SONAR_TOKEN              SonarCloud authentication token
    echo   SIMULATOR_NUM_MESSAGES   Number of messages for stress tests ^(default: 1000000^)
    echo.
    exit /b 0
)

REM Navigate to repository root
pushd "%~dp0"
set "ROOT=%CD%"

echo ================================================================
echo    Local GitHub Workflows Runner - Parallel Execution
echo ================================================================
echo Repository: %ROOT%
echo Timestamp: %DATE% %TIME%
echo.

REM Check prerequisites
echo === Checking Prerequisites ===
call :check_dotnet
if errorlevel 1 exit /b 1

if %SKIP_SONAR%==0 (
    call :check_java
    if errorlevel 1 (
        echo WARNING: Java not found. SonarCloud analysis will be skipped.
        set SKIP_SONAR=1
    )
)

if %SKIP_STRESS%==0 (
    call :check_docker
    if errorlevel 1 (
        echo WARNING: Docker not available. Stress tests will be skipped.
        set SKIP_STRESS=1
    )
)

call :check_pwsh
if errorlevel 1 exit /b 1

echo Prerequisites check completed.
echo.

REM Create output directories for parallel execution
if not exist "%ROOT%\workflow-logs" mkdir "%ROOT%\workflow-logs"

REM Set environment variables
if not defined SIMULATOR_NUM_MESSAGES set SIMULATOR_NUM_MESSAGES=1000000
if not defined MAX_ALLOWED_TIME_MS set MAX_ALLOWED_TIME_MS=10000
set ASPIRE_ALLOW_UNSECURED_TRANSPORT=true

echo === Starting Workflows in Parallel ===
echo Unit Tests: workflow-logs\unit-tests.log
echo SonarCloud: workflow-logs\sonarcloud.log
if %SKIP_STRESS%==0 echo Stress Tests: workflow-logs\stress-tests.log
if %SKIP_STRESS%==1 echo Stress Tests: SKIPPED (--skip-stress or Docker unavailable)
echo Integration Tests: workflow-logs\integration-tests.log
echo.

REM Start workflows in parallel using PowerShell background jobs
powershell -Command "& {
    # Start Unit Tests workflow
    $unitTestsJob = Start-Job -ScriptBlock {
        param($root)
        Set-Location $root
        & pwsh -File '%ROOT%\run-unit-tests-workflow.ps1' 2>&1
    } -ArgumentList '%ROOT%'
    
    # Start SonarCloud workflow (conditional)
    $sonarJob = $null
    if (%SKIP_SONAR% -eq 0) {
        $sonarJob = Start-Job -ScriptBlock {
            param($root)
            Set-Location $root
            & pwsh -File '%ROOT%\run-sonarcloud-workflow.ps1' 2>&1
        } -ArgumentList '%ROOT%'
    }
    
    # Start Stress Tests workflow (conditional)
    $stressJob = $null
    if (%SKIP_STRESS% -eq 0) {
        $stressJob = Start-Job -ScriptBlock {
            param($root)
            Set-Location $root
            & pwsh -File '%ROOT%\run-stress-tests-workflow.ps1' 2>&1
        } -ArgumentList '%ROOT%'
    }
    
    # Start Integration Tests workflow
    $integrationJob = Start-Job -ScriptBlock {
        param($root)
        Set-Location $root
        & pwsh -File '%ROOT%\run-integration-tests-workflow.ps1' 2>&1
    } -ArgumentList '%ROOT%'
    
    # Collect all jobs
    $jobs = @($unitTestsJob)
    if ($sonarJob) { $jobs += $sonarJob }
    if ($stressJob) { $jobs += $stressJob }
    $jobs += $integrationJob
    
    # Monitor progress
    $completed = @()
    $failed = @()
    
    while ($jobs.Count -gt 0) {
        foreach ($job in $jobs) {
            if ($job.State -eq 'Completed') {
                $result = Receive-Job -Job $job
                $completed += $job.Name
                Write-Host \"[COMPLETED] $($job.Name) workflow finished\"
                $jobs = $jobs | Where-Object { $_.Id -ne $job.Id }
                Remove-Job -Job $job
            } elseif ($job.State -eq 'Failed') {
                $error = Receive-Job -Job $job
                $failed += $job.Name
                Write-Host \"[FAILED] $($job.Name) workflow failed\" -ForegroundColor Red
                $jobs = $jobs | Where-Object { $_.Id -ne $job.Id }
                Remove-Job -Job $job
            }
        }
        Start-Sleep -Seconds 2
    }
    
    # Report final results
    Write-Host \"\"
    Write-Host \"=== Workflow Execution Summary ===\" 
    Write-Host \"Completed: $($completed.Count)\"
    Write-Host \"Failed: $($failed.Count)\"
    
    if ($failed.Count -gt 0) {
        Write-Host \"Failed workflows: $($failed -join ', ')\" -ForegroundColor Red
        exit 1
    } else {
        Write-Host \"All workflows completed successfully!\" -ForegroundColor Green
        exit 0
    }
}"

set WORKFLOW_EXIT=%ERRORLEVEL%

echo.
echo === Final Results ===
if %WORKFLOW_EXIT%==0 (
    echo ✅ All workflows completed successfully!
    echo Check workflow-logs\ directory for detailed output.
) else (
    echo ❌ One or more workflows failed.
    echo Check workflow-logs\ directory for error details.
)

popd
endlocal
exit /b %WORKFLOW_EXIT%

REM ================ HELPER FUNCTIONS ================

:check_dotnet
where dotnet >NUL 2>&1
if errorlevel 1 (
    echo ❌ .NET SDK not found. Please install .NET 8.0 or later.
    echo    Download from: https://dotnet.microsoft.com/download
    exit /b 1
)
for /f "tokens=*" %%i in ('dotnet --version 2^>nul') do set DOTNET_VERSION=%%i
echo ✅ .NET SDK: %DOTNET_VERSION%
exit /b 0

:check_java
where java >NUL 2>&1
if errorlevel 1 exit /b 1
for /f "tokens=3" %%i in ('java -version 2^>^&1 ^| findstr "version"') do (
    set JAVA_VERSION=%%i
    set JAVA_VERSION=!JAVA_VERSION:"=!
)
echo ✅ Java: %JAVA_VERSION%
exit /b 0

:check_docker
docker info >NUL 2>&1
if errorlevel 1 exit /b 1
echo ✅ Docker is running
exit /b 0

:check_pwsh
where pwsh >NUL 2>&1
if errorlevel 1 (
    echo ❌ PowerShell Core ^(pwsh^) not found. 
    echo    Please install PowerShell 7+ from: https://github.com/PowerShell/PowerShell
    exit /b 1
)
for /f "tokens=*" %%i in ('pwsh -Command "$PSVersionTable.PSVersion.ToString()" 2^>nul') do set PWSH_VERSION=%%i
echo ✅ PowerShell Core: %PWSH_VERSION%
exit /b 0