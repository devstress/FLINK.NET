@echo off
REM run-full-development-lifecycle.cmd - Complete Development Lifecycle with Parallel Testing
REM This script builds all solutions and runs all tests in parallel like GitHub workflows
REM 
REM Lifecycle steps:
REM 1. Build All Solutions  - Restore and build all .NET solutions
REM 2. Parallel Testing     - Run all test suites simultaneously
REM    - Unit Tests         - .NET unit tests with coverage
REM    - Integration Tests  - Aspire integration tests
REM    - Stress Tests       - Aspire stress tests with containers
REM    - Reliability Tests  - Fault tolerance testing
REM    - SonarCloud         - Code analysis and quality checks
REM
REM Usage: run-full-development-lifecycle.cmd [options]
REM Options:
REM   --skip-sonar        Skip SonarCloud analysis 
REM   --skip-stress       Skip stress tests
REM   --skip-reliability  Skip reliability tests
REM   --help              Show this help message

setlocal enabledelayedexpansion

REM Check if running as administrator
call :check_admin
if errorlevel 1 (
    echo [ERROR] This script requires administrator privileges.
    echo         Please run as Administrator for Docker operations and installations.
    exit /b 1
)
echo [OK] Administrator privileges confirmed

REM Parse command line arguments
set SKIP_SONAR=0
set SKIP_STRESS=0
set SKIP_RELIABILITY=0

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
if "%~1"=="--skip-reliability" (
    set SKIP_RELIABILITY=1
    shift
    goto parse_args
)
if "%~1"=="--help" (
    echo Complete Development Lifecycle Script
    echo.
    echo Builds all solutions and runs all tests in parallel:
    echo   1. Build all .NET solutions
    echo   2. Run unit tests, integration tests, stress tests in parallel
    echo   3. Run SonarCloud analysis and reliability tests
    echo.
    echo Options:
    echo   --skip-sonar        Skip SonarCloud analysis
    echo   --skip-stress       Skip stress tests
    echo   --skip-reliability  Skip reliability tests
    echo   --help              Show this help
    echo.
    exit /b 0
)
if not "%~1"=="" (
    echo Unknown option: %~1
    echo Use --help for usage information
    exit /b 1
)

REM Navigate to repository root
pushd "%~dp0"
set "ROOT=%CD%"

echo ================================================================
echo    Complete Development Lifecycle - Build All + Parallel Tests
echo ================================================================
echo Repository: %ROOT%
echo Timestamp: %DATE% %TIME%
echo.

REM Quick prerequisites check
echo === Quick Prerequisites Check ===
call :check_dotnet
if errorlevel 1 exit /b 1

if %SKIP_SONAR%==0 (
    call :check_java
    if errorlevel 1 (
        echo [WARNING] Java not found. SonarCloud analysis will be skipped.
        set SKIP_SONAR=1
    )
)

call :check_docker
if errorlevel 1 (
    echo [WARNING] Docker not available. Stress and reliability tests will be skipped.
    set SKIP_STRESS=1
    set SKIP_RELIABILITY=1
)

call :check_powershell
if errorlevel 1 exit /b 1

echo Prerequisites check completed.
echo.

REM Step 1: Build All Solutions (like build-all.cmd)
echo === Step 1: Building All Solutions ===
call :BuildSolution "%ROOT%\FlinkDotNet\FlinkDotNet.sln"
if errorlevel 1 goto :BuildError

call :BuildSolution "%ROOT%\FlinkDotNetAspire\FlinkDotNetAspire.sln" 
if errorlevel 1 goto :BuildError

call :BuildSolution "%ROOT%\FlinkDotNet.WebUI\FlinkDotNet.WebUI.sln"
if errorlevel 1 goto :BuildError

echo [OK] All solutions built successfully!
echo.

REM Step 2: Run All Tests in Parallel
echo === Step 2: Running All Tests in Parallel ===

REM Create logs directory
if not exist "%ROOT%\test-logs" mkdir "%ROOT%\test-logs"

REM Set environment variables for tests
if not defined SIMULATOR_NUM_MESSAGES set SIMULATOR_NUM_MESSAGES=1000000
if not defined FLINKDOTNET_STANDARD_TEST_MESSAGES set FLINKDOTNET_STANDARD_TEST_MESSAGES=100000
set ASPIRE_ALLOW_UNSECURED_TRANSPORT=true

echo Starting parallel test execution (running silently in background)
echo.

REM Start all tests in parallel using simple background processes with proper logging
echo [INFO] Starting Unit Tests (log: %ROOT%\test-logs\unit-tests.log)...
start "" /B cmd /c powershell -File "%ROOT%\scripts\run-local-unit-tests.ps1" ^> "%ROOT%\test-logs\unit-tests.log" 2^>^&1 ^&^& echo [OK] Unit Tests completed successfully ^> "%ROOT%\test-logs\unit-tests.status" ^|^| echo [ERROR] Unit Tests failed ^> "%ROOT%\test-logs\unit-tests.status"

echo [INFO] Starting Integration Tests (log: %ROOT%\test-logs\integration-tests.log)...
start "" /B cmd /c powershell -File "%ROOT%\scripts\run-integration-tests-in-windows-os.ps1" ^> "%ROOT%\test-logs\integration-tests.log" 2^>^&1

if %SKIP_STRESS%==0 (
    echo [INFO] Starting Stress Tests (log: %ROOT%\test-logs\stress-tests.log)...
    start "" /B cmd /c powershell -File "%ROOT%\scripts\run-local-stress-tests.ps1" ^> "%ROOT%\test-logs\stress-tests.log" 2^>^&1 ^&^& echo [OK] Stress Tests completed successfully ^> "%ROOT%\test-logs\stress-tests.status" ^|^| echo [ERROR] Stress Tests failed ^> "%ROOT%\test-logs\stress-tests.status"
)

if %SKIP_RELIABILITY%==0 (
    echo [INFO] Starting Reliability Tests (log: %ROOT%\test-logs\reliability-tests.log)...
    start "" /B cmd /c powershell -File "%ROOT%\scripts\run-local-reliability-tests.ps1" ^> "%ROOT%\test-logs\reliability-tests.log" 2^>^&1 ^&^& echo [OK] Reliability Tests completed successfully ^> "%ROOT%\test-logs\reliability-tests.status" ^|^| echo [ERROR] Reliability Tests failed ^> "%ROOT%\test-logs\reliability-tests.status"
)

if %SKIP_SONAR%==0 (
    echo [INFO] Starting SonarCloud Analysis (log: %ROOT%\test-logs\sonarcloud.log)...
    start "" /B cmd /c powershell -File "%ROOT%\scripts\run-local-sonarcloud.ps1" ^> "%ROOT%\test-logs\sonarcloud.log" 2^>^&1 ^&^& echo [OK] SonarCloud completed successfully ^> "%ROOT%\test-logs\sonarcloud.status" ^|^| echo [ERROR] SonarCloud failed ^> "%ROOT%\test-logs\sonarcloud.status"
)

echo.
echo [INFO] All tests started in background. Monitoring progress...
echo.

set MONITOR_ARGS=-LogDir "%ROOT%\test-logs"
if %SKIP_SONAR%==1 set MONITOR_ARGS=!MONITOR_ARGS! -SkipSonar
if %SKIP_STRESS%==1 set MONITOR_ARGS=!MONITOR_ARGS! -SkipStress
if %SKIP_RELIABILITY%==1 set MONITOR_ARGS=!MONITOR_ARGS! -SkipReliability

powershell -ExecutionPolicy Bypass -File "%ROOT%\scripts\monitor-test-progress.ps1" !MONITOR_ARGS!

echo.
echo === All Tests Completed ===
echo [OK] Complete development lifecycle finished!
echo.
echo === Test Results and Logs ===
echo Check the following log files for detailed results:
if exist test-logs\unit-tests.log echo   - Unit Tests: test-logs\unit-tests.log
if exist test-logs\integration-tests.log echo   - Integration Tests: test-logs\integration-tests.log
if exist test-logs\stress-tests.log echo   - Stress Tests: test-logs\stress-tests.log
if exist test-logs\reliability-tests.log echo   - Reliability Tests: test-logs\reliability-tests.log
if exist test-logs\sonarcloud.log echo   - SonarCloud Analysis: test-logs\sonarcloud.log
echo.
echo Log directory contents:
dir test-logs\*.log /B 2>nul || echo   No log files found

popd
endlocal
exit /b 0

REM ================ HELPER FUNCTIONS ================

:check_admin
REM Check if running with administrator privileges
net session >nul 2>&1
if errorlevel 1 exit /b 1
exit /b 0

:check_dotnet
where dotnet >NUL 2>&1
if errorlevel 1 (
    echo [ERROR] .NET SDK not found. Please install .NET 8.0 or later.
    echo          Download from: https://dotnet.microsoft.com/download
    exit /b 1
)
for /f "tokens=*" %%i in ('dotnet --version 2^>nul') do set DOTNET_VERSION=%%i
echo [OK] .NET SDK: !DOTNET_VERSION!
exit /b 0

:check_java
where java >NUL 2>&1
if errorlevel 1 exit /b 1
for /f "tokens=3" %%i in ('java -version 2^>^&1 ^| findstr "version"') do (
    set JAVA_VERSION=%%i
    set JAVA_VERSION=!JAVA_VERSION:~1,-1!
)
echo [OK] Java: !JAVA_VERSION!
exit /b 0

:check_docker
REM First check if Docker Desktop is installed
if exist "%ProgramFiles%\Docker\Docker\Docker Desktop.exe" (
    echo [INFO] Docker Desktop found. Checking if running
    docker info >NUL 2>&1
    if errorlevel 1 (
        echo [INFO] Docker Desktop not running. Attempting to start
        start "" "%ProgramFiles%\Docker\Docker\Docker Desktop.exe"
        echo [INFO] Waiting for Docker Desktop to start (timeout: 60 seconds)
        set /a TIMEOUT_COUNT=0
        :docker_wait_loop
        timeout /t 2 /nobreak >nul
        docker info >NUL 2>&1
        if not errorlevel 1 (
            echo [OK] Docker Desktop started successfully
            exit /b 0
        )
        set /a TIMEOUT_COUNT+=2
        if !TIMEOUT_COUNT! LSS 60 goto docker_wait_loop
        echo [ERROR] Docker Desktop startup timeout after 60 seconds. Please start Docker Desktop manually.
        exit /b 1
    ) else (
        echo [OK] Docker Desktop is already running
        exit /b 0
    )
) else (
    echo [ERROR] Docker Desktop is not installed
    exit /b 1
)

:check_powershell
where powershell >NUL 2>&1
if errorlevel 1 (
    echo [ERROR] Windows PowerShell not found. 
    echo          Please ensure Windows PowerShell is available on your system.
    exit /b 1
)
for /f "tokens=*" %%i in ('powershell -Command "$PSVersionTable.PSVersion.ToString()" 2^>nul') do set POWERSHELL_VERSION=%%i
echo [OK] Windows PowerShell: !POWERSHELL_VERSION!
exit /b 0

:BuildSolution
set "SLN=%~1"

if not exist "%SLN%" (
    echo Solution not found: %SLN%
    exit /b 1
)

echo === Restoring %SLN% ===
dotnet restore "%SLN%"
if errorlevel 1 (
    echo Error restoring %SLN%
    exit /b %errorlevel%
)

echo === Building %SLN% ===
dotnet build "%SLN%"
if errorlevel 1 (
    echo Error building %SLN%
    exit /b %errorlevel%
)

echo.
exit /b 0

:BuildError
popd
endlocal
echo.
echo [ERROR] Build failed. Please check the error messages above.
echo.
exit /b 1