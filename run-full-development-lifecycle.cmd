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

call :check_pwsh
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

echo Starting parallel test execution
echo Unit Tests: test-logs\unit-tests.log
echo Integration Tests: test-logs\integration-tests.log
if %SKIP_STRESS%==0 echo Stress Tests: test-logs\stress-tests.log
if %SKIP_RELIABILITY%==0 echo Reliability Tests: test-logs\reliability-tests.log
if %SKIP_SONAR%==0 echo SonarCloud: test-logs\sonarcloud.log
echo.

REM Start all tests in parallel using simple start command
start "Unit Tests" cmd /c "pwsh -File scripts\run-local-unit-tests.ps1 > test-logs\unit-tests.log 2>&1 & echo Unit Tests completed > test-logs\unit-tests.done"

start "Integration Tests" cmd /c "pwsh -File scripts\run-integration-tests-in-windows-os.ps1 > test-logs\integration-tests.log 2>&1 & echo Integration Tests completed > test-logs\integration-tests.done"

if %SKIP_STRESS%==0 (
    start "Stress Tests" cmd /c "pwsh -File scripts\run-local-stress-tests.ps1 > test-logs\stress-tests.log 2>&1 & echo Stress Tests completed > test-logs\stress-tests.done"
)

if %SKIP_RELIABILITY%==0 (
    start "Reliability Tests" cmd /c "pwsh -File scripts\run-local-reliability-tests.ps1 > test-logs\reliability-tests.log 2>&1 & echo Reliability Tests completed > test-logs\reliability-tests.done"
)

if %SKIP_SONAR%==0 (
    start "SonarCloud" cmd /c "pwsh -File scripts\run-local-sonarcloud.ps1 > test-logs\sonarcloud.log 2>&1 & echo SonarCloud completed > test-logs\sonarcloud.done"
)

REM Wait for all tests to complete
echo Waiting for all tests to complete
:wait_loop
set ALL_DONE=1

if not exist test-logs\unit-tests.done set ALL_DONE=0
if not exist test-logs\integration-tests.done set ALL_DONE=0

if %SKIP_STRESS%==0 (
    if not exist test-logs\stress-tests.done set ALL_DONE=0
)

if %SKIP_RELIABILITY%==0 (
    if not exist test-logs\reliability-tests.done set ALL_DONE=0
)

if %SKIP_SONAR%==0 (
    if not exist test-logs\sonarcloud.done set ALL_DONE=0
)

if !ALL_DONE!==0 (
    timeout /t 5 /nobreak >nul
    goto wait_loop
)

echo.
echo === All Tests Completed ===
echo [OK] Complete development lifecycle finished!
echo.
echo Check test-logs\ directory for detailed results:
dir test-logs\*.log

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

:check_pwsh
where pwsh >NUL 2>&1
if errorlevel 1 (
    echo [ERROR] PowerShell Core (pwsh) not found. 
    echo          Please install PowerShell 7+ from: https://github.com/PowerShell/PowerShell
    exit /b 1
)
for /f "tokens=*" %%i in ('pwsh -Command "$PSVersionTable.PSVersion.ToString()" 2^>nul') do set PWSH_VERSION=%%i
echo [OK] PowerShell Core: !PWSH_VERSION!
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