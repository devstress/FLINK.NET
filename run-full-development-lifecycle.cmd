@echo off
REM run-full-development-lifecycle.cmd - Run all GitHub workflows locally in parallel
REM This script mirrors the GitHub Actions workflows to run locally for development validation
REM 
REM Workflows executed in parallel:
REM 1. Unit Tests          - Runs .NET unit tests with coverage collection
REM 2. SonarCloud Analysis - Builds solutions and performs code analysis  
REM 3. Stress Tests        - Runs Aspire stress tests with Redis/Kafka
REM 4. Reliability Tests   - Runs Aspire reliability tests with fault tolerance
REM 5. Integration Tests   - Runs Aspire integration tests
REM
REM Usage: run-full-development-lifecycle.cmd [options]
REM Options:
REM   --skip-sonar     Skip SonarCloud analysis (if SONAR_TOKEN not available)
REM   --skip-stress    Skip stress tests (if Docker not available)
REM   --help           Show this help message

setlocal enabledelayedexpansion

REM Check if running as administrator
call :check_admin
if errorlevel 1 (
    echo ❌ This script requires administrator privileges.
    echo    Please run as Administrator:
    echo    - Right-click on Command Prompt and select "Run as administrator"
    echo    - Or run from an elevated PowerShell prompt
    echo.
    echo    Administrator privileges are required for:
    echo    - Installing missing prerequisites ^(.NET, Java, Docker, PowerShell^)
    echo    - Docker Desktop operations
    echo    - System-wide tool installations
    exit /b 1
)
echo ✅ Administrator privileges confirmed

REM Parse command line arguments
set SKIP_SONAR=0
set SKIP_STRESS=0
set SKIP_RELIABILITY=0
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
if "%~1"=="--skip-reliability" (
    set SKIP_RELIABILITY=1
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
    echo   4. Reliability Tests   - Aspire reliability tests with fault tolerance
    echo   5. Integration Tests   - Aspire integration tests
    echo.
    echo Usage: run-full-development-lifecycle.cmd [options]
    echo.
    echo Options:
    echo   --skip-sonar         Skip SonarCloud analysis ^(useful if no SONAR_TOKEN^)
    echo   --skip-stress        Skip stress tests ^(useful if Docker unavailable^)
    echo   --skip-reliability   Skip reliability tests ^(useful if Docker unavailable^)
    echo   --help               Show this help message
    echo.
    echo Prerequisites:
    echo   - .NET 8.0 SDK
    echo   - Java 17+ ^(for SonarCloud^)
    echo   - Docker ^(for stress/integration tests^)
    echo   - PowerShell Core ^(pwsh^)
    echo.
    echo Environment Variables:
    echo   SONAR_TOKEN                      SonarCloud authentication token
    echo   SIMULATOR_NUM_MESSAGES           Number of messages for stress tests ^(default: 1000000^)
    echo   FLINKDOTNET_STANDARD_TEST_MESSAGES   Number of messages for reliability tests ^(default: 100000^)
    echo.
    exit /b 0
)

REM Navigate to repository root (since script is now in root)
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
if errorlevel 1 call :install_dotnet
if errorlevel 1 exit /b 1

if %SKIP_SONAR%==0 (
    call :check_java
    if errorlevel 1 (
        call :install_java
        if errorlevel 1 (
            echo WARNING: Java installation failed. SonarCloud analysis will be skipped.
            set SKIP_SONAR=1
        )
    )
)

if %SKIP_STRESS%==0 (
    call :check_docker
    if errorlevel 1 (
        call :install_docker
        if errorlevel 1 (
            echo WARNING: Docker installation failed. Stress and reliability tests will be skipped.
            set SKIP_STRESS=1
            set SKIP_RELIABILITY=1
        )
    )
)

if %SKIP_RELIABILITY%==0 (
    if %SKIP_STRESS%==1 (
        echo WARNING: Docker not available. Reliability tests will be skipped.
        set SKIP_RELIABILITY=1
    )
)

call :check_pwsh
if errorlevel 1 call :install_pwsh
if errorlevel 1 exit /b 1

echo Prerequisites check completed.
echo.

REM Create output directories for parallel execution  
if not exist "%ROOT%\workflow-logs" mkdir "%ROOT%\workflow-logs"

REM Set environment variables
if not defined SIMULATOR_NUM_MESSAGES set SIMULATOR_NUM_MESSAGES=1000000
if not defined FLINKDOTNET_STANDARD_TEST_MESSAGES set FLINKDOTNET_STANDARD_TEST_MESSAGES=100000
if not defined MAX_ALLOWED_TIME_MS set MAX_ALLOWED_TIME_MS=1000
set ASPIRE_ALLOW_UNSECURED_TRANSPORT=true

echo === Starting Workflows in Parallel ===
echo Unit Tests: workflow-logs\unit-tests.log
echo SonarCloud: workflow-logs\sonarcloud.log
if %SKIP_STRESS%==0 echo Stress Tests: workflow-logs\stress-tests.log
if %SKIP_STRESS%==1 echo Stress Tests: SKIPPED (--skip-stress or Docker unavailable)
if %SKIP_RELIABILITY%==0 echo Reliability Tests: workflow-logs\reliability-tests.log
if %SKIP_RELIABILITY%==1 echo Reliability Tests: SKIPPED (--skip-reliability or Docker unavailable)
echo Integration Tests: workflow-logs\integration-tests.log
echo.

REM Start workflows in parallel using PowerShell background jobs
powershell -Command "& {
    param($skipSonar, $skipStress, $skipReliability, $rootPath)
    
    # Start Unit Tests workflow
    $unitTestsJob = Start-Job -ScriptBlock {
        param($root)
        Set-Location $root
        & pwsh -File ($root + '\scripts\run-local-unit-tests.ps1') 2>&1
    } -ArgumentList $rootPath
    
    # Start SonarCloud workflow (conditional)
    $sonarJob = $null
    if ($skipSonar -eq 0) {
        $sonarJob = Start-Job -ScriptBlock {
            param($root)
            Set-Location $root
            & pwsh -File ($root + '\scripts\run-local-sonarcloud.ps1') 2>&1
        } -ArgumentList $rootPath
    }
    
    # Start Stress Tests workflow (conditional)
    $stressJob = $null
    if ($skipStress -eq 0) {
        $stressJob = Start-Job -ScriptBlock {
            param($root)
            Set-Location $root
            & pwsh -File ($root + '\scripts\run-local-stress-tests.ps1') 2>&1
        } -ArgumentList $rootPath
    }
    
    # Start Reliability Tests workflow (conditional)
    $reliabilityJob = $null
    if ($skipReliability -eq 0) {
        $reliabilityJob = Start-Job -ScriptBlock {
            param($root)
            Set-Location $root
            & pwsh -File ($root + '\scripts\run-local-reliability-tests.ps1') 2>&1
        } -ArgumentList $rootPath
    }
    
    # Start Integration Tests workflow
    $integrationJob = Start-Job -ScriptBlock {
        param($root)
        Set-Location $root
        & pwsh -File ($root + '\scripts\run-integration-tests-in-windows-os.ps1') 2>&1
    } -ArgumentList $rootPath
    
    # Collect all jobs
    $jobs = @($unitTestsJob)
    if ($sonarJob) { $jobs += $sonarJob }
    if ($stressJob) { $jobs += $stressJob }
    if ($reliabilityJob) { $jobs += $reliabilityJob }
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
}" %SKIP_SONAR% %SKIP_STRESS% %SKIP_RELIABILITY% "%ROOT%"

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

:check_admin
REM Check if running with administrator privileges
net session >nul 2>&1
if errorlevel 1 exit /b 1
exit /b 0

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
REM First check if Docker Desktop is installed
if exist "%ProgramFiles%\Docker\Docker\Docker Desktop.exe" (
    set "DOCKER_DESKTOP_PATH=%ProgramFiles%\Docker\Docker\Docker Desktop.exe"
    echo ✅ Docker Desktop is installed
) else (
    if exist "%ProgramFiles(x86)%\Docker\Docker\Docker Desktop.exe" (
        set "DOCKER_DESKTOP_PATH=%ProgramFiles(x86)%\Docker\Docker\Docker Desktop.exe"
        echo ✅ Docker Desktop is installed
    ) else (
        echo ❌ Docker Desktop is not installed
        exit /b 1
    )
)

REM Then check if Docker is running
docker info >NUL 2>&1
if errorlevel 1 (
    echo ❌ Docker Desktop is installed but not running
    echo    Attempting to start Docker Desktop...
    start "" "%DOCKER_DESKTOP_PATH%"
    
    REM Wait for Docker to start (max 60 seconds)
    set /a WAIT_COUNT=0
    :docker_wait_loop
    timeout /t 5 /nobreak >NUL 2>&1
    set /a WAIT_COUNT+=5
    docker info >NUL 2>&1
    if not errorlevel 1 (
        echo ✅ Docker Desktop started successfully
        exit /b 0
    )
    if %WAIT_COUNT% lss 60 goto docker_wait_loop
    
    echo ❌ Docker Desktop failed to start automatically
    echo    Please start Docker Desktop manually before running this script
    exit /b 1
)
echo ✅ Docker Desktop is running
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

:install_dotnet
echo 🔧 Installing .NET SDK...
echo Downloading .NET 8.0 installer...
powershell -Command "Invoke-WebRequest -Uri 'https://download.microsoft.com/download/a/b/c/abc6f64a-d78c-4e73-b5fb-ab0c2eaad1f4/dotnet-sdk-8.0.404-win-x64.exe' -OutFile 'dotnet-installer.exe'"
if errorlevel 1 (
    echo ❌ Failed to download .NET installer
    exit /b 1
)
echo Running .NET installer (this may take a few minutes)...
dotnet-installer.exe /quiet /norestart
set INSTALL_RESULT=%ERRORLEVEL%
del dotnet-installer.exe 2>nul
if %INSTALL_RESULT% neq 0 (
    echo ❌ .NET installation failed
    exit /b 1
)
echo ✅ .NET SDK installed successfully
echo Please restart your command prompt and run this script again
exit /b 0

:install_java
echo 🔧 Installing Java 17...
echo Downloading Microsoft OpenJDK 17...
powershell -Command "Invoke-WebRequest -Uri 'https://aka.ms/download-jdk/microsoft-jdk-17.0.7-windows-x64.msi' -OutFile 'openjdk-installer.msi'"
if errorlevel 1 (
    echo ❌ Failed to download Java installer
    exit /b 1
)
echo Running Java installer (this may take a few minutes)...
msiexec /i openjdk-installer.msi /quiet /norestart
set INSTALL_RESULT=%ERRORLEVEL%
del openjdk-installer.msi 2>nul
if %INSTALL_RESULT% neq 0 (
    echo ❌ Java installation failed
    exit /b 1
)
echo ✅ Java installed successfully
echo Please restart your command prompt and run this script again
exit /b 0

:install_docker
echo 🔧 Installing Docker Desktop...
echo Downloading Docker Desktop installer...
powershell -Command "Invoke-WebRequest -Uri 'https://desktop.docker.com/win/main/amd64/Docker%20Desktop%20Installer.exe' -OutFile 'docker-installer.exe'"
if errorlevel 1 (
    echo ❌ Failed to download Docker installer
    exit /b 1
)
echo Running Docker installer (this may take several minutes)...
docker-installer.exe install --quiet
set INSTALL_RESULT=%ERRORLEVEL%
del docker-installer.exe 2>nul
if %INSTALL_RESULT% neq 0 (
    echo ❌ Docker installation failed
    exit /b 1
)
echo ✅ Docker Desktop installed successfully
echo Please restart your computer and run this script again
exit /b 0

:install_pwsh
echo 🔧 Installing PowerShell Core...
echo Downloading PowerShell 7 installer...
powershell -Command "Invoke-WebRequest -Uri 'https://github.com/PowerShell/PowerShell/releases/download/v7.4.0/PowerShell-7.4.0-win-x64.msi' -OutFile 'pwsh-installer.msi'"
if errorlevel 1 (
    echo ❌ Failed to download PowerShell installer
    exit /b 1
)
echo Running PowerShell installer (this may take a few minutes)...
msiexec /i pwsh-installer.msi /quiet /norestart
set INSTALL_RESULT=%ERRORLEVEL%
del pwsh-installer.msi 2>nul
if %INSTALL_RESULT% neq 0 (
    echo ❌ PowerShell installation failed
    exit /b 1
)
echo ✅ PowerShell Core installed successfully
echo Please restart your command prompt and run this script again
exit /b 0