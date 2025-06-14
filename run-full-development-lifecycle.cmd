@echo off
REM run-full-development-lifecycle.cmd - Run all GitHub workflows locally in parallel
REM This script mirrors the GitHub Actions workflows to run locally for development validation
REM 
REM Workflows executed in parallel:
REM 1. Unit Tests          - Runs .NET unit tests with coverage collection
REM 2. SonarCloud Analysis - Builds solutions and performs code analysis  
REM 3. Stress Tests        - Runs Aspire stress tests with Redis/Kafka
REM 4. Integration Tests   - Runs Aspire integration tests
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
    echo Usage: run-full-development-lifecycle.cmd [options]
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

REM Navigate to repository root (since script is now in root)
pushd "%~dp0"
set "ROOT=%CD%"

echo ================================================================
echo    Local GitHub Workflows Runner - Parallel Execution
echo ================================================================
echo Repository: %ROOT%
echo Timestamp: %DATE% %TIME%
echo.

REM Check prerequisites in parallel for improved speed
echo === Checking Prerequisites ===

REM Use PowerShell for parallel prerequisite checking
powershell -Command "& {
    Write-Host 'Running prerequisite checks in parallel...'
    
    # Start parallel jobs for each prerequisite check
    $dotnetJob = Start-Job -ScriptBlock {
        try {
            where.exe dotnet 2>$null
            if ($LASTEXITCODE -eq 0) {
                $version = & dotnet --version 2>$null
                return @{Success=$true; Tool='dotnet'; Version=$version; Message='✅ .NET SDK: ' + $version}
            } else {
                return @{Success=$false; Tool='dotnet'; Message='❌ .NET SDK not found. Please install .NET 8.0 or later.'; Url='https://dotnet.microsoft.com/download'}
            }
        } catch {
            return @{Success=$false; Tool='dotnet'; Message='❌ .NET SDK check failed'; Error=$_.Exception.Message}
        }
    }
    
    $javaJob = $null
    if (%SKIP_SONAR% -eq 0) {
        $javaJob = Start-Job -ScriptBlock {
            try {
                where.exe java 2>$null
                if ($LASTEXITCODE -eq 0) {
                    $versionOutput = & java -version 2>&1
                    $version = ($versionOutput | Select-String 'version' | Select-Object -First 1).ToString()
                    if ($version -match '\"([^\"]+)\"') {
                        $cleanVersion = $matches[1]
                        return @{Success=$true; Tool='java'; Version=$cleanVersion; Message='✅ Java: ' + $cleanVersion}
                    } else {
                        return @{Success=$true; Tool='java'; Version='unknown'; Message='✅ Java: found'}
                    }
                } else {
                    return @{Success=$false; Tool='java'; Message='❌ Java not found.'; Url='https://adoptopenjdk.net/'}
                }
            } catch {
                return @{Success=$false; Tool='java'; Message='❌ Java check failed'; Error=$_.Exception.Message}
            }
        }
    }
    
    $dockerJob = $null
    if (%SKIP_STRESS% -eq 0) {
        $dockerJob = Start-Job -ScriptBlock {
            try {
                # First check if Docker Desktop is installed
                $dockerDesktopExists = $false
                if (Test-Path "$env:ProgramFiles\Docker\Docker\Docker Desktop.exe") {
                    $dockerDesktopExists = $true
                } elseif (Test-Path "${env:ProgramFiles(x86)}\Docker\Docker\Docker Desktop.exe") {
                    $dockerDesktopExists = $true
                }
                
                if (-not $dockerDesktopExists) {
                    return @{Success=$false; Tool='docker'; Message='❌ Docker Desktop is not installed'; Url='https://docker.com/'}
                }
                
                # Then check if Docker is running
                & docker info 2>$null
                if ($LASTEXITCODE -eq 0) {
                    return @{Success=$true; Tool='docker'; Message='✅ Docker Desktop is installed and running'}
                } else {
                    return @{Success=$false; Tool='docker'; Message='❌ Docker Desktop is installed but not running. Please start Docker Desktop.'; Url='https://docker.com/'}
                }
            } catch {
                return @{Success=$false; Tool='docker'; Message='❌ Docker check failed'; Error=$_.Exception.Message}
            }
        }
    }
    
    $pwshJob = Start-Job -ScriptBlock {
        try {
            where.exe pwsh 2>$null
            if ($LASTEXITCODE -eq 0) {
                $version = & pwsh -Command '$PSVersionTable.PSVersion.ToString()' 2>$null
                return @{Success=$true; Tool='pwsh'; Version=$version; Message='✅ PowerShell Core: ' + $version}
            } else {
                return @{Success=$false; Tool='pwsh'; Message='❌ PowerShell Core (pwsh) not found.'; Url='https://github.com/PowerShell/PowerShell'}
            }
        } catch {
            return @{Success=$false; Tool='pwsh'; Message='❌ PowerShell Core check failed'; Error=$_.Exception.Message}
        }
    }
    
    # Collect all jobs
    $jobs = @($dotnetJob)
    if ($javaJob) { $jobs += $javaJob }
    if ($dockerJob) { $jobs += $dockerJob }
    $jobs += $pwshJob
    
    # Wait for all jobs and collect results
    $results = @()
    foreach ($job in $jobs) {
        $results += Wait-Job $job | Receive-Job
        Remove-Job $job
    }
    
    # Process results and handle installations
    $failedTools = @()
    $skipSonar = %SKIP_SONAR%
    $skipStress = %SKIP_STRESS%
    
    foreach ($result in $results) {
        Write-Host $result.Message
        if (-not $result.Success) {
            if ($result.Tool -eq 'dotnet' -or $result.Tool -eq 'pwsh') {
                $failedTools += $result.Tool
            } elseif ($result.Tool -eq 'java') {
                Write-Host 'WARNING: Java not found. SonarCloud analysis will be skipped.'
                $skipSonar = 1
            } elseif ($result.Tool -eq 'docker') {
                Write-Host 'WARNING: Docker not available. Stress tests will be skipped.'
                $skipStress = 1
            }
        }
    }
    
    # Update environment variables
    $env:SKIP_SONAR_UPDATED = $skipSonar
    $env:SKIP_STRESS_UPDATED = $skipStress
    
    # Return failed tools for installation
    return $failedTools
}"

REM Get the results from PowerShell execution
for /f "tokens=*" %%i in ('powershell -Command "$env:SKIP_SONAR_UPDATED"') do set SKIP_SONAR=%%i
for /f "tokens=*" %%i in ('powershell -Command "$env:SKIP_STRESS_UPDATED"') do set SKIP_STRESS=%%i

REM Handle tool installations if needed (these still need to be sequential due to system requirements)
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
            echo WARNING: Docker installation failed. Stress tests will be skipped.
            set SKIP_STRESS=1
        )
    )
)

call :check_pwsh
if errorlevel 1 call :install_pwsh
if errorlevel 1 exit /b 1

echo Prerequisites check completed.
echo.

REM Create output directories for parallel execution  
if not exist "%ROOT%\workflow-logs" mkdir "%ROOT%\workflow-logs"
if not exist "%ROOT%\scripts" mkdir "%ROOT%\scripts"

REM Set environment variables
if not defined SIMULATOR_NUM_MESSAGES set SIMULATOR_NUM_MESSAGES=1000000
if not defined MAX_ALLOWED_TIME_MS set MAX_ALLOWED_TIME_MS=1000
set ASPIRE_ALLOW_UNSECURED_TRANSPORT=true

echo === Starting Workflows in Parallel ===
echo Unit Tests: workflow-logs\unit-tests.log
echo SonarCloud: workflow-logs\sonarcloud.log
if %SKIP_STRESS%==0 echo Stress Tests: workflow-logs\stress-tests.log
if %SKIP_STRESS%==1 echo Stress Tests: SKIPPED (--skip-stress or Docker unavailable)
echo Integration Tests: workflow-logs\integration-tests.log
echo.

REM Start workflows in parallel using PowerShell background jobs
REM Note: Individual workflow scripts should already exist in scripts/ folder
powershell -Command "& {
    Write-Host 'Starting workflows in parallel...'
    # Start Unit Tests workflow
    $unitTestsJob = Start-Job -ScriptBlock {
        param($root)
        Set-Location $root
        & pwsh -File '%ROOT%\scripts\run-unit-tests-workflow.ps1' 2>&1
    } -ArgumentList '%ROOT%'
    
    # Start SonarCloud workflow (conditional)
    $sonarJob = $null
    if (%SKIP_SONAR% -eq 0) {
        $sonarJob = Start-Job -ScriptBlock {
            param($root)
            Set-Location $root
            & pwsh -File '%ROOT%\scripts\run-sonarcloud-workflow.ps1' 2>&1
        } -ArgumentList '%ROOT%'
    }
    
    # Start Stress Tests workflow (conditional)
    $stressJob = $null
    if (%SKIP_STRESS% -eq 0) {
        $stressJob = Start-Job -ScriptBlock {
            param($root)
            Set-Location $root
            & pwsh -File '%ROOT%\scripts\run-stress-tests-workflow.ps1' 2>&1
        } -ArgumentList '%ROOT%'
    }
    
    # Start Integration Tests workflow
    $integrationJob = Start-Job -ScriptBlock {
        param($root)
        Set-Location $root
        & pwsh -File '%ROOT%\scripts\run-integration-tests-workflow.ps1' 2>&1
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
        Start-Sleep -Seconds 1  # Reduced from 2 seconds to 1 second for faster monitoring
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
    echo ✅ Docker Desktop is installed
) else (
    if exist "%ProgramFiles(x86)%\Docker\Docker\Docker Desktop.exe" (
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
    echo    Please start Docker Desktop before running this script
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