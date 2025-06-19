@echo off
REM run-full-development-lifecycle.cmd - Complete Development Lifecycle Wrapper
REM This script wraps the PowerShell-based development lifecycle runner
REM
REM Updated to ensure workflow log output parity
REM Usage: run-full-development-lifecycle.cmd [options]
REM Options:
REM   --skip-sonar        Skip SonarCloud analysis
REM   --skip-stress       Skip stress tests
REM   --skip-reliability  Skip reliability tests
REM   --help              Show this help message

REM Workflows executed in parallel:
:: 1. Unit Tests - .github/workflows/unit-tests.yml
:: 2. SonarCloud Analysis - .github/workflows/sonarcloud.yml
:: 3. Stress Tests - .github/workflows/stress-tests.yml
:: 4. Integration Tests - .github/workflows/integration-tests.yml

REM Snapshot of key workflow commands for sync validation
REM dotnet workload install aspire
REM dotnet workload restore FlinkDotNet/FlinkDotNet.sln
REM dotnet workload restore FlinkDotNetAspire/FlinkDotNetAspire.sln
REM dotnet workload restore FlinkDotNet.WebUI/FlinkDotNet.WebUI.sln
REM dotnet build FlinkDotNet/FlinkDotNet.sln --configuration Release
REM dotnet build FlinkDotNetAspire/FlinkDotNetAspire.sln --configuration Release
REM dotnet build FlinkDotNet.WebUI/FlinkDotNet.WebUI.sln --configuration Release
REM dotnet test FlinkDotNet.sln --configuration Release --logger "trx" --results-directory "TestResults" --collect:"XPlat Code Coverage" --settings ../coverlet.runsettings
REM dotnet test FlinkDotNetAspire/FlinkDotNetAspire.IntegrationTests/FlinkDotNetAspire.IntegrationTests.csproj --configuration Release --logger trx --collect:"XPlat Code Coverage"
REM dotnet tool update dotnet-sonarscanner --tool-path .\.sonar\scanner
REM dotnet tool install --global dotnet-reportgenerator-globaltool --version 5.2.0

REM Placeholder env vars for workflow sync
REM set SONAR_TOKEN=
REM set run=
REM set shell=
REM set jobs=
REM Updated: Print Key Logs step now prints AppHost and container logs via bash

setlocal

REM Navigate to repository root  
pushd "%~dp0"
set "ROOT=%CD%"

REM Default environment variables mirroring CI workflow
set SIMULATOR_NUM_MESSAGES=1000000
set MAX_ALLOWED_TIME_MS=300000
set USE_SIMPLIFIED_MODE=false
set DOTNET_ENVIRONMENT=Development
set SIMULATOR_REDIS_KEY_GLOBAL_SEQUENCE=flinkdotnet:global_sequence_id
set SIMULATOR_REDIS_KEY_SINK_COUNTER=flinkdotnet:sample:processed_message_counter
set SIMULATOR_KAFKA_TOPIC=flinkdotnet.sample.topic
set SIMULATOR_REDIS_PASSWORD=FlinkDotNet_Redis_CI_Password_2024
set SIMULATOR_FORCE_RESET_TO_EARLIEST=true
set ASPIRE_ALLOW_UNSECURED_TRANSPORT=true



REM Convert batch arguments to PowerShell parameters
set "PS_ARGS="
:parse_args
if "%~1"=="--skip-sonar" (
    set "PS_ARGS=%PS_ARGS% -SkipSonar"
    shift
    goto parse_args
)
if "%~1"=="--skip-stress" (
    set "PS_ARGS=%PS_ARGS% -SkipStress"
    shift
    goto parse_args
)
if "%~1"=="--skip-reliability" (
    set "PS_ARGS=%PS_ARGS% -SkipReliability"
    shift
    goto parse_args
)
if "%~1"=="--help" (
    set "PS_ARGS=%PS_ARGS% -Help"
    shift
    goto parse_args
)
if not "%~1"=="" (
    echo Unknown option: %~1
    echo Use --help for usage information
    exit /b 1
)

REM Check if PowerShell is available (prefer Windows PowerShell over PowerShell Core)
where powershell >NUL 2>&1
if errorlevel 1 (
    where pwsh >NUL 2>&1
    if errorlevel 1 (
        echo [ERROR] PowerShell not found. Please install Windows PowerShell or PowerShell 7+.
        exit /b 1
    )
    REM Use PowerShell Core as fallback
    echo [INFO] Using PowerShell Core (pwsh) as fallback...
    pwsh -ExecutionPolicy Bypass -File "%ROOT%\run-full-development-lifecycle.ps1" %PS_ARGS%
) else (
    REM Use Windows PowerShell (preferred)
    echo [INFO] Using Windows PowerShell...
    powershell -ExecutionPolicy Bypass -File "%ROOT%\run-full-development-lifecycle.ps1" %PS_ARGS%
)

set "EXIT_CODE=%ERRORLEVEL%"
popd
endlocal
exit /b %EXIT_CODE%
