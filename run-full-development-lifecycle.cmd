@echo off
REM run-full-development-lifecycle.cmd - Complete Development Lifecycle Wrapper
REM This script wraps the PowerShell-based development lifecycle runner
REM 
REM Usage: run-full-development-lifecycle.cmd [options]
REM Options:
REM   --skip-sonar        Skip SonarCloud analysis 
REM   --skip-stress       Skip stress tests
REM   --skip-reliability  Skip reliability tests
REM   --help              Show this help message

setlocal

REM Navigate to repository root  
pushd "%~dp0"
set "ROOT=%CD%"

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

REM Check if PowerShell is available
where pwsh >NUL 2>&1
if errorlevel 1 (
    where powershell >NUL 2>&1
    if errorlevel 1 (
        echo [ERROR] PowerShell not found. Please install PowerShell 7+ or Windows PowerShell.
        exit /b 1
    )
    REM Use Windows PowerShell as fallback
    echo [INFO] Using Windows PowerShell...
    powershell -ExecutionPolicy Bypass -File "%ROOT%\run-full-development-lifecycle.ps1" %PS_ARGS%
) else (
    REM Use PowerShell Core (preferred)
    echo [INFO] Using PowerShell Core...
    pwsh -File "%ROOT%\run-full-development-lifecycle.ps1" %PS_ARGS%
)

set "EXIT_CODE=%ERRORLEVEL%"
popd
endlocal
exit /b %EXIT_CODE%