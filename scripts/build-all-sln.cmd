@echo off
REM build-all-sln.cmd - Restore and build all major solutions in sequence.
REM Usage: double-click or run from command prompt.

setlocal

REM Navigate to repository root
pushd "%~dp0\.."
set "ROOT=%CD%"

REM Ensure dotnet CLI is available
where dotnet >NUL 2>&1
if errorlevel 1 (
    echo .NET SDK not found. Please install .NET 8.0 or later.
    exit /b 1
)

call :BuildSolution "%ROOT%\FlinkDotNet\FlinkDotNet.sln"
call :BuildSolution "%ROOT%\FlinkDotNetAspire\FlinkDotNetAspire.sln"
call :BuildSolution "%ROOT%\FlinkDotNet.WebUI\FlinkDotNet.WebUI.sln"

popd
endlocal
exit /b 0

:BuildSolution
set "SLN=%~1"

if not exist "%SLN%" (
    echo Solution not found: %SLN%
    exit /b 1
)

echo === Restoring %SLN% ===
dotnet restore "%SLN%"
if errorlevel 1 exit /b %errorlevel%

echo === Building %SLN% ===
dotnet build "%SLN%"
if errorlevel 1 exit /b %errorlevel%

echo.
exit /b 0
