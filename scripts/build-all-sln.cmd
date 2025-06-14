@echo off
REM build-all-sln.cmd - Restore and build all major solutions in sequence.
REM Usage: double-click or run from command prompt.
REM 
REM For advanced build with warning detection, use: scripts/local-build-analysis.ps1

setlocal

REM Navigate to repository root (parent of scripts folder)
pushd "%~dp0\.."
REM If the script is directly in C:\GitHub\FLINK.NET\, then ROOT is just the current directory
set "ROOT=%CD%"

REM Ensure dotnet CLI is available
where dotnet >NUL 2>&1
if errorlevel 1 (
    echo .NET SDK not found. Please install .NET 8.0 or later.
    echo.
    pause
    exit /b 1
)

REM Use the correct solution paths relative to ROOT
call :BuildSolution "%ROOT%\FlinkDotNet\FlinkDotNet.sln"
if errorlevel 1 goto :BuildError
call :BuildSolution "%ROOT%\FlinkDotNetAspire\FlinkDotNetAspire.sln"
if errorlevel 1 goto :BuildError
call :BuildSolution "%ROOT%\FlinkDotNet.WebUI\FlinkDotNet.WebUI.sln"
if errorlevel 1 goto :BuildError

popd
endlocal
echo.
echo All solutions built successfully.
echo.
pause
exit /b 0

:BuildSolution
set "SLN=%~1"

if not exist "%SLN%" (
    echo Solution not found: %SLN%
    echo.
    exit /b 1
)

echo === Restoring %SLN% ===
dotnet restore "%SLN%"
if errorlevel 1 (
    echo Error restoring %SLN%
    echo.
    exit /b %errorlevel%
)

echo === Building %SLN% ===
dotnet build "%SLN%"
if errorlevel 1 (
    echo Error building %SLN%
    echo.
    exit /b %errorlevel%
)

echo.
exit /b 0

:BuildError
popd
endlocal
echo.
echo An error occurred during the build process.
echo.
pause
exit /b 1