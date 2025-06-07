<#
.SYNOPSIS
    Generates a code metrics report using Metrics.exe, parses it, and checks against predefined thresholds.
.DESCRIPTION
    This script invokes the Visual Studio Metrics.exe tool to analyze a given solution or specific assemblies.
    It then parses the XML output and checks Cyclomatic Complexity, Maintainability Index, and Class Coupling
    against configurable thresholds.

    Prerequisites:
    - Visual Studio Professional or Enterprise (for Metrics.exe).
    - The path to Metrics.exe might need adjustment based on VS version and installation.
.PARAMETER MetricsExePath
    The full path to Metrics.exe.
.PARAMETER SolutionPath
    The full path to the .sln file to analyze. Can be relative to the script's location.
.PARAMETER OutputXmlPath
    The path where the raw XML metrics report will be saved. Can be relative to the script's location.
.PARAMETER FailBuildOnError
    If $true, the script will exit with a non-zero code if any 'Error' thresholds are breached.
    If $false, it will only print warnings and errors but always exit with 0.
#>
param(
    [string]$MetricsExePath = "C:\Program Files\Microsoft Visual Studio2\Enterprise\Team Tools\Static Analysis Tools\FxCop\Metrics.exe", # Default for VS 2022 Enterprise
    [string]$SolutionPath = "..\FlinkDotNet\FlinkDotNet.sln", # Assuming script is in 'scripts' dir, solution is one level up in 'FlinkDotNet'
    [string]$OutputXmlPath = ".\metrics_report.xml", # Output XML next to the script
    [bool]$FailBuildOnError = $true # Default to failing the build on error
)

#region Threshold Definitions
# Cyclomatic Complexity (Method)
$CyclomaticComplexityMethodWarning = 15
$CyclomaticComplexityMethodError = 25

# Cyclomatic Complexity (Type - Sum of members, if Metrics.exe provides it directly for type)
$CyclomaticComplexityTypeWarning = 100
$CyclomaticComplexityTypeError = 150

# Maintainability Index (Lower is worse. Range 0-100)
$MaintainabilityIndexWarning = 20 # Warn if < 20
$MaintainabilityIndexError = 10   # Error if < 10

# Class Coupling (Type)
$ClassCouplingWarning = 10
$ClassCouplingError = 15
#endregion Threshold Definitions

# --- Script Body ---
Write-Output "Starting Code Metrics Analysis..."
# Set location to script's directory for relative paths. $MyInvocation.MyCommand.Definition is script path when sourced/run.
$ScriptDirectory = Split-Path -Parent $MyInvocation.MyCommand.Definition
Push-Location $ScriptDirectory

# Validate Metrics.exe path
if (-not (Test-Path $MetricsExePath -PathType Leaf)) {
    Write-Error "Metrics.exe not found at specified path: '$MetricsExePath'. Please verify the path and VS installation."
    Pop-Location
    exit 1
}

# Resolve SolutionPath (now relative to script's dir after Push-Location)
$AbsoluteSolutionPath = Resolve-Path $SolutionPath -ErrorAction SilentlyContinue
if (-not $AbsoluteSolutionPath -or -not (Test-Path $AbsoluteSolutionPath -PathType Leaf)) {
    Write-Error "Solution file not found at: '$SolutionPath' (resolved to '$AbsoluteSolutionPath'). Current script directory: '$ScriptDirectory'"
    Pop-Location
    exit 1
}
Write-Output "Analyzing solution: '$AbsoluteSolutionPath'"

# Resolve OutputXmlPath (now relative to script's dir after Push-Location)
$AbsoluteOutputXmlPath = Resolve-Path $OutputXmlPath -ErrorAction SilentlyContinue
if (-not $AbsoluteOutputXmlPath) { # If it doesn't exist, just get the absolute path string
    $AbsoluteOutputXmlPath = [System.IO.Path]::GetFullPath((Join-Path (Get-Location) $OutputXmlPath))
}
Write-Output "Metrics report will be saved to: '$AbsoluteOutputXmlPath'"
