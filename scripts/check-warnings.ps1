#!/usr/bin/env pwsh

<#
.SYNOPSIS
    Check for build warnings across all solutions
.DESCRIPTION
    This script builds all solutions and reports any warnings found.
    Useful for enforcing warning-free builds in CI/CD pipelines.
#>

param(
    [switch]$FailOnWarnings = $false
)

$solutions = @(
    "FlinkDotNet/FlinkDotNet.sln",
    "FlinkDotNet.WebUI/FlinkDotNet.WebUI.sln", 
    "FlinkDotNetAspire/FlinkDotNetAspire.sln"
)

$totalWarnings = 0
$buildFailed = $false

Write-Host "🔍 Checking for warnings across all solutions..." -ForegroundColor Yellow

foreach ($solution in $solutions) {
    Write-Host "`n📁 Building: $solution" -ForegroundColor Cyan
    
    $output = & dotnet build $solution --verbosity minimal 2>&1
    $exitCode = $LASTEXITCODE
    
    if ($exitCode -ne 0) {
        Write-Host "❌ Build failed for $solution" -ForegroundColor Red
        $buildFailed = $true
        continue
    }
    
    $warnings = $output | Where-Object { $_ -match "warning" }
    $warningCount = $warnings.Count
    
    if ($warningCount -gt 0) {
        Write-Host "⚠️  Found $warningCount warning(s) in $solution" -ForegroundColor Yellow
        $warnings | ForEach-Object { Write-Host "  $_" -ForegroundColor Gray }
        $totalWarnings += $warningCount
    } else {
        Write-Host "✅ No warnings in $solution" -ForegroundColor Green
    }
}

Write-Host "`n📊 Summary:" -ForegroundColor White
Write-Host "  Total warnings: $totalWarnings" -ForegroundColor $(if ($totalWarnings -eq 0) { "Green" } else { "Yellow" })

if ($buildFailed) {
    Write-Host "❌ Some builds failed" -ForegroundColor Red
    exit 1
}

if ($FailOnWarnings -and $totalWarnings -gt 0) {
    Write-Host "❌ Failing due to warnings (FailOnWarnings enabled)" -ForegroundColor Red
    exit 1
}

if ($totalWarnings -eq 0) {
    Write-Host "🎉 All solutions build warning-free!" -ForegroundColor Green
} else {
    Write-Host "⚠️  Please address the warnings above" -ForegroundColor Yellow
}

exit $($totalWarnings -gt 0 ? 1 : 0)