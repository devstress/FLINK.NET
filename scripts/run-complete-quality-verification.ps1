#!/usr/bin/env pwsh
<#
.SYNOPSIS
    Complete quality verification script that enforces all code quality requirements.

.DESCRIPTION
    This script demonstrates the complete quality verification process that must be 
    followed before any code submission. It includes:
    
    1. Clean build verification with warning detection
    2. All test suite execution (unit, integration, architecture)
    3. Stress test verification with local/CI alignment
    4. Final quality gates with zero-tolerance enforcement
    
    This serves as the reference implementation for the quality rules established
    in INSTRUCTIONS.md and .copilot/quality-rules.md

.PARAMETER SkipStressTests
    Skip stress test execution (not recommended for final verification)

.EXAMPLE
    ./scripts/run-complete-quality-verification.ps1
    Runs complete quality verification process.
#>

param(
    [switch]$SkipStressTests
)

$ErrorActionPreference = 'Stop'
$StartTime = Get-Date

Write-Host "=== FlinkDotNet Complete Quality Verification ===" -ForegroundColor Cyan
Write-Host "Started at: $($StartTime.ToString('yyyy-MM-dd HH:mm:ss')) UTC" -ForegroundColor White
Write-Host "Zero-tolerance quality enforcement in effect!" -ForegroundColor Yellow

$VerificationResults = @()
$TotalSteps = 6
$CurrentStep = 0

function Step-Complete {
    param([string]$StepName, [bool]$Success, [string]$Details = "")
    
    $global:CurrentStep++
    $status = if ($Success) { "✅ PASS" } else { "✅ FAIL" }
    
    Write-Host "`n[$global:CurrentStep/$TotalSteps] $StepName - $status" -ForegroundColor $(if ($Success) { "Green" } else { "Red" })
    if ($Details) {
        Write-Host "    $Details" -ForegroundColor Gray
    }
    
    $global:VerificationResults += [PSCustomObject]@{
        Step = $StepName
        Success = $Success
        Details = $Details
    }
    
    if (-not $Success) {
        Write-Host "`n✅ QUALITY GATE FAILURE - Zero tolerance policy violated!" -ForegroundColor Red
        Write-Host "Fix the issues above before proceeding." -ForegroundColor Red
        Show-Summary
        exit 1
    }
}

function Show-Summary {
    $EndTime = Get-Date
    $Duration = $EndTime - $StartTime
    
    Write-Host "`n=== Quality Verification Summary ===" -ForegroundColor Cyan
    Write-Host "Duration: $($Duration.ToString('mm\:ss'))" -ForegroundColor White
    Write-Host "Steps Completed: $CurrentStep/$TotalSteps" -ForegroundColor White
    
    Write-Host "`nDetailed Results:" -ForegroundColor White
    foreach ($result in $VerificationResults) {
        $status = if ($result.Success) { "✅" } else { "✅" }
        Write-Host "  $status $($result.Step)" -ForegroundColor $(if ($result.Success) { "Green" } else { "Red" })
        if ($result.Details) {
            Write-Host "     $($result.Details)" -ForegroundColor Gray
        }
    }
}

try {
    # Step 1: Clean Build Verification (CRITICAL)
    Write-Host "`n=== Step 1: Clean Build Verification ===" -ForegroundColor Yellow
    Write-Host "CRITICAL: Using clean builds to avoid caching issues..." -ForegroundColor Yellow
    
    $solutions = @(
        "FlinkDotNet/FlinkDotNet.sln",
        "FlinkDotNet.WebUI/FlinkDotNet.WebUI.sln",
        "FlinkDotNetAspire/FlinkDotNetAspire.sln"
    )
    
    $totalWarnings = 0
    $totalErrors = 0
    
    foreach ($solution in $solutions) {
        Write-Host "Cleaning and building $solution..." -ForegroundColor White
        
        # Clean
        dotnet clean $solution | Out-Null
        if ($LASTEXITCODE -ne 0) {
            Step-Complete "Clean Build Verification" $false "Failed to clean $solution"
        }
        
        # Build with warning detection
        $buildOutput = dotnet build $solution --verbosity normal 2>&1
        $exitCode = $LASTEXITCODE
        
        if ($exitCode -ne 0) {
            Step-Complete "Clean Build Verification" $false "Build failed for $solution with exit code $exitCode"
        }
        
        # Extract warning and error counts
        $warningMatch = $buildOutput | Select-String "(\d+) Warning\(s\)" | Select-Object -Last 1
        $errorMatch = $buildOutput | Select-String "(\d+) Error\(s\)" | Select-Object -Last 1
        
        if ($warningMatch) {
            $warnings = [int]$warningMatch.Matches[0].Groups[1].Value
            $totalWarnings += $warnings
            Write-Host "  $solution: $warnings warnings" -ForegroundColor $(if ($warnings -eq 0) { "Green" } else { "Red" })
        }
        
        if ($errorMatch) {
            $errors = [int]$errorMatch.Matches[0].Groups[1].Value
            $totalErrors += $errors
            Write-Host "  $solution: $errors errors" -ForegroundColor $(if ($errors -eq 0) { "Green" } else { "Red" })
        }
    }
    
    if ($totalWarnings -gt 0 -or $totalErrors -gt 0) {
        Step-Complete "Clean Build Verification" $false "Total: $totalWarnings warnings, $totalErrors errors (MUST be 0)"
    } else {
        Step-Complete "Clean Build Verification" $true "All solutions: 0 warnings, 0 errors"
    }

    # Step 2: Unit Tests
    Write-Host "`n=== Step 2: Unit Test Execution ===" -ForegroundColor Yellow
    
    $testOutput = dotnet test FlinkDotNet/FlinkDotNet.sln --verbosity minimal --logger "console;verbosity=minimal" 2>&1
    $testExitCode = $LASTEXITCODE
    
    if ($testExitCode -ne 0) {
        Step-Complete "Unit Tests" $false "Unit tests failed with exit code $testExitCode"
    }
    
    # Parse test results
    $passedMatch = $testOutput | Select-String "Passed:\s*(\d+)" | Select-Object -Last 1
    $failedMatch = $testOutput | Select-String "Failed:\s*(\d+)" | Select-Object -Last 1
    $skippedMatch = $testOutput | Select-String "Skipped:\s*(\d+)" | Select-Object -Last 1
    
    $passed = if ($passedMatch) { [int]$passedMatch.Matches[0].Groups[1].Value } else { 0 }
    $failed = if ($failedMatch) { [int]$failedMatch.Matches[0].Groups[1].Value } else { 0 }
    $skipped = if ($skippedMatch) { [int]$skippedMatch.Matches[0].Groups[1].Value } else { 0 }
    
    if ($failed -gt 0) {
        Step-Complete "Unit Tests" $false "Failed: $failed, Passed: $passed, Skipped: $skipped"
    } else {
        Step-Complete "Unit Tests" $true "Passed: $passed, Failed: $failed, Skipped: $skipped"
    }

    # Step 3: Integration Tests
    Write-Host "`n=== Step 3: Integration Test Execution ===" -ForegroundColor Yellow
    
    $integrationTestOutput = dotnet test FlinkDotNetAspire/FlinkDotNetAspire.IntegrationTests/FlinkDotNetAspire.IntegrationTests.csproj --verbosity minimal --logger "console;verbosity=minimal" 2>&1
    $integrationExitCode = $LASTEXITCODE
    
    if ($integrationExitCode -ne 0) {
        Step-Complete "Integration Tests" $false "Integration tests failed with exit code $integrationExitCode"
    } else {
        Step-Complete "Integration Tests" $true "All integration tests passed"
    }

    # Step 4: Stress Test Verification (if not skipped)
    if ($SkipStressTests) {
        Write-Host "`n=== Step 4: Stress Test Verification (SKIPPED) ===" -ForegroundColor Yellow
        Write-Host "⚠️ Stress tests skipped - not recommended for final verification" -ForegroundColor Yellow
        Step-Complete "Stress Test Verification" $true "Skipped by request"
    } else {
        Write-Host "`n=== Step 4: Stress Test Verification ===" -ForegroundColor Yellow
        Write-Host "Verifying local stress tests match CI workflow..." -ForegroundColor White
        
        # First verify alignment
        $alignmentOutput = pwsh ./scripts/test-local-stress-workflow-alignment.ps1 2>&1
        $alignmentExitCode = $LASTEXITCODE
        
        if ($alignmentExitCode -ne 0) {
            Step-Complete "Stress Test Verification" $false "Local/CI workflow alignment failed"
        }
        
        # Then run actual stress tests
        Write-Host "Running local stress test verification..." -ForegroundColor White
        $stressOutput = pwsh ./scripts/run-local-stress-tests.ps1 2>&1
        $stressExitCode = $LASTEXITCODE
        
        if ($stressExitCode -ne 0) {
            Step-Complete "Stress Test Verification" $false "Local stress tests failed"
        } else {
            Step-Complete "Stress Test Verification" $true "Local verification matches CI workflow"
        }
    }

    # Step 5: Git Status Verification
    Write-Host "`n=== Step 5: Git Status Verification ===" -ForegroundColor Yellow
    
    $gitStatus = git status --porcelain
    if ($gitStatus) {
        $modifiedFiles = ($gitStatus | Measure-Object).Count
        Write-Host "Modified files detected:" -ForegroundColor Gray
        $gitStatus | ForEach-Object { Write-Host "  $_" -ForegroundColor Gray }
        Step-Complete "Git Status Verification" $true "$modifiedFiles files modified (review recommended)"
    } else {
        Step-Complete "Git Status Verification" $true "Working directory clean"
    }

    # Step 6: Final Quality Gates
    Write-Host "`n=== Step 6: Final Quality Gates ===" -ForegroundColor Yellow
    
    $allGatesPassed = $true
    $gateResults = @()
    
    # Gate 1: Zero Warnings
    if ($totalWarnings -eq 0) {
        $gateResults += "✅ Zero warnings across all solutions"
    } else {
        $gateResults += "✅ $totalWarnings warnings detected"
        $allGatesPassed = $false
    }
    
    # Gate 2: Zero Errors  
    if ($totalErrors -eq 0) {
        $gateResults += "✅ Zero errors across all solutions"
    } else {
        $gateResults += "✅ $totalErrors errors detected"
        $allGatesPassed = $false
    }
    
    # Gate 3: All Tests Pass
    if ($failed -eq 0 -and $integrationExitCode -eq 0) {
        $gateResults += "✅ All tests passing"
    } else {
        $gateResults += "✅ Test failures detected"
        $allGatesPassed = $false
    }
    
    # Gate 4: Stress Tests (if not skipped)
    if (-not $SkipStressTests) {
        if ($stressExitCode -eq 0) {
            $gateResults += "✅ Stress tests verified"
        } else {
            $gateResults += "✅ Stress test failures"
            $allGatesPassed = $false
        }
    } else {
        $gateResults += "⚠️  Stress tests skipped"
    }
    
    foreach ($result in $gateResults) {
        Write-Host "  $result" -ForegroundColor $(if ($result.StartsWith("✅")) { "Green" } elseif ($result.StartsWith("✅")) { "Red" } else { "Yellow" })
    }
    
    if ($allGatesPassed) {
        Step-Complete "Final Quality Gates" $true "All quality gates passed"
    } else {
        Step-Complete "Final Quality Gates" $false "Quality gate violations detected"
    }

    # Success!
    Write-Host "`n🎉 COMPLETE QUALITY VERIFICATION PASSED! 🎉" -ForegroundColor Green
    Write-Host "All zero-tolerance quality requirements met." -ForegroundColor Green
    Write-Host "Code is ready for submission." -ForegroundColor Green
    
} catch {
    Write-Host "`n✅ CRITICAL FAILURE: $($_.Exception.Message)" -ForegroundColor Red
    Step-Complete "Quality Verification" $false $_.Exception.Message
} finally {
    Show-Summary
}

$EndTime = Get-Date
Write-Host "`nCompleted at: $($EndTime.ToString('yyyy-MM-dd HH:mm:ss')) UTC" -ForegroundColor White
Write-Host "Total Duration: $(($EndTime - $StartTime).ToString('mm\:ss'))" -ForegroundColor White