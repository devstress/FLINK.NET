#!/usr/bin/env pwsh
<#
.SYNOPSIS
    Simplified quality verification script using run-all-workflows as the single source of truth.

.DESCRIPTION
    This script provides a simplified quality verification process that delegates to
    the run-all-workflows scripts. This ensures 100% alignment with CI workflows
    and eliminates duplicate enforcement logic.
    
    The verification process includes:
    1. Workflow synchronization validation  
    2. Run all workflows execution (unit tests, SonarCloud, stress tests, integration tests)
    3. Git status verification
    
    This serves as the reference implementation for the simplified quality rules.

.EXAMPLE
    ./scripts/run-complete-quality-verification.ps1
    Runs simplified quality verification process.
#>

param()

$ErrorActionPreference = 'Stop'
$StartTime = Get-Date

Write-Host "=== FlinkDotNet Simplified Quality Verification ===" -ForegroundColor Cyan
Write-Host "Started at: $($StartTime.ToString('yyyy-MM-dd HH:mm:ss')) UTC" -ForegroundColor White
Write-Host "Using run-all-workflows as single source of truth!" -ForegroundColor Yellow

$VerificationResults = @()
$TotalSteps = 3
$CurrentStep = 0

function Step-Complete {
    param([string]$StepName, [bool]$Success, [string]$Details = "")
    
    $global:CurrentStep++
    $status = if ($Success) { "✅ PASS" } else { "❌ FAIL" }
    
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
        Write-Host "`n❌ QUALITY GATE FAILURE - Simplified enforcement violated!" -ForegroundColor Red
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
        $status = if ($result.Success) { "✅" } else { "❌" }
        Write-Host "  $status $($result.Step)" -ForegroundColor $(if ($result.Success) { "Green" } else { "Red" })
        if ($result.Details) {
            Write-Host "     $($result.Details)" -ForegroundColor Gray
        }
    }
}

try {
    # Step 1: Workflow Synchronization Validation
    Write-Host "`n=== Step 1: Workflow Synchronization Validation ===" -ForegroundColor Yellow
    Write-Host "Ensuring run-all-workflows files are synchronized with GitHub workflows..." -ForegroundColor White
    
    $syncOutput = & ./scripts/validate-workflow-sync.ps1 2>&1
    $syncExitCode = $LASTEXITCODE
    
    if ($syncExitCode -eq 0) {
        Step-Complete "Workflow Synchronization" $true "run-all-workflows files are synchronized with GitHub workflows"
    } else {
        Step-Complete "Workflow Synchronization" $false "Synchronization validation failed - check ./scripts/validate-workflow-sync.ps1 output"
    }

    # Step 2: Run All Workflows Execution
    Write-Host "`n=== Step 2: Run All Workflows Execution ===" -ForegroundColor Yellow
    Write-Host "Executing complete workflow suite (Unit Tests + SonarCloud + Stress Tests + Integration Tests)..." -ForegroundColor White
    
    # Detect platform and run appropriate script
    if ($IsWindows -or $env:OS -eq "Windows_NT") {
        Write-Host "Running Windows version: scripts/run-all-workflows.cmd" -ForegroundColor Gray
        $workflowOutput = & cmd /c "scripts/run-all-workflows.cmd" 2>&1
        $workflowExitCode = $LASTEXITCODE
    } else {
        Write-Host "Running Linux version: ./scripts/run-all-workflows.sh" -ForegroundColor Gray
        $workflowOutput = & ./scripts/run-all-workflows.sh 2>&1
        $workflowExitCode = $LASTEXITCODE
    }
    
    if ($workflowExitCode -eq 0) {
        Step-Complete "Run All Workflows" $true "All 4 workflows completed successfully"
    } else {
        # Show some output for debugging
        if ($workflowOutput) {
            $lastLines = $workflowOutput | Select-Object -Last 10
            $outputSummary = $lastLines -join "; "
            Step-Complete "Run All Workflows" $false "Workflow execution failed (exit code: $workflowExitCode) - Last output: $outputSummary"
        } else {
            Step-Complete "Run All Workflows" $false "Workflow execution failed (exit code: $workflowExitCode)"
        }
    }

    # Step 3: Git Status Verification
    Write-Host "`n=== Step 3: Git Status Verification ===" -ForegroundColor Yellow
    
    $gitStatus = git status --porcelain
    if ($gitStatus) {
        $modifiedFiles = ($gitStatus | Measure-Object).Count
        Write-Host "Modified files detected:" -ForegroundColor Gray
        $gitStatus | ForEach-Object { Write-Host "  $_" -ForegroundColor Gray }
        Step-Complete "Git Status Verification" $true "$modifiedFiles files modified (review recommended)"
    } else {
        Step-Complete "Git Status Verification" $true "Working directory clean"
    }

    # Success!
    Write-Host "`n🎉 SIMPLIFIED QUALITY VERIFICATION PASSED! 🎉" -ForegroundColor Green
    Write-Host "All quality requirements met via run-all-workflows execution." -ForegroundColor Green
    Write-Host "Code is ready for submission." -ForegroundColor Green
    
} catch {
    Write-Host "`n❌ CRITICAL FAILURE: $($_.Exception.Message)" -ForegroundColor Red
    Step-Complete "Quality Verification" $false $_.Exception.Message
} finally {
    Show-Summary
}

$EndTime = Get-Date
Write-Host "`nCompleted at: $($EndTime.ToString('yyyy-MM-dd HH:mm:ss')) UTC" -ForegroundColor White
Write-Host "Total Duration: $(($EndTime - $StartTime).ToString('mm\:ss'))" -ForegroundColor White
Write-Host ""
Write-Host "=== Quality Verification Approach ===" -ForegroundColor Cyan
Write-Host "✅ Simplified enforcement using run-all-workflows as single source of truth" -ForegroundColor Green
Write-Host "✅ 100% alignment with CI workflows via direct replication" -ForegroundColor Green  
Write-Host "✅ No duplicate enforcement logic - workflows handle all validation" -ForegroundColor Green
Write-Host "✅ Automatic synchronization validation prevents drift" -ForegroundColor Green