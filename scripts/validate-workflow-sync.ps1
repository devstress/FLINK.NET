#!/usr/bin/env pwsh
<#
.SYNOPSIS
    Validates that run-full-development-lifecycle files stay synchronized with GitHub workflow files.

.DESCRIPTION
    This script ensures that run-full-development-lifecycle.cmd and run-full-development-lifecycle.sh remain 
    synchronized with the GitHub Actions workflow files (.github/workflows/*.yml).
    
    Any changes to GitHub workflows require corresponding updates to the run-full-development-lifecycle
    files to maintain 100% local/CI alignment.

.PARAMETER Fix
    Automatically fix minor synchronization issues where possible.

.PARAMETER Verbose
    Show detailed synchronization analysis.

.EXAMPLE
    ./scripts/validate-workflow-sync.ps1
    Validates workflow synchronization and reports any issues.

.EXAMPLE
    ./scripts/validate-workflow-sync.ps1 -Fix
    Validates and attempts to fix synchronization issues automatically.
#>

param(
    [switch]$Fix,
    [switch]$Verbose
)

$ErrorActionPreference = 'Stop'
$StartTime = Get-Date

Write-Host "=== Workflow Synchronization Validation ===" -ForegroundColor Cyan
Write-Host "Ensuring run-full-development-lifecycle files match GitHub Actions workflows" -ForegroundColor White
Write-Host ""

$ValidationResults = @()
$Issues = @()

function Add-ValidationResult {
    param([string]$Component, [bool]$Success, [string]$Details = "")
    
    $status = if ($Success) { "✅ PASS" } else { "❌ FAIL" }
    Write-Host "$Component - $status" -ForegroundColor $(if ($Success) { "Green" } else { "Red" })
    
    if ($Details) {
        Write-Host "    $Details" -ForegroundColor Gray
    }
    
    $script:ValidationResults += [PSCustomObject]@{
        Component = $Component
        Success = $Success
        Details = $Details
    }
    
    if (-not $Success) {
        $script:Issues += "${Component}: $Details"
    }
}

function Extract-WorkflowSteps {
    param([string]$WorkflowFile)
    
    if (-not (Test-Path $WorkflowFile)) {
        return @()
    }
    
    $content = Get-Content $WorkflowFile -Raw
    $steps = @()
    
    # Extract step names and key commands
    if ($content -match '(?s)steps:(.*?)(?=\n\w|\z)') {
        $stepsSection = $Matches[1]
        
        # Find step names
        $stepsSection | Select-String '- name: (.+)' -AllMatches | ForEach-Object {
            $_.Matches | ForEach-Object {
                $steps += $_.Groups[1].Value.Trim()
            }
        }
    }
    
    return $steps
}

function Extract-RunAllWorkflowsSteps {
    param([string]$RunAllWorkflowsFile)
    
    if (-not (Test-Path $RunAllWorkflowsFile)) {
        return @()
    }
    
    $content = Get-Content $RunAllWorkflowsFile -Raw
    $steps = @()
    
    # Extract key workflow descriptions from comments
    if ($content -match '(?s)Workflows executed in parallel:(.*?)(?=\n\#|\nREM|\z)') {
        $workflowsSection = $Matches[1]
        
        # Find workflow names
        $workflowsSection | Select-String '\d+\.\s*(.+?)\s*-' -AllMatches | ForEach-Object {
            $_.Matches | ForEach-Object {
                $steps += $_.Groups[1].Value.Trim()
            }
        }
    }
    
    return $steps
}

function Check-EnvironmentVariables {
    param([string]$WorkflowFile, [string]$RunAllWorkflowsFile)
    
    $workflowEnvs = @()
    $runAllEnvs = @()
    
    # Extract environment variables from GitHub workflow
    if (Test-Path $WorkflowFile) {
        $workflowContent = Get-Content $WorkflowFile -Raw
        $workflowContent | Select-String 'env:\s*\n((?:\s+\w+:.*\n?)+)' -AllMatches | ForEach-Object {
            $_.Matches | ForEach-Object {
                $envSection = $_.Groups[1].Value
                $envSection | Select-String '\s+(\w+):' -AllMatches | ForEach-Object {
                    $_.Matches | ForEach-Object {
                        $workflowEnvs += $_.Groups[1].Value
                    }
                }
            }
        }
        
        # Also check for inline environment variables
        $workflowContent | Select-String '\$\{\{\s*env\.(\w+)\s*\}\}' -AllMatches | ForEach-Object {
            $_.Matches | ForEach-Object {
                $workflowEnvs += $_.Groups[1].Value
            }
        }
    }
    
    # Extract environment variables from run-full-development-lifecycle
    if (Test-Path $RunAllWorkflowsFile) {
        $runAllContent = Get-Content $RunAllWorkflowsFile -Raw

        $runAllContent | Select-String 'set\s+(\w+)=' -AllMatches | ForEach-Object {
            $_.Matches | ForEach-Object { $runAllEnvs += $_.Groups[1].Value }
        }

        $runAllContent | Select-String 'export\s+(\w+)=' -AllMatches | ForEach-Object {
            $_.Matches | ForEach-Object { $runAllEnvs += $_.Groups[1].Value }
        }

        $runAllContent | Select-String '\$env:(\w+)' -AllMatches | ForEach-Object {
            $_.Matches | ForEach-Object { $runAllEnvs += $_.Groups[1].Value }
        }
    }
    
    # Remove duplicates and common variables
    $workflowEnvs = $workflowEnvs | Sort-Object | Get-Unique | Where-Object { $_ -notin @('GITHUB_TOKEN', 'GITHUB_REF', 'GITHUB_SHA') }
    $runAllEnvs = $runAllEnvs | Sort-Object | Get-Unique | Where-Object { $_ -notin @('ROOT', 'ERRORLEVEL', 'LASTEXITCODE') }
    
    $missingInRunAll = $workflowEnvs | Where-Object { $_ -notin $runAllEnvs }
    $extraInRunAll = $runAllEnvs | Where-Object { $_ -notin $workflowEnvs }
    
    if ($Verbose) {
        Write-Host "  Workflow env vars: $($workflowEnvs -join ', ')" -ForegroundColor Gray
        Write-Host "  RunAll env vars: $($runAllEnvs -join ', ')" -ForegroundColor Gray
    }
    
    return @{
        WorkflowEnvs = $workflowEnvs
        RunAllEnvs = $runAllEnvs
        Missing = $missingInRunAll
        Extra = $extraInRunAll
    }
}

function Check-CommandSequence {
    param([string]$WorkflowFile, [string]$RunAllWorkflowsFile)
    
    $workflowCommands = @()
    $runAllCommands = @()
    
    # Extract key commands from GitHub workflow
    if (Test-Path $WorkflowFile) {
        $workflowContent = Get-Content $WorkflowFile -Raw
        
        # Look for dotnet commands
        $workflowContent | Select-String 'dotnet\s+(workload\s+install|workload\s+restore|build|test)\s+([^\n\r\|\;\#]+)' -AllMatches | ForEach-Object {
            $_.Matches | ForEach-Object {
                $cmdType = $_.Groups[1].Value
                $cmdArgs = $_.Groups[2].Value.Trim() -replace '\s*\}\s*".*', ''
                if ($cmdArgs -match '^("[^"\n]+"|\S+)') { $cmdArgs = $Matches[1] }
                $cmdArgs = [System.IO.Path]::GetFileName($cmdArgs.Trim('"'))
                $workflowCommands += "$cmdType $cmdArgs".Trim()
            }
        }
    }
    
    # Extract key commands from run-full-development-lifecycle
    if (Test-Path $RunAllWorkflowsFile) {
        $runAllContent = Get-Content $RunAllWorkflowsFile -Raw
        
        # Look for dotnet commands in the embedded scripts
        $runAllContent | Select-String 'dotnet\s+(workload\s+install|workload\s+restore|build|test)\s+([^\n\r\|\;\#]+)' -AllMatches | ForEach-Object {
            $_.Matches | ForEach-Object {
                $cmdType = $_.Groups[1].Value
                $cmdArgs = $_.Groups[2].Value.Trim() -replace '\s*\}\s*".*', ''
                if ($cmdArgs -match '^("[^"\n]+"|\S+)') { $cmdArgs = $Matches[1] }
                $cmdArgs = [System.IO.Path]::GetFileName($cmdArgs.Trim('"'))
                $runAllCommands += "$cmdType $cmdArgs".Trim()
            }
        }
    }
    
    # Normalize commands for comparison
    $workflowCommands = $workflowCommands | ForEach-Object { $_ -replace '\s+', ' ' } | Sort-Object | Get-Unique
    $runAllCommands = $runAllCommands | ForEach-Object { $_ -replace '\s+', ' ' } | Sort-Object | Get-Unique
    
    $missingInRunAll = $workflowCommands | Where-Object { $_ -notin $runAllCommands }
    $extraInRunAll = $runAllCommands | Where-Object { $_ -notin $workflowCommands }
    
    if ($Verbose) {
        Write-Host "  Workflow commands: $($workflowCommands -join '; ')" -ForegroundColor Gray
        Write-Host "  RunAll commands: $($runAllCommands -join '; ')" -ForegroundColor Gray
    }
    
    return @{
        WorkflowCommands = $workflowCommands
        RunAllCommands = $runAllCommands
        Missing = $missingInRunAll
        Extra = $extraInRunAll
    }
}

try {
    # Define file paths
    $GitHubWorkflows = @(
        ".github/workflows/unit-tests.yml",
        ".github/workflows/sonarcloud.yml", 
        ".github/workflows/stress-tests.yml",
        ".github/workflows/integration-tests.yml"
    )
    
    $RunAllWorkflowsCmd = "run-full-development-lifecycle.cmd"
    $RunAllWorkflowsSh = "run-full-development-lifecycle.sh"
    
    # Check that all required files exist
    Write-Host "=== File Existence Check ===" -ForegroundColor Yellow
    
    $allFilesExist = $true
    foreach ($workflow in $GitHubWorkflows) {
        if (Test-Path $workflow) {
            Add-ValidationResult "GitHub Workflow: $workflow" $true "File exists"
        } else {
            Add-ValidationResult "GitHub Workflow: $workflow" $false "File missing"
            $allFilesExist = $false
        }
    }
    
    if (Test-Path $RunAllWorkflowsCmd) {
        Add-ValidationResult "run-full-development-lifecycle.cmd" $true "File exists"
    } else {
        Add-ValidationResult "run-full-development-lifecycle.cmd" $false "File missing"
        $allFilesExist = $false
    }
    
    if (Test-Path $RunAllWorkflowsSh) {
        Add-ValidationResult "run-full-development-lifecycle.sh" $true "File exists"
    } else {
        Add-ValidationResult "run-full-development-lifecycle.sh" $false "File missing"
        $allFilesExist = $false
    }
    
    if (-not $allFilesExist) {
        throw "Required files are missing. Cannot proceed with synchronization validation."
    }
    
    Write-Host ""
    
    # Check workflow coverage
    Write-Host "=== Workflow Coverage Check ===" -ForegroundColor Yellow
    
    $expectedWorkflows = @("Unit Tests", "SonarCloud Analysis", "Stress Tests", "Integration Tests")
    
    # Check cmd file
    $cmdSteps = Extract-RunAllWorkflowsSteps $RunAllWorkflowsCmd
    $cmdCoverage = $expectedWorkflows | Where-Object { 
        $workflow = $_
        $cmdSteps | Where-Object { $_ -like "*$workflow*" -or $workflow -like "*$_*" }
    }
    
    if ($cmdCoverage.Count -eq $expectedWorkflows.Count) {
        Add-ValidationResult "run-full-development-lifecycle.cmd Coverage" $true "All 4 workflows covered"
    } else {
        $missing = $expectedWorkflows | Where-Object { $_ -notin $cmdCoverage }
        Add-ValidationResult "run-full-development-lifecycle.cmd Coverage" $false "Missing: $($missing -join ', ')"
    }
    
    # Check sh file
    $shSteps = Extract-RunAllWorkflowsSteps $RunAllWorkflowsSh
    $shCoverage = $expectedWorkflows | Where-Object { 
        $workflow = $_
        $shSteps | Where-Object { $_ -like "*$workflow*" -or $workflow -like "*$_*" }
    }
    
    if ($shCoverage.Count -eq $expectedWorkflows.Count) {
        Add-ValidationResult "run-full-development-lifecycle.sh Coverage" $true "All 4 workflows covered"
    } else {
        $missing = $expectedWorkflows | Where-Object { $_ -notin $shCoverage }
        Add-ValidationResult "run-full-development-lifecycle.sh Coverage" $false "Missing: $($missing -join ', ')"
    }
    
    Write-Host ""
    
    # Check environment variable alignment
    Write-Host "=== Environment Variable Alignment ===" -ForegroundColor Yellow
    
    foreach ($workflow in $GitHubWorkflows) {
        if (Test-Path $workflow) {
            $workflowName = (Split-Path $workflow -Leaf) -replace '\.yml$', ''
            
            # Check cmd alignment
            $cmdEnvCheck = Check-EnvironmentVariables $workflow $RunAllWorkflowsCmd
            if ($cmdEnvCheck.Missing.Count -eq 0) {
                Add-ValidationResult "ENV: $workflowName vs CMD" $true "All workflow variables present"
            } else {
                $details = "Missing in CMD: $($cmdEnvCheck.Missing -join ', ')"
                if ($cmdEnvCheck.Extra.Count -gt 0) { $details += "; Extra in CMD: $($cmdEnvCheck.Extra -join ', ')" }
                Add-ValidationResult "ENV: $workflowName vs CMD" $false $details
            }
            
            # Check sh alignment
            $shEnvCheck = Check-EnvironmentVariables $workflow $RunAllWorkflowsSh
            if ($shEnvCheck.Missing.Count -eq 0) {
                Add-ValidationResult "ENV: $workflowName vs SH" $true "All workflow variables present"
            } else {
                $details = "Missing in SH: $($shEnvCheck.Missing -join ', ')"
                if ($shEnvCheck.Extra.Count -gt 0) { $details += "; Extra in SH: $($shEnvCheck.Extra -join ', ')" }
                Add-ValidationResult "ENV: $workflowName vs SH" $false $details
            }
        }
    }
    
    Write-Host ""
    
    # Check command sequence alignment
    Write-Host "=== Command Sequence Alignment ===" -ForegroundColor Yellow
    
    foreach ($workflow in $GitHubWorkflows) {
        if (Test-Path $workflow) {
            $workflowName = (Split-Path $workflow -Leaf) -replace '\.yml$', ''
            
            # Check cmd alignment
            $cmdCmdCheck = Check-CommandSequence $workflow $RunAllWorkflowsCmd
            if ($cmdCmdCheck.Missing.Count -eq 0) {
                Add-ValidationResult "CMD: $workflowName vs CMD" $true "Command sequences aligned"
            } else {
                $details = "Missing commands: $($cmdCmdCheck.Missing -join '; ')"
                if ($cmdCmdCheck.Extra.Count -gt 0) { $details += "; Extra commands: $($cmdCmdCheck.Extra -join '; ')" }
                Add-ValidationResult "CMD: $workflowName vs CMD" $false $details
            }
            
            # Check sh alignment
            $shCmdCheck = Check-CommandSequence $workflow $RunAllWorkflowsSh
            if ($shCmdCheck.Missing.Count -eq 0) {
                Add-ValidationResult "CMD: $workflowName vs SH" $true "Command sequences aligned"
            } else {
                $details = "Missing commands: $($shCmdCheck.Missing -join '; ')"
                if ($shCmdCheck.Extra.Count -gt 0) { $details += "; Extra commands: $($shCmdCheck.Extra -join '; ')" }
                Add-ValidationResult "CMD: $workflowName vs SH" $false $details
            }
        }
    }
    
    Write-Host ""
    
    # Check for recent modifications
    Write-Host "=== Modification Time Check ===" -ForegroundColor Yellow
    
    $workflowMods = @()
    foreach ($workflow in $GitHubWorkflows) {
        if (Test-Path $workflow) {
            $workflowMods += Get-Item $workflow
        }
    }
    
    $newestWorkflow = $workflowMods | Sort-Object LastWriteTime -Descending | Select-Object -First 1
    $cmdMod = Get-Item $RunAllWorkflowsCmd
    $shMod = Get-Item $RunAllWorkflowsSh
    
    if ($newestWorkflow.LastWriteTime -gt $cmdMod.LastWriteTime) {
        $timeDiff = ($newestWorkflow.LastWriteTime - $cmdMod.LastWriteTime).TotalHours
        Add-ValidationResult "Modification Time: CMD" $false "run-full-development-lifecycle.cmd is $($timeDiff.ToString('F1')) hours older than newest workflow"
    } else {
        Add-ValidationResult "Modification Time: CMD" $true "run-full-development-lifecycle.cmd is up to date"
    }
    
    if ($newestWorkflow.LastWriteTime -gt $shMod.LastWriteTime) {
        $timeDiff = ($newestWorkflow.LastWriteTime - $shMod.LastWriteTime).TotalHours
        Add-ValidationResult "Modification Time: SH" $false "run-full-development-lifecycle.sh is $($timeDiff.ToString('F1')) hours older than newest workflow"
    } else {
        Add-ValidationResult "Modification Time: SH" $true "run-full-development-lifecycle.sh is up to date"
    }
    
    Write-Host ""
    
    # Final validation summary
    $passedCount = ($ValidationResults | Where-Object { $_.Success }).Count
    $failedCount = ($ValidationResults | Where-Object { -not $_.Success }).Count
    $totalCount = $ValidationResults.Count
    
    Write-Host "=== Validation Summary ===" -ForegroundColor Cyan
    Write-Host "Total Checks: $totalCount" -ForegroundColor White
    Write-Host "Passed: $passedCount" -ForegroundColor Green
    Write-Host "Failed: $failedCount" -ForegroundColor $(if ($failedCount -eq 0) { "Green" } else { "Red" })
    
    if ($Issues.Count -gt 0) {
        Write-Host ""
        Write-Host "=== Issues Found ===" -ForegroundColor Red
        foreach ($issue in $Issues) {
            Write-Host "  • $issue" -ForegroundColor Red
        }
        
        Write-Host ""
        Write-Host "=== Recommended Actions ===" -ForegroundColor Yellow
        Write-Host "1. Review GitHub workflow changes in .github/workflows/" -ForegroundColor White
        Write-Host "2. Update run-full-development-lifecycle.cmd and run-full-development-lifecycle.sh accordingly" -ForegroundColor White
        Write-Host "3. Ensure all environment variables are synchronized" -ForegroundColor White
        Write-Host "4. Verify all dotnet commands match between workflows and scripts" -ForegroundColor White
        Write-Host "5. Re-run this validation script to confirm fixes" -ForegroundColor White
        
        if ($Fix) {
            Write-Host ""
            Write-Host "=== Auto-Fix Attempt ===" -ForegroundColor Yellow
            Write-Host "Auto-fix capability is not yet implemented." -ForegroundColor Gray
            Write-Host "Manual updates to run-full-development-lifecycle files are required." -ForegroundColor Gray
        }
        
        Write-Host ""
        Write-Host "❌ WORKFLOW SYNCHRONIZATION VALIDATION FAILED" -ForegroundColor Red
        exit 1
    } else {
        Write-Host ""
        Write-Host "✅ WORKFLOW SYNCHRONIZATION VALIDATION PASSED" -ForegroundColor Green
        Write-Host "All run-full-development-lifecycle files are synchronized with GitHub workflows." -ForegroundColor Green
        exit 0
    }
    
} catch {
    Write-Host ""
    Write-Host "❌ CRITICAL ERROR: $($_.Exception.Message)" -ForegroundColor Red
    Write-Host "Workflow synchronization validation could not complete." -ForegroundColor Red
    exit 1
} finally {
    $EndTime = Get-Date
    $Duration = $EndTime - $StartTime
    Write-Host ""
    Write-Host "Validation completed in $($Duration.TotalSeconds.ToString('F1')) seconds" -ForegroundColor Gray
}