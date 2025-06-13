#!/usr/bin/env pwsh
<#
.SYNOPSIS
    Unit tests to verify local stress test verification matches CI workflow.

.DESCRIPTION
    This script contains automated tests that verify the local stress test
    verification script properly matches the GitHub Actions workflow behavior.
    
    Tests cover:
    - Script existence and executability
    - Environment variable setup matching workflow
    - Build process alignment
    - Health check process matching
    - Verification test execution alignment
    - Cleanup process verification

.EXAMPLE
    ./scripts/test-local-stress-workflow-alignment.ps1
    Runs all unit tests to verify local/CI alignment.
#>

$ErrorActionPreference = 'Stop'

Write-Host "=== Local Stress Test Workflow Alignment Tests ===" -ForegroundColor Cyan
Write-Host "Started at: $(Get-Date -Format 'yyyy-MM-dd HH:mm:ss') UTC" -ForegroundColor White

$TestResults = @()
$TotalTests = 0
$PassedTests = 0

function Test-Case {
    param(
        [string]$Name,
        [scriptblock]$Test,
        [string]$Description = ""
    )
    
    $global:TotalTests++
    Write-Host "`n--- Test $global:TotalTests`: $Name ---" -ForegroundColor Yellow
    if ($Description) {
        Write-Host "Description: $Description" -ForegroundColor Gray
    }
    
    try {
        $result = & $Test
        if ($result -eq $true -or $result -eq $null) {
            Write-Host "✅ PASS: $Name" -ForegroundColor Green
            $global:PassedTests++
            $global:TestResults += [PSCustomObject]@{
                Test = $Name
                Result = "PASS"
                Error = $null
            }
        } else {
            Write-Host "✅ FAIL: $Name - $result" -ForegroundColor Red
            $global:TestResults += [PSCustomObject]@{
                Test = $Name
                Result = "FAIL"
                Error = $result
            }
        }
    } catch {
        Write-Host "✅ FAIL: $Name - Exception: $($_.Exception.Message)" -ForegroundColor Red
        $global:TestResults += [PSCustomObject]@{
            Test = $Name
            Result = "FAIL"
            Error = $_.Exception.Message
        }
    }
}

# Test 1: Local stress test script exists and is executable
Test-Case "Local Stress Test Script Exists" {
    $scriptPath = "./scripts/run-local-stress-tests.ps1"
    if (-not (Test-Path $scriptPath)) {
        return "Local stress test script not found at $scriptPath"
    }
    return $true
} "Verifies the local stress test script exists at the expected location"

# Test 2: Workflow file exists and contains expected steps
Test-Case "Stress Test Workflow File Structure" {
    $workflowPath = "./.github/workflows/stress-tests.yml"
    if (-not (Test-Path $workflowPath)) {
        return "Stress test workflow file not found at $workflowPath"
    }
    
    $workflowContent = Get-Content $workflowPath -Raw
    $requiredSteps = @(
        "Discover Available Ports",
        "Build Solutions",
        "Start Aspire AppHost", 
        "Health Check Loop",
        "Run Verification Tests",
        "Stop Aspire AppHost"
    )
    
    foreach ($step in $requiredSteps) {
        if ($workflowContent -notmatch $step) {
            return "Required workflow step missing: $step"
        }
    }
    return $true
} "Verifies the CI workflow contains all required steps that local script should replicate"

# Test 3: Environment variables alignment
Test-Case "Environment Variables Alignment" {
    $workflowPath = "./.github/workflows/stress-tests.yml"
    $workflowContent = Get-Content $workflowPath -Raw
    
    # Extract environment variables from workflow
    $workflowEnvVars = @(
        "SIMULATOR_NUM_MESSAGES_CI",
        "MAX_ALLOWED_TIME_MS",
        "ASPIRE_ALLOW_UNSECURED_TRANSPORT",
        "DOTNET_ENVIRONMENT"
    )
    
    $localScriptPath = "./scripts/run-local-stress-tests.ps1"
    $localScriptContent = Get-Content $localScriptPath -Raw
    
    foreach ($envVar in $workflowEnvVars) {
        # Check if the environment variable concept exists in local script
        $baseVar = $envVar -replace "_CI$", ""
        if ($localScriptContent -notmatch $baseVar -and $localScriptContent -notmatch $envVar) {
            return "Environment variable not handled in local script: $envVar"
        }
    }
    return $true
} "Verifies local script handles same environment variables as CI workflow"

# Test 4: IntegrationTestVerifier usage alignment
Test-Case "IntegrationTestVerifier Usage Alignment" {
    $workflowPath = "./.github/workflows/stress-tests.yml"
    $workflowContent = Get-Content $workflowPath -Raw
    
    $localScriptPath = "./scripts/run-local-stress-tests.ps1"
    $localScriptContent = Get-Content $localScriptPath -Raw
    
    # Check that both use the same verifier DLL path
    $verifierPattern = "IntegrationTestVerifier/bin/Release/net8.0/FlinkDotNet.IntegrationTestVerifier.dll"
    
    if ($workflowContent -notmatch [regex]::Escape($verifierPattern)) {
        return "Workflow does not reference expected verifier DLL path"
    }
    
    if ($localScriptContent -notmatch [regex]::Escape($verifierPattern)) {
        return "Local script does not reference expected verifier DLL path"
    }
    
    # Check that both use --health-check flag
    if ($workflowContent -notmatch "--health-check") {
        return "Workflow does not use --health-check flag"
    }
    
    if ($localScriptContent -notmatch "--health-check") {
        return "Local script does not use --health-check flag"
    }
    
    return $true
} "Verifies both workflow and local script use IntegrationTestVerifier in the same way"

# Test 5: Build process alignment
Test-Case "Build Process Alignment" {
    $workflowPath = "./.github/workflows/stress-tests.yml"
    $workflowContent = Get-Content $workflowPath -Raw
    
    $localScriptPath = "./scripts/run-local-stress-tests.ps1"
    $localScriptContent = Get-Content $localScriptPath -Raw
    
    # Check that both build the same solutions
    $requiredBuilds = @(
        "FlinkDotNet/FlinkDotNet.sln",
        "FlinkDotNetAspire/FlinkDotNetAspire.sln"
    )
    
    foreach ($build in $requiredBuilds) {
        if ($workflowContent -notmatch [regex]::Escape($build)) {
            return "Workflow does not build: $build"
        }
        
        if ($localScriptContent -notmatch [regex]::Escape($build)) {
            return "Local script does not build: $build"
        }
    }
    
    # Check that both use Release configuration
    if ($workflowContent -notmatch "--configuration Release") {
        return "Workflow does not use Release configuration"
    }
    
    if ($localScriptContent -notmatch "--configuration Release") {
        return "Local script does not use Release configuration"
    }
    
    return $true
} "Verifies both workflow and local script build the same solutions with same configuration"

# Test 6: Port discovery integration
Test-Case "Port Discovery Integration" {
    $localScriptPath = "./scripts/run-local-stress-tests.ps1"
    $localScriptContent = Get-Content $localScriptPath -Raw
    
    $portDiscoveryScript = "./scripts/find-available-ports.ps1"
    if (-not (Test-Path $portDiscoveryScript)) {
        return "Port discovery script not found: $portDiscoveryScript"
    }
    
    if ($localScriptContent -notmatch [regex]::Escape($portDiscoveryScript)) {
        return "Local script does not call port discovery script"
    }
    
    return $true
} "Verifies local script integrates with port discovery like the workflow"

# Test 7: AppHost startup process alignment
Test-Case "AppHost Startup Process Alignment" {
    $workflowPath = "./.github/workflows/stress-tests.yml"
    $workflowContent = Get-Content $workflowPath -Raw
    
    $localScriptPath = "./scripts/run-local-stress-tests.ps1"
    $localScriptContent = Get-Content $localScriptPath -Raw
    
    # Check AppHost project reference
    $apphostProject = "FlinkDotNetAspire/FlinkDotNetAspire.AppHost.AppHost/FlinkDotNetAspire.AppHost.AppHost.csproj"
    
    if ($workflowContent -notmatch [regex]::Escape($apphostProject)) {
        return "Workflow does not reference correct AppHost project"
    }
    
    if ($localScriptContent -notmatch [regex]::Escape($apphostProject)) {
        return "Local script does not reference correct AppHost project"
    }
    
    # Check for output redirection setup
    if ($localScriptContent -notmatch "apphost\.out\.log") {
        return "Local script does not set up output logging like workflow"
    }
    
    return $true
} "Verifies local script starts AppHost the same way as workflow"

# Test 8: Cleanup process verification
Test-Case "Cleanup Process Verification" {
    $localScriptPath = "./scripts/run-local-stress-tests.ps1"
    $localScriptContent = Get-Content $localScriptPath -Raw
    
    $requiredCleanupItems = @(
        "apphost.pid",
        "apphost.out.log", 
        "apphost.err.log",
        "Stop-Process",
        "Remove-Job"
    )
    
    foreach ($item in $requiredCleanupItems) {
        if ($localScriptContent -notmatch [regex]::Escape($item)) {
            return "Local script cleanup missing: $item"
        }
    }
    
    return $true
} "Verifies local script has proper cleanup process like workflow"

# Test 9: Error handling alignment
Test-Case "Error Handling Alignment" {
    $localScriptPath = "./scripts/run-local-stress-tests.ps1"
    $localScriptContent = Get-Content $localScriptPath -Raw
    
    # Check for proper error handling patterns
    $errorHandlingPatterns = @(
        "LASTEXITCODE",
        "ErrorActionPreference",
        "try",
        "finally",
        "trap"
    )
    
    foreach ($pattern in $errorHandlingPatterns) {
        if ($localScriptContent -notmatch $pattern) {
            return "Local script missing error handling pattern: $pattern"
        }
    }
    
    return $true
} "Verifies local script has robust error handling"

# Test 10: Parameter compatibility  
Test-Case "Parameter Compatibility" {
    $localScriptPath = "./scripts/run-local-stress-tests.ps1"
    $localScriptContent = Get-Content $localScriptPath -Raw
    
    # Check for key parameters that allow matching workflow behavior
    $requiredParams = @(
        "MessageCount",
        "MaxTimeMs",
        "SkipCleanup"
    )
    
    foreach ($param in $requiredParams) {
        if ($localScriptContent -notmatch "param.*$param") {
            return "Local script missing parameter: $param"
        }
    }
    
    return $true
} "Verifies local script accepts parameters for workflow simulation"

# Summary Report
Write-Host "`n=== Test Results Summary ===" -ForegroundColor Cyan
Write-Host "Total Tests: $TotalTests" -ForegroundColor White
Write-Host "Passed: $PassedTests" -ForegroundColor Green
Write-Host "Failed: $($TotalTests - $PassedTests)" -ForegroundColor Red

if ($PassedTests -eq $TotalTests) {
    Write-Host "`n✅ ALL TESTS PASSED - Local stress test verification properly matches CI workflow!" -ForegroundColor Green
    $exitCode = 0
} else {
    Write-Host "`n✅ SOME TESTS FAILED - Local/CI workflow alignment issues detected:" -ForegroundColor Red
    $failedTests = $TestResults | Where-Object { $_.Result -eq "FAIL" }
    foreach ($test in $failedTests) {
        Write-Host "  - $($test.Test): $($test.Error)" -ForegroundColor Red
    }
    $exitCode = 1
}

Write-Host "`n=== Detailed Test Results ===" -ForegroundColor White
$TestResults | Format-Table -AutoSize

Write-Host "`nCompleted at: $(Get-Date -Format 'yyyy-MM-dd HH:mm:ss') UTC" -ForegroundColor White
exit $exitCode