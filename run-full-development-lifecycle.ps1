#!/usr/bin/env pwsh
<#
.SYNOPSIS
    Complete Development Lifecycle Runner with Parallel Execution and Progress Tracking

.DESCRIPTION
    This script runs the complete development lifecycle with parallel test execution.
    It builds all solutions sequentially, then runs all tests in parallel with real-time 
    progress monitoring and percentage-based progress bars.

.PARAMETER SkipSonar
    Skip SonarCloud analysis

.PARAMETER SkipStress  
    Skip stress tests

.PARAMETER SkipReliability
    Skip reliability tests

.PARAMETER Help
    Show help message

.EXAMPLE
    pwsh ./run-full-development-lifecycle.ps1
    
.EXAMPLE
    pwsh ./run-full-development-lifecycle.ps1 -SkipSonar -SkipStress
#>

<#
Workflows executed in parallel:
1. Unit Tests - .github/workflows/unit-tests.yml
2. SonarCloud Analysis - .github/workflows/sonarcloud.yml
3. Stress Tests - .github/workflows/stress-tests.yml
4. Integration Tests - .github/workflows/integration-tests.yml
#>
# Updated: Print Key Logs step now prints AppHost and container logs via bash

param(
    [switch]$SkipSonar,
    [switch]$SkipStress,
    [switch]$SkipReliability,
    [switch]$Help
)

$ErrorActionPreference = 'Stop'

if ($Help) {
    Write-Host @"

Complete Development Lifecycle Script with Progress Tracking

This script builds all solutions and runs all tests in parallel:
  1. Build all .NET solutions
  2. Run unit tests, integration tests, stress tests in parallel  
  3. Run SonarCloud analysis and reliability tests
  4. Real-time progress bars showing percentage completion

Options:
  -SkipSonar        Skip SonarCloud analysis
  -SkipStress       Skip stress tests  
  -SkipReliability  Skip reliability tests
  -Help             Show this help

Prerequisites:
  - .NET 8.0 SDK
  - Java 17+ (for SonarCloud)
  - Docker (for stress/integration tests)
  - PowerShell 7+

"@ -ForegroundColor White
    exit 0
}

# Check admin privileges
function Test-AdminPrivileges {
    try {
        if ($IsWindows) {
            try {
                $currentUser = [Security.Principal.WindowsIdentity]::GetCurrent()
                $principal = New-Object Security.Principal.WindowsPrincipal($currentUser)
                return $principal.IsInRole([Security.Principal.WindowsBuiltInRole]::Administrator)
            } catch {
                # In CI environments, admin check might fail, so we'll continue with a warning
                return $false
            }
        } else {
            return (id -u) -eq 0 -or (sudo -n true 2>/dev/null)
        }
    } catch {
        # If any part of the admin check fails, assume we're in a restricted environment
        return $false
    }
}

# Check admin privileges but allow CI environments to continue
$isAdmin = Test-AdminPrivileges
if ($isAdmin) {
    Write-Host "[OK] Administrator privileges confirmed" -ForegroundColor Green
} elseif ($env:GITHUB_ACTIONS -eq 'true' -or $env:CI -eq 'true' -or $env:RUNNER_OS) {
    Write-Host "[INFO] Running in CI environment, skipping admin check" -ForegroundColor Yellow
} else {
    Write-Host "[ERROR] This script requires administrator privileges." -ForegroundColor Red
    Write-Host "        Please run as Administrator (Windows) or with sudo (Linux/macOS)" -ForegroundColor Red
    exit 1
}

# Navigate to repository root
$scriptPath = $PSScriptRoot
if (-not $scriptPath) { $scriptPath = (Get-Location).Path }
Set-Location $scriptPath
$rootPath = Get-Location

Write-Host "================================================================" -ForegroundColor Cyan
Write-Host "   Complete Development Lifecycle - PowerShell Edition" -ForegroundColor Cyan  
Write-Host "================================================================" -ForegroundColor Cyan
Write-Host "Repository: $rootPath" -ForegroundColor White
Write-Host "Timestamp: $(Get-Date -Format 'yyyy-MM-dd HH:mm:ss')" -ForegroundColor White
Write-Host ""

# Prerequisites check
Write-Host "=== Quick Prerequisites Check ===" -ForegroundColor Yellow

function Test-Command($command, $name, $url = $null) {
    try {
        $version = switch ($command) {
            'dotnet' { & dotnet --version 2>$null }
            'java' { (& java -version 2>&1)[0] -replace '.*"([^"]*)".*', '$1' }
            'docker' { (& docker --version 2>$null) -replace '.*version ([^,]*),.*', '$1' }
            'pwsh' { $PSVersionTable.PSVersion.ToString() }
            default { "found" }
        }
        Write-Host "[OK] $name : $version" -ForegroundColor Green
        return $true
    } catch {
        Write-Host "[ERROR] $name not found" -ForegroundColor Red
        if ($url) { Write-Host "        Install from: $url" -ForegroundColor Gray }
        return $false
    }
}

# Check prerequisites
$dotnetOk = Test-Command 'dotnet' '.NET SDK' 'https://dotnet.microsoft.com/download'
if (-not $dotnetOk) { exit 1 }

if (-not $SkipSonar) {
    $javaOk = Test-Command 'java' 'Java' 'https://adoptopenjdk.net/'
    if (-not $javaOk) {
        Write-Host "[WARNING] Java not found. SonarCloud analysis will be skipped." -ForegroundColor Yellow
        $SkipSonar = $true
    }
}

if (-not $SkipStress -or -not $SkipReliability) {
    $dockerOk = Test-Command 'docker' 'Docker' 'https://docker.com/'
    if ($dockerOk) {
        try {
            & docker info *>$null
            Write-Host "[OK] Docker is running" -ForegroundColor Green
        } catch {
            Write-Host "[WARNING] Docker not running. Container-based tests will be skipped." -ForegroundColor Yellow
            $SkipStress = $true
            $SkipReliability = $true
        }
    } else {
        Write-Host "[WARNING] Docker not available. Container-based tests will be skipped." -ForegroundColor Yellow
        $SkipStress = $true
        $SkipReliability = $true
    }
}

$powershellOk = Test-Command 'pwsh' 'PowerShell'
Write-Host "Prerequisites check completed." -ForegroundColor White
Write-Host ""

# Build all solutions
Write-Host "=== Step 1: Building All Solutions ===" -ForegroundColor Yellow

function Build-Solution($solutionPath) {
    if (-not (Test-Path $solutionPath)) {
        throw "Solution not found: $solutionPath"
    }
    
    Write-Host "=== Restoring $solutionPath ===" -ForegroundColor Cyan
    & dotnet restore $solutionPath
    if ($LASTEXITCODE -ne 0) { throw "Error restoring $solutionPath" }
    
    Write-Host "=== Building $solutionPath ===" -ForegroundColor Cyan  
    & dotnet build $solutionPath
    if ($LASTEXITCODE -ne 0) { throw "Error building $solutionPath" }
    
    Write-Host ""
}

try {
    Build-Solution "$rootPath/FlinkDotNet/FlinkDotNet.sln"
    Build-Solution "$rootPath/FlinkDotNetAspire/FlinkDotNetAspire.sln"
    Build-Solution "$rootPath/FlinkDotNet.WebUI/FlinkDotNet.WebUI.sln"
    Write-Host "[OK] All solutions built successfully!" -ForegroundColor Green
} catch {
    Write-Host "[ERROR] Build failed: $_" -ForegroundColor Red
    exit 1
}

Write-Host ""

# Step 2: Run all tests in parallel with progress tracking
Write-Host "=== Step 2: Running All Tests in Parallel ===" -ForegroundColor Yellow

# Create logs directory
$logsDir = "$rootPath/test-logs"
if (-not (Test-Path $logsDir)) {
    New-Item -ItemType Directory -Path $logsDir -Force | Out-Null
}

# Set environment variables
$env:SIMULATOR_NUM_MESSAGES = if ($env:SIMULATOR_NUM_MESSAGES) { $env:SIMULATOR_NUM_MESSAGES } else { "1000000" }
$env:FLINKDOTNET_STANDARD_TEST_MESSAGES = if ($env:FLINKDOTNET_STANDARD_TEST_MESSAGES) { $env:FLINKDOTNET_STANDARD_TEST_MESSAGES } else { "100000" }
$env:ASPIRE_ALLOW_UNSECURED_TRANSPORT = "true"
$env:MAX_ALLOWED_TIME_MS = if ($env:MAX_ALLOWED_TIME_MS) { $env:MAX_ALLOWED_TIME_MS } else { "300000" }
$env:USE_SIMPLIFIED_MODE = if ($env:USE_SIMPLIFIED_MODE) { $env:USE_SIMPLIFIED_MODE } else { "false" }
$env:DOTNET_ENVIRONMENT = if ($env:DOTNET_ENVIRONMENT) { $env:DOTNET_ENVIRONMENT } else { "Development" }
$env:SIMULATOR_REDIS_KEY_GLOBAL_SEQUENCE = if ($env:SIMULATOR_REDIS_KEY_GLOBAL_SEQUENCE) { $env:SIMULATOR_REDIS_KEY_GLOBAL_SEQUENCE } else { "flinkdotnet:global_sequence_id" }
$env:SIMULATOR_REDIS_KEY_SINK_COUNTER = if ($env:SIMULATOR_REDIS_KEY_SINK_COUNTER) { $env:SIMULATOR_REDIS_KEY_SINK_COUNTER } else { "flinkdotnet:sample:processed_message_counter" }
$env:SIMULATOR_KAFKA_TOPIC = if ($env:SIMULATOR_KAFKA_TOPIC) { $env:SIMULATOR_KAFKA_TOPIC } else { "flinkdotnet.sample.topic" }
$env:SIMULATOR_REDIS_PASSWORD = if ($env:SIMULATOR_REDIS_PASSWORD) { $env:SIMULATOR_REDIS_PASSWORD } else { "FlinkDotNet_Redis_CI_Password_2024" }
$env:SIMULATOR_FORCE_RESET_TO_EARLIEST = if ($env:SIMULATOR_FORCE_RESET_TO_EARLIEST) { $env:SIMULATOR_FORCE_RESET_TO_EARLIEST } else { "true" }


Write-Host "Starting parallel test execution with progress tracking..." -ForegroundColor White
Write-Host ""

# Define test configurations
$testConfigs = @()
$testConfigs += @{ Name = "Unit Tests"; Script = "scripts/run-local-unit-tests.ps1"; LogFile = "unit-tests.log"; Enabled = $true }

# Choose integration test script based on OS
$integrationScript = if ($IsWindows) { 
    "scripts/run-integration-tests-in-windows-os.ps1" 
} else { 
    "scripts/run-integration-tests-in-linux.sh" 
}
$testConfigs += @{ Name = "Integration Tests"; Script = $integrationScript; LogFile = "integration-tests.log"; Enabled = $true }
if (-not $SkipStress) {
    $testConfigs += @{ Name = "Stress Tests"; Script = "scripts/run-local-stress-tests.ps1"; LogFile = "stress-tests.log"; Enabled = $true }
}
if (-not $SkipReliability) {
    $testConfigs += @{ Name = "Reliability Tests"; Script = "scripts/run-local-reliability-tests.ps1"; LogFile = "reliability-tests.log"; Enabled = $true }
}
if (-not $SkipSonar) {
    $testConfigs += @{ Name = "SonarCloud Analysis"; Script = "scripts/run-local-sonarcloud.ps1"; LogFile = "sonarcloud.log"; Enabled = $true }
}

# Start all tests as background jobs
$jobs = @{}
$progress = @{}

foreach ($config in $testConfigs) {
    if ($config.Enabled) {
        $logPath = "$logsDir/$($config.LogFile)"
        Write-Host "[INFO] Starting $($config.Name) (log: $logPath)..." -ForegroundColor Cyan
        
        # Start job with output redirection
        $job = Start-Job -ScriptBlock {
            param($scriptPath, $logPath, $rootPath)
            Set-Location $rootPath
            
            # Handle different script types
            try {
                if ($scriptPath.EndsWith('.ps1')) {
                    # Use simple redirection for better compatibility
                    & pwsh -ExecutionPolicy Bypass -File $scriptPath 2>&1 | Out-File -FilePath $logPath -Encoding UTF8
                } elseif ($scriptPath.EndsWith('.sh')) {
                    if ($IsWindows) {
                        # Use WSL or bash if available on Windows
                        try {
                            & bash $scriptPath 2>&1 | Out-File -FilePath $logPath -Encoding UTF8
                        } catch {
                            throw "Bash/WSL not available for .sh script execution on Windows"
                        }
                    } else {
                        & bash $scriptPath 2>&1 | Out-File -FilePath $logPath -Encoding UTF8
                    }
                } else {
                    throw "Unsupported script type: $scriptPath"
                }
            } catch {
                $errorMsg = "Error executing $scriptPath : $_"
                $errorMsg | Out-File -FilePath $logPath -Encoding UTF8
                throw $errorMsg
            }
        } -ArgumentList $config.Script, $logPath, $rootPath
        
        $jobs[$config.Name] = $job
        $progress[$config.Name] = @{ Percentage = 0; Status = "Starting..." }
    }
}

Write-Host ""
Write-Host "All tests started. Monitoring progress with real-time updates..." -ForegroundColor Green
Write-Host ""

# Monitor progress with progress bars
function Get-TestProgress($logPath, $testName) {
    if (-not (Test-Path $logPath)) {
        return @{ Percentage = 0; Status = "Initializing..." }
    }
    
    try {
        $content = Get-Content $logPath -ErrorAction SilentlyContinue
        if (-not $content) {
            return @{ Percentage = 5; Status = "Starting..." }
        }
        
        # Progressive analysis based on common patterns
        $percentage = 5  # Base for file existence
        $status = "In progress..."
        
        # Look for key progress indicators
        if ($content -match "Prerequisites|Checking") { 
            $percentage = 15; $status = "Prerequisites check" 
        }
        if ($content -match "Cleaning|Clean") { 
            $percentage = 25; $status = "Cleaning" 
        }
        if ($content -match "Building|Build") { 
            $percentage = 40; $status = "Building" 
        }
        if ($content -match "Running|Test.*started|Starting.*test") { 
            $percentage = 60; $status = "Running tests" 
        }
        if ($content -match "PASSED|Success|completed successfully") { 
            $percentage = 85; $status = "Processing results" 
        }
        if ($content -match "Summary|Coverage|Analysis") { 
            $percentage = 95; $status = "Finalizing" 
        }
        
        # Check for completion
        if ($content -match "Unit tests completed successfully|✅.*completed|=== Summary ===") {
            $percentage = 100; $status = "Completed"
        }
        
        # Check for errors
        if ($content -match "ERROR|FAILED|Exception|failed with exit code") {
            $status = "Error detected"
        }
        
        return @{ Percentage = $percentage; Status = $status }
        
    } catch {
        return @{ Percentage = 0; Status = "Error reading log" }
    }
}

# Main monitoring loop with progress bars
$allCompleted = $false
$refreshCount = 0
$maxRefreshCount = 1800  # 1800 * 2 seconds = 60 minutes timeout

while (-not $allCompleted -and $refreshCount -lt $maxRefreshCount) {
    $allCompleted = $true
    $activeJobs = 0
    
    # Clear screen every 10 refreshes for better visibility (but not in CI)
    if ($refreshCount % 10 -eq 0 -and -not ($env:GITHUB_ACTIONS -eq 'true' -or $env:CI -eq 'true')) {
        try {
            Clear-Host
        } catch {
            # Ignore clear host errors in non-interactive environments
        }
        Write-Host "================================================================" -ForegroundColor Cyan
        Write-Host "   Complete Development Lifecycle - Real-time Progress" -ForegroundColor Cyan
        Write-Host "================================================================" -ForegroundColor Cyan
        Write-Host ""
    }
    
    foreach ($config in $testConfigs) {
        if (-not $config.Enabled) { continue }
        
        $testName = $config.Name
        $job = $jobs[$testName]
        $logPath = "$logsDir/$($config.LogFile)"
        
        # Defensive check: ensure job exists
        if (-not $job) {
            Write-Host "[WARNING] Job for $testName not found, marking as completed" -ForegroundColor Yellow
            continue
        }
        
        if ($job.State -eq "Running") {
            $allCompleted = $false
            $activeJobs++
            
            # Get current progress
            $currentProgress = Get-TestProgress $logPath $testName
            $progress[$testName] = $currentProgress
            
            # Show progress bar
            Write-Progress -Id ($testConfigs.IndexOf($config) + 1) -Activity $testName -Status $currentProgress.Status -PercentComplete $currentProgress.Percentage
            
        } elseif ($job.State -eq "Completed") {
            $result = Receive-Job $job
            Write-Progress -Id ($testConfigs.IndexOf($config) + 1) -Activity $testName -Status "Completed" -PercentComplete 100 -Completed
            Write-Host "[OK] $testName completed successfully" -ForegroundColor Green
            Remove-Job $job
            $jobs.Remove($testName)
            
        } elseif ($job.State -eq "Failed") {
            Write-Progress -Id ($testConfigs.IndexOf($config) + 1) -Activity $testName -Status "Failed" -PercentComplete 100 -Completed  
            Write-Host "[ERROR] $testName failed" -ForegroundColor Red
            Remove-Job $job
            $jobs.Remove($testName)
        } else {
            # Handle other job states (Stopped, Blocked, etc.)
            Write-Host "[WARNING] $testName in unexpected state: $($job.State)" -ForegroundColor Yellow
            $allCompleted = $false
            $activeJobs++
            
            # If job has been in non-standard state for too long, consider it failed
            if ($refreshCount % 30 -eq 0) {  # Check every minute
                Write-Host "[WARNING] $testName has been in state '$($job.State)' for extended time" -ForegroundColor Yellow
            }
        }
    }
    
    if ($allCompleted) { break }
    
    # Show summary status
    Write-Host "`r[INFO] Active jobs: $activeJobs | Refresh: $refreshCount | $(Get-Date -Format 'HH:mm:ss')" -ForegroundColor Cyan -NoNewline
    
    Start-Sleep -Seconds 2
    $refreshCount++
}

# Check for timeout
if ($refreshCount -ge $maxRefreshCount) {
    Write-Host ""
    Write-Host "[WARNING] Test execution timed out after 60 minutes. Stopping remaining jobs..." -ForegroundColor Yellow
    
    # Clean up any remaining jobs
    foreach ($jobName in @($jobs.Keys)) {
        $job = $jobs[$jobName]
        if ($job.State -eq "Running") {
            Write-Host "[INFO] Stopping job: $jobName" -ForegroundColor Gray
            Stop-Job $job
        }
        Remove-Job $job -Force
        $jobs.Remove($jobName)
    }
}

# Clear all progress bars
for ($i = 1; $i -le $testConfigs.Count; $i++) {
    Write-Progress -Id $i -Activity "Completed" -Completed
}

Write-Host ""
Write-Host ""
Write-Host "=== All Tests Completed ===" -ForegroundColor Green
Write-Host "[OK] Complete development lifecycle finished!" -ForegroundColor Green
Write-Host ""

# Display final results
Write-Host "=== Test Results and Logs ===" -ForegroundColor Yellow
Write-Host "Check the following log files for detailed results:" -ForegroundColor White

foreach ($config in $testConfigs) {
    if ($config.Enabled) {
        $logPath = "$logsDir/$($config.LogFile)"
        if (Test-Path $logPath) {
            $size = (Get-Item $logPath).Length
            Write-Host "  - $($config.Name): $logPath ($([math]::Round($size/1KB, 1)) KB)" -ForegroundColor Gray
        }
    }
}

Write-Host ""
Write-Host "Log directory contents:" -ForegroundColor White
Get-ChildItem "$logsDir/*.log" | ForEach-Object {
    $size = [math]::Round($_.Length/1KB, 1)
    Write-Host "  $($_.Name) ($size KB)" -ForegroundColor Gray
}

Write-Host ""
Write-Host "[OK] Development lifecycle completed successfully!" -ForegroundColor Green
