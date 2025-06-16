#!/usr/bin/env pwsh
<#
.SYNOPSIS
    Comprehensive Aspire sub-task log capture with error and warning detection.

.DESCRIPTION
    This script captures logs from all Aspire sub-tasks including:
    - JobManager process logs
    - All 20 TaskManager process logs  
    - FlinkJobSimulator process logs
    - Redis container logs
    - Kafka container logs
    - Aggregated error and warning summary

.PARAMETER LogDirectory
    Directory to store captured logs (default: aspire-logs)

.PARAMETER MonitorDurationSeconds
    How long to monitor for logs (default: 300 = 5 minutes)

.EXAMPLE
    ./scripts/capture-aspire-logs.ps1
    Captures all Aspire sub-task logs with default settings.

.EXAMPLE
    ./scripts/capture-aspire-logs.ps1 -LogDirectory "debug-logs" -MonitorDurationSeconds 600
    Captures logs for 10 minutes into debug-logs directory.
#>

param(
    [string]$LogDirectory = "aspire-logs",
    [int]$MonitorDurationSeconds = 300
)

$ErrorActionPreference = 'Continue'  # Don't stop on individual log failures
$VerbosePreference = 'Continue'

Write-Host "=== 📋 ASPIRE SUB-TASK LOG CAPTURE ===" -ForegroundColor Cyan
Write-Host "Started at: $(Get-Date -Format 'yyyy-MM-dd HH:mm:ss') UTC" -ForegroundColor White
Write-Host "Log Directory: $LogDirectory" -ForegroundColor White
Write-Host "Monitor Duration: $MonitorDurationSeconds seconds" -ForegroundColor White

# Create log directory
if (-not (Test-Path $LogDirectory)) {
    New-Item -Path $LogDirectory -ItemType Directory -Force | Out-Null
    Write-Host "✅ Created log directory: $LogDirectory" -ForegroundColor Green
}

# Global tracking variables
$global:ErrorCount = 0
$global:WarningCount = 0
$global:CapturedLogs = @()

function Write-LogSummary {
    param([string]$LogType, [string]$Source, [string]$Message, [string]$Level = "INFO")
    
    $timestamp = Get-Date -Format 'yyyy-MM-dd HH:mm:ss.fff'
    $logEntry = @{
        Timestamp = $timestamp
        LogType = $LogType
        Source = $Source
        Level = $Level
        Message = $Message
    }
    
    $global:CapturedLogs += $logEntry
    
    if ($Level -eq "ERROR") {
        $global:ErrorCount++
        Write-Host "❌ [$LogType] ${Source}: $Message" -ForegroundColor Red
    } elseif ($Level -eq "WARNING") {
        $global:WarningCount++
        Write-Host "⚠️ [$LogType] ${Source}: $Message" -ForegroundColor Yellow
    } else {
        Write-Host "ℹ️ [$LogType] ${Source}: $Message" -ForegroundColor Gray
    }
}

function Capture-DotNetProcessLogs {
    param([string]$ProcessPattern, [string]$LogFileName, [string]$FriendlyName)
    
    Write-Host "`n🔍 Capturing logs for $FriendlyName..." -ForegroundColor Cyan
    
    try {
        # Find processes matching the pattern
        $processes = Get-Process | Where-Object { $_.ProcessName -like "*$ProcessPattern*" -or $_.MainWindowTitle -like "*$ProcessPattern*" }
        
        if (-not $processes) {
            Write-LogSummary "PROCESS" $FriendlyName "No processes found matching pattern: $ProcessPattern" "WARNING"
            return
        }
        
        $logPath = Join-Path $LogDirectory $LogFileName
        $logContent = @()
        $logContent += "=== $FriendlyName LOG CAPTURE ==="
        $logContent += "Capture Time: $(Get-Date -Format 'yyyy-MM-dd HH:mm:ss') UTC"
        $logContent += "Process Pattern: $ProcessPattern"
        $logContent += "Found Processes: $($processes.Count)"
        $logContent += ""
        
        foreach ($process in $processes) {
            $logContent += "--- Process: $($process.ProcessName) (PID: $($process.Id)) ---"
            $logContent += "Start Time: $($process.StartTime)"
            $logContent += "CPU Time: $($process.TotalProcessorTime)"
            $logContent += "Memory: $([math]::Round($process.WorkingSet64 / 1MB, 2)) MB"
            $logContent += "Threads: $($process.Threads.Count)"
            
            # Try to capture recent logs from Windows Event Log
            try {
                $recentEvents = Get-WinEvent -FilterHashtable @{LogName='Application'; ID=1000,1001,1026; StartTime=(Get-Date).AddMinutes(-10)} -ErrorAction SilentlyContinue | 
                    Where-Object { $_.ProcessId -eq $process.Id -or $_.Message -like "*$($process.ProcessName)*" } |
                    Select-Object -First 10
                
                if ($recentEvents) {
                    $logContent += "Recent Events:"
                    foreach ($event in $recentEvents) {
                        $level = if ($event.LevelDisplayName -eq "Error") { "ERROR" } elseif ($event.LevelDisplayName -eq "Warning") { "WARNING" } else { "INFO" }
                        $logContent += "  $($event.TimeCreated): [$($event.LevelDisplayName)] $($event.Message)"
                        
                        if ($level -ne "INFO") {
                            Write-LogSummary "PROCESS" $FriendlyName "$($event.Message)" $level
                        }
                    }
                } else {
                    $logContent += "No recent events found in Windows Event Log"
                }
            } catch {
                $logContent += "Could not access Windows Event Log: $($_.Exception.Message)"
            }
            
            $logContent += ""
        }
        
        # Write to log file
        $logContent | Out-File -FilePath $logPath -Encoding UTF8 -Force
        Write-LogSummary "PROCESS" $FriendlyName "Log captured to $logPath with $($processes.Count) processes" "INFO"
        
    } catch {
        Write-LogSummary "PROCESS" $FriendlyName "Failed to capture logs: $($_.Exception.Message)" "ERROR"
    }
}

function Capture-ContainerLogs {
    param([string]$ContainerNamePattern, [string]$LogFileName, [string]$FriendlyName)
    
    Write-Host "`n🐳 Capturing container logs for $FriendlyName..." -ForegroundColor Cyan
    
    try {
        # Find containers matching the pattern
        $containerIds = docker ps -q --filter "name=$ContainerNamePattern" 2>$null
        
        if (-not $containerIds) {
            # Try with image name pattern
            $containerIds = docker ps -q --filter "ancestor=*$ContainerNamePattern*" 2>$null
        }
        
        if (-not $containerIds) {
            Write-LogSummary "CONTAINER" $FriendlyName "No containers found matching pattern: $ContainerNamePattern" "WARNING"
            return
        }
        
        $logPath = Join-Path $LogDirectory $LogFileName
        $logContent = @()
        $logContent += "=== $FriendlyName CONTAINER LOG CAPTURE ==="
        $logContent += "Capture Time: $(Get-Date -Format 'yyyy-MM-dd HH:mm:ss') UTC"
        $logContent += "Container Pattern: $ContainerNamePattern"
        $logContent += ""
        
        foreach ($containerId in $containerIds) {
            $containerInfo = docker inspect $containerId --format '{{.Name}} {{.State.Status}} {{.Config.Image}}' 2>$null
            $logContent += "--- Container: $containerInfo ---"
            
            # Get container logs (last 100 lines)
            $containerLogs = docker logs --tail 100 $containerId 2>&1
            
            if ($containerLogs) {
                $logContent += "Recent Logs:"
                foreach ($line in $containerLogs) {
                    $logContent += "  $line"
                    
                    # Check for errors and warnings in container logs
                    if ($line -match "ERROR|FATAL|Exception|Failed|failed|Error") {
                        Write-LogSummary "CONTAINER" $FriendlyName "$line" "ERROR"
                    } elseif ($line -match "WARN|WARNING|Warn|warning") {
                        Write-LogSummary "CONTAINER" $FriendlyName "$line" "WARNING"
                    }
                }
            } else {
                $logContent += "No logs available"
            }
            
            # Get container stats
            $stats = docker stats --no-stream --format "table {{.CPUPerc}}\t{{.MemUsage}}\t{{.NetIO}}\t{{.BlockIO}}" $containerId 2>$null
            if ($stats) {
                $logContent += "Current Stats: $stats"
            }
            
            $logContent += ""
        }
        
        # Write to log file
        $logContent | Out-File -FilePath $logPath -Encoding UTF8 -Force
        Write-LogSummary "CONTAINER" $FriendlyName "Log captured to $logPath with $($containerIds.Count) containers" "INFO"
        
    } catch {
        Write-LogSummary "CONTAINER" $FriendlyName "Failed to capture logs: $($_.Exception.Message)" "ERROR"
    }
}

function Capture-AspireServiceLogs {
    Write-Host "`n🏗️ Capturing Aspire service discovery logs..." -ForegroundColor Cyan
    
    try {
        $logPath = Join-Path $LogDirectory "aspire-services.log"
        $logContent = @()
        $logContent += "=== ASPIRE SERVICE DISCOVERY LOG ==="
        $logContent += "Capture Time: $(Get-Date -Format 'yyyy-MM-dd HH:mm:ss') UTC"
        $logContent += ""
        
        # Get all running containers and their port mappings
        $logContent += "--- CONTAINER SERVICES ---"
        $containers = docker ps --format "table {{.ID}}\t{{.Image}}\t{{.Status}}\t{{.Ports}}\t{{.Names}}" 2>$null
        if ($containers) {
            $logContent += $containers
        } else {
            $logContent += "No containers found"
            Write-LogSummary "ASPIRE" "ServiceDiscovery" "No Docker containers found" "WARNING"
        }
        
        # Get Aspire-specific processes
        $logContent += ""
        $logContent += "--- .NET PROJECT SERVICES ---"
        $dotnetProcesses = Get-Process | Where-Object { 
            $_.ProcessName -like "*FlinkDotNet*" -or 
            $_.ProcessName -like "*TaskManager*" -or 
            $_.ProcessName -like "*JobManager*" -or
            $_.ProcessName -like "*Simulator*" -or
            $_.MainWindowTitle -like "*Aspire*"
        }
        
        if ($dotnetProcesses) {
            foreach ($proc in $dotnetProcesses) {
                $procInfo = "$($proc.ProcessName) (PID: $($proc.Id)) - Memory: $([math]::Round($proc.WorkingSet64 / 1MB, 2)) MB"
                $logContent += $procInfo
                Write-LogSummary "ASPIRE" "ServiceDiscovery" "Found service: $procInfo" "INFO"
            }
        } else {
            $logContent += "No .NET Aspire processes found"
            Write-LogSummary "ASPIRE" "ServiceDiscovery" "No .NET Aspire processes found" "WARNING"
        }
        
        # Check for environment variables
        $logContent += ""
        $logContent += "--- ENVIRONMENT VARIABLES ---"
        $aspireEnvVars = @(
            "DOTNET_REDIS_URL",
            "DOTNET_KAFKA_BOOTSTRAP_SERVERS", 
            "SIMULATOR_NUM_MESSAGES",
            "SIMULATOR_KAFKA_TOPIC",
            "SIMULATOR_REDIS_KEY_SINK_COUNTER",
            "ASPIRE_ALLOW_UNSECURED_TRANSPORT",
            "STRESS_TEST_MODE"
        )
        
        foreach ($varName in $aspireEnvVars) {
            $varValue = [Environment]::GetEnvironmentVariable($varName)
            if ($varValue) {
                $logContent += "$varName = $varValue"
            } else {
                $logContent += "$varName = <NOT SET>"
                Write-LogSummary "ASPIRE" "Environment" "Missing environment variable: $varName" "WARNING"
            }
        }
        
        # Write to log file
        $logContent | Out-File -FilePath $logPath -Encoding UTF8 -Force
        Write-LogSummary "ASPIRE" "ServiceDiscovery" "Aspire service logs captured to $logPath" "INFO"
        
    } catch {
        Write-LogSummary "ASPIRE" "ServiceDiscovery" "Failed to capture Aspire service logs: $($_.Exception.Message)" "ERROR"
    }
}

function Capture-FlinkJobSimulatorLogs {
    Write-Host "`n🎯 Capturing FlinkJobSimulator specific logs..." -ForegroundColor Cyan
    
    try {
        $logPath = Join-Path $LogDirectory "flinkjobsimulator.log"
        $logContent = @()
        $logContent += "=== FLINKJOBSIMULATOR SPECIFIC LOGS ==="
        $logContent += "Capture Time: $(Get-Date -Format 'yyyy-MM-dd HH:mm:ss') UTC"
        $logContent += ""
        
        # Check for FlinkJobSimulator log files
        $simulatorLogFiles = @(
            "flinkjobsimulator_startup.log",
            "flinkjobsimulator_consumer.log",
            "flinkjobsimulator_status.log",
            "flinkjobsimulator_state.log"
        )
        
        foreach ($logFile in $simulatorLogFiles) {
            $logContent += "--- $logFile ---"
            if (Test-Path $logFile) {
                $fileContent = Get-Content $logFile -Raw
                $logContent += $fileContent
                
                # Check for errors in FlinkJobSimulator logs
                if ($fileContent -like "*ERROR*" -or $fileContent -like "*FAILED*" -or $fileContent -like "*Exception*") {
                    Write-LogSummary "SIMULATOR" $logFile "Contains errors - check log content" "ERROR"
                } elseif ($fileContent -like "*WARN*" -or $fileContent -like "*WARNING*") {
                    Write-LogSummary "SIMULATOR" $logFile "Contains warnings - check log content" "WARNING"
                } else {
                    Write-LogSummary "SIMULATOR" $logFile "Log file captured successfully" "INFO"
                }
            } else {
                $logContent += "File not found"
                Write-LogSummary "SIMULATOR" $logFile "Log file not found" "WARNING"
            }
            $logContent += ""
        }
        
        # Write to log file
        $logContent | Out-File -FilePath $logPath -Encoding UTF8 -Force
        Write-LogSummary "SIMULATOR" "FlinkJobSimulator" "FlinkJobSimulator logs captured to $logPath" "INFO"
        
    } catch {
        Write-LogSummary "SIMULATOR" "FlinkJobSimulator" "Failed to capture FlinkJobSimulator logs: $($_.Exception.Message)" "ERROR"
    }
}

function Generate-ErrorWarningReport {
    Write-Host "`n📊 Generating comprehensive error and warning report..." -ForegroundColor Cyan
    
    try {
        $reportPath = Join-Path $LogDirectory "error-warning-report.log"
        $reportContent = @()
        $reportContent += "=== ASPIRE SUB-TASK ERROR AND WARNING REPORT ==="
        $reportContent += "Generated: $(Get-Date -Format 'yyyy-MM-dd HH:mm:ss') UTC"
        $reportContent += "Monitor Duration: $MonitorDurationSeconds seconds"
        $reportContent += "Total Errors: $global:ErrorCount"
        $reportContent += "Total Warnings: $global:WarningCount"
        $reportContent += ""
        
        if ($global:CapturedLogs.Count -gt 0) {
            # Group by level
            $errors = $global:CapturedLogs | Where-Object { $_.Level -eq "ERROR" }
            $warnings = $global:CapturedLogs | Where-Object { $_.Level -eq "WARNING" }
            
            if ($errors.Count -gt 0) {
                $reportContent += "--- ERRORS FOUND ---"
                foreach ($error in $errors) {
                    $reportContent += "[$($error.Timestamp)] $($error.LogType)/$($error.Source): $($error.Message)"
                }
                $reportContent += ""
            }
            
            if ($warnings.Count -gt 0) {
                $reportContent += "--- WARNINGS FOUND ---"
                foreach ($warning in $warnings) {
                    $reportContent += "[$($warning.Timestamp)] $($warning.LogType)/$($warning.Source): $($warning.Message)"
                }
                $reportContent += ""
            }
            
            # Group by source
            $reportContent += "--- SUMMARY BY SOURCE ---"
            $groupedBySource = $global:CapturedLogs | Group-Object Source
            foreach ($group in $groupedBySource) {
                $errorCount = ($group.Group | Where-Object { $_.Level -eq "ERROR" }).Count
                $warningCount = ($group.Group | Where-Object { $_.Level -eq "WARNING" }).Count
                $reportContent += "$($group.Name): $errorCount errors, $warningCount warnings"
            }
        } else {
            $reportContent += "No errors or warnings captured during monitoring period."
        }
        
        # Write to report file
        $reportContent | Out-File -FilePath $reportPath -Encoding UTF8 -Force
        
        Write-Host "📋 ERROR AND WARNING SUMMARY:" -ForegroundColor Yellow
        Write-Host "  Total Errors: $global:ErrorCount" -ForegroundColor Red
        Write-Host "  Total Warnings: $global:WarningCount" -ForegroundColor Yellow
        Write-Host "  Report saved to: $reportPath" -ForegroundColor Green
        
    } catch {
        Write-Host "❌ Failed to generate error/warning report: $($_.Exception.Message)" -ForegroundColor Red
    }
}

# Main execution
Write-Host "`n🚀 Starting comprehensive Aspire sub-task log capture..." -ForegroundColor Green

# Capture logs from different components
Capture-DotNetProcessLogs "JobManager" "jobmanager.log" "JobManager"
Capture-DotNetProcessLogs "TaskManager" "taskmanager.log" "TaskManagers"
Capture-DotNetProcessLogs "FlinkJobSimulator" "flinkjobsimulator-process.log" "FlinkJobSimulator Process"
Capture-ContainerLogs "redis" "redis-container.log" "Redis"
Capture-ContainerLogs "kafka" "kafka-container.log" "Kafka"
Capture-AspireServiceLogs
Capture-FlinkJobSimulatorLogs

# Wait and monitor for additional logs if requested
if ($MonitorDurationSeconds -gt 60) {
    Write-Host "`n⏳ Monitoring for additional logs for $MonitorDurationSeconds seconds..." -ForegroundColor Yellow
    
    $monitorEndTime = (Get-Date).AddSeconds($MonitorDurationSeconds - 60)  # We already spent ~60 seconds capturing
    
    while ((Get-Date) -lt $monitorEndTime) {
        Start-Sleep -Seconds 30
        
        # Re-capture container logs to catch new errors
        Capture-ContainerLogs "redis" "redis-container-update.log" "Redis Update"
        Capture-ContainerLogs "kafka" "kafka-container-update.log" "Kafka Update"
        
        Write-Host "." -NoNewline -ForegroundColor Gray
    }
    Write-Host " Monitoring complete." -ForegroundColor Green
}

# Generate final report
Generate-ErrorWarningReport

Write-Host "`n=== 📋 ASPIRE LOG CAPTURE COMPLETE ===" -ForegroundColor Cyan
Write-Host "Logs stored in: $LogDirectory" -ForegroundColor Green
Write-Host "Check error-warning-report.log for comprehensive analysis" -ForegroundColor Green
Write-Host "Completed at: $(Get-Date -Format 'yyyy-MM-dd HH:mm:ss') UTC" -ForegroundColor White