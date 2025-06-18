#!/usr/bin/env pwsh

<#
.SYNOPSIS
    Waits for FlinkJobSimulator to complete message processing with progress tracking

.DESCRIPTION
    This script monitors the FlinkJobSimulator progress by checking Redis counter values
    and provides real-time progress updates. It implements dynamic timeout extension
    when progress is detected and tracks message processing rate.

.PARAMETER ExpectedMessages
    The number of messages expected to be processed (default: 1000000)

.PARAMETER RedisCounterKey
    The Redis key to monitor for message count (default: "flinkdotnet:sample:processed_message_counter")

.PARAMETER MaxWaitSeconds
    Maximum time to wait for completion in seconds (default: 10)

.PARAMETER CheckIntervalSeconds
    Interval between progress checks in seconds (default: 1)

.EXAMPLE
    ./wait-for-flinkjobsimulator-completion.ps1 -ExpectedMessages 1000000
#>

param(
    [string]$ExpectedMessagesString = $env:SIMULATOR_NUM_MESSAGES ?? "1000000",
    [int]$ExpectedMessages = [int]$ExpectedMessagesString,
    [string]$RedisCounterKey = $env:SIMULATOR_REDIS_KEY_SINK_COUNTER ?? "flinkdotnet:sample:processed_message_counter",
    [int]$MaxWaitSeconds = 10,
    [int]$CheckIntervalSeconds = 1
)

Write-Host "🕐 Waiting for FlinkJobSimulator to complete message processing..."
Write-Host "Expected messages: $ExpectedMessages"
Write-Host "Redis counter key: $RedisCounterKey"

$maxWaitSeconds = $MaxWaitSeconds  # Short timeout forces consumer to be responsive like production systems
$checkIntervalSeconds = $CheckIntervalSeconds  # Poll every second for progress detection
# Apache Flink 2.0 Pattern: FlinkJobSimulator starts before producer and waits for messages
# This is normal behavior - consumers should be ready when messages arrive
$expectedMessages = $ExpectedMessages
$waitStartTime = Get-Date

$completed = $false
$completionReason = "Unknown"
$counterNotInitializedAttempts = 0
$maxCounterNotInitializedAttempts = 1
$messageProcessingStarted = $false  # Track when message processing begins

# Progress stall detection: timeout if message count doesn't change for 5 seconds
$lastMessageCount = -1
$lastMessageCountTime = Get-Date
$stallTimeoutSeconds = 5  # Timeout if no progress for 5 seconds

# Dynamic timeout extension: add 5 seconds whenever progress is detected
$dynamicTimeoutExtension = 0  # Additional seconds added due to progress
$originalMaxWaitSeconds = $maxWaitSeconds  # Keep track of original timeout

# Performance tracking like producer script
$processingStartTime = $null
$lastProgressTime = Get-Date

# Progress display tracking
$script:lastDisplayContent = ""

while (-not $completed) {
    $currentElapsed = ((Get-Date) - $waitStartTime).TotalSeconds
    
    # Apache Flink 2.0 timeout logic with dynamic extension:
    # - If no messages started processing yet: be patient (allow more time for startup)
    # - If messages started processing: use dynamic timeout that extends with progress
    $effectiveTimeout = $maxWaitSeconds + $dynamicTimeoutExtension
    $shouldTimeout = $false
    if ($messageProcessingStarted) {
        # Once processing starts, use dynamic timeout that extends with progress
        $shouldTimeout = $currentElapsed -gt $effectiveTimeout
    } else {
        # Before processing starts, allow more time for FlinkJobSimulator startup and producer to run
        # This handles the Apache Flink 2.0 pattern where consumer starts before producer
        $shouldTimeout = $currentElapsed -gt ($effectiveTimeout * 3)  # 30+ seconds for startup
    }
    
    if ($shouldTimeout) {
        break
    }
    try {
        # Check completion status first
        $statusCommand = "docker exec -i $(docker ps -q --filter 'ancestor=redis:7.4' | Select-Object -First 1) redis-cli -a FlinkDotNet_Redis_CI_Password_2024 get `"flinkdotnet:job_completion_status`""
        $completionStatus = Invoke-Expression $statusCommand 2>$null
        
        if ($completionStatus -eq "SUCCESS") {
            Write-Host "✅ FlinkJobSimulator reported SUCCESS completion status"
            $completed = $true
            $completionReason = "Success"
            break
        } elseif ($completionStatus -eq "FAILED") {
            Write-Host "❌ FlinkJobSimulator reported FAILED completion status"
            $completed = $true
            $completionReason = "Failed"
            break
        }
        
        # Check for execution errors
        $errorCommand = "docker exec -i $(docker ps -q --filter 'ancestor=redis:7.4' | Select-Object -First 1) redis-cli -a FlinkDotNet_Redis_CI_Password_2024 get `"flinkdotnet:job_execution_error`""
        $errorValue = Invoke-Expression $errorCommand 2>$null
        if ($errorValue -and $errorValue -ne "(nil)") {
            Write-Host "❌ Found job execution error in Redis: $errorValue"
            $completed = $true
            $completionReason = "Error"
            break
        }
        
        # Check message counter progress
        $redisCommand = "docker exec -i $(docker ps -q --filter 'ancestor=redis:7.4' | Select-Object -First 1) redis-cli -a FlinkDotNet_Redis_CI_Password_2024 get `"$RedisCounterKey`""
        $counterValue = Invoke-Expression $redisCommand 2>$null
        
        if ($counterValue -match '^\d+$') {
            $currentCount = [int]$counterValue
            Write-Host "💡 Found `"$RedisCounterKey`" = $currentCount"
            
            # Progress stall detection and dynamic timeout extension
            if ($currentCount -ne $lastMessageCount) {
                # Progress detected: extend timeout and update tracking
                if ($lastMessageCount -ge 0) {  # Don't extend on first detection (from -1)
                    $dynamicTimeoutExtension += 5
                    # Show progress change every second as requested
                    Write-Host "🔄 Progress detected! Redis counter changed from $lastMessageCount to $currentCount"
                }
                $lastMessageCount = $currentCount
                $lastMessageCountTime = Get-Date
            } else {
                # Message count hasn't changed - check for stall timeout
                $stallTime = ((Get-Date) - $lastMessageCountTime).TotalSeconds
                if ($messageProcessingStarted -and $stallTime -gt $stallTimeoutSeconds) {
                    Write-Host "❌ Message processing stalled! No progress for $stallTime seconds"
                    Write-Host "💡 Last message count: $currentCount (unchanged for ${stallTime}s)"
                    $completed = $true
                    $completionReason = "ProcessingStalled"
                    break
                }
            }
            
            # Mark that message processing has started if we see any count > 0
            if ($currentCount -gt 0 -and -not $messageProcessingStarted) {
                $messageProcessingStarted = $true
                $processingStartTime = Get-Date
                Write-Host "🚀 Message processing started! FlinkJobSimulator is consuming messages..."
            }
            
            # Display progress using Redis counter directly (per second tracking)
            $progressPercent = [math]::Round(($currentCount / $expectedMessages) * 100, 1)
            $currentTime = Get-Date
            $timeSinceLastProgress = ($currentTime - $lastProgressTime).TotalSeconds
            
            # Print progress per second
            if ($timeSinceLastProgress -ge 1.0) {
                if ($processingStartTime -and $messageProcessingStarted) {
                    $elapsedProcessingSeconds = ($currentTime - $processingStartTime).TotalSeconds
                    $rate = if ($elapsedProcessingSeconds -gt 0) { [math]::Round($currentCount / $elapsedProcessingSeconds, 0) } else { 0 }
                    Write-Host "[PROGRESS] Sent=$($currentCount.ToString('N0'))  Rate=$($rate.ToString('N0')) msg/sec"
                }
                $lastProgressTime = $currentTime
            }
            
            if ($currentCount -ge $expectedMessages) {
                # Calculate final rate like producer script
                if ($processingStartTime) {
                    Write-Host "✅ [FINISH] FlinkJobSimulator completed! Total: $($currentCount.ToString('N0'))"
                } else {
                    Write-Host "✅ FlinkJobSimulator completed message processing! Messages processed: $currentCount"
                }
                $completed = $true
                $completionReason = "MessageCountReached"
                break
            } else {
                $remainingSeconds = $maxWaitSeconds - ((Get-Date) - $waitStartTime).TotalSeconds
                $progressPercent = [math]::Round(($currentCount / $expectedMessages) * 100, 1)
                
                # Apache Flink 2.0 Pattern: Count = 0 means consumer is ready and waiting for messages
                if ($currentCount -eq 0 -and -not $messageProcessingStarted) {
                    # Only log once when counter is initialized but no messages processed yet
                    if ($counterNotInitializedAttempts -eq 0) {
                        Write-Host "💡 FlinkJobSimulator ready and waiting for messages (Apache Flink 2.0 pattern)"
                        $counterNotInitializedAttempts = 1  # Mark as logged once
                    }
                } else {
                    # Normal progress logging for when processing has started
                    # Only log progress every 10% milestones to prevent spam
                    $shouldLog = $false
                    if ($currentCount -gt 0 -and $progressPercent % 10 -eq 0) {
                        $shouldLog = $true  # Log at 10%, 20%, 30%, etc.
                    } elseif ($currentCount -gt 0 -and $currentCount % [math]::Max(1, $expectedMessages / 10) -eq 0) {
                        $shouldLog = $true  # Log at message count milestones
                    }
                    
                    if ($shouldLog) {
                        # Calculate appropriate remaining time based on processing state
                        if ($messageProcessingStarted) {
                            $remainingSeconds = $maxWaitSeconds - $currentElapsed
                        } else {
                            $remainingSeconds = ($maxWaitSeconds * 3) - $currentElapsed  # Startup timeout
                        }
                        
                        # Calculate message rate like producer script
                        $currentTime = Get-Date
                        if ($processingStartTime -and $messageProcessingStarted) {
                            $elapsedProcessingSeconds = ($currentTime - $processingStartTime).TotalSeconds
                            $rate = if ($elapsedProcessingSeconds -gt 0) { [math]::Round($currentCount / $elapsedProcessingSeconds, 0) } else { 0 }
                            Write-Host "📊 [PROGRESS] Processed=$($currentCount.ToString('N0')) / $($expectedMessages.ToString('N0'))  Rate=$($rate.ToString('N0')) msg/sec  Progress=$progressPercent%"
                        } else {
                            Write-Host "📊 Current message count: $currentCount / $expectedMessages"
                            Write-Host "⏳ Progress: $progressPercent% (${remainingSeconds:F0}s remaining)"
                        }
                    }
                }
            }
        } else {
            # Apache Flink 2.0 Pattern: Counter not yet initialized means FlinkJobSimulator is starting
            # This is normal - consumers need time to initialize Redis connections
            $counterNotInitializedAttempts++
            
            if ($counterNotInitializedAttempts -eq 1) {
                Write-Host "⏳ FlinkJobSimulator initializing Redis connection (Apache Flink 2.0 startup)"
            } elseif ($counterNotInitializedAttempts -ge $maxCounterNotInitializedAttempts) {
                Write-Host "❌ FlinkJobSimulator failed to initialize Redis counter after $maxCounterNotInitializedAttempts attempts"
                Write-Host "💡 This indicates FlinkJobSimulator may not be running or cannot connect to Redis"
                $completed = $true
                $completionReason = "FlinkJobSimulatorNotStarted"
                break
            }
        }
        
        # Check if AppHost/FlinkJobSimulator process is still alive
        if (Test-Path apphost.pid) {
            $apphostPid = Get-Content apphost.pid
            $apphostProcess = Get-Process -Id $apphostPid -ErrorAction SilentlyContinue
            if (-not $apphostProcess) {
                Write-Host "💥 AppHost process (PID $apphostPid) has terminated unexpectedly!"
                $completed = $true
                $completionReason = "AppHostTerminated"
                break
            }
        } else {
            Write-Host "💥 AppHost PID file not found - process may have crashed"
            $completed = $true
            $completionReason = "AppHostPidMissing"
            break
        }
        
        Start-Sleep -Seconds $checkIntervalSeconds
    } catch {
        Write-Host "⏳ Waiting for Redis to be accessible... ($($_.Exception.Message))"
        Start-Sleep -Seconds $checkIntervalSeconds
    }
}

# Report final status with dynamic timeout information
Write-Host "`n🎯 === WAIT COMPLETION SUMMARY ==="
Write-Host "Completion reason: $completionReason"
Write-Host "Wait duration: $([math]::Round(((Get-Date) - $waitStartTime).TotalSeconds, 1))s"

if (-not $completed) {
    $finalElapsed = ((Get-Date) - $waitStartTime).TotalSeconds
    $effectiveFinalTimeout = $maxWaitSeconds + $dynamicTimeoutExtension
    if ($messageProcessingStarted) {
        Write-Host "❌ FlinkJobSimulator did not complete processing within $effectiveFinalTimeout seconds after starting"
        Write-Host "⏰ PROCESSING TIMEOUT - Messages started but processing didn't complete quickly enough despite dynamic timeout extensions"
    } else {
        Write-Host "❌ FlinkJobSimulator did not start processing messages within $([math]::Round($finalElapsed, 1)) seconds"
        Write-Host "⏰ STARTUP TIMEOUT - FlinkJobSimulator never began consuming messages (Apache Flink 2.0 pattern issue)"
    }
    
    # Final comprehensive check
    try {
        $finalCounterCommand = "docker exec -i $(docker ps -q --filter 'ancestor=redis:7.4' | Select-Object -First 1) redis-cli -a FlinkDotNet_Redis_CI_Password_2024 get `"$RedisCounterKey`""
        $finalCounterValue = Invoke-Expression $finalCounterCommand 2>$null
        Write-Host "📊 Final Redis counter value: $finalCounterValue"
        
        $finalStatusCommand = "docker exec -i $(docker ps -q --filter 'ancestor=redis:7.4' | Select-Object -First 1) redis-cli -a FlinkDotNet_Redis_CI_Password_2024 get `"flinkdotnet:job_completion_status`""
        $finalStatusValue = Invoke-Expression $finalStatusCommand 2>$null
        Write-Host "📊 Final completion status: $finalStatusValue"
        
        $finalErrorCommand = "docker exec -i $(docker ps -q --filter 'ancestor=redis:7.4' | Select-Object -First 1) redis-cli -a FlinkDotNet_Redis_CI_Password_2024 get `"flinkdotnet:job_execution_error`""
        $finalErrorValue = Invoke-Expression $finalErrorCommand 2>$null
        if ($finalErrorValue -and $finalErrorValue -ne "(nil)") {
            Write-Host "📊 Final error marker: $finalErrorValue"
        }
        
        # Check if any messages were actually processed
        if ($finalCounterValue -match '^\d+$' -and [int]$finalCounterValue -gt 0) {
            $processedCount = [int]$finalCounterValue
            if ($processedCount -ge $expectedMessages) {
                Write-Host "✅ FlinkJobSimulator processed $processedCount messages (complete success - exceeded target)" -ForegroundColor Green
                $completed = $true
                $completionReason = "MessageCountReached"
            } else {
                Write-Host "📊 FlinkJobSimulator processed $processedCount messages (partial success)" -ForegroundColor Yellow
                $completed = $true
                $completionReason = "PartialCompletion"
            }
        } else {
            Write-Host "❌ FlinkJobSimulator processed 0 messages - this is a FAILURE" -ForegroundColor Red
            $completionReason = "NoMessagesProcessed"
            throw "FlinkJobSimulator failed to process any messages within timeout period"
        }
    } catch {
        Write-Host "💥 Could not perform final Redis check: $($_.Exception.Message)"
        $completionReason = "TimeoutWithErrors"
        throw "FlinkJobSimulator completion timeout - unable to verify message processing"
    }
}

# Check final success condition
if ($completionReason -eq "Success" -or $completionReason -eq "MessageCountReached" -or $completionReason -eq "PartialCompletion") {
    if ($completionReason -eq "PartialCompletion") {
        Write-Host "⚠️ FlinkJobSimulator completed with partial message processing!" -ForegroundColor Yellow
    } elseif ($completionReason -eq "MessageCountReached") {
        Write-Host "✅ FlinkJobSimulator completed successfully! All expected messages processed." -ForegroundColor Green
    } else {
        Write-Host "✅ FlinkJobSimulator completed successfully!" -ForegroundColor Green
    }
} else {
    Write-Host "❌ FlinkJobSimulator failed: $completionReason"
    throw "FlinkJobSimulator execution failed"
}