#!/usr/bin/env pwsh

param(
    [string]$ExpectedMessagesString = $env:SIMULATOR_NUM_MESSAGES ?? "1000000",
    [int]$ExpectedMessages = [int]$ExpectedMessagesString,
    [string]$RedisCounterKey = $env:SIMULATOR_REDIS_KEY_SINK_COUNTER ?? "flinkdotnet:sample:processed_message_counter",
    [int]$MaxWaitSeconds = 300,  # Increased to 5 minutes for CI environments
    [int]$CheckIntervalSeconds = 2  # Reduced check frequency to avoid spam
)

Write-Host "🕐 Waiting for FlinkJobSimulator to complete message processing..."
Write-Host "Expected messages: $ExpectedMessages"
Write-Host "Redis counter key: $RedisCounterKey"
Write-Host "Max wait time: $MaxWaitSeconds seconds"
Write-Host "Check interval: $CheckIntervalSeconds seconds"

$startTime = Get-Date
$lastMessageCount = 0
$lastProgressTime = Get-Date
$dynamicTimeoutExtension = 0
$redisConnectAttempts = 0
$maxRedisConnectAttempts = 10

while ($true) {
    $now = Get-Date
    $elapsed = ($now - $startTime).TotalSeconds
    $effectiveTimeout = $MaxWaitSeconds + $dynamicTimeoutExtension

    $redisPort = if ($env:DOTNET_REDIS_PORT) { $env:DOTNET_REDIS_PORT } elseif ($env:DOTNET_REDIS_URL -match ':([0-9]+)$') { $Matches[1] } else { '6379' }
    if (Get-Command redis-cli -ErrorAction SilentlyContinue) {
        $redisCommand = "redis-cli -h localhost -p $redisPort -a `"FlinkDotNet_Redis_CI_Password_2024`" get `"$RedisCounterKey`""
    }
    else {
        $containerId = docker ps --filter 'ancestor=redis:7.4' --format '{{.ID}}' | Select-Object -First 1
        if (-not $containerId) {
            Write-Host "⚠️ Redis container not found. Checking for any Redis containers..." -ForegroundColor Yellow
            $anyRedisContainer = docker ps --filter 'name=redis' --format '{{.ID}}' | Select-Object -First 1
            if ($anyRedisContainer) {
                $containerId = $anyRedisContainer
                Write-Host "🔍 Found Redis container by name: $containerId" -ForegroundColor Yellow
            } else {
                Write-Host "❌ No Redis containers found at all" -ForegroundColor Red
                $redisConnectAttempts++
                if ($redisConnectAttempts -gt $maxRedisConnectAttempts) {
                    Write-Host "❌ Failed to find Redis container after $maxRedisConnectAttempts attempts" -ForegroundColor Red
                    throw "RedisContainerNotFound"
                }
                Start-Sleep -Seconds $CheckIntervalSeconds
                continue
            }
        }
        $redisCommand = "docker exec -i $containerId redis-cli -a `"FlinkDotNet_Redis_CI_Password_2024`" get `"$RedisCounterKey`""
    }
    try {
        $counterValue = Invoke-Expression $redisCommand 2>$null
        $redisConnectAttempts = 0  # Reset on successful connection
    } catch {
        Write-Host "⚠️ Redis not accessible: $($_.Exception.Message)" -ForegroundColor Yellow
        $redisConnectAttempts++
        if ($redisConnectAttempts -gt $maxRedisConnectAttempts) {
            Write-Host "❌ Failed to connect to Redis after $maxRedisConnectAttempts attempts" -ForegroundColor Red
            Write-Host "💡 This usually means the FlinkJobSimulator infrastructure is not running properly" -ForegroundColor Yellow
            Write-Host "💡 Check that AppHost started correctly and JobManager/TaskManagers are running" -ForegroundColor Yellow
            throw "RedisConnectionFailed"
        }
        Start-Sleep -Seconds $CheckIntervalSeconds
        continue
    }

    if ($counterValue -match '^\d+$') {
        $currentCount = [int]$counterValue
        $elapsedProcessing = ($now - $startTime).TotalSeconds
        $rate = if ($elapsedProcessing -gt 0) { [math]::Round($currentCount / $elapsedProcessing, 2) } else { 0 }

        Write-Host "[PROGRESS] RedisCount=$($currentCount.ToString('N0'))  Rate=$rate msg/sec"

        if ($currentCount -ne $lastMessageCount) {
            $lastMessageCount = $currentCount
            $lastProgressTime = $now
            $dynamicTimeoutExtension += 10  # More generous timeout extension
            Write-Host "🔄 Progress detected! Timeout extended by 10s (total extension: ${dynamicTimeoutExtension}s)" -ForegroundColor Green
        }

        if ($currentCount -ge $ExpectedMessages) {
            Write-Host "✅ FlinkJobSimulator completed successfully! Total=$($currentCount.ToString('N0'))"
            break
        }
    } else {
        Write-Host "⏳ Waiting for Redis counter '$RedisCounterKey' to initialize... (${elapsed}s elapsed)" -ForegroundColor Yellow
    }

    if ($elapsed -gt $effectiveTimeout) {
        Write-Host "❌ Timeout exceeded after $([math]::Round($elapsed, 2))s! Incomplete processing." -ForegroundColor Red
        Write-Host "💡 Diagnostic Information:" -ForegroundColor Yellow
        Write-Host "   - Expected messages: $ExpectedMessages" -ForegroundColor Gray
        Write-Host "   - Redis counter key: $RedisCounterKey" -ForegroundColor Gray
        Write-Host "   - Last counter value: $($counterValue ?? 'NULL')" -ForegroundColor Gray
        Write-Host "   - Max wait time: $MaxWaitSeconds seconds" -ForegroundColor Gray
        Write-Host "   - Timeout extensions: $dynamicTimeoutExtension seconds" -ForegroundColor Gray
        Write-Host "   - Total effective timeout: $effectiveTimeout seconds" -ForegroundColor Gray
        Write-Host "💡 This usually indicates:" -ForegroundColor Yellow
        Write-Host "   - FlinkJobSimulator failed to start or connect to JobManager" -ForegroundColor Gray
        Write-Host "   - JobManager/TaskManager gRPC connections are failing" -ForegroundColor Gray
        Write-Host "   - Redis connection issues preventing counter initialization" -ForegroundColor Gray
        Write-Host "   - Check AppHost logs for infrastructure startup errors" -ForegroundColor Gray
        throw "TimeoutExceeded"
    }

    Start-Sleep -Seconds $CheckIntervalSeconds
}
