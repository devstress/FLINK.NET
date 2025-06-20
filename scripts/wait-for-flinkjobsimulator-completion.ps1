#!/usr/bin/env pwsh

param(
    [string]$ExpectedMessagesString = $env:SIMULATOR_NUM_MESSAGES ?? "1000000",
    [int]$ExpectedMessages = [int]$ExpectedMessagesString,
    [string]$RedisCounterKey = $env:SIMULATOR_REDIS_KEY_SINK_COUNTER ?? "flinkdotnet:sample:processed_message_counter",
    [int]$MaxWaitSeconds = 5,
    [int]$CheckIntervalSeconds = 1
)

Write-Host "🕐 Waiting for FlinkJobSimulator to complete message processing..."
Write-Host "Expected messages: $ExpectedMessages"
Write-Host "Redis counter key: $RedisCounterKey"

$startTime = Get-Date
$lastMessageCount = 0
$lastProgressTime = Get-Date
$dynamicTimeoutExtension = 0

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
        $redisCommand = "docker exec -i $containerId redis-cli -a `"FlinkDotNet_Redis_CI_Password_2024`" get `"$RedisCounterKey`""
    }
    try {
        $counterValue = Invoke-Expression $redisCommand 2>$null
    } catch {
        Write-Host "⚠️ Redis not accessible: $($_.Exception.Message)"
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
            $dynamicTimeoutExtension += 5
            Write-Host "🔄 Progress detected! Timeout extended to $($MaxWaitSeconds + $dynamicTimeoutExtension)s"
        }

        if ($currentCount -ge $ExpectedMessages) {
            Write-Host "✅ FlinkJobSimulator completed successfully! Total=$($currentCount.ToString('N0'))"
            break
        }
    } else {
        Write-Host "⏳ Waiting for Redis counter '$RedisCounterKey' to initialize..."
    }

    if ($elapsed -gt $effectiveTimeout) {
        Write-Host "❌ Timeout exceeded after $([math]::Round($elapsed, 2))s! Incomplete processing."
        throw "TimeoutExceeded"
    }

    Start-Sleep -Seconds $CheckIntervalSeconds
}
