#!/usr/bin/env pwsh
<#
.SYNOPSIS
    Discovers the actual ports used by Aspire Docker containers for Redis and Kafka.

.DESCRIPTION
    This script inspects running Docker containers to find the host ports
    that are mapped to Redis (6379) and Kafka (9092) services.
    This is needed because Aspire uses dynamic port allocation.
#>

function Get-RedisConnectionInfo {
    try {
        # Get Redis port
        $redisContainer = docker ps --filter "ancestor=redis" --format "{{.Ports}}" | Select-Object -First 1
        $redisPort = $null
        if ($redisContainer -and $redisContainer -match "127\.0\.0\.1:(\d+)->6379/tcp") {
            $redisPort = [int]$Matches[1]
        } else {
            # Fallback: try by container name pattern
            $allContainers = docker ps --format "table {{.Names}}\t{{.Ports}}" | Where-Object { $_ -match "redis.*6379" }
            foreach ($container in $allContainers) {
                if ($container -match "127\.0\.0\.1:(\d+)->6379/tcp") {
                    $redisPort = [int]$Matches[1]
                    break
                }
            }
        }
        
        if (-not $redisPort) {
            return $null
        }
        
        # Get Redis password from container environment
        $containerId = docker ps --filter "ancestor=redis:7.4" --format "{{.ID}}" | Select-Object -First 1
        if ($containerId) {
            $envOutput = docker inspect $containerId | ConvertFrom-Json
            $redisPassword = $null
            foreach ($env in $envOutput[0].Config.Env) {
                if ($env -match "REDIS_PASSWORD=(.+)") {
                    $redisPassword = $Matches[1]
                    break
                }
            }
            
            if ($redisPassword) {
                return @{
                    Port = $redisPort
                    Password = $redisPassword
                    ConnectionString = "localhost:$redisPort,password=$redisPassword"
                }
            }
        }
        
        # Fallback without password
        return @{
            Port = $redisPort
            Password = $null
            ConnectionString = "localhost:$redisPort"
        }
    } catch {
        Write-Host "Error discovering Redis connection info: $_" -ForegroundColor Red
    }
    return $null
}

function Get-RedisPort {
    $info = Get-RedisConnectionInfo
    if ($info) {
        return $info.Port
    } else {
        return $null
    }
}

function Get-KafkaPort {
    try {
        $kafkaContainer = docker ps --filter "ancestor=confluentinc/confluent-local" --format "{{.Ports}}" | Select-Object -First 1
        if ($kafkaContainer -and $kafkaContainer -match "127\.0\.0\.1:(\d+)->9092/tcp") {
            return [int]$Matches[1]
        }
        
        # Fallback: try by container name pattern
        $allContainers = docker ps --format "table {{.Names}}\t{{.Ports}}" | Where-Object { $_ -match "kafka.*9092" }
        foreach ($container in $allContainers) {
            if ($container -match "127\.0\.0\.1:(\d+)->9092/tcp") {
                return [int]$Matches[1]
            }
        }
    } catch {
        Write-Host "Error discovering Kafka port: $_" -ForegroundColor Red
    }
    return $null
}

# Main execution
Write-Host "=== Discovering Aspire Container Ports ===" -ForegroundColor Cyan

# Show current Docker containers
Write-Host "Current Docker containers:" -ForegroundColor White
docker ps --format "table {{.Names}}\t{{.Image}}\t{{.Ports}}"

$redisInfo = Get-RedisConnectionInfo
$kafkaPort = Get-KafkaPort

Write-Host ""
Write-Host "=== Discovery Results ===" -ForegroundColor Cyan

if ($redisInfo) {
    Write-Host "✅ Redis discovered on port: $($redisInfo.Port)" -ForegroundColor Green
    if ($redisInfo.Password) {
        Write-Host "✅ Redis password discovered" -ForegroundColor Green
    }
    $env:DOTNET_REDIS_PORT = $redisInfo.Port.ToString()
    $env:DOTNET_REDIS_URL = $redisInfo.ConnectionString
} else {
    Write-Host "❌ Redis connection info not discovered" -ForegroundColor Red
    return 1
}

if ($kafkaPort) {
    Write-Host "✅ Kafka discovered on port: $kafkaPort" -ForegroundColor Green
    $env:DOTNET_KAFKA_PORT = $kafkaPort.ToString()
    $env:DOTNET_KAFKA_BOOTSTRAP_SERVERS = "localhost:$kafkaPort"
} else {
    Write-Host "❌ Kafka port not discovered" -ForegroundColor Red
    return 1
}

Write-Host ""
Write-Host "Environment variables set:" -ForegroundColor Cyan
Write-Host "  DOTNET_REDIS_URL: $env:DOTNET_REDIS_URL" -ForegroundColor Gray
Write-Host "  DOTNET_KAFKA_BOOTSTRAP_SERVERS: $env:DOTNET_KAFKA_BOOTSTRAP_SERVERS" -ForegroundColor Gray

# Export for GitHub Actions if in CI
if ($env:GITHUB_ENV) {
    "DOTNET_REDIS_PORT=$($redisInfo.Port)" | Out-File -FilePath $env:GITHUB_ENV -Append
    "DOTNET_KAFKA_PORT=$kafkaPort" | Out-File -FilePath $env:GITHUB_ENV -Append
    "DOTNET_REDIS_URL=$($redisInfo.ConnectionString)" | Out-File -FilePath $env:GITHUB_ENV -Append
    "DOTNET_KAFKA_BOOTSTRAP_SERVERS=localhost:$kafkaPort" | Out-File -FilePath $env:GITHUB_ENV -Append
}

Write-Host "Port discovery completed successfully!" -ForegroundColor Green
return 0