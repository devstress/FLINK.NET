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
        # Get Redis containers with different image patterns
        $redisContainers = @()
        $redisContainers += docker ps --filter "ancestor=redis" --format "{{.ID}}" 2>/dev/null
        $redisContainers += docker ps --filter "ancestor=redis:7.4" --format "{{.ID}}" 2>/dev/null
        $redisContainers += docker ps --filter "ancestor=redis:latest" --format "{{.ID}}" 2>/dev/null
        
        # If no exact matches, try pattern matching in names/images
        if (-not ($redisContainers | Where-Object { $_ -and $_.Trim() })) {
            Write-Host "No exact Redis ancestor matches, checking all containers for Redis..." -ForegroundColor Yellow
            $allContainers = docker ps --format "{{.ID}}\t{{.Image}}" 2>/dev/null
            foreach ($line in $allContainers) {
                if ($line -match "redis") {
                    $containerId = ($line -split '\t')[0]
                    if ($containerId -and $containerId.Length -gt 5) { # Valid container ID should be longer
                        $redisContainers += $containerId
                        Write-Host "Found Redis container by image pattern: $containerId" -ForegroundColor Green
                    }
                }
            }
        }
        
        $redisContainers = $redisContainers | Where-Object { $_ -and $_.Trim() -and $_.Length -gt 5 } | Select-Object -Unique

        if (-not $redisContainers) {
            Write-Host "No Redis containers found" -ForegroundColor Yellow
            return $null
        }

        # Ensure we get the first container as a string, not a character
        if ($redisContainers -is [array]) {
            $containerId = $redisContainers[0]
        } else {
            $containerId = $redisContainers
        }
        Write-Host "Using Redis container: $containerId" -ForegroundColor Green

        # Get port mapping using docker port command
        $portInfo = docker port $containerId 6379 2>/dev/null
        $redisPort = $null
        if ($portInfo -and $portInfo -match "127\.0\.0\.1:(\d+)") {
            $redisPort = [int]$Matches[1]
            Write-Host "Redis mapped to host port: $redisPort" -ForegroundColor Green
        } else {
            Write-Host "Could not determine Redis host port mapping from: '$portInfo'" -ForegroundColor Red
            return $null
        }

        # Try to get Redis password from container environment
        $envOutput = docker inspect $containerId 2>/dev/null | ConvertFrom-Json
        $redisPassword = $null
        if ($envOutput -and $envOutput[0].Config.Env) {
            foreach ($env in $envOutput[0].Config.Env) {
                if ($env -match "REDIS_PASSWORD=(.+)") {
                    $redisPassword = $Matches[1]
                    Write-Host "Redis password found in container environment" -ForegroundColor Green
                    break
                }
            }
        }

        # Build connection string based on Aspire/StackExchange.Redis format
        if ($redisPassword) {
            $connectionString = "localhost:$redisPort,password=$redisPassword"
        } else {
            $connectionString = "localhost:$redisPort"
        }

        return @{
            Port = $redisPort
            Password = $redisPassword
            ConnectionString = $connectionString
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
        # Get Kafka containers with different image patterns
        $kafkaContainers = @()
        $kafkaContainers += docker ps --filter "ancestor=confluentinc/confluent-local" --format "{{.ID}}" 2>/dev/null
        $kafkaContainers += docker ps --filter "ancestor=confluentinc/confluent-local:7.9.0" --format "{{.ID}}" 2>/dev/null
        $kafkaContainers += docker ps --filter "ancestor=apache/kafka" --format "{{.ID}}" 2>/dev/null
        $kafkaContainers += docker ps --filter "ancestor=confluentinc/cp-kafka" --format "{{.ID}}" 2>/dev/null

        # If no exact matches, try pattern matching in names/images
        if (-not ($kafkaContainers | Where-Object { $_ -and $_.Trim() })) {
            Write-Host "No exact Kafka ancestor matches, checking all containers for Kafka..." -ForegroundColor Yellow
            $allContainers = docker ps --format "{{.ID}}\t{{.Image}}" 2>/dev/null
            foreach ($line in $allContainers) {
                if ($line -match "kafka" -or $line -match "confluent") {
                    $containerId = ($line -split '\t')[0]
                    if ($containerId -and $containerId.Length -gt 5) { # Valid container ID should be longer
                        $kafkaContainers += $containerId
                        Write-Host "Found Kafka container by image pattern: $containerId" -ForegroundColor Green
                    }
                }
            }
        }
        
        $kafkaContainers = $kafkaContainers | Where-Object { $_ -and $_.Trim() -and $_.Length -gt 5 } | Select-Object -Unique

        if (-not $kafkaContainers) {
            Write-Host "No Kafka containers found" -ForegroundColor Yellow
            return $null
        }

        # Ensure we get the first container as a string, not a character
        if ($kafkaContainers -is [array]) {
            $containerId = $kafkaContainers[0]
        } else {
            $containerId = $kafkaContainers
        }
        Write-Host "Using Kafka container: $containerId" -ForegroundColor Green

        # Get port mapping
        $portInfo = docker port $containerId 9092 2>/dev/null
        if ($portInfo -and $portInfo -match "127\.0\.0\.1:(\d+)") {
            $kafkaPort = [int]$Matches[1]
            Write-Host "Kafka mapped to host port: $kafkaPort" -ForegroundColor Green
            return $kafkaPort
        } else {
            Write-Host "Could not determine Kafka host port mapping from: '$portInfo'" -ForegroundColor Red
            return $null
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