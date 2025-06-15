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
    param(
        [int]$MaxRetries = 3,
        [int]$DelaySeconds = 5
    )
    
    for ($retry = 1; $retry -le $MaxRetries; $retry++) {
        try {
            Write-Host "Redis discovery attempt $retry/$MaxRetries..." -ForegroundColor Yellow
            
            # Get Redis containers with different image patterns
            $redisContainers = @()
            
            # Try multiple Redis image patterns that Aspire might use
            $imagePatterns = @(
                "redis",
                "redis:7.4",
                "redis:latest", 
                "redis:7.2",
                "redis:alpine"
            )
            
            foreach ($pattern in $imagePatterns) {
                $containers = docker ps --filter "ancestor=$pattern" --format "{{.ID}}" 2>/dev/null
                if ($containers) {
                    $redisContainers += $containers
                    Write-Host "Found Redis containers with image $pattern" -ForegroundColor Green
                }
            }
            
            # If no exact matches, try pattern matching in names/images
            if (-not ($redisContainers | Where-Object { $_ -and $_.Trim() })) {
                Write-Host "No exact Redis ancestor matches, checking all containers for Redis..." -ForegroundColor Yellow
                $allContainers = docker ps --format "{{.ID}}\t{{.Image}}\t{{.Names}}" 2>/dev/null
                foreach ($line in $allContainers) {
                    if ($line -match "redis" -or $line -match "Redis") {
                        $containerId = ($line -split '\t')[0]
                        if ($containerId -and $containerId.Length -gt 5) { # Valid container ID should be longer
                            $redisContainers += $containerId
                            Write-Host "Found Redis container by pattern: $containerId" -ForegroundColor Green
                        }
                    }
                }
            }
            
            $redisContainers = $redisContainers | Where-Object { $_ -and $_.Trim() -and $_.Length -gt 5 } | Select-Object -Unique

            if (-not $redisContainers) {
                Write-Host "No Redis containers found in attempt $retry" -ForegroundColor Yellow
                if ($retry -lt $MaxRetries) {
                    Write-Host "Waiting $DelaySeconds seconds before retry..." -ForegroundColor Yellow
                    Start-Sleep -Seconds $DelaySeconds
                    continue
                }
                return $null
            }

            # Ensure we get the first container as a string, not a character
            if ($redisContainers -is [array]) {
                $containerId = $redisContainers[0]
            } else {
                $containerId = $redisContainers
            }
            Write-Host "Using Redis container: $containerId" -ForegroundColor Green

            # Get port mapping using docker port command with multiple port checks
            $portMappings = @()
            $portMappings += docker port $containerId 6379 2>/dev/null
            $portMappings += docker port $containerId 2>/dev/null | Where-Object { $_ -match "6379" }
            
            $redisPort = $null
            foreach ($portInfo in $portMappings) {
                if ($portInfo -and $portInfo -match "(?:127\.0\.0\.1|0\.0\.0\.0|\[::\]):(\d+)") {
                    $redisPort = [int]$Matches[1]
                    Write-Host "Redis mapped to host port: $redisPort" -ForegroundColor Green
                    break
                }
            }
            
            if (-not $redisPort) {
                Write-Host "Could not determine Redis host port mapping from: '$($portMappings -join ', ')'" -ForegroundColor Red
                if ($retry -lt $MaxRetries) {
                    Write-Host "Waiting $DelaySeconds seconds before retry..." -ForegroundColor Yellow
                    Start-Sleep -Seconds $DelaySeconds
                    continue
                }
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

            # Build connection string based on Redis URI format for better compatibility
            if ($redisPassword) {
                $connectionString = "redis://:$redisPassword@localhost:$redisPort"
            } else {
                # Use Redis URI format with empty credentials for CI compatibility
                $connectionString = "redis://:@localhost:$redisPort"
            }

            # Test the connection before returning
            Write-Host "Testing Redis connection at localhost:$redisPort..." -ForegroundColor Yellow
            try {
                # Test Redis connection with redis-cli ping
                $testResult = docker exec $containerId redis-cli -p 6379 ping 2>/dev/null
                if ($testResult -eq "PONG") {
                    Write-Host "Redis connection test successful (no auth required)" -ForegroundColor Green
                    $connectionString = "redis://:@localhost:$redisPort"  # Use Redis URI format consistently
                } elseif ($testResult -match "NOAUTH") {
                    Write-Host "Redis requires authentication - attempting to discover password or disable auth" -ForegroundColor Yellow
                    # Try different approaches for CI compatibility
                    $authHandled = $false
                    
                    # Approach 1: Try with empty password (some Redis configurations)
                    try {
                        $testResult2 = docker exec $containerId redis-cli -p 6379 -a "" ping 2>/dev/null
                        if ($testResult2 -eq "PONG") {
                            Write-Host "Redis accepts empty password" -ForegroundColor Green
                            $connectionString = "redis://:@localhost:$redisPort"
                            $authHandled = $true
                        }
                    } catch {
                        # Continue to next approach
                    }
                    
                    # Approach 2: Try to disable authentication (for CI environments only)
                    if (-not $authHandled -and ($env:CI -eq "true" -or $env:GITHUB_ACTIONS -eq "true")) {
                        try {
                            # Try to get current password and disable auth
                            $currentAuth = docker exec $containerId redis-cli -p 6379 CONFIG GET requirepass 2>/dev/null
                            if ($currentAuth) {
                                docker exec $containerId redis-cli -p 6379 CONFIG SET requirepass "" 2>/dev/null
                                $testResult3 = docker exec $containerId redis-cli -p 6379 ping 2>/dev/null
                                if ($testResult3 -eq "PONG") {
                                    Write-Host "Redis authentication disabled successfully for CI testing" -ForegroundColor Green
                                    $connectionString = "redis://:@localhost:$redisPort"
                                    $authHandled = $true
                                }
                            }
                        } catch {
                            # Continue to next approach
                        }
                    }
                    
                    # Approach 3: Use Redis URI format with empty credentials and let the application handle auth
                    if (-not $authHandled) {
                        Write-Host "Could not resolve Redis authentication automatically - using Redis URI format with empty credentials" -ForegroundColor Yellow
                        $connectionString = "redis://:@localhost:$redisPort"
                    }
                } else {
                    Write-Host "Redis connection test inconclusive: '$testResult' - proceeding with discovered connection" -ForegroundColor Yellow
                }
            } catch {
                Write-Host "Redis connection test failed with error: $_ - proceeding with discovered port" -ForegroundColor Yellow
            }

            return @{
                Port = $redisPort
                Password = $redisPassword
                ConnectionString = $connectionString
            }
        } catch {
            Write-Host "Error in Redis discovery attempt $retry : $_" -ForegroundColor Red
            if ($retry -lt $MaxRetries) {
                Write-Host "Waiting $DelaySeconds seconds before retry..." -ForegroundColor Yellow
                Start-Sleep -Seconds $DelaySeconds
            }
        }
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
    param(
        [int]$MaxRetries = 3,
        [int]$DelaySeconds = 5
    )
    
    for ($retry = 1; $retry -le $MaxRetries; $retry++) {
        try {
            Write-Host "Kafka discovery attempt $retry/$MaxRetries..." -ForegroundColor Yellow
            
            # Get Kafka containers with different image patterns
            $kafkaContainers = @()
            
            # Try multiple Kafka image patterns that Aspire might use  
            $imagePatterns = @(
                "confluentinc/confluent-local",
                "confluentinc/confluent-local:7.9.0",
                "confluentinc/confluent-local:latest", 
                "apache/kafka",
                "apache/kafka:latest",
                "confluentinc/cp-kafka",
                "confluentinc/cp-kafka:7.4.0",
                "confluentinc/cp-kafka:latest",
                "bitnami/kafka",
                "bitnami/kafka:latest"
            )
            
            foreach ($pattern in $imagePatterns) {
                $containers = docker ps --filter "ancestor=$pattern" --format "{{.ID}}" 2>/dev/null
                if ($containers) {
                    $kafkaContainers += $containers
                    Write-Host "Found Kafka containers with image $pattern" -ForegroundColor Green
                }
            }

            # If no exact matches, try pattern matching in names/images
            if (-not ($kafkaContainers | Where-Object { $_ -and $_.Trim() })) {
                Write-Host "No exact Kafka ancestor matches, checking all containers for Kafka..." -ForegroundColor Yellow
                $allContainers = docker ps --format "{{.ID}}\t{{.Image}}\t{{.Names}}" 2>/dev/null
                foreach ($line in $allContainers) {
                    if ($line -match "kafka" -or $line -match "confluent" -or $line -match "Kafka") {
                        $containerId = ($line -split '\t')[0]
                        if ($containerId -and $containerId.Length -gt 5) { # Valid container ID should be longer
                            $kafkaContainers += $containerId
                            Write-Host "Found Kafka container by pattern: $containerId" -ForegroundColor Green
                        }
                    }
                }
            }
            
            $kafkaContainers = $kafkaContainers | Where-Object { $_ -and $_.Trim() -and $_.Length -gt 5 } | Select-Object -Unique

            if (-not $kafkaContainers) {
                Write-Host "No Kafka containers found in attempt $retry" -ForegroundColor Yellow
                if ($retry -lt $MaxRetries) {
                    Write-Host "Waiting $DelaySeconds seconds before retry..." -ForegroundColor Yellow
                    Start-Sleep -Seconds $DelaySeconds
                    continue
                }
                return $null
            }

            # Ensure we get the first container as a string, not a character
            if ($kafkaContainers -is [array]) {
                $containerId = $kafkaContainers[0]
            } else {
                $containerId = $kafkaContainers
            }
            Write-Host "Using Kafka container: $containerId" -ForegroundColor Green

            # Get port mapping with multiple port checks
            $portMappings = @()
            $portMappings += docker port $containerId 9092 2>/dev/null
            $portMappings += docker port $containerId 2>/dev/null | Where-Object { $_ -match "9092" }
            
            $kafkaPort = $null
            foreach ($portInfo in $portMappings) {
                if ($portInfo -and $portInfo -match "(?:127\.0\.0\.1|0\.0\.0\.0|\[::\]):(\d+)") {
                    $kafkaPort = [int]$Matches[1]
                    Write-Host "Kafka mapped to host port: $kafkaPort" -ForegroundColor Green
                    break
                }
            }
            
            if (-not $kafkaPort) {
                Write-Host "Could not determine Kafka host port mapping from: '$($portMappings -join ', ')'" -ForegroundColor Red
                if ($retry -lt $MaxRetries) {
                    Write-Host "Waiting $DelaySeconds seconds before retry..." -ForegroundColor Yellow
                    Start-Sleep -Seconds $DelaySeconds
                    continue
                }
                return $null
            }
            
            # Test Kafka connectivity if possible
            Write-Host "Testing Kafka connectivity at localhost:$kafkaPort..." -ForegroundColor Yellow
            try {
                # Try to check if the port is responding
                $tcpClient = New-Object System.Net.Sockets.TcpClient
                $connectTask = $tcpClient.ConnectAsync("127.0.0.1", $kafkaPort)
                $taskCompleted = $connectTask.Wait(5000)
                if ($taskCompleted -and $tcpClient.Connected) {
                    Write-Host "Kafka port $kafkaPort is accessible" -ForegroundColor Green
                    $tcpClient.Close()
                } else {
                    Write-Host "Kafka port $kafkaPort connection test inconclusive but proceeding" -ForegroundColor Yellow
                }
                $tcpClient.Dispose()
            } catch {
                Write-Host "Kafka connection test failed, but proceeding with discovered port" -ForegroundColor Yellow
            }
            
            # Ensure we return a clean integer port value to avoid tuple issues
            return [int]$kafkaPort
        } catch {
            Write-Host "Error in Kafka discovery attempt $retry : $_" -ForegroundColor Red
            if ($retry -lt $MaxRetries) {
                Write-Host "Waiting $DelaySeconds seconds before retry..." -ForegroundColor Yellow
                Start-Sleep -Seconds $DelaySeconds
            }
        }
    }
    return $null
}

# Main execution
Write-Host "=== Discovering Aspire Container Ports ===" -ForegroundColor Cyan
Write-Host "CI Environment: $($env:CI -eq 'true' -or $env:GITHUB_ACTIONS -eq 'true')" -ForegroundColor Gray

# Show current Docker containers
Write-Host "Current Docker containers:" -ForegroundColor White
docker ps --format "table {{.Names}}\t{{.Image}}\t{{.Ports}}\t{{.Status}}"

# Wait a bit for containers to stabilize if just started
if ($env:CI -eq "true" -or $env:GITHUB_ACTIONS -eq "true") {
    Write-Host "CI environment detected, waiting additional 10 seconds for container stabilization..." -ForegroundColor Yellow
    Start-Sleep -Seconds 10
}

$redisInfo = Get-RedisConnectionInfo -MaxRetries 5 -DelaySeconds 3
$kafkaPort = Get-KafkaPort -MaxRetries 5 -DelaySeconds 3

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
    # Ensure kafkaPort is treated as an integer to avoid tuple issues
    $kafkaPortInt = [int]$kafkaPort
    $env:DOTNET_KAFKA_PORT = $kafkaPortInt.ToString()
    $env:DOTNET_KAFKA_BOOTSTRAP_SERVERS = "localhost:$kafkaPortInt"
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
    "DOTNET_KAFKA_PORT=$([int]$kafkaPort)" | Out-File -FilePath $env:GITHUB_ENV -Append
    "DOTNET_REDIS_URL=$($redisInfo.ConnectionString)" | Out-File -FilePath $env:GITHUB_ENV -Append
    "DOTNET_KAFKA_BOOTSTRAP_SERVERS=localhost:$([int]$kafkaPort)" | Out-File -FilePath $env:GITHUB_ENV -Append
}

Write-Host "Port discovery completed successfully!" -ForegroundColor Green
return 0