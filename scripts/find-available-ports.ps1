#!/usr/bin/env pwsh
<#
.SYNOPSIS
    Finds available ports for Redis and Kafka services for stress testing.

.DESCRIPTION
    This script attempts to find available ports by checking if the default ports
    are free, and if not, finds alternative available ports. It outputs environment
    variables that can be used by the stress test workflow.

.PARAMETER PreferredRedisPort
    Preferred port for Redis service (default: 6379)

.PARAMETER PreferredKafkaPort
    Preferred port for Kafka service (default: 9092)

.PARAMETER OutputFormat
    Output format: 'env' for environment variables, 'json' for JSON (default: env)
#>

param(
    [int]$PreferredRedisPort = 6379,
    [int]$PreferredKafkaPort = 9092,
    [string]$OutputFormat = "env"
)

function Test-PortAvailable {
    param([int]$Port)
    
    try {
        $tcpConnection = New-Object System.Net.Sockets.TcpClient
        $tcpConnection.ConnectAsync("localhost", $Port).Wait(1000)
        $tcpConnection.Close()
        return $false  # Port is in use
    } catch {
        return $true   # Port is available
    }
}

function Find-AvailablePort {
    param(
        [int]$PreferredPort,
        [int]$StartRange = 10000,
        [int]$EndRange = 20000
    )
    
    if (Test-PortAvailable -Port $PreferredPort) {
        return $PreferredPort
    }
    
    Write-Host "Port $PreferredPort is not available, searching for alternative..." -ForegroundColor Yellow
    
    for ($port = $StartRange; $port -le $EndRange; $port++) {
        if (Test-PortAvailable -Port $port) {
            Write-Host "Found available port: $port" -ForegroundColor Green
            return $port
        }
    }
    
    throw "No available ports found in range $StartRange-$EndRange"
}

function Get-ProcessUsingPort {
    param([int]$Port)
    
    try {
        if ($IsWindows -or $env:OS -eq "Windows_NT") {
            $process = netstat -ano | Select-String ":$Port " | ForEach-Object {
                $parts = $_.ToString() -split '\s+'
                if ($parts.Length -ge 5) {
                    $pid = $parts[4]
                    Get-Process -Id $pid -ErrorAction SilentlyContinue
                }
            } | Select-Object -First 1
        } else {
            $process = lsof -ti:$Port | ForEach-Object {
                Get-Process -Id $_ -ErrorAction SilentlyContinue
            } | Select-Object -First 1
        }
        
        if ($process) {
            return "$($process.ProcessName) (PID: $($process.Id))"
        }
    } catch {
        # Ignore errors
    }
    
    return "Unknown process"
}

# Main execution
Write-Host "=== Port Discovery for FlinkDotNet Stress Tests ===" -ForegroundColor Cyan

# Check Redis port
Write-Host "Checking Redis port $PreferredRedisPort..." -ForegroundColor White
$redisPort = $PreferredRedisPort
if (-not (Test-PortAvailable -Port $PreferredRedisPort)) {
    $process = Get-ProcessUsingPort -Port $PreferredRedisPort
    Write-Host "  Port $PreferredRedisPort is in use by: $process" -ForegroundColor Red
    $redisPort = Find-AvailablePort -PreferredPort $PreferredRedisPort
    Write-Host "  Using alternative Redis port: $redisPort" -ForegroundColor Green
} else {
    Write-Host "  Port $PreferredRedisPort is available for Redis" -ForegroundColor Green
}

# Check Kafka port
Write-Host "Checking Kafka port $PreferredKafkaPort..." -ForegroundColor White
$kafkaPort = $PreferredKafkaPort
if (-not (Test-PortAvailable -Port $PreferredKafkaPort)) {
    $process = Get-ProcessUsingPort -Port $PreferredKafkaPort
    Write-Host "  Port $PreferredKafkaPort is in use by: $process" -ForegroundColor Red
    $kafkaPort = Find-AvailablePort -PreferredPort $PreferredKafkaPort
    Write-Host "  Using alternative Kafka port: $kafkaPort" -ForegroundColor Green
} else {
    Write-Host "  Port $PreferredKafkaPort is available for Kafka" -ForegroundColor Green
}

# Output results
Write-Host ""
Write-Host "=== Results ===" -ForegroundColor Cyan

if ($OutputFormat -eq "json") {
    $result = @{
        redis = @{
            port = $redisPort
            url = "localhost:$redisPort"
        }
        kafka = @{
            port = $kafkaPort
            bootstrapServers = "localhost:$kafkaPort"
        }
    }
    
    $result | ConvertTo-Json -Depth 3
} else {
    # Environment variable format
    Write-Host "DOTNET_REDIS_PORT=$redisPort"
    Write-Host "DOTNET_KAFKA_PORT=$kafkaPort"
    Write-Host "DOTNET_REDIS_URL=localhost:$redisPort"
    Write-Host "DOTNET_KAFKA_BOOTSTRAP_SERVERS=localhost:$kafkaPort"
    
    # Also output to GitHub Actions if running in CI
    if ($env:GITHUB_ENV) {
        Write-Host ""
        Write-Host "Writing to GitHub Actions environment..." -ForegroundColor Cyan
        "DOTNET_REDIS_PORT=$redisPort" | Out-File -FilePath $env:GITHUB_ENV -Append
        "DOTNET_KAFKA_PORT=$kafkaPort" | Out-File -FilePath $env:GITHUB_ENV -Append
        "DOTNET_REDIS_URL=localhost:$redisPort" | Out-File -FilePath $env:GITHUB_ENV -Append
        "DOTNET_KAFKA_BOOTSTRAP_SERVERS=localhost:$kafkaPort" | Out-File -FilePath $env:GITHUB_ENV -Append
    }
}

Write-Host ""
Write-Host "Port discovery completed successfully!" -ForegroundColor Green