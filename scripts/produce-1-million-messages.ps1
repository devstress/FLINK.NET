#!/usr/bin/env pwsh

param(
    [long]$MessageCount = 1000000,
    [string]$Topic = "flinkdotnet.sample.topic",
    [int]$Partitions = 100,
    [int]$ParallelProducers = 64,
    [switch]$ForceRebuild
)
[Console]::OutputEncoding = [System.Text.Encoding]::UTF8
$ErrorActionPreference = 'Stop'
Write-Host "=== Flink.NET Kafka Producer RC7.2.2 ===" -ForegroundColor Cyan

$scriptDir = Split-Path -Parent $PSCommandPath
$outputDir = Join-Path $scriptDir "producer"
$sourceFile = Join-Path $outputDir "Producer.cs"
$projectFile = Join-Path $outputDir "Producer.csproj"
$platform = if ($IsWindows -or ($env:OS -eq "Windows_NT")) { @{ Rid = "win-x64"; Exe = "producer.exe" } } else { @{ Rid = "linux-x64"; Exe = "producer" } }
$exePath = Join-Path $outputDir $platform.Exe

function Build-Producer {
    if (!(Test-Path $sourceFile)) {
        Write-Error "❌ Missing source file: $sourceFile"
        exit 1
    }

    if (!$ForceRebuild -and (Test-Path $exePath)) {
        Write-Host "✅ Found cached executable: $exePath" -ForegroundColor Green
        return
    }

    Write-Host "🛠️ Building .NET Producer..." -ForegroundColor Yellow
    if (!(Test-Path $outputDir)) { New-Item -ItemType Directory -Path $outputDir | Out-Null }

    if ($ForceRebuild -or !(Test-Path $projectFile)) {
        @"
<Project Sdk="Microsoft.NET.Sdk">
  <PropertyGroup>
    <OutputType>Exe</OutputType>
    <TargetFramework>net8.0</TargetFramework>
    <ImplicitUsings>enable</ImplicitUsings>
    <Nullable>enable</Nullable>
  </PropertyGroup>
  <ItemGroup>
    <PackageReference Include="Confluent.Kafka" Version="2.10.1" />
  </ItemGroup>
</Project>
"@ | Set-Content -Path $projectFile -Encoding UTF8
    }

    # Run publish and capture result
    dotnet publish $projectFile `
        -c Release `
        -r $platform.Rid `
        --self-contained true `
        /p:PublishSingleFile=true `
        /p:EnableCompressionInSingleFile=true `
        -o $outputDir

    if ($LASTEXITCODE -ne 0) {
        Write-Error "❌ .NET publish failed with exit code $LASTEXITCODE. Aborting."
        exit $LASTEXITCODE
    }
}

function Get-KafkaBootstrapServers {
    Write-Host "🔍 Discovering Kafka bootstrap servers..."
    if ($env:DOTNET_KAFKA_BOOTSTRAP_SERVERS) {
        return $env:DOTNET_KAFKA_BOOTSTRAP_SERVERS -replace 'localhost|::1', '127.0.0.1'
    }

    # Use same discovery logic as discover-aspire-ports.ps1
    $kafkaContainers = @()
    
    # Try multiple Kafka image patterns that Aspire might use
    $imagePatterns = @(
        "confluentinc/confluent-local:7.4.0",
        "confluentinc/confluent-local",
        "confluentinc/cp-kafka:7.4.0",
        "confluentinc/cp-kafka"
    )
    
    foreach ($pattern in $imagePatterns) {
        $containers = docker ps --filter "ancestor=$pattern" --format "{{.ID}}" 2>/dev/null
        if ($containers) {
            $kafkaContainers += $containers
            Write-Host "Found Kafka containers with image $pattern" -ForegroundColor Green
        }
    }

    # If no exact matches, try pattern matching in names/images (excluding kafka-init containers)
    if (-not ($kafkaContainers | Where-Object { $_ -and $_.Trim() })) {
        Write-Host "No exact Kafka ancestor matches, checking all containers for Kafka..." -ForegroundColor Yellow
        $allContainers = docker ps --format "{{.ID}}`t{{.Image}}`t{{.Names}}" 2>/dev/null
        foreach ($line in $allContainers) {
            # Look for kafka but exclude init containers and prioritize confluent-local
            if ($line -match "kafka" -and $line -notmatch "kafka-init" -and $line -notmatch "-init") {
                $containerId = ($line -split "`t")[0]
                if ($containerId -and $containerId.Length -gt 5) {
                    $kafkaContainers += $containerId
                    Write-Host "Found Kafka container by pattern: $containerId" -ForegroundColor Green
                }
            }
        }
    }
    
    $kafkaContainers = $kafkaContainers | Where-Object { $_ -and $_.Trim() -and $_.Length -gt 5 } | Select-Object -Unique

    if (-not $kafkaContainers) {
        Write-Error "❌ No Kafka containers found"
        exit 1
    }

    # Use the first container
    $containerId = if ($kafkaContainers -is [array]) { $kafkaContainers[0] } else { $kafkaContainers }
    Write-Host "Using Kafka container: $containerId" -ForegroundColor Green

    # Get port mapping with multiple port checks
    $portMappings = @()
    $portMappings += docker port $containerId 9092 2>/dev/null
    $portMappings += docker port $containerId 2>/dev/null | Where-Object { $_ -match "9092" }
    
    $kafkaPort = $null
    foreach ($portInfo in $portMappings) {
        if ($portInfo -and $portInfo -match "(?:127\.0\.0\.1|0\.0\.0\.0|\[::\]):(\d+)") {
            $kafkaPort = [int]$Matches[1]
            Write-Host "✅ Kafka detected on 127.0.0.1:$kafkaPort" -ForegroundColor Green
            return "127.0.0.1:$kafkaPort"
        }
    }

    Write-Error "❌ Failed to discover Kafka port"
    exit 1
}

function Get-KafkaContainerName {
    Write-Host "🔍 Discovering Kafka container name..."
    $name = docker ps --filter "name=kafka" --format "{{.Names}}" | Select-Object -First 1
    if (-not $name) {
        Write-Error "❌ Kafka container not found."
        exit 1
    }
    Write-Host "✅ Kafka container: $name"
    return $name
}

function Run-Producers {
    param($ExecutablePath, $BootstrapServers, $Topic, $MessageCount, $Partitions, $ParallelProducers)
    Write-Host "🚀 Starting Producer..."
    & "$ExecutablePath" "$BootstrapServers" "$Topic" "$MessageCount" "$ParallelProducers" "$Partitions"
}

# === MAIN EXECUTION ===

# Build producer first before discovery
Build-Producer

$bootstrapServers = Get-KafkaBootstrapServers
$kafkaContainer = Get-KafkaContainerName

docker exec $kafkaContainer bash -c "shopt -s nullglob; for f in /var/lib/kafka/data/*/*; do cat \$f > /dev/null; done"

Run-Producers $exePath $bootstrapServers $Topic $MessageCount $Partitions $ParallelProducers
