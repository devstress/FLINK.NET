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

    if ($ForceRebuild) {
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

    $ports = docker ps --filter "name=kafka" --format "{{.Ports}}"
    foreach ($line in $ports -split '\s+') {
        if ($line -match ":(\d+)->9092/tcp") {
            $port = $matches[1]
            Write-Host "✅ Kafka detected on 127.0.0.1:$port"
            return "127.0.0.1:$port"
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
$bootstrapServers = Get-KafkaBootstrapServers
$kafkaContainer = Get-KafkaContainerName

docker exec $kafkaContainer bash -c "shopt -s nullglob; for f in /var/lib/kafka/data/*/*; do cat \$f > /dev/null; done"

Build-Producer
Run-Producers $exePath $bootstrapServers $Topic $MessageCount $Partitions $ParallelProducers
