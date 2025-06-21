#!/usr/bin/env pwsh

param(
    [long]$MessageCount = 1000000,
    [string]$Topic = "flinkdotnet.sample.topic",
    [int]$Partitions = 100,
    [int]$ParallelProducers = 128,  # Increased from 64 for maximum parallelism
    [switch]$ForceRebuild
)
[Console]::OutputEncoding = [System.Text.Encoding]::UTF8
$ErrorActionPreference = 'Stop'
Write-Host "=== Flink.NET NATIVE Kafka Producer v8.0 ===" -ForegroundColor Cyan
Write-Host "🚀 Using Native librdkafka for 1M+ msg/sec Performance" -ForegroundColor Green

$scriptDir = Split-Path -Parent $PSCommandPath
$outputDir = Join-Path $scriptDir "producer-native"
$sourceFile = Join-Path $outputDir "ProducerNative.cs"
$projectFile = Join-Path $outputDir "ProducerNative.csproj"
$platform = if ($IsWindows -or ($env:OS -eq "Windows_NT")) { @{ Rid = "win-x64"; Exe = "ProducerNative.exe" } } else { @{ Rid = "linux-x64"; Exe = "ProducerNative" } }
$exePath = Join-Path $outputDir $platform.Exe

function Build-NativeProducer {
    if (!(Test-Path $sourceFile)) {
        Write-Error "❌ Missing native producer source file: $sourceFile"
        exit 1
    }

    if (!$ForceRebuild -and (Test-Path $exePath)) {
        Write-Host "✅ Found cached native producer executable: $exePath" -ForegroundColor Green
        return
    }

    Write-Host "🛠️ Building Native .NET Producer with librdkafka..." -ForegroundColor Yellow
    if (!(Test-Path $outputDir)) { New-Item -ItemType Directory -Path $outputDir | Out-Null }

    # Build the native producer
    try {
        $buildOutput = dotnet publish $projectFile -c Release -r $platform.Rid --self-contained true --output $outputDir 2>&1
        if ($LASTEXITCODE -ne 0) {
            Write-Error "❌ Build failed: $buildOutput"
            exit 1
        }
        Write-Host "✅ Native producer built successfully" -ForegroundColor Green
    }
    catch {
        Write-Error "❌ Build error: $_"
        exit 1
    }
}

    # Restore packages if needed (faster than always restoring)
    if ($ForceRebuild -or !(Test-Path "$outputDir/obj")) {
        Write-Host "📦 Restoring packages..." -ForegroundColor Yellow
        dotnet restore $projectFile -r $platform.Rid --verbosity quiet
        if ($LASTEXITCODE -ne 0) {
            Write-Error "❌ Package restore failed with exit code $LASTEXITCODE. Aborting."
            exit $LASTEXITCODE
        }
    }

    # Run publish and capture result with performance optimizations
    dotnet publish $projectFile `
        -c Release `
        -r $platform.Rid `
        --self-contained true `
        /p:PublishSingleFile=true `
        /p:EnableCompressionInSingleFile=false `
        /p:PublishTrimmed=false `
        /p:PublishReadyToRun=false `
        --no-restore `
        --verbosity quiet `
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
    Write-Host "🚀 Starting NATIVE High-Performance Producer..." -ForegroundColor Green
    Write-Host "📊 Configuration: $MessageCount messages, $ParallelProducers producers" -ForegroundColor Cyan
    Write-Host "🎯 Target: 1M+ msg/sec with native librdkafka" -ForegroundColor Yellow
    
    $timer = [System.Diagnostics.Stopwatch]::StartNew()
    & "$ExecutablePath" "$BootstrapServers" "$Topic" "$MessageCount" "$ParallelProducers" "$Partitions"
    $timer.Stop()
    
    $actualRate = $MessageCount / $timer.Elapsed.TotalSeconds
    Write-Host ""
    Write-Host "🏁 NATIVE PRODUCER PERFORMANCE ANALYSIS:" -ForegroundColor Green
    Write-Host "   💫 Total Messages: $($MessageCount.ToString('N0'))" -ForegroundColor White
    Write-Host "   ⏱️  Elapsed Time: $($timer.Elapsed.TotalSeconds.ToString('F3'))s" -ForegroundColor White
    Write-Host "   🚀 Final Rate: $($actualRate.ToString('N0')) msg/sec" -ForegroundColor White
    
    if ($actualRate -gt 1000000) {
        Write-Host "   🏆 EXCELLENT: >1M msg/s achieved! Native librdkafka optimization successful!" -ForegroundColor Green
    } elseif ($actualRate -gt 500000) {
        Write-Host "   ✅ GOOD: High throughput with native implementation" -ForegroundColor Green
    } else {
        Write-Host "   ⚠️ OPTIMIZATION NEEDED: Continue tuning native bridge for 1M+ target" -ForegroundColor Yellow
    }
}

# === MAIN EXECUTION ===
$bootstrapServers = Get-KafkaBootstrapServers
$kafkaContainer = Get-KafkaContainerName

Write-Host "🔧 Warming up Native Kafka Producer & librdkafka bridge..." -ForegroundColor Magenta
docker exec $kafkaContainer bash -c "shopt -s nullglob; for f in /var/lib/kafka/data/*/*; do cat \$f > /dev/null; done"

Build-NativeProducer
Run-Producers $exePath $bootstrapServers $Topic $MessageCount $Partitions $ParallelProducers
