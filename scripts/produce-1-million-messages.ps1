#!/usr/bin/env pwsh

param(
    [long]$MessageCount = 1000000,
    [string]$Topic = "flinkdotnet.sample.topic",
    [int]$Partitions = 100,
    [int]$ParallelProducers = 256,
    [switch]$ForceRebuild
)
[Console]::OutputEncoding = [System.Text.Encoding]::UTF8
$ErrorActionPreference = 'Stop'
Write-Host "=== Flink.NET Kafka Producer RC7.2.2 ===" -ForegroundColor Cyan

$scriptDir = Split-Path -Parent $PSCommandPath
$outputDir = Join-Path $scriptDir "producer"
$sourceFile = Join-Path $outputDir "Producer.cs"
$projectFile = Join-Path $outputDir "Producer.csproj"
$platform = if ($IsWindows -or ($env:OS -eq "Windows_NT")) { @{ Rid = "win-x64"; Exe = "Producer.exe" } } else { @{ Rid = "linux-x64"; Exe = "Producer" } }
$exePath = Join-Path $outputDir $platform.Exe

function Test-TcpPort {
    param([string]$TestHost, [int]$TestPort)
    try {
        $client = [System.Net.Sockets.TcpClient]::new()
        $task = $client.ConnectAsync($TestHost, $TestPort)
        if (-not $task.Wait(1000) -or $task.IsFaulted) { return $false }
        $client.Close()
        return $true
    } catch {
        return $false
    }
}

function Build-Producer {
    if (!(Test-Path $sourceFile)) {
        Write-Error "❌ Missing source file: $sourceFile"
        exit 1
    }

    if (!$ForceRebuild -and (Test-Path $exePath)) {
        Write-Host "✅ Found cached executable: $exePath" -ForegroundColor Green
        return
    }

    $nativeDir = Join-Path $scriptDir "..\NativeKafkaBridge"
    if (Test-Path (Join-Path $nativeDir "Makefile")) {
        Write-Host "🛠️ Building native bridge..." -ForegroundColor Yellow
        Push-Location $nativeDir
        make
        Pop-Location
        Copy-Item (Join-Path $nativeDir "libnativekafkabridge.so") $outputDir -Force
    }

    Write-Host "🛠️ Building .NET Producer..." -ForegroundColor Yellow
    if (!(Test-Path $outputDir)) { New-Item -ItemType Directory -Path $outputDir | Out-Null }

    # Create project file if missing or if ForceRebuild is requested
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
    <ProjectReference Include="..\..\NativeKafkaBridge\NativeKafkaBridge.csproj" />
  </ItemGroup>
</Project>
"@ | Set-Content -Path $projectFile -Encoding UTF8
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
    if (Get-Command docker -ErrorAction SilentlyContinue) {
        $ports = docker ps --filter "name=kafka" --format "{{.Ports}}"
        foreach ($line in $ports -split '\s+') {
            if ($line -match ":(\d+)->9092/tcp") {
                $port = $matches[1]
                Write-Host "✅ Kafka detected on 127.0.0.1:$port"
                return "127.0.0.1:$port"
            }
        }
    }

    Write-Host "⚠️ Docker not available. Using default 127.0.0.1:9092" -ForegroundColor Yellow
    return "127.0.0.1:9092"
}

function Get-KafkaContainerName {
    Write-Host "🔍 Discovering Kafka container name..."
    if (Get-Command docker -ErrorAction SilentlyContinue) {
        $name = docker ps --filter "name=kafka" --format "{{.Names}}" | Select-Object -First 1
        if ($name) {
            Write-Host "✅ Kafka container: $name"
            return $name
        }
        Write-Error "❌ Kafka container not found."
        exit 1
    }
    Write-Host "⚠️ Docker not available. Skipping container discovery" -ForegroundColor Yellow
    return $null
}

function Run-Producers {
    param($ExecutablePath, $BootstrapServers, $Topic, $MessageCount, $Partitions, $ParallelProducers)
    Write-Host "🚀 Starting Producer..."
    & "$ExecutablePath" "$BootstrapServers" "$Topic" "$MessageCount" "$ParallelProducers" "$Partitions"
}

# === MAIN EXECUTION ===

$bootstrapServers = Get-KafkaBootstrapServers
$kafkaContainer = Get-KafkaContainerName

$serverParts = $bootstrapServers.Split(":")
$kHost = $serverParts[0]
$kPort = [int]$serverParts[1]
if (-not (Test-TcpPort $kHost $kPort)) {
    Write-Error "❌ Kafka not reachable at $bootstrapServers. Please start Kafka."
    exit 1
}

$redisPort = $env:DOTNET_REDIS_PORT
if (-not $redisPort) { $redisPort = 6379 }
if (-not (Test-TcpPort "127.0.0.1" ([int]$redisPort))) {
    Write-Error "❌ Redis not reachable on 127.0.0.1:$redisPort. Please start Redis."
    exit 1
}

if ($kafkaContainer) {
    docker exec $kafkaContainer bash -c "shopt -s nullglob; for f in /var/lib/kafka/data/*/*; do cat \$f > /dev/null; done"
}

Build-Producer
Run-Producers $exePath $bootstrapServers $Topic $MessageCount $Partitions $ParallelProducers
