#!/usr/bin/env pwsh

#
# IMPORTANT: DELETE THE BUILT EXECUTABLE IF YOU PLAN TO UPDATE THIS SCRIPT
#
# This script uses a pre-built executable (producer-rc720-microbatch.exe/.exe)
# stored in the same directory for performance. If you modify the producer code
# in this script, delete the executable first to force a rebuild:
#
# Windows: rm .\producer-rc720-microbatch.exe
# Linux:   rm ./producer-rc720-microbatch
#
# The executable will be automatically rebuilt on the next run.
#

param(
    [long]$MessageCount = 1000000,
    [string]$Topic = "flinkdotnet.sample.topic",
    [int]$Partitions = 100,
    [int]$ParallelProducers = 64
)

$ErrorActionPreference = 'Stop'
Write-Host "=== Flink.NET Kafka Producer RC7.2.0 MICRO-BATCH AUTOTUNED BUILD ===" -ForegroundColor Cyan

function Get-KafkaBootstrapServers {
    Write-Host "🔍 Discovering Kafka bootstrap servers..."

    # Use environment variable first
    if ($env:DOTNET_KAFKA_BOOTSTRAP_SERVERS) {
        # Replace localhost or ::1 with 127.0.0.1 to force IPv4 if needed
        $bootstrap = $env:DOTNET_KAFKA_BOOTSTRAP_SERVERS
        $bootstrap = $bootstrap -replace 'localhost', '127.0.0.1'
        $bootstrap = $bootstrap -replace '::1', '127.0.0.1'
        Write-Host "✅ Using environment variable DOTNET_KAFKA_BOOTSTRAP_SERVERS: $bootstrap"
        return $bootstrap
    }

    # Fallback to docker discovery
    Write-Host "⚠️ DOTNET_KAFKA_BOOTSTRAP_SERVERS not set, falling back to Docker discovery..."
    $containerPorts = docker ps --filter "name=kafka" --format "{{.Ports}}"
    if ([string]::IsNullOrWhiteSpace($containerPorts)) { Write-Error "❌ Kafka container not found."; exit 1 }
    $portsArray = $containerPorts -split '\s+'
    foreach ($portMapping in $portsArray) {
        if ($portMapping -match "127\.0\.0\.1:(\d+)->9092/tcp") {
            $hostPort = $matches[1]
            $bootstrapServers = "127.0.0.1:$hostPort"
            Write-Host "✅ Discovered Kafka bootstrap server via Docker: $bootstrapServers"
            return $bootstrapServers
        }
    }
    Write-Error "❌ Unable to parse Kafka 9092 port mapping."; exit 1
}

function Get-KafkaContainerName {
    Write-Host "🔍 Discovering Kafka container name..."
    $containerName = docker ps --filter "ancestor=confluentinc/cp-kafka:7.4.0" --format "{{.Names}}"
    if ([string]::IsNullOrWhiteSpace($containerName)) { Write-Error "❌ Kafka container not found."; exit 1 }
    Write-Host "✅ Kafka container found: $containerName"
    return $containerName
}

$bootstrapServers = Get-KafkaBootstrapServers
Write-Host "📡 Kafka Broker: $bootstrapServers" -ForegroundColor Green

$kafkaContainer = Get-KafkaContainerName
Write-Host "🔧 Warming up Kafka Broker disk & page cache..." -ForegroundColor Yellow
docker exec $kafkaContainer bash -c "shopt -s nullglob; for f in /var/lib/kafka/data/*/*; do cat \$f > /dev/null; done"

function Get-PlatformInfo {
    if ($IsWindows -or ($env:OS -eq "Windows_NT")) {
        return @{
            RuntimeId = "win-x64"
            ExecutableName = "producer-rc720-microbatch.exe"
            PathSeparator = "\"
        }
    } else {
        return @{
            RuntimeId = "linux-x64"
            ExecutableName = "producer-rc720-microbatch"
            PathSeparator = "/"
        }
    }
}

function Build-Producer {
    Write-Host "🛠️ Building .NET Producer..."
    $platform = Get-PlatformInfo
    
    # Check for existing executable in the script directory first
    $scriptDir = Split-Path -Parent $PSCommandPath
    $localExecutable = Join-Path $scriptDir $platform.ExecutableName
    
    if (Test-Path $localExecutable) {
        Write-Host "✅ Found existing executable: $localExecutable" -ForegroundColor Green
        Write-Host "💡 If you need to rebuild, delete the executable first" -ForegroundColor Yellow
        return $localExecutable
    }
    
    Write-Host "🔨 Building new executable as none found in: $scriptDir" -ForegroundColor Yellow
    $tempDir = Join-Path ([System.IO.Path]::GetTempPath()) "producer-rc720-microbatch"
    if (Test-Path $tempDir) { Remove-Item $tempDir -Recurse -Force }
    New-Item -ItemType Directory -Path $tempDir | Out-Null

    dotnet new console -f net8.0 --force --output $tempDir | Out-Null
    $projectFile = Join-Path $tempDir "producer-rc720-microbatch.csproj"
    dotnet add "$projectFile" package Confluent.Kafka | Out-Null

@"
using System;
using System.Linq;
using System.Threading.Tasks;
using System.Threading;
using System.Diagnostics;
using Confluent.Kafka;

class Program
{
    static async Task Main(string[] args)
    {
        string bootstrap = args[0];
        string topic = args[1];
        long messageCount = long.Parse(args[2]);
        int producers = int.Parse(args[3]);
        int partitions = int.Parse(args[4]);

        PreheatPartitions(bootstrap, topic, partitions);
        var payloads = PreGeneratePayloads(messageCount);

        var perProducerCounter = new long[producers];
        var sw = Stopwatch.StartNew();

        var progressTask = Task.Run(() =>
        {
            while (true)
            {
                long totalSent = perProducerCounter.Sum();
                double elapsed = sw.Elapsed.TotalSeconds;
                double rate = totalSent / (elapsed > 0 ? elapsed : 1);
                Console.Write($"\r[PROGRESS] Sent={totalSent:N0}  Rate={rate:N0} msg/sec");
                if (totalSent >= messageCount && !sw.IsRunning) break;
                Thread.Sleep(200);
            }
        });

        var tasks = Enumerable.Range(0, producers).Select(id => Task.Run(() =>
        {
            var config = new ProducerConfig
            {
                BootstrapServers = bootstrap,
                Acks = Acks.None,
                LingerMs = 2,
                BatchSize = 524288,
                CompressionType = CompressionType.None,
                QueueBufferingMaxKbytes = 64 * 1024 * 1024,
                QueueBufferingMaxMessages = 20_000_000,
                SocketTimeoutMs = 60000,
                SocketKeepaliveEnable = true,
                ClientId = $"producer-{id}",
                EnableDeliveryReports = false
            };

            using var producer = new ProducerBuilder<long, byte[]>(config)
                .SetKeySerializer(Serializers.Int64)
                .SetValueSerializer(Serializers.ByteArray)
                .Build();

            long sliceSize = messageCount / producers;
            long sliceStart = id * sliceSize;
            long sliceEnd = sliceStart + sliceSize;

            for (long i = sliceStart; i < sliceEnd; i++)
            {
                producer.Produce(topic, new Message<long, byte[]> { Key = i, Value = payloads[i] });
                perProducerCounter[id]++;
            }

            producer.Flush(TimeSpan.FromSeconds(10));
        })).ToArray();

        await Task.WhenAll(tasks);
        sw.Stop();
        await progressTask;

        long finalSent = perProducerCounter.Sum();
        Console.WriteLine($"\n[FINISH] Total: {finalSent:N0} Time: {sw.Elapsed.TotalSeconds:F3}s Rate: {finalSent / sw.Elapsed.TotalSeconds:N0} msg/sec");
    }

    static byte[][] PreGeneratePayloads(long totalMessages)
    {
        var payloads = new byte[totalMessages][];
        Parallel.For(0, (int)totalMessages, i =>
        {
            var buffer = new byte[32];
            BitConverter.TryWriteBytes(buffer.AsSpan(0, 8), i);
            BitConverter.TryWriteBytes(buffer.AsSpan(8, 8), DateTimeOffset.UtcNow.ToUnixTimeMilliseconds());
            Random.Shared.NextBytes(buffer.AsSpan(16, 16));
            payloads[i] = buffer;
        });
        return payloads;
    }

    static void PreheatPartitions(string bootstrap, string topic, int partitions)
    {
        var config = new ProducerConfig { BootstrapServers = bootstrap };
        using var producer = new ProducerBuilder<Null, byte[]>(config).SetValueSerializer(Serializers.ByteArray).Build();
        var payload = new byte[16];
        for (int pid = 0; pid < partitions; pid++)
        {
            producer.Produce(new TopicPartition(topic, new Partition(pid)), new Message<Null, byte[]> { Value = payload });
        }
        producer.Flush(TimeSpan.FromSeconds(10));
    }
}
"@ | Out-File (Join-Path $tempDir "Program.cs") -Encoding UTF8

    $publishOutputDir = Join-Path $tempDir "publish"
    dotnet publish "$projectFile" -c Release -r $platform.RuntimeId --self-contained true -p:PublishSingleFile=true -p:PublishTrimmed=false -o $publishOutputDir | Out-Null
    
    $tempExecutable = Join-Path $publishOutputDir $platform.ExecutableName
    
    # Copy the built executable and any native dependencies to the script directory for future use
    try {
        Copy-Item $tempExecutable $localExecutable -Force
        
        # Also copy any native libraries that might be needed (like librdkafka)
        $publishDir = Split-Path $tempExecutable -Parent
        $nativeLibs = Get-ChildItem -Path $publishDir -Filter "*.so" -ErrorAction SilentlyContinue
        foreach ($lib in $nativeLibs) {
            $targetLib = Join-Path $scriptDir $lib.Name
            Copy-Item $lib.FullName $targetLib -Force -ErrorAction SilentlyContinue
        }
        
        Write-Host "✅ Executable cached to: $localExecutable" -ForegroundColor Green
        
        # Clean up temp directory
        Remove-Item $tempDir -Recurse -Force -ErrorAction SilentlyContinue
        
        return $localExecutable
    } catch {
        Write-Host "⚠️ Could not cache executable, using temp location: $tempExecutable" -ForegroundColor Yellow
        return $tempExecutable
    }
}

function Run-Producers {
    param($ExecutablePath, $BootstrapServers, $Topic, $MessageCount, $Partitions, $ParallelProducers)
    & "$ExecutablePath" "$BootstrapServers" "$Topic" "$MessageCount" "$ParallelProducers" "$Partitions"
}

# Build and run the producer
$exePath = Build-Producer
Run-Producers $exePath $bootstrapServers $Topic $MessageCount $Partitions $ParallelProducers

# Clean up temp directory if we used one (don't delete cached executable)
$scriptDir = Split-Path -Parent $PSCommandPath
if (-not $exePath.StartsWith($scriptDir)) {
    Remove-Item (Split-Path $exePath -Parent) -Recurse -Force -ErrorAction SilentlyContinue
}
