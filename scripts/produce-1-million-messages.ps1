#!/usr/bin/env pwsh
<#
Flink.NET Kafka Producer RC4.1 — Progress Full Edition
#>

param(
    [long]$MessageCount = 1000000,
    [string]$Topic = "flinkdotnet.sample.topic",
    [int]$ParallelProducers = 50
)

$ErrorActionPreference = 'Stop'

Write-Host "=== Flink.NET Kafka Producer RC4.1 ===" -ForegroundColor Cyan

# Kafka discovery (stable)
function Get-KafkaBootstrapServers {
    Write-Host "🔍 Discovering Kafka bootstrap servers..."
    $bootstrapServers = $env:DOTNET_KAFKA_BOOTSTRAP_SERVERS
    if ($bootstrapServers) { return $bootstrapServers.Replace("localhost", "127.0.0.1") }
    $bootstrapServers = $env:ConnectionStrings__kafka
    if ($bootstrapServers) { return $bootstrapServers.Replace("localhost", "127.0.0.1") }
    foreach ($port in @(9092, 32769, 32770, 49303)) {
        try { $client = New-Object System.Net.Sockets.TcpClient; $client.Connect("127.0.0.1", $port); if ($client.Connected) { $client.Close(); return "127.0.0.1:$port" }} catch {}
    }
    return "127.0.0.1:9092"
}

# Build once
function Build-Producer {
    $tempDir = Join-Path ([System.IO.Path]::GetTempPath()) "producer-rc41"
    if (Test-Path $tempDir) { Remove-Item $tempDir -Recurse -Force }
    New-Item -ItemType Directory -Path $tempDir | Out-Null

    Push-Location $tempDir
    dotnet new console -f net8.0 --force | Out-Null
    dotnet add package Confluent.Kafka | Out-Null
    dotnet add package System.Text.Json | Out-Null

@"
using System;
using System.Text.Json;
using System.Threading.Tasks;
using System.Collections.Generic;
using System.Threading;
using System.Diagnostics;
using Confluent.Kafka;

class Program {
    static async Task Main(string[] args) {
        string bootstrap = args[0];
        string topic = args[1];
        long totalMessages = long.Parse(args[2]);
        int producers = int.Parse(args[3]);

        var options = new ParallelOptions { MaxDegreeOfParallelism = producers };
        long globalCounter = 0;
        object lockObj = new();
        var sw = Stopwatch.StartNew();

        Parallel.For(0, producers, options, producerId => {
            var config = new ProducerConfig {
                BootstrapServers = bootstrap,
                EnableIdempotence = true,
                Acks = Acks.All
            };
            using var producer = new ProducerBuilder<string, string>(config).Build();

            long perProducer = totalMessages / producers;
            long localSent = 0;

            for (long i = 0; i < perProducer; i++) {
                var id = producerId * perProducer + i;
                var payload = JsonSerializer.Serialize(new { id = id, msg = $"hello-{id}" });
                producer.Produce(topic, new Message<string, string> { Key = id.ToString(), Value = payload });
                localSent++;

                if (localSent % 1000 == 0) {
                    lock (lockObj) {
                        globalCounter += 1000;
                        double percent = globalCounter * 100.0 / totalMessages;
                        double rate = globalCounter / sw.Elapsed.TotalSeconds;
                        Console.WriteLine($"[PROGRESS] {percent:F2}% - TotalSent={globalCounter} - Rate={rate:F0} msg/sec");
                    }
                }
            }
            producer.Flush();
            lock (lockObj) { globalCounter += (perProducer - localSent); }
            Console.WriteLine($"[DONE] Producer {producerId} finished - sent {perProducer}");
        });

        sw.Stop();
        double finalRate = globalCounter / sw.Elapsed.TotalSeconds;
        Console.WriteLine($"[SUMMARY] ✅ All producers finished.");
        Console.WriteLine($"[SUMMARY] Total Sent: {globalCounter}, Time: {sw.Elapsed.TotalSeconds:F1}s, Final Rate: {finalRate:F0} msg/sec");
    }
}
"@ | Out-File "Program.cs" -Encoding UTF8

    dotnet build -c Release | Out-Null
    Pop-Location
    return $tempDir
}

# Execute
function Run-Producers {
    param($ProjectDir, $BootstrapServers, $Topic, $MessageCount, $ParallelProducers)
    Push-Location $ProjectDir
    dotnet run --configuration Release -- $BootstrapServers $Topic $MessageCount $ParallelProducers
    Pop-Location
}

# Main
$bootstrapServers = Get-KafkaBootstrapServers
Write-Host "📡 Using Kafka: $bootstrapServers" -ForegroundColor Green

$projectDir = Build-Producer
try { Run-Producers $projectDir $bootstrapServers $Topic $MessageCount $ParallelProducers }
finally { Remove-Item $projectDir -Recurse -Force -ErrorAction SilentlyContinue }
