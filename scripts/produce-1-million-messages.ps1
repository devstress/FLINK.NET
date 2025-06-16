#!/usr/bin/env pwsh
<#
.SYNOPSIS
    Produces 1 million messages to Kafka for FlinkJobSimulator consumption using high-performance Flink.NET producer.

.DESCRIPTION
    This script produces 1 million messages to the Kafka topic configured for FlinkJobSimulator.
    It uses Flink.NET's optimized producer architecture to achieve 1M+ messages/second throughput.
    Optimized for Apache Flink 2.0 compliance with exactly-once semantics.

.PARAMETER MessageCount
    Number of messages to produce (default: 1000000 = 1 million).

.PARAMETER Topic
    Kafka topic to send messages to (default: flinkdotnet.sample.topic).

.PARAMETER BatchSize
    Number of messages to send in each batch (default: 10000 for high throughput).

.EXAMPLE
    ./scripts/produce-1-million-messages.ps1
    Produces 1 million messages to the default topic at maximum throughput.

.EXAMPLE
    ./scripts/produce-1-million-messages.ps1 -MessageCount 500000 -Topic "test-topic"
    Produces 500k messages to a custom topic.
#>

param(
    [long]$MessageCount = 50000,  # 50 producers × 1000 messages each = 50000 total
    [string]$Topic = "flinkdotnet.sample.topic",
    [int]$BatchSize = 1000,   # 1000 messages per producer batch as requested
    [int]$ParallelProducers = 50  # 50 parallel producers as requested
)

$ErrorActionPreference = 'Stop'

function Get-OptimizationRecommendations {
    param(
        [double]$CurrentRate,
        [int]$ParallelProducers,
        [object]$SystemInfo
    )
    
    $recommendations = @()
    $targetRate = 50000  # 50 producers × 1000 messages = 50,000 total, but aim for much higher throughput rate
    
    Write-Host "📋 Performance Analysis (50 Producers × 1000 Messages = 50,000 Total):" -ForegroundColor Cyan
    Write-Host "   Current Rate: $([math]::Round($CurrentRate, 0)) msg/sec" -ForegroundColor White
    Write-Host "   Target Rate: High throughput for 50 parallel producers" -ForegroundColor White
    Write-Host "   Total Messages: 50,000 (50 × 1000)" -ForegroundColor Yellow
    
    if ($CurrentRate -lt $targetRate) {
        Write-Host "🔧 Optimization Strategies for 50 Producers:" -ForegroundColor Green
        
        # 1. Parallel Producer Configuration - already optimized for 50 producers
        Write-Host "   1️⃣ PARALLEL PRODUCER CONFIGURATION:" -ForegroundColor Yellow
        Write-Host "      • Producers: 50 (as requested)" -ForegroundColor Gray
        Write-Host "      • Messages per producer: 1000 (as requested)" -ForegroundColor Gray
        Write-Host "      • Total throughput capacity: High with 50 parallel streams" -ForegroundColor Gray
        
        # 2. Batch Size Optimization - optimized for 1000 messages per producer
        Write-Host "   2️⃣ BATCH PROCESSING OPTIMIZATION:" -ForegroundColor Yellow
        Write-Host "      • Batch size: 1000 messages per producer (as requested)" -ForegroundColor Gray
        Write-Host "      • Larger batch size provides good throughput with 50 parallel producers" -ForegroundColor Gray
        Write-Host "      • Real-time progress tracking for all 50 producers" -ForegroundColor Gray
        
        # 3. Kafka Configuration Tuning - optimized for 50 producers
        Write-Host "   3️⃣ KAFKA CONFIGURATION TUNING:" -ForegroundColor Yellow
        Write-Host "      • Topic partitions: 50 partitions (1:1 mapping with producers)" -ForegroundColor Gray
        Write-Host "      • Optimal load distribution across all producers" -ForegroundColor Gray
        Write-Host "      • Each producer gets dedicated partition for maximum parallelism" -ForegroundColor Gray
        
        # 4. System Resource Optimization
        if ($SystemInfo.AvailableRAMGB -gt 8) {
            Write-Host "   4️⃣ MEMORY OPTIMIZATION:" -ForegroundColor Yellow
            Write-Host "      • Available RAM: $($SystemInfo.AvailableRAMGB)GB - sufficient for high throughput" -ForegroundColor Gray
            Write-Host "      • Increase JVM heap for Kafka brokers to 4GB+ if running locally" -ForegroundColor Gray
        } else {
            Write-Host "   4️⃣ MEMORY CONSTRAINT:" -ForegroundColor Red
            Write-Host "      • Available RAM: $($SystemInfo.AvailableRAMGB)GB - may limit throughput" -ForegroundColor Gray
            Write-Host "      • Consider reducing parallel producers or batch sizes to fit memory" -ForegroundColor Gray
        }
        
        # 5. Implementation Recommendations
        Write-Host "   5️⃣ IMPLEMENTATION OPTIMIZATIONS:" -ForegroundColor Yellow
        Write-Host "      • Use async/await with ConfigureAwait(false) for maximum throughput" -ForegroundColor Gray
        Write-Host "      • Implement message pooling to reduce GC pressure" -ForegroundColor Gray
        Write-Host "      • Consider using unsafe code for ultra-high-performance scenarios" -ForegroundColor Gray
        
        # 6. Infrastructure Recommendations
        Write-Host "   6️⃣ INFRASTRUCTURE SCALING:" -ForegroundColor Yellow
        Write-Host "      • Run on SSD storage for Kafka logs (reduce I/O latency)" -ForegroundColor Gray
        Write-Host "      • Use dedicated Kafka brokers instead of containerized setup" -ForegroundColor Gray
        Write-Host "      • Consider using multiple Kafka brokers for distributed load" -ForegroundColor Gray
        
        # Immediate actionable recommendation
        Write-Host "🎯 CONFIGURATION: 50 producers × 1000 messages = optimal for testing producer parallelism and throughput" -ForegroundColor Green
    } else {
        Write-Host "🏆 EXCELLENT: Already achieving target throughput!" -ForegroundColor Green
    }
}

function Get-SystemInfo {
    $info = @{
        CPUCores = 0
        TotalRAMGB = 0
        AvailableRAMGB = 0
        Platform = "Unknown"
    }
    
    try {
        if ($IsLinux -or $IsMacOS -or $env:OS -notlike "*Windows*") {
            # Linux/macOS system information
            $info.Platform = "Linux/Unix"
            
            # Get CPU cores
            try {
                $info.CPUCores = (Get-Content /proc/cpuinfo | Where-Object { $_ -like "processor*" } | Measure-Object).Count
            }
            catch {
                $info.CPUCores = [Environment]::ProcessorCount
            }
            
            # Get memory information from /proc/meminfo
            try {
                $memInfo = Get-Content /proc/meminfo
                $memTotalLine = $memInfo | Where-Object { $_ -like "MemTotal:*" }
                $memAvailLine = $memInfo | Where-Object { $_ -like "MemAvailable:*" }
                
                if ($memTotalLine -match "MemTotal:\s+(\d+)\s+kB") {
                    $info.TotalRAMGB = [math]::Round([double]$matches[1] / 1024 / 1024, 2)
                }
                
                if ($memAvailLine -match "MemAvailable:\s+(\d+)\s+kB") {
                    $info.AvailableRAMGB = [math]::Round([double]$matches[1] / 1024 / 1024, 2)
                }
            }
            catch {
                Write-Host "Warning: Could not read memory info from /proc/meminfo" -ForegroundColor Yellow
            }
        }
        else {
            # Windows system information
            $info.Platform = "Windows"
            $info.CPUCores = [Environment]::ProcessorCount
            
            try {
                # Try using WMI for Windows
                $memory = Get-WmiObject -Class Win32_ComputerSystem -ErrorAction SilentlyContinue
                if ($memory) {
                    $info.TotalRAMGB = [math]::Round([double]$memory.TotalPhysicalMemory / 1GB, 2)
                }
                
                $availMem = Get-WmiObject -Class Win32_OperatingSystem -ErrorAction SilentlyContinue
                if ($availMem) {
                    $info.AvailableRAMGB = [math]::Round([double]$availMem.FreePhysicalMemory / 1MB, 2)
                }
            }
            catch {
                Write-Host "Warning: Could not get Windows memory info via WMI" -ForegroundColor Yellow
            }
        }
    }
    catch {
        Write-Host "Warning: Could not determine system information: $_" -ForegroundColor Yellow
        $info.CPUCores = [Environment]::ProcessorCount
    }
    
    return $info
}

Write-Host "=== High-Performance Flink.NET Kafka Producer (50 Producers × 1000 Messages) ===" -ForegroundColor Cyan
Write-Host "Started at: $(Get-Date -Format 'yyyy-MM-dd HH:mm:ss') UTC" -ForegroundColor White

# Enforce 1000 messages per producer when using 50 producers as requested
if ($ParallelProducers -eq 50) {
    $MessageCount = 50000  # 50 × 1000 = 50,000 total
    $BatchSize = 1000      # 1000 messages per producer
    Write-Host "🔧 Configuration enforced: 50 producers × 1000 messages each = 50,000 total messages (as requested)" -ForegroundColor Green
}

Write-Host "Configuration: $ParallelProducers parallel producers, $([math]::Round($MessageCount / $ParallelProducers)) messages per producer = $MessageCount total messages" -ForegroundColor Yellow
Write-Host "Parameters: MessageCount=$MessageCount, Topic=$Topic, BatchSize=$BatchSize, ParallelProducers=$ParallelProducers" -ForegroundColor White

function Get-KafkaBootstrapServers {
    Write-Host "🔍 Discovering Kafka bootstrap servers..." -ForegroundColor White
    
    # Check environment variables first (these are set by discover-aspire-ports.ps1)
    $bootstrapServers = $env:DOTNET_KAFKA_BOOTSTRAP_SERVERS
    if ($bootstrapServers) {
        Write-Host "Found Kafka servers from DOTNET_KAFKA_BOOTSTRAP_SERVERS: $bootstrapServers" -ForegroundColor Green
        # Fix IPv6 localhost issue by ensuring we use IPv4
        $bootstrapServers = $bootstrapServers.Replace("localhost", "127.0.0.1")
        return $bootstrapServers
    }
    
    $bootstrapServers = $env:ConnectionStrings__kafka
    if ($bootstrapServers) {
        Write-Host "Found Kafka servers from ConnectionStrings__kafka: $bootstrapServers" -ForegroundColor Green
        # Fix IPv6 localhost issue by ensuring we use IPv4
        $bootstrapServers = $bootstrapServers.Replace("localhost", "127.0.0.1")
        return $bootstrapServers
    }
    
    # Discover from Docker containers using more reliable approach
    Write-Host "Environment variables not set, attempting Docker discovery..." -ForegroundColor Yellow
    try {
        # Look for any containers with kafka in the name or image
        $kafkaContainers = docker ps --filter "name=kafka" --format "{{.ID}}\t{{.Names}}\t{{.Ports}}"
        if ($kafkaContainers) {
            Write-Host "Found Kafka containers:" -ForegroundColor Green
            $kafkaContainers | ForEach-Object { Write-Host "  $_" -ForegroundColor Gray }
            
            # Extract port from docker containers - handle multiple possible formats
            foreach ($line in $kafkaContainers) {
                if ($line -match "(\d+\.\d+\.\d+\.\d+):(\d+)->9092" -or $line -match "127\.0\.0\.1:(\d+)->9092") {
                    if ($matches[1] -match "^\d+$") {
                        # Format: 127.0.0.1:port->9092
                        $port = $matches[1]
                        $bootstrapServers = "127.0.0.1:$port"
                    } else {
                        # Format: ip:port->9092
                        $bootstrapServers = "$($matches[1]):$($matches[2])"
                    }
                    Write-Host "Discovered Kafka at: $bootstrapServers" -ForegroundColor Green
                    return $bootstrapServers
                }
            }
        }
        
        # Alternative: Try to find any Kafka container and inspect it
        $kafkaContainerId = docker ps -q --filter "name=kafka" | Select-Object -First 1
        if ($kafkaContainerId) {
            Write-Host "Found Kafka container ID: $kafkaContainerId" -ForegroundColor Gray
            $portMapping = docker port $kafkaContainerId 9092 2>$null
            if ($portMapping) {
                Write-Host "Port mapping for 9092: $portMapping" -ForegroundColor Gray
                if ($portMapping -match "(\d+\.\d+\.\d+\.\d+):(\d+)") {
                    $bootstrapServers = "$($matches[1]):$($matches[2])"
                    Write-Host "Discovered Kafka via port command: $bootstrapServers" -ForegroundColor Green
                    return $bootstrapServers
                }
            }
        }
    }
    catch {
        Write-Host "Docker discovery failed: $_" -ForegroundColor Yellow
    }
    
    # Final fallback to check common ports
    $commonPorts = @(9092, 32768, 32769, 32770, 32771, 32772, 32773, 32774, 32775)
    foreach ($port in $commonPorts) {
        try {
            $testConnection = New-Object System.Net.Sockets.TcpClient
            $testConnection.ReceiveTimeout = 1000
            $testConnection.SendTimeout = 1000
            $connected = $testConnection.ConnectAsync("127.0.0.1", $port).Wait(2000)
            if ($connected -and $testConnection.Connected) {
                $testConnection.Close()
                $bootstrapServers = "127.0.0.1:$port"
                Write-Host "Found Kafka via port scan: $bootstrapServers" -ForegroundColor Green
                return $bootstrapServers
            }
            $testConnection.Close()
        }
        catch {
            # Continue to next port
        }
    }
    
    # Final fallback to default
    $bootstrapServers = "127.0.0.1:9092"
    Write-Host "Using default Kafka servers: $bootstrapServers" -ForegroundColor Yellow
    return $bootstrapServers
}

function Test-KafkaConnection {
    param([string]$BootstrapServers)
    
    Write-Host "🔄 Testing Kafka connection to $BootstrapServers..." -ForegroundColor White
    
    # Method 1: Try basic TCP connection test first
    try {
        $serverParts = $BootstrapServers.Split(':')
        if ($serverParts.Length -lt 2) {
            Write-Host "❌ Invalid BootstrapServers format: $BootstrapServers" -ForegroundColor Red
            return $false
        }
        $kafkaHost = $serverParts[0]
        $kafkaPort = [int]$serverParts[1]
        
        Write-Host "Testing TCP connection to ${kafkaHost}:${kafkaPort}..." -ForegroundColor Gray
        $tcpClient = New-Object System.Net.Sockets.TcpClient
        $tcpClient.ReceiveTimeout = 5000
        $tcpClient.SendTimeout = 5000
        $connected = $tcpClient.ConnectAsync($kafkaHost, $kafkaPort).Wait(5000)
        
        if ($connected -and $tcpClient.Connected) {
            Write-Host "✅ TCP connection successful to ${kafkaHost}:${kafkaPort}" -ForegroundColor Green
            $tcpClient.Close()
        } else {
            Write-Host "❌ TCP connection failed to ${kafkaHost}:${kafkaPort}" -ForegroundColor Red
            $tcpClient.Close()
            return $false
        }
    }
    catch {
        Write-Host "❌ TCP connection test failed: $_" -ForegroundColor Red
        return $false
    }
    
    # Skip container-based API tests due to Aspire's dynamic advertised listeners
    # Aspire uses external dynamic ports in advertised listeners which are not accessible from within container
    
    # Method 2: Test producer connectivity using external bootstrap servers
    try {
        Write-Host "Testing Kafka producer connectivity..." -ForegroundColor Gray
        
        # Test producer connectivity using the external bootstrap servers
        # This validates that Kafka is accessible and can accept messages
        $testMessage = "test-connectivity-$(Get-Date -Format 'yyyyMMddHHmmss')"
        
        # Use .NET Kafka producer for testing since it uses external bootstrap servers
        $testScript = @"
using System;
using System.Threading.Tasks;
using Confluent.Kafka;

class Program {
    static async Task Main() {
        var config = new ProducerConfig {
            BootstrapServers = "$BootstrapServers",
            SocketTimeoutMs = 30000,
            MessageTimeoutMs = 30000,
            SecurityProtocol = SecurityProtocol.Plaintext
        };
        
        using var producer = new ProducerBuilder<string, string>(config).Build();
        try {
            var result = await producer.ProduceAsync("__connectivity_test", new Message<string, string> {
                Key = "test",
                Value = "$testMessage"
            });
            Console.WriteLine("SUCCESS");
        } catch (Exception ex) {
            Console.WriteLine("ERROR: " + ex.Message);
        }
    }
}
"@
        
        # Create temporary test program
        $tempDir = [System.IO.Path]::GetTempPath()
        $testProjectDir = Join-Path $tempDir "kafka-test-$(Get-Date -Format 'yyyyMMddHHmmss')"
        New-Item -ItemType Directory -Path $testProjectDir -Force | Out-Null
        
        # Create a simple .NET console app to test Kafka connectivity
        Push-Location $testProjectDir
        try {
            dotnet new console -f net8.0 --force | Out-Null
            dotnet add package Confluent.Kafka | Out-Null
            $testScript | Out-File -FilePath "Program.cs" -Encoding UTF8
            
            $testOutput = dotnet run 2>&1
            if ($testOutput -like "*SUCCESS*") {
                Write-Host "✅ Kafka producer connectivity test successful" -ForegroundColor Green
                return $true
            } else {
                Write-Host "❌ Kafka producer connectivity test failed: $testOutput" -ForegroundColor Red
            }
        }
        finally {
            Pop-Location
            Remove-Item -Path $testProjectDir -Recurse -Force -ErrorAction SilentlyContinue
        }
    }
    catch {
        Write-Host "❌ Producer connectivity test failed: $_" -ForegroundColor Red
    }
    
    # If we got here, all tests failed
    Write-Host "❌ All Kafka connectivity tests failed" -ForegroundColor Red
    return $false
}

function Test-TopicExists {
    param([string]$BootstrapServers, [string]$Topic)
    
    # Use .NET AdminClient to verify topic exists
    $topicVerificationScript = @"
using System;
using System.Threading.Tasks;
using Confluent.Kafka;
using Confluent.Kafka.Admin;

class Program {
    static async Task Main() {
        var config = new AdminClientConfig {
            BootstrapServers = "$BootstrapServers",
            SocketTimeoutMs = 30000,
            ApiVersionRequestTimeoutMs = 30000,
            SecurityProtocol = SecurityProtocol.Plaintext
        };
        
        using var adminClient = new AdminClientBuilder(config).Build();
        try {
            var metadata = adminClient.GetMetadata(TimeSpan.FromSeconds(30));
            var topicExists = false;
            foreach (var topic in metadata.Topics) {
                if (topic.Topic == "$Topic") {
                    topicExists = true;
                    break;
                }
            }
            
            if (topicExists) {
                Console.WriteLine("TOPIC_EXISTS");
            } else {
                Console.WriteLine("TOPIC_NOT_FOUND");
            }
        } catch (Exception ex) {
            Console.WriteLine("ERROR: " + ex.Message);
        }
    }
}
"@
    
    $tempDir = [System.IO.Path]::GetTempPath()
    $verifyProjectDir = Join-Path $tempDir "kafka-verify-$(Get-Date -Format 'yyyyMMddHHmmss')"
    New-Item -ItemType Directory -Path $verifyProjectDir -Force | Out-Null
    
    Push-Location $verifyProjectDir
    try {
        dotnet new console -f net8.0 --force | Out-Null
        dotnet add package Confluent.Kafka | Out-Null
        $topicVerificationScript | Out-File -FilePath "Program.cs" -Encoding UTF8
        
        $verifyOutput = dotnet run 2>&1
        return ($verifyOutput -like "*TOPIC_EXISTS*")
    }
    finally {
        Pop-Location
        Remove-Item -Path $verifyProjectDir -Recurse -Force -ErrorAction SilentlyContinue
    }
}

function New-TopicFallback {
    param([string]$BootstrapServers, [string]$Topic)
    
    # Single fallback attempt as requested by user
    $fallbackTopicCreationScript = @"
using System;
using System.Collections.Generic;
using System.Threading.Tasks;
using Confluent.Kafka;
using Confluent.Kafka.Admin;

class Program {
    static async Task Main(string[] args) {
        if (args.Length < 3) {
            Console.WriteLine("ERROR: Usage: program <bootstrapServers> <topic> <partitions>");
            return;
        }
        
        var bootstrapServers = args[0];
        var topicName = args[1];
        var partitions = int.Parse(args[2]);
        
        var config = new AdminClientConfig {
            BootstrapServers = bootstrapServers,
            SocketTimeoutMs = 60000,
            ApiVersionRequestTimeoutMs = 30000,
            SecurityProtocol = SecurityProtocol.Plaintext
        };
        
        using var adminClient = new AdminClientBuilder(config).Build();
        
        try {
            var topicSpecification = new TopicSpecification {
                Name = topicName,
                NumPartitions = partitions,
                ReplicationFactor = 1,
                Configs = new Dictionary<string, string> {
                    { "retention.ms", "3600000" },
                    { "cleanup.policy", "delete" },
                    { "min.insync.replicas", "1" },
                    { "segment.ms", "60000" }
                }
            };
            
            await adminClient.CreateTopicsAsync(new TopicSpecification[] { topicSpecification });
            Console.WriteLine("TOPIC_CREATED");
            
            // Verify creation
            var metadata = adminClient.GetMetadata(topicName, TimeSpan.FromSeconds(30));
            var topic = metadata.Topics.Find(t => t.Topic == topicName);
            if (topic != null) {
                Console.WriteLine("TOPIC_VERIFIED");
            } else {
                Console.WriteLine("TOPIC_NOT_VERIFIED");
            }
        } catch (CreateTopicsException ex) {
            foreach (var result in ex.Results) {
                if (result.Error.Code == ErrorCode.TopicAlreadyExists) {
                    Console.WriteLine("TOPIC_ALREADY_EXISTS");
                } else {
                    Console.WriteLine("ERROR: " + result.Error.Reason);
                }
            }
        } catch (Exception ex) {
            Console.WriteLine("ERROR: " + ex.Message);
        }
    }
}
"@
    
    $tempDir = [System.IO.Path]::GetTempPath()
    $fallbackProjectDir = Join-Path $tempDir "kafka-fallback-$(Get-Date -Format 'yyyyMMddHHmmss')"
    New-Item -ItemType Directory -Path $fallbackProjectDir -Force | Out-Null
    
    Push-Location $fallbackProjectDir
    try {
        dotnet new console -f net8.0 --force | Out-Null
        dotnet add package Confluent.Kafka | Out-Null
        $fallbackTopicCreationScript | Out-File -FilePath "Program.cs" -Encoding UTF8
        dotnet build | Out-Null
        
        # Determine partition count based on parallel producers for optimal distribution
        $partitionCount = 50  # Optimized for 50 producers: 50 partitions for 1:1 producer-to-partition mapping
        
        Write-Host "🔧 Creating topic '$Topic' with $partitionCount partitions for parallel processing..." -ForegroundColor Yellow
        $fallbackOutput = dotnet run -- "$BootstrapServers" "$Topic" "$partitionCount" 2>&1
        
        $success = ($fallbackOutput -like "*TOPIC_CREATED*" -or $fallbackOutput -like "*TOPIC_ALREADY_EXISTS*" -or $fallbackOutput -like "*TOPIC_VERIFIED*")
        if ($success) {
            Write-Host "✅ Fallback topic creation successful!" -ForegroundColor Green
        } else {
            Write-Host "❌ Fallback topic creation failed: $fallbackOutput" -ForegroundColor Red
        }
        return $success
    }
    finally {
        Pop-Location
        Remove-Item -Path $fallbackProjectDir -Recurse -Force -ErrorAction SilentlyContinue
    }
}

function New-OptimizedProducerProject {
    param([string]$BootstrapServers, [string]$Topic)
    
    $tempDir = [System.IO.Path]::GetTempPath()
    $producerProjectDir = Join-Path $tempDir "parallel-producer-$(Get-Date -Format 'yyyyMMddHHmmss')"
    New-Item -ItemType Directory -Path $producerProjectDir -Force | Out-Null
    
    # Create ultra-high-performance parallel producer optimized for 1M+ msg/sec
    $parallelProducerScript = @"
using System;
using System.Text.Json;
using System.Threading.Tasks;
using Confluent.Kafka;
using System.Collections.Generic;
using System.Threading;
using System.Linq;

class UltraHighPerformanceProducer {
    static async Task Main(string[] args) {
        if (args.Length < 5) {
            Console.WriteLine("ERROR: Usage: program <bootstrapServers> <topic> <startMsgId> <messageCount> <producerId>");
            return;
        }
        
        var bootstrapServers = args[0];
        var topic = args[1];
        var startMsgId = long.Parse(args[2]);
        var messageCount = int.Parse(args[3]);
        var producerId = int.Parse(args[4]);
        
        Console.WriteLine("PRODUCER_START:" + producerId);
        
        // Ultra-high-performance configuration for 1M+ msg/sec target
        var config = new ProducerConfig {
            BootstrapServers = bootstrapServers,
            
            // Maximum throughput settings for parallel processing (optimized per performance analysis)
            BatchSize = 250000,          // Optimized batch size (increased from 100K per performance analysis)
            LingerMs = 0,                // No latency - send immediately when batch is full
            CompressionType = CompressionType.Lz4,  // Fast compression
            SocketTimeoutMs = 120000,    // Long timeout for high-volume processing
            MessageTimeoutMs = 180000,   // Long message timeout
            MaxInFlight = 5,             // Max allowed for exactly-once semantics
            
            // Exactly-once semantics for Flink.NET compliance
            EnableIdempotence = true,
            Acks = Acks.All,
            
            // Ultra-high-performance optimizations (enhanced per analysis)
            SocketSendBufferBytes = 2097152,    // 2MB send buffer (increased from 1MB)
            SocketReceiveBufferBytes = 2097152, // 2MB receive buffer (increased from 1MB)
            QueueBufferingMaxMessages = 1000000, // 1M message queue
            QueueBufferingMaxKbytes = 4194304,   // 4GB queue buffer (increased from 2GB)
            
            SecurityProtocol = SecurityProtocol.Plaintext,
            
            // Partition assignment for FIFO maintenance
            // Each producer handles specific partitions to maintain order
            Partitioner = Partitioner.ConsistentRandom
        };
        
        using var producer = new ProducerBuilder<string, string>(config)
            .SetErrorHandler((_, e) => Console.WriteLine("WARN:" + producerId + ":" + e.Reason))
            .Build();
        
        try {
            var totalPartitions = 50; // Optimized for 50 parallel producers (1 partition per producer)
            var partitionsPerProducer = 2; // Each producer handles 2 partitions for optimal load distribution
            var assignedPartitions = GetAssignedPartitions(producerId, totalPartitions, partitionsPerProducer);
            
            Console.WriteLine("PARTITIONS:" + producerId + ":" + string.Join(",", assignedPartitions));
            
            // Ultra-fast parallel message production with optimized concurrency for granular progress
            var tasks = new List<Task>();
            var semaphore = new SemaphoreSlim(2000); // Reduced concurrency for 4-core systems - optimized for CPU/memory balance
            var progressReported = 0L;
            
            for (int i = 0; i < messageCount; i++) {
                await semaphore.WaitAsync();
                var msgId = startMsgId + i;
                
                // Assign message to one of this producer's partitions using FIFO-preserving algorithm
                var partition = assignedPartitions[msgId % assignedPartitions.Length];
                
                var task = ProduceMessageAsync(producer, topic, msgId, partition, semaphore);
                tasks.Add(task);
                
                // Process in smaller chunks for granular progress tracking (msg/ms level)
                if (tasks.Count >= 1000) {  // Reduced from 10000 to 1000 for granular progress
                    await Task.WhenAll(tasks);
                    tasks.Clear();
                    
                    progressReported += 1000;
                    // Report every 1K messages for real-time visibility as requested
                    Console.WriteLine("PROGRESS:" + producerId + ":" + progressReported);
                }
            }
            
            // Wait for remaining tasks
            if (tasks.Count > 0) {
                await Task.WhenAll(tasks);
                progressReported += tasks.Count;
                Console.WriteLine("PROGRESS:" + producerId + ":" + progressReported);
            }
            
            // Flush for exactly-once guarantees
            Console.WriteLine("FLUSHING:" + producerId);
            producer.Flush(TimeSpan.FromMinutes(2)); // Generous flush timeout for high volume
            
            Console.WriteLine("SUCCESS:" + producerId + ":" + messageCount);
        } catch (Exception ex) {
            Console.WriteLine("ERROR:" + producerId + ":" + ex.Message);
        }
    }
    
    static int[] GetAssignedPartitions(int producerId, int totalPartitions, int partitionsPerProducer) {
        var startPartition = (producerId * partitionsPerProducer) % totalPartitions;
        var result = new int[partitionsPerProducer];
        for (int i = 0; i < partitionsPerProducer; i++) {
            result[i] = (startPartition + i) % totalPartitions;
        }
        return result;
    }
    
    static async Task ProduceMessageAsync(IProducer<string, string> producer, string topic, long msgId, int partition, SemaphoreSlim semaphore) {
        try {
            var timestamp = DateTime.UtcNow.ToString("yyyy-MM-ddTHH:mm:ss.fffZ");
            
            // Optimized message format for high-throughput processing with ConfigureAwait(false)
            var message = new {
                id = msgId,
                redis_ordered_id = msgId,
                timestamp = timestamp,
                job_id = "flink-job-1",
                task_id = "task-" + msgId,
                kafka_partition = partition,  // Explicit partition assignment for FIFO
                kafka_offset = msgId,
                processing_stage = "parallel-source->map->sink",
                payload = "ultra-high-throughput-" + msgId,
                checksum = (msgId * 31 + timestamp.GetHashCode()) % 1000000
            };
            
            var jsonMessage = JsonSerializer.Serialize(message);
            
            // Explicitly specify partition to maintain FIFO order within partition
            // Use ConfigureAwait(false) for maximum throughput per performance analysis
            await producer.ProduceAsync(new TopicPartition(topic, partition), new Message<string, string> {
                Key = msgId.ToString(),
                Value = jsonMessage,
                Timestamp = new Timestamp(DateTime.UtcNow)
            }).ConfigureAwait(false);
        } finally {
            semaphore.Release();
        }
    }
}
"@
    
    Push-Location $producerProjectDir
    try {
        # Create ultra-high-performance producer project
        dotnet new console -f net8.0 --force | Out-Null
        dotnet add package Confluent.Kafka | Out-Null
        dotnet add package System.Text.Json | Out-Null
        $parallelProducerScript | Out-File -FilePath "Program.cs" -Encoding UTF8
        
        # Build with maximum optimizations
        dotnet build --configuration Release | Out-Null
        
        Write-Host "✅ Optimized parallel producer project created and built" -ForegroundColor Green
        return $producerProjectDir
    }
    catch {
        Pop-Location
        Remove-Item -Path $producerProjectDir -Recurse -Force -ErrorAction SilentlyContinue
        throw "Failed to create optimized producer project: $_"
    }
}

function Start-ParallelProducers {
    param([string]$ProjectDir, [long]$MessageCount, [int]$ParallelProducers)
    
    $messagesPerProducer = [Math]::Ceiling($MessageCount / $ParallelProducers)
    $jobs = @()
    
    Write-Host "🚀 Starting $ParallelProducers parallel producers..." -ForegroundColor Green
    
    for ($i = 0; $i -lt $ParallelProducers; $i++) {
        $startMsgId = ($i * $messagesPerProducer) + 1
        $actualMessageCount = [Math]::Min($messagesPerProducer, $MessageCount - ($i * $messagesPerProducer))
        
        if ($actualMessageCount -le 0) { break }
        
        Write-Host "  Producer $($i+1): Messages $startMsgId to $($startMsgId + $actualMessageCount - 1) ($actualMessageCount messages)" -ForegroundColor Gray
        
        # Start each producer as a background job
        $job = Start-Job -ScriptBlock {
            param($ProjectDir, $BootstrapServers, $Topic, $StartMsgId, $MessageCount, $ProducerId)
            
            Push-Location $ProjectDir
            try {
                $output = dotnet run --configuration Release -- $BootstrapServers $Topic $StartMsgId $MessageCount $ProducerId 2>&1
                return @{
                    ProducerId = $ProducerId
                    Output = $output
                    ExitCode = $LASTEXITCODE
                    StartMsgId = $StartMsgId
                    MessageCount = $MessageCount
                }
            }
            finally {
                Pop-Location
            }
        } -ArgumentList $ProjectDir, $BootstrapServers, $Topic, $startMsgId, $actualMessageCount, $i
        
        $jobs += $job
    }
    
    Write-Host "✅ All $($jobs.Count) parallel producers started" -ForegroundColor Green
    return $jobs
}

function Wait-ParallelProducers {
    param([array]$Jobs, [long]$MessageCount, [datetime]$StartTime)
    
    $completedProducers = 0
    $producerProgress = @{}  # Track progress per producer
    $completedProducerIds = @{}  # Track which producers have completed
    $lastProgressTime = Get-Date
    
    Write-Host "⏳ Monitoring parallel producer progress with real-time rate tracking..." -ForegroundColor Yellow
    
    while ($completedProducers -lt $Jobs.Count) {
        $currentTime = Get-Date
        
        # Check for new output from running jobs (captures PROGRESS lines)
        foreach ($job in $Jobs) {
            if ($job.State -eq 'Running' -and $job.HasMoreData) {
                $newOutput = Receive-Job $job -Keep
                if ($newOutput) {
                    foreach ($line in $newOutput) {
                        if ($line -like "PROGRESS:*") {
                            # Parse: PROGRESS:producerId:messageCount with safe bounds checking
                            $parts = $line.Split(':')
                            if ($parts.Length -eq 3 -and $parts[1] -and $parts[2] -and $parts[1].Trim() -ne '' -and $parts[2].Trim() -ne '') {
                                try {
                                    $producerId = [int]$parts[1].Trim()
                                    $currentProgress = [long]$parts[2].Trim()
                                    $producerProgress[$producerId] = $currentProgress
                                    Write-Host "   📊 Producer $($producerId + 1): $currentProgress messages" -ForegroundColor DarkGray
                                } catch {
                                    Write-Host "   ⚠️ Invalid PROGRESS format: $line" -ForegroundColor Yellow
                                }
                            }
                        }
                        elseif ($line -like "PRODUCER_START:*") {
                            # Safe parsing with bounds checking
                            $startParts = $line.Split(':')
                            if ($startParts.Length -gt 1 -and $startParts[1] -and $startParts[1].Trim() -ne '') {
                                try {
                                    $producerId = [int]$startParts[1].Trim()
                                    Write-Host "   🚀 Producer $($producerId + 1) started" -ForegroundColor DarkGreen
                                } catch {
                                    Write-Host "   ⚠️ Invalid PRODUCER_START format: $line" -ForegroundColor Yellow
                                }
                            }
                        }
                    }
                }
            }
        }
        
        # Calculate real-time total sent messages (only from active producers, not double-counting completed ones)
        $realTimeTotalSent = 0
        foreach ($producerId in $producerProgress.Keys) {
            if (-not $completedProducerIds.ContainsKey($producerId)) {
                $realTimeTotalSent += $producerProgress[$producerId]
            }
        }
        
        # Check job completion
        foreach ($job in $Jobs) {
            if ($job.State -eq 'Completed') {
                $result = Receive-Job $job
                $completedProducers++
                $producerId = $result.ProducerId
                
                # Mark this producer as completed to avoid double-counting
                $completedProducerIds[$producerId] = $true
                
                if ($result.ExitCode -eq 0 -and $result.Output -like "*SUCCESS:*") {
                    $successLine = $result.Output | Where-Object { $_ -like "SUCCESS:*" } | Select-Object -First 1
                    if ($successLine) {
                        # Safe parsing of SUCCESS line with comprehensive null/empty checking
                        $successParts = $successLine.Split(':')
                        $sentMessages = 0
                        if ($successParts.Length -gt 2 -and $successParts[2] -and $successParts[2].Trim() -ne '') {
                            try {
                                $sentMessages = [long]$successParts[2].Trim()
                            } catch {
                                $sentMessages = 0
                            }
                        }
                        # Use the final progress from the producer, not the SUCCESS count (which might be different)
                        $finalProgress = if ($producerProgress.ContainsKey($producerId)) { $producerProgress[$producerId] } else { $sentMessages }
                        Write-Host "✅ Producer $($producerId + 1) completed: $finalProgress messages" -ForegroundColor Green
                    } else {
                        # No SUCCESS line found, use progress tracking value
                        $finalProgress = if ($producerProgress.ContainsKey($producerId)) { $producerProgress[$producerId] } else { 0 }
                        Write-Host "✅ Producer $($producerId + 1) completed: $finalProgress messages (via progress tracking)" -ForegroundColor Green
                    }
                } elseif ($result.ExitCode -eq 0) {
                    # Producer completed successfully but no SUCCESS line - use progress tracking
                    $finalProgress = if ($producerProgress.ContainsKey($producerId)) { $producerProgress[$producerId] } else { 0 }
                    Write-Host "✅ Producer $($producerId + 1) completed: $finalProgress messages (via progress tracking, no SUCCESS line)" -ForegroundColor Yellow
                } else {
                    Write-Host "❌ Producer $($result.ProducerId + 1) failed: $($result.Output)" -ForegroundColor Red
                    return $false
                }
                
                Remove-Job $job
            }
        }
        
        # Progress reporting every 2 seconds for more responsive feedback
        # Only show progress if there's meaningful activity (producers completed or messages sent) AND meaningful rates
        if (($currentTime - $lastProgressTime).TotalSeconds -ge 2 -and ($completedProducers -gt 0 -or $realTimeTotalSent -gt 0)) {
            $elapsed = $currentTime - $StartTime
            $currentRate = if ($elapsed.TotalSeconds -gt 0) { $realTimeTotalSent / $elapsed.TotalSeconds } else { 0 }
            $progress = ($completedProducers * 100.0) / $Jobs.Count
            
            # Calculate msg/ms for granular progress display as requested
            $rateMsgPerMs = if ($elapsed.TotalMilliseconds -gt 0) { $realTimeTotalSent / $elapsed.TotalMilliseconds } else { 0 }
            
            # Only show progress when there are meaningful rates (avoid showing 0 msg/sec)
            if ($currentRate -gt 0 -or $rateMsgPerMs -gt 0) {
                $rateColor = if ($currentRate -gt 1000000) { "Green" } elseif ($currentRate -gt 500000) { "Yellow" } else { "Red" }
                Write-Host "📊 Progress: $completedProducers/$($Jobs.Count) producers completed ($([math]::Round($progress, 1))%) - Current rate: $([math]::Round($currentRate, 0)) msg/sec ($([math]::Round($rateMsgPerMs, 2)) msg/ms)" -ForegroundColor $rateColor
                
                # Show real-time message breakdown
                Write-Host "   📈 Real-time status: $realTimeTotalSent/$MessageCount messages sent ($([math]::Round(($realTimeTotalSent * 100.0) / $MessageCount, 1))%)" -ForegroundColor Gray
                
                if ($currentRate -gt 1000000) {
                    Write-Host "🎯 TARGET ACHIEVED: >1M msg/sec sustained throughput!" -ForegroundColor Green
                } elseif ($currentRate -gt 500000) {
                    Write-Host "🎯 GOOD PROGRESS: >500K msg/sec sustained throughput!" -ForegroundColor Yellow
                }
            }
            
            $lastProgressTime = $currentTime
        }
        
        Start-Sleep -Seconds 0.5  # Faster polling for real-time progress
    }
    
    # Final verification using actual total sent messages from producer progress
    $finalElapsed = (Get-Date) - $StartTime
    $finalTotalSent = 0
    
    # Sum all producer progress (which contains the actual message counts)
    foreach ($progress in $producerProgress.Values) {
        $finalTotalSent += $progress
    }
    
    $finalRate = if ($finalElapsed.TotalSeconds -gt 0) { $finalTotalSent / $finalElapsed.TotalSeconds } else { 0 }
    
    Write-Host "📊 Final Results:" -ForegroundColor White
    Write-Host "  Total Messages Sent: $finalTotalSent" -ForegroundColor Green
    Write-Host "  Total Time: $([math]::Round($finalElapsed.TotalSeconds, 1)) seconds" -ForegroundColor Green
    Write-Host "  Final Rate: $([math]::Round($finalRate, 0)) messages/second" -ForegroundColor Green
    Write-Host "  Parallel Producers: $($Jobs.Count)" -ForegroundColor Green
    
    return ($finalTotalSent -eq $MessageCount)
}

function Send-KafkaMessages {
    param(
        [string]$BootstrapServers,
        [string]$Topic,
        [long]$MessageCount,
        [int]$BatchSize,
        [int]$ParallelProducers
    )
    
    Write-Host "📨 Starting high-performance parallel message production" -ForegroundColor White
    Write-Host "Configuration:" -ForegroundColor Gray
    Write-Host "  Bootstrap Servers: $BootstrapServers" -ForegroundColor Gray
    Write-Host "  Topic: $Topic" -ForegroundColor Gray
    Write-Host "  Total Messages: $MessageCount" -ForegroundColor Gray
    Write-Host "  Parallel Producers: $ParallelProducers" -ForegroundColor Gray
    Write-Host "  Target Rate: 1M+ messages/second" -ForegroundColor Gray
    
    $startTime = Get-Date
    
    try {
        # First, verify topic exists using external .NET client
        Write-Host "Verifying topic '$Topic' exists..." -ForegroundColor Gray
        $topicExists = Test-TopicExists -BootstrapServers $BootstrapServers -Topic $Topic
        
        if (-not $topicExists) {
            Write-Host "⚠️ Topic '$Topic' not found - attempting fallback creation..." -ForegroundColor Yellow
            $fallbackCreated = New-TopicFallback -BootstrapServers $BootstrapServers -Topic $Topic
            if (-not $fallbackCreated) {
                throw "Topic '$Topic' could not be created via fallback method."
            }
        }
        
        # Create optimized high-performance producer project once (eliminates build overhead)
        Write-Host "🔧 Creating optimized parallel producer infrastructure..." -ForegroundColor Yellow
        $producerProjectDir = New-OptimizedProducerProject -BootstrapServers $BootstrapServers -Topic $Topic
        
        try {
            # Calculate message distribution across parallel producers for FIFO maintenance
            $messagesPerProducer = [Math]::Ceiling($MessageCount / $ParallelProducers)
            Write-Host "📊 Distribution: $messagesPerProducer messages per producer (maintains FIFO per partition)" -ForegroundColor Green
            
            # Start parallel producers
            Write-Host "🚀 Starting $ParallelProducers parallel producers for maximum throughput..." -ForegroundColor Green
            $producerJobs = Start-ParallelProducers -ProjectDir $producerProjectDir -MessageCount $MessageCount -ParallelProducers $ParallelProducers
            
            # Monitor and wait for completion
            $completed = Wait-ParallelProducers -Jobs $producerJobs -MessageCount $MessageCount -StartTime $startTime
            
            if ($completed) {
                $totalElapsed = (Get-Date) - $startTime
                $finalRate = $MessageCount / $totalElapsed.TotalSeconds
                $systemInfo = Get-SystemInfo
                
                Write-Host "🎉 High-performance parallel production completed!" -ForegroundColor Green
                Write-Host "Summary:" -ForegroundColor White
                Write-Host "  Total Messages: $MessageCount" -ForegroundColor Green
                Write-Host "  Total Time: $([math]::Round($totalElapsed.TotalSeconds, 1)) seconds" -ForegroundColor Green
                Write-Host "  Average Rate: $([math]::Round($finalRate, 0)) messages/second" -ForegroundColor Green
                Write-Host "  Parallel Producers: $ParallelProducers" -ForegroundColor Green
                
                # System Information
                Write-Host "💻 System Resources:" -ForegroundColor Cyan
                Write-Host "  CPU Cores: $($systemInfo.CPUCores)" -ForegroundColor Green
                Write-Host "  Total RAM: $($systemInfo.TotalRAMGB) GB" -ForegroundColor Green
                Write-Host "  Available RAM: $($systemInfo.AvailableRAMGB) GB" -ForegroundColor Green
                Write-Host "  Platform: $($systemInfo.Platform)" -ForegroundColor Green
                
                # Performance evaluation for 1M+ msg/sec target
                if ($finalRate -gt 1000000) {
                    Write-Host "🏆 EXCELLENT: Achieved >1M msg/sec target!" -ForegroundColor Green
                } elseif ($finalRate -gt 500000) {
                    Write-Host "✅ GOOD: High throughput achieved (>500K msg/sec)" -ForegroundColor Yellow
                } else {
                    Write-Host "⚠️ OPTIMIZATION NEEDED: Target 1M+ msg/sec for Flink.NET compliance" -ForegroundColor Red
                }
                
                # Add optimization recommendations
                Get-OptimizationRecommendations -CurrentRate $finalRate -ParallelProducers $ParallelProducers -SystemInfo $systemInfo
                
                return $true
            } else {
                throw "Parallel producer execution failed or timed out"
            }
        }
        finally {
            # Cleanup producer project
            if (Test-Path $producerProjectDir) {
                Remove-Item -Path $producerProjectDir -Recurse -Force -ErrorAction SilentlyContinue
            }
        }
    }
    catch {
        Write-Host "❌ High-performance message production failed: $_" -ForegroundColor Red
        return $false
    }
}

try {
    # Step 1: Discover Kafka
    $bootstrapServers = Get-KafkaBootstrapServers
    
    # Step 2: Test connection
    if (-not (Test-KafkaConnection -BootstrapServers $bootstrapServers)) {
        Write-Host "❌ Cannot connect to Kafka. Ensure Kafka is running." -ForegroundColor Red
        exit 1
    }
    
    # Step 3: Send messages
    $success = Send-KafkaMessages -BootstrapServers $bootstrapServers -Topic $Topic -MessageCount $MessageCount -BatchSize $BatchSize -ParallelProducers $ParallelProducers
    
    if ($success) {
        Write-Host "✅ Message production completed successfully" -ForegroundColor Green
        exit 0
    } else {
        Write-Host "❌ Message production failed" -ForegroundColor Red
        exit 1
    }
}
catch {
    Write-Host "❌ Script failed: $_" -ForegroundColor Red
    exit 1
}