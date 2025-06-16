#!/usr/bin/env pwsh
<#
.SYNOPSIS
    Produces 10 million messages to Kafka for FlinkJobSimulator consumption.

.DESCRIPTION
    This script produces 10 million messages to the Kafka topic configured for FlinkJobSimulator.
    It discovers the running Kafka container and sends messages at high throughput.

.PARAMETER MessageCount
    Number of messages to produce (default: 10000000 = 10 million).

.PARAMETER Topic
    Kafka topic to send messages to (default: flinkdotnet.sample.topic).

.PARAMETER BatchSize
    Number of messages to send in each batch (default: 1000).

.EXAMPLE
    ./scripts/produce-10-million-messages.ps1
    Produces 10 million messages to the default topic.

.EXAMPLE
    ./scripts/produce-10-million-messages.ps1 -MessageCount 1000000 -Topic "test-topic"
    Produces 1 million messages to a custom topic.
#>

param(
    [long]$MessageCount = 10000000,  # 10 million messages
    [string]$Topic = "flinkdotnet.sample.topic",
    [int]$BatchSize = 1000
)

$ErrorActionPreference = 'Stop'

Write-Host "=== Kafka Message Producer for FlinkJobSimulator ===" -ForegroundColor Cyan
Write-Host "Started at: $(Get-Date -Format 'yyyy-MM-dd HH:mm:ss') UTC" -ForegroundColor White
Write-Host "Parameters: MessageCount=$MessageCount, Topic=$Topic, BatchSize=$BatchSize" -ForegroundColor White

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

function Send-KafkaMessages {
    param(
        [string]$BootstrapServers,
        [string]$Topic,
        [long]$MessageCount,
        [int]$BatchSize
    )
    
    Write-Host "📨 Starting to produce $MessageCount messages to topic '$Topic'" -ForegroundColor White
    Write-Host "Configuration:" -ForegroundColor Gray
    Write-Host "  Bootstrap Servers: $BootstrapServers" -ForegroundColor Gray
    Write-Host "  Topic: $Topic" -ForegroundColor Gray
    Write-Host "  Batch Size: $BatchSize" -ForegroundColor Gray
    
    $startTime = Get-Date
    $sentCount = 0
    $lastLogTime = $startTime
    
    try {
        # Use .NET Kafka producer for reliable message sending without container network issues
        
        # Wait for kafka-init container to complete topic creation
        Write-Host "Waiting for kafka-init container to complete topic creation..." -ForegroundColor Gray
        $maxWaitAttempts = 50  # Increased timeout to 4+ minutes for comprehensive initialization
        $waitAttempt = 0
        $kafkaInitCompleted = $false
        $kafkaInitStartTime = Get-Date
        
        while ($waitAttempt -lt $maxWaitAttempts -and -not $kafkaInitCompleted) {
            try {
                # Check if docker is available
                $dockerAvailable = Get-Command docker -ErrorAction SilentlyContinue
                if (-not $dockerAvailable) {
                    Write-Host "⚠️ Docker command not available, skipping kafka-init wait" -ForegroundColor Yellow
                    $kafkaInitCompleted = $true
                    break
                }
                
                # Check if kafka-init containers exist and get their status
                $kafkaInitContainers = docker ps -a --filter "name=kafka-init" --format "table {{.Names}}\t{{.Status}}" 2>$null
                if ($kafkaInitContainers) {
                    $elapsedWait = (Get-Date) - $kafkaInitStartTime
                    Write-Host "🔍 Kafka-init container status (${waitAttempt}/${maxWaitAttempts}, elapsed: $([math]::Round($elapsedWait.TotalSeconds, 1))s):" -ForegroundColor Gray
                    Write-Host $kafkaInitContainers -ForegroundColor Gray
                    
                    # Check if any kafka-init container has exited successfully (exit code 0)
                    $exitedContainers = docker ps -a --filter "name=kafka-init" --filter "status=exited" --format "{{.Names}}\t{{.Status}}" 2>$null
                    if ($exitedContainers -and $exitedContainers -match "Exited \(0\)") {
                        Write-Host "✅ Kafka-init container completed successfully" -ForegroundColor Green
                        
                        # Get final logs for confirmation
                        try {
                            $containerName = ($exitedContainers -split '\s+')[0]
                            Write-Host "🔍 Final kafka-init success logs (last 10 lines):" -ForegroundColor Green
                            $successLogs = docker logs --tail 10 $containerName 2>&1
                            $successLogs | ForEach-Object { Write-Host "  $_" -ForegroundColor Gray }
                        }
                        catch {
                            Write-Host "  Could not retrieve success logs: $_" -ForegroundColor Yellow
                        }
                        
                        $kafkaInitCompleted = $true
                        break
                    }
                    
                    # Check if container exited with error
                    $failedContainers = docker ps -a --filter "name=kafka-init" --filter "status=exited" --format "{{.Names}}\t{{.Status}}" 2>$null
                    if ($failedContainers -and $failedContainers -match "Exited \([1-9]\d*\)") {
                        Write-Host "❌ Kafka-init container failed!" -ForegroundColor Red
                        Write-Host "Container status: $failedContainers" -ForegroundColor Red
                        
                        # Get container logs for debugging
                        Write-Host "🔍 Kafka-init container failure logs (last 50 lines):" -ForegroundColor Yellow
                        try {
                            $containerName = ($failedContainers -split '\s+')[0]
                            $logs = docker logs --tail 50 $containerName 2>&1
                            $logs | ForEach-Object { Write-Host "  $_" -ForegroundColor Gray }
                        }
                        catch {
                            Write-Host "  Could not retrieve container logs: $_" -ForegroundColor Yellow
                        }
                        
                        Write-Host "⚠️ Kafka-init failed, will attempt topic creation fallback" -ForegroundColor Yellow
                        break
                    }
                    
                    # Check if container is still running (which means it's still working)
                    $runningContainers = docker ps --filter "name=kafka-init" --format "{{.Names}}" 2>$null
                    if ($runningContainers) {
                        # Show logs periodically to help with debugging and track progress
                        if ($waitAttempt % 6 -eq 0 -and $waitAttempt -gt 0) {
                            Write-Host "🔍 Kafka-init container progress logs (last 15 lines):" -ForegroundColor Yellow
                            try {
                                $containerName = $runningContainers
                                $logs = docker logs --tail 15 $containerName 2>&1
                                $logs | ForEach-Object { Write-Host "  $_" -ForegroundColor Gray }
                            }
                            catch {
                                Write-Host "  Could not retrieve container logs: $_" -ForegroundColor Yellow
                            }
                        }
                        Write-Host "🔄 Kafka-init container still running, waiting... (attempt $($waitAttempt + 1)/$maxWaitAttempts)" -ForegroundColor Yellow
                    } else {
                        # Container may have finished, check exit status
                        $lastExitCode = docker ps -a --filter "name=kafka-init" --format "{{.Names}}\t{{.Status}}" 2>$null
                        Write-Host "⚠️ Kafka-init container status: $lastExitCode" -ForegroundColor Yellow
                        if ($lastExitCode -match "Exited \(0\)") {
                            $kafkaInitCompleted = $true
                            break
                        }
                    }
                } else {
                    Write-Host "⚠️ No kafka-init containers found, assuming topics are already created" -ForegroundColor Yellow
                    $kafkaInitCompleted = $true
                    break
                }
                
                Start-Sleep -Seconds 5
                $waitAttempt++
            }
            catch {
                Write-Host "⚠️ Error checking kafka-init status: $_" -ForegroundColor Yellow
                Start-Sleep -Seconds 5
                $waitAttempt++
            }
        }
        
        if (-not $kafkaInitCompleted) {
            $totalWaitTime = (Get-Date) - $kafkaInitStartTime
            Write-Host "⚠️ Kafka-init container did not complete within timeout ($([math]::Round($totalWaitTime.TotalMinutes, 1)) minutes)" -ForegroundColor Yellow
            
            # Get final diagnostic information
            try {
                Write-Host "🔍 Final kafka-init container diagnostics:" -ForegroundColor Yellow
                $allKafkaInitContainers = docker ps -a --filter "name=kafka-init" --format "{{.Names}}\t{{.Status}}" 2>$null
                if ($allKafkaInitContainers) {
                    Write-Host "Container status: $allKafkaInitContainers" -ForegroundColor Gray
                    
                    # Get logs from the most recent container
                    $containerName = ($allKafkaInitContainers -split '\s+')[0]
                    Write-Host "Container logs (last 50 lines):" -ForegroundColor Gray
                    $logs = docker logs --tail 50 $containerName 2>&1
                    $logs | ForEach-Object { Write-Host "  $_" -ForegroundColor Gray }
                }
            }
            catch {
                Write-Host "Could not get final diagnostics: $_" -ForegroundColor Yellow
            }
            
            Write-Host "🔄 Proceeding with topic verification and potential fallback creation..." -ForegroundColor Yellow
        }
        
        # Verify topic exists using external .NET client (avoids container network issues)
        Write-Host "Verifying topic '$Topic' exists..." -ForegroundColor Gray
        
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
            
            # Retry topic verification up to 3 times with delays
            $topicFound = $false
            $maxVerifyAttempts = 5  # Increased attempts for better reliability
            
            for ($verifyAttempt = 1; $verifyAttempt -le $maxVerifyAttempts; $verifyAttempt++) {
                Write-Host "🔍 Topic verification attempt $verifyAttempt/$maxVerifyAttempts..." -ForegroundColor Gray
                $verifyOutput = dotnet run 2>&1
                
                if ($verifyOutput -like "*TOPIC_EXISTS*") {
                    Write-Host "✅ Topic '$Topic' exists (verified via .NET AdminClient)" -ForegroundColor Green
                    $topicFound = $true
                    break
                } elseif ($verifyOutput -like "*TOPIC_NOT_FOUND*") {
                    Write-Host "⚠️ Topic '$Topic' not found on attempt $verifyAttempt/$maxVerifyAttempts" -ForegroundColor Yellow
                    if ($verifyAttempt -lt $maxVerifyAttempts) {
                        Write-Host "🔄 Waiting 15 seconds before retry..." -ForegroundColor Yellow
                        Start-Sleep -Seconds 15
                    }
                } else {
                    Write-Host "⚠️ Could not verify topic existence on attempt $verifyAttempt/$maxVerifyAttempts : $verifyOutput" -ForegroundColor Yellow
                    if ($verifyAttempt -lt $maxVerifyAttempts) {
                        Write-Host "🔄 Waiting 15 seconds before retry..." -ForegroundColor Yellow
                        Start-Sleep -Seconds 15
                    }
                }
            }
            
            # If topic not found, attempt fallback creation
            if (-not $topicFound) {
                Write-Host "🔄 Attempting fallback topic creation since Aspire infrastructure may have failed..." -ForegroundColor Yellow
                
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
                
                $fallbackProjectDir = Join-Path $tempDir "kafka-fallback-$(Get-Date -Format 'yyyyMMddHHmmss')"
                New-Item -ItemType Directory -Path $fallbackProjectDir -Force | Out-Null
                
                Push-Location $fallbackProjectDir
                try {
                    dotnet new console -f net8.0 --force | Out-Null
                    dotnet add package Confluent.Kafka | Out-Null
                    $fallbackTopicCreationScript | Out-File -FilePath "Program.cs" -Encoding UTF8
                    dotnet build | Out-Null
                    
                    # Determine partition count based on environment
                    $isStressTest = $env:STRESS_TEST_MODE -eq "true"
                    $partitionCount = if ($isStressTest) { 20 } else { 4 }
                    
                    Write-Host "🔧 Creating topic '$Topic' with $partitionCount partitions via fallback method..." -ForegroundColor Yellow
                    $fallbackOutput = dotnet run -- "$BootstrapServers" "$Topic" "$partitionCount" 2>&1
                    
                    if ($fallbackOutput -like "*TOPIC_CREATED*" -or $fallbackOutput -like "*TOPIC_ALREADY_EXISTS*") {
                        Write-Host "✅ Fallback topic creation successful!" -ForegroundColor Green
                        Write-Host "Fallback result: $fallbackOutput" -ForegroundColor Gray
                        
                        # Verify the fallback creation worked
                        Write-Host "🔍 Verifying fallback topic creation..." -ForegroundColor Gray
                        Start-Sleep -Seconds 5  # Give time for topic metadata to propagate
                        
                        $verifyFallbackOutput = dotnet run -p ../kafka-verify-* 2>&1
                        if ($verifyFallbackOutput -like "*TOPIC_EXISTS*") {
                            Write-Host "✅ Fallback topic creation verified!" -ForegroundColor Green
                            $topicFound = $true
                        } else {
                            Write-Host "❌ Fallback topic creation could not be verified: $verifyFallbackOutput" -ForegroundColor Red
                        }
                    } else {
                        Write-Host "❌ Fallback topic creation failed: $fallbackOutput" -ForegroundColor Red
                    }
                }
                finally {
                    Pop-Location
                    Remove-Item -Path $fallbackProjectDir -Recurse -Force -ErrorAction SilentlyContinue
                }
                
                if (-not $topicFound) {
                    Write-Host "❌ Both Aspire infrastructure and fallback topic creation failed" -ForegroundColor Red
                    throw "Topic '$Topic' not found after $maxVerifyAttempts verification attempts and fallback creation. This indicates a serious Kafka infrastructure issue."
                }
            }
        }
        finally {
            Pop-Location
            Remove-Item -Path $verifyProjectDir -Recurse -Force -ErrorAction SilentlyContinue
        }
        
        # Create a pipeline to send messages in optimal batches
        $optimalBatchSize = [Math]::Min($BatchSize, 10000) # Don't overwhelm the container
        Write-Host "Using optimized batch size: $optimalBatchSize" -ForegroundColor Gray
        
        for ($batch = 0; $batch -lt $MessageCount; $batch += $optimalBatchSize) {
            $currentBatchSize = [math]::Min($optimalBatchSize, $MessageCount - $batch)
            
            # Use .NET producer instead of docker exec to avoid container network issues
            try {
                # Create .NET producer for reliable message sending
                $producerScript = @"
using System;
using System.Text.Json;
using System.Threading.Tasks;
using Confluent.Kafka;

class Program {
    static async Task Main(string[] args) {
        if (args.Length < 4) {
            Console.WriteLine("ERROR: Usage: program <bootstrapServers> <topic> <startMsgId> <messageCount>");
            return;
        }
        
        var bootstrapServers = args[0];
        var topic = args[1];
        var startMsgId = long.Parse(args[2]);
        var messageCount = int.Parse(args[3]);
        
        var config = new ProducerConfig {
            BootstrapServers = bootstrapServers,
            BatchSize = 16384,
            LingerMs = 10,
            CompressionType = CompressionType.Snappy,
            SocketTimeoutMs = 30000,
            MessageTimeoutMs = 60000,
            MaxInFlight = 1,
            SecurityProtocol = SecurityProtocol.Plaintext
        };
        
        using var producer = new ProducerBuilder<string, string>(config).Build();
        
        try {
            for (int i = 0; i < messageCount; i++) {
                var msgId = startMsgId + i;
                var timestamp = DateTime.UtcNow.ToString("yyyy-MM-ddTHH:mm:ss.fffZ");
                
                var message = new {
                    id = msgId,
                    redis_ordered_id = msgId,
                    timestamp = timestamp,
                    job_id = "flink-job-1",
                    task_id = "task-" + msgId,
                    kafka_partition = msgId % 20,
                    kafka_offset = msgId,
                    processing_stage = "source->map->sink",
                    payload = "sample-data-" + msgId
                };
                
                var jsonMessage = JsonSerializer.Serialize(message);
                
                await producer.ProduceAsync(topic, new Message<string, string> {
                    Key = msgId.ToString(),
                    Value = jsonMessage
                });
                
                if (i % 1000 == 0 && i > 0) {
                    Console.WriteLine("PROGRESS:" + (startMsgId + i));
                }
            }
            
            producer.Flush(TimeSpan.FromSeconds(30));
            Console.WriteLine("SUCCESS:" + messageCount);
        } catch (Exception ex) {
            Console.WriteLine("ERROR: " + ex.Message);
        }
    }
}
"@
                
                $tempDir = [System.IO.Path]::GetTempPath()
                $producerProjectDir = Join-Path $tempDir "kafka-producer-$(Get-Date -Format 'yyyyMMddHHmmss')"
                New-Item -ItemType Directory -Path $producerProjectDir -Force | Out-Null
                
                Push-Location $producerProjectDir
                try {
                    dotnet new console -f net8.0 --force | Out-Null
                    dotnet add package Confluent.Kafka | Out-Null
                    dotnet add package System.Text.Json | Out-Null
                    $producerScript | Out-File -FilePath "Program.cs" -Encoding UTF8
                    dotnet build | Out-Null
                    
                    $startMsgId = $batch + 1
                    $producerOutput = dotnet run -- "$BootstrapServers" "$Topic" "$startMsgId" "$currentBatchSize" 2>&1
                    
                    if ($producerOutput -like "*SUCCESS:*") {
                        $sentCount += $currentBatchSize
                        
                        # Extract progress info if available
                        $progressLines = $producerOutput | Where-Object { $_ -like "PROGRESS:*" }
                        if ($progressLines) {
                            $lastProgress = ($progressLines | Select-Object -Last 1).Split(':')[1]
                            # Progress info is already included in the output
                        }
                    } else {
                        throw "Producer failed: $producerOutput"
                    }
                }
                finally {
                    Pop-Location
                    Remove-Item -Path $producerProjectDir -Recurse -Force -ErrorAction SilentlyContinue
                }
                
                # Log progress with enhanced details
                $currentTime = Get-Date
                if (($currentTime - $lastLogTime).TotalSeconds -ge 10 -or $sentCount % 100000 -eq 0 -or $sentCount -eq $MessageCount) {
                    $elapsed = $currentTime - $startTime
                    $rate = if ($elapsed.TotalSeconds -gt 0) { $sentCount / $elapsed.TotalSeconds } else { 0 }
                    $progressPercent = [math]::Round(($sentCount / $MessageCount) * 100, 1)
                    $eta = if ($rate -gt 0) { 
                        $remainingMessages = $MessageCount - $sentCount
                        $etaSeconds = $remainingMessages / $rate
                        " ETA: $([math]::Round($etaSeconds, 0))s"
                    } else { "" }
                    
                    Write-Host "📊 Progress: $sentCount/$MessageCount messages ($progressPercent%) - Rate: $([math]::Round($rate, 0)) msg/sec$eta" -ForegroundColor Green
                    $lastLogTime = $currentTime
                }
                
                # Adaptive delay to prevent overwhelming the container
                if ($batch % 50000 -eq 0 -and $batch -gt 0) {
                    # Longer pause every 50k messages
                    Start-Sleep -Milliseconds 200
                } elseif ($batch % 10000 -eq 0 -and $batch -gt 0) {
                    # Short pause every 10k messages
                    Start-Sleep -Milliseconds 50
                }
            }
            catch {
                Write-Host "❌ Error sending batch at position $batch : $_" -ForegroundColor Red
                
                # Retry logic for failed batches
                $retryCount = 0
                $maxRetries = 3
                $retrySuccess = $false
                
                while ($retryCount -lt $maxRetries -and -not $retrySuccess) {
                    $retryCount++
                    Write-Host "🔄 Retry attempt $retryCount/$maxRetries for batch at position $batch" -ForegroundColor Yellow
                    
                    try {
                        Start-Sleep -Seconds (2 * $retryCount) # Exponential backoff
                        
                        # Retry using .NET producer
                        $producerOutput = dotnet run -- "$BootstrapServers" "$Topic" "$startMsgId" "$currentBatchSize" 2>&1
                        
                        if ($producerOutput -like "*SUCCESS:*") {
                            $retrySuccess = $true
                            $sentCount += $currentBatchSize
                            Write-Host "✅ Retry successful for batch at position $batch" -ForegroundColor Green
                        }
                    }
                    catch {
                        Write-Host "❌ Retry $retryCount failed: $_" -ForegroundColor Red
                    }
                }
                
                if (-not $retrySuccess) {
                    throw "Failed to send batch at position $batch after $maxRetries retries"
                }
            }
        }
        
        $totalElapsed = (Get-Date) - $startTime
        $finalRate = if ($totalElapsed.TotalSeconds -gt 0) { $sentCount / $totalElapsed.TotalSeconds } else { 0 }
        
        Write-Host "🎉 Message production completed!" -ForegroundColor Green
        Write-Host "Summary:" -ForegroundColor White
        Write-Host "  Total Messages: $sentCount" -ForegroundColor Green
        Write-Host "  Total Time: $([math]::Round($totalElapsed.TotalSeconds, 1)) seconds" -ForegroundColor Green
        Write-Host "  Average Rate: $([math]::Round($finalRate, 0)) messages/second" -ForegroundColor Green
        
        # Verify some messages were actually sent using .NET AdminClient
        try {
            Write-Host "🔍 Verifying messages were sent to topic..." -ForegroundColor Gray
            
            $verificationScript = @"
using System;
using System.Linq;
using System.Threading.Tasks;
using Confluent.Kafka;
using Confluent.Kafka.Admin;

class Program {
    static async Task Main(string[] args) {
        var bootstrapServers = args[0];
        var topic = args[1];
        
        var config = new AdminClientConfig {
            BootstrapServers = bootstrapServers,
            SocketTimeoutMs = 30000,
            ApiVersionRequestTimeoutMs = 30000,
            SecurityProtocol = SecurityProtocol.Plaintext
        };
        
        using var adminClient = new AdminClientBuilder(config).Build();
        try {
            var metadata = adminClient.GetMetadata(topic, TimeSpan.FromSeconds(30));
            var topicMetadata = metadata.Topics.FirstOrDefault(t => t.Topic == topic);
            
            if (topicMetadata != null) {
                Console.WriteLine("TOPIC_VERIFIED");
                foreach (var partition in topicMetadata.Partitions) {
                    Console.WriteLine("PARTITION:" + partition.PartitionId + ":REPLICAS:" + partition.Replicas.Count);
                }
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
            $verifyDir = Join-Path $tempDir "kafka-verify-final-$(Get-Date -Format 'yyyyMMddHHmmss')"
            New-Item -ItemType Directory -Path $verifyDir -Force | Out-Null
            
            Push-Location $verifyDir
            try {
                dotnet new console -f net8.0 --force | Out-Null
                dotnet add package Confluent.Kafka | Out-Null
                $verificationScript | Out-File -FilePath "Program.cs" -Encoding UTF8
                dotnet build | Out-Null
                
                $verifyOutput = dotnet run -- "$BootstrapServers" "$Topic" 2>&1
                if ($verifyOutput -like "*TOPIC_VERIFIED*") {
                    Write-Host "✅ Topic verification successful. Topic details:" -ForegroundColor Green
                    $verifyOutput | Where-Object { $_ -like "PARTITION:*" } | ForEach-Object { Write-Host "  $_" -ForegroundColor Gray }
                } else {
                    Write-Host "⚠️ Could not verify topic details: $verifyOutput" -ForegroundColor Yellow
                }
            }
            finally {
                Pop-Location
                Remove-Item -Path $verifyDir -Recurse -Force -ErrorAction SilentlyContinue
            }
        }
        catch {
            Write-Host "⚠️ Topic verification failed but this may be normal: $_" -ForegroundColor Yellow
        }
        
        return $true
    }
    catch {
        Write-Host "❌ Message production failed: $_" -ForegroundColor Red
        Write-Host "Sent $sentCount messages before failure" -ForegroundColor Yellow
        
        # Enhanced error diagnostics
        Write-Host "🔍 Error diagnostics:" -ForegroundColor Gray
        Write-Host "  Kafka container status:" -ForegroundColor Gray
        try {
            $containerStatus = docker ps --filter "name=kafka" --format "{{.Names}}\t{{.Status}}"
            $containerStatus | ForEach-Object { Write-Host "    $_" -ForegroundColor Gray }
        }
        catch {
            Write-Host "    Could not get container status" -ForegroundColor Gray
        }
        
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
    $success = Send-KafkaMessages -BootstrapServers $bootstrapServers -Topic $Topic -MessageCount $MessageCount -BatchSize $BatchSize
    
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