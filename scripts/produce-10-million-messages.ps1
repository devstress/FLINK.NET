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
    
    # Method 2: Try to find and use Kafka container for topic listing
    try {
        $kafkaContainer = docker ps -q --filter "name=kafka" | Select-Object -First 1
        if ($kafkaContainer) {
            Write-Host "Found Kafka container: $kafkaContainer" -ForegroundColor Gray
            Write-Host "Testing Kafka API via container..." -ForegroundColor Gray
            
            # Use a timeout and internal network address to avoid advertised listener issues
            # Set KAFKA_OPTS to override client configuration for container-internal commands
            $kafkaOpts = "-Dbootstrap.servers=kafka:9092 -Dclient.dns.lookup=use_all_dns_ips -Dconnections.max.idle.ms=30000 -Drequest.timeout.ms=30000"
            $result = docker exec -e KAFKA_OPTS="$kafkaOpts" $kafkaContainer kafka-topics --bootstrap-server kafka:9092 --command-config /dev/null --list 2>&1
            if ($LASTEXITCODE -eq 0) {
                Write-Host "✅ Kafka API connection successful via container" -ForegroundColor Green
                Write-Host "Available topics:" -ForegroundColor Gray
                $result | ForEach-Object { Write-Host "  $_" -ForegroundColor Gray }
                return $true
            } else {
                Write-Host "❌ Kafka API test via container failed: $result" -ForegroundColor Red
                # Continue to producer connectivity test instead of failing
            }
        } else {
            Write-Host "❌ No Kafka container found for API testing" -ForegroundColor Red
        }
    }
    catch {
        Write-Host "❌ Container-based Kafka test failed: $_" -ForegroundColor Red
    }
    
    # Method 3: Try using kafka-console-producer as a connectivity test
    try {
        $kafkaContainer = docker ps -q --filter "name=kafka" | Select-Object -First 1
        if ($kafkaContainer) {
            Write-Host "Testing Kafka producer connectivity..." -ForegroundColor Gray
            
            # Send a simple test message to verify the producer works
            # Use producer configuration to override any advertised listener issues
            $testMessage = "test-connectivity-$(Get-Date -Format 'yyyyMMddHHmmss')"
            $kafkaOpts = "-Dbootstrap.servers=kafka:9092 -Dconnections.max.idle.ms=30000 -Drequest.timeout.ms=30000 -Dretries=3"
            $testResult = echo $testMessage | docker exec -i -e KAFKA_OPTS="$kafkaOpts" $kafkaContainer kafka-console-producer --bootstrap-server kafka:9092 --topic __connectivity_test --producer-property request.timeout.ms=30000 --producer-property retries=3 2>&1
            
            if ($LASTEXITCODE -eq 0) {
                Write-Host "✅ Kafka producer connectivity test successful" -ForegroundColor Green
                return $true
            } else {
                Write-Host "❌ Kafka producer connectivity test failed: $testResult" -ForegroundColor Red
            }
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
        # Find Kafka container
        $kafkaContainer = docker ps -q --filter "name=kafka" | Select-Object -First 1
        if (-not $kafkaContainer) {
            throw "No Kafka container found. Available containers: $(docker ps --format '{{.Names}}' | Join-String -Separator ', ')"
        }
        
        Write-Host "Using Kafka container: $kafkaContainer" -ForegroundColor Gray
        
        # Verify topic exists (topic should be created by Aspire infrastructure)
        Write-Host "Verifying topic '$Topic' exists..." -ForegroundColor Gray
        $topicExists = docker exec $kafkaContainer kafka-topics --bootstrap-server kafka:9092 --list | Where-Object { $_ -eq $Topic }
        if (-not $topicExists) {
            Write-Host "❌ Topic '$Topic' does not exist. Topic should be created by Aspire infrastructure." -ForegroundColor Red
            throw "Topic '$Topic' not found. Ensure Aspire infrastructure is properly initialized."
        } else {
            Write-Host "✅ Topic '$Topic' exists (created by Aspire infrastructure)" -ForegroundColor Green
        }
        
        # Create a pipeline to send messages in optimal batches
        $optimalBatchSize = [Math]::Min($BatchSize, 10000) # Don't overwhelm the container
        Write-Host "Using optimized batch size: $optimalBatchSize" -ForegroundColor Gray
        
        for ($batch = 0; $batch -lt $MessageCount; $batch += $optimalBatchSize) {
            $currentBatchSize = [math]::Min($optimalBatchSize, $MessageCount - $batch)
            
            # Generate batch of messages
            $messages = @()
            for ($i = 0; $i -lt $currentBatchSize; $i++) {
                $msgId = $batch + $i + 1
                $timestamp = Get-Date -Format "yyyy-MM-ddTHH:mm:ss.fffZ"
                $message = @{
                    id = $msgId
                    redis_ordered_id = $msgId
                    timestamp = $timestamp
                    job_id = "flink-job-1"
                    task_id = "task-$msgId"
                    kafka_partition = $msgId % 20  # Distribute across 20 partitions
                    kafka_offset = $msgId
                    processing_stage = "source->map->sink"
                    payload = "sample-data-$msgId"
                } | ConvertTo-Json -Compress
                
                $messages += $message
            }
            
            # Send batch to Kafka with optimized settings
            $messagesText = $messages -join "`n"
            
            try {
                # Use optimized producer settings with robust configuration
                $kafkaOpts = "-Dbootstrap.servers=kafka:9092 -Dconnections.max.idle.ms=60000 -Drequest.timeout.ms=60000 -Dretries=5"
                $producerCmd = "kafka-console-producer --bootstrap-server kafka:9092 --topic $Topic --batch-size 1000 --linger-ms 10 --compression-type snappy --request-timeout-ms 30000 --producer-property retries=5 --producer-property max.in.flight.requests.per.connection=1"
                $messagesText | docker exec -i -e KAFKA_OPTS="$kafkaOpts" $kafkaContainer $producerCmd
                
                if ($LASTEXITCODE -ne 0) {
                    throw "Producer command failed with exit code $LASTEXITCODE for batch starting at message $($batch + 1)"
                }
                
                $sentCount += $currentBatchSize
                
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
                        $messagesText | docker exec -i -e KAFKA_OPTS="$kafkaOpts" $kafkaContainer $producerCmd
                        
                        if ($LASTEXITCODE -eq 0) {
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
        
        # Verify some messages were actually sent by checking topic
        try {
            Write-Host "🔍 Verifying messages were sent to topic..." -ForegroundColor Gray
            $kafkaOpts = "-Dbootstrap.servers=kafka:9092 -Dconnections.max.idle.ms=30000 -Drequest.timeout.ms=30000"
            $topicInfo = docker exec -e KAFKA_OPTS="$kafkaOpts" $kafkaContainer kafka-run-class kafka.tools.GetOffsetShell --broker-list kafka:9092 --topic $Topic 2>&1
            if ($LASTEXITCODE -eq 0 -and $topicInfo) {
                Write-Host "✅ Topic verification successful. Offset information:" -ForegroundColor Green
                $topicInfo | ForEach-Object { Write-Host "  $_" -ForegroundColor Gray }
            } else {
                Write-Host "⚠️ Could not verify topic offsets, but messages were sent" -ForegroundColor Yellow
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