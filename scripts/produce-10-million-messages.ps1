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
    
    # Check environment variables first
    $bootstrapServers = $env:DOTNET_KAFKA_BOOTSTRAP_SERVERS
    if ($bootstrapServers) {
        Write-Host "Found Kafka servers from DOTNET_KAFKA_BOOTSTRAP_SERVERS: $bootstrapServers" -ForegroundColor Green
        return $bootstrapServers
    }
    
    $bootstrapServers = $env:ConnectionStrings__kafka
    if ($bootstrapServers) {
        Write-Host "Found Kafka servers from ConnectionStrings__kafka: $bootstrapServers" -ForegroundColor Green
        return $bootstrapServers
    }
    
    # Discover from Docker containers
    try {
        $kafkaContainers = docker ps --filter "name=kafka" --format "table {{.ID}}\t{{.Names}}\t{{.Ports}}"
        if ($kafkaContainers -and $kafkaContainers.Count -gt 1) {
            Write-Host "Found Kafka containers:" -ForegroundColor Green
            $kafkaContainers | ForEach-Object { Write-Host "  $_" -ForegroundColor Gray }
            
            # Extract port from the first kafka container
            $kafkaContainer = $kafkaContainers[1] # Skip header
            $portInfo = $kafkaContainer.Split("`t")[2]
            
            if ($portInfo -match "127\.0\.0\.1:(\d+)->9092") {
                $port = $matches[1]
                $bootstrapServers = "127.0.0.1:$port"
                Write-Host "Discovered Kafka at: $bootstrapServers" -ForegroundColor Green
                return $bootstrapServers
            }
        }
    }
    catch {
        Write-Host "Docker discovery failed: $_" -ForegroundColor Yellow
    }
    
    # Fallback to default
    $bootstrapServers = "127.0.0.1:9092"
    Write-Host "Using default Kafka servers: $bootstrapServers" -ForegroundColor Yellow
    return $bootstrapServers
}

function Test-KafkaConnection {
    param([string]$BootstrapServers)
    
    Write-Host "🔄 Testing Kafka connection to $BootstrapServers..." -ForegroundColor White
    
    try {
        # Use kafka-topics to test connection
        $kafkaContainer = docker ps --filter "name=kafka" --format "{{.ID}}" | Select-Object -First 1
        if ($kafkaContainer) {
            $result = docker exec $kafkaContainer kafka-topics --bootstrap-server localhost:9092 --list 2>&1
            if ($LASTEXITCODE -eq 0) {
                Write-Host "✅ Kafka connection successful" -ForegroundColor Green
                Write-Host "Available topics:" -ForegroundColor Gray
                $result | ForEach-Object { Write-Host "  $_" -ForegroundColor Gray }
                return $true
            }
        }
    }
    catch {
        Write-Host "❌ Kafka connection test failed: $_" -ForegroundColor Red
    }
    
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
        # Use kafka-console-producer through Docker
        $kafkaContainer = docker ps --filter "name=kafka" --format "{{.ID}}" | Select-Object -First 1
        if (-not $kafkaContainer) {
            throw "No Kafka container found"
        }
        
        Write-Host "Using Kafka container: $kafkaContainer" -ForegroundColor Gray
        
        # Create a pipeline to send messages
        for ($batch = 0; $batch -lt $MessageCount; $batch += $BatchSize) {
            $currentBatchSize = [math]::Min($BatchSize, $MessageCount - $batch)
            
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
            
            # Send batch to Kafka
            $messagesText = $messages -join "`n"
            $messagesText | docker exec -i $kafkaContainer kafka-console-producer --bootstrap-server localhost:9092 --topic $Topic --batch-size 1000 --linger-ms 10
            
            if ($LASTEXITCODE -ne 0) {
                throw "Failed to send batch starting at message $($batch + 1)"
            }
            
            $sentCount += $currentBatchSize
            
            # Log progress
            $currentTime = Get-Date
            if (($currentTime - $lastLogTime).TotalSeconds -ge 10 -or $sentCount % 100000 -eq 0 -or $sentCount -eq $MessageCount) {
                $elapsed = $currentTime - $startTime
                $rate = $sentCount / $elapsed.TotalSeconds
                $progressPercent = [math]::Round(($sentCount / $MessageCount) * 100, 1)
                
                Write-Host "📊 Progress: $sentCount/$MessageCount messages ($progressPercent%) - Rate: $([math]::Round($rate, 0)) msg/sec" -ForegroundColor Green
                $lastLogTime = $currentTime
            }
            
            # Small delay to prevent overwhelming
            if ($batch % 10000 -eq 0) {
                Start-Sleep -Milliseconds 100
            }
        }
        
        $totalElapsed = (Get-Date) - $startTime
        $finalRate = $sentCount / $totalElapsed.TotalSeconds
        
        Write-Host "🎉 Message production completed!" -ForegroundColor Green
        Write-Host "Summary:" -ForegroundColor White
        Write-Host "  Total Messages: $sentCount" -ForegroundColor Green
        Write-Host "  Total Time: $([math]::Round($totalElapsed.TotalSeconds, 1)) seconds" -ForegroundColor Green
        Write-Host "  Average Rate: $([math]::Round($finalRate, 0)) messages/second" -ForegroundColor Green
        
        return $true
    }
    catch {
        Write-Host "❌ Message production failed: $_" -ForegroundColor Red
        Write-Host "Sent $sentCount messages before failure" -ForegroundColor Yellow
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