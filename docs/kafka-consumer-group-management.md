# Apache Flink Consumer Group Management in Flink.NET

## Overview

This document describes how Flink.NET implements Apache Flink-style consumer group management for Kafka integration, providing the same level of reliability and consistency guarantees as the original Apache Flink framework.

## Background: How Apache Flink Manages Consumer Groups

Apache Flink provides sophisticated consumer group management that goes beyond standard Kafka consumer APIs:

### 1. **Checkpoint-based Offset Management**
- Apache Flink doesn't rely on Kafka's built-in offset management
- Instead, it uses its own checkpointing mechanism to track and commit offsets
- This provides exactly-once processing guarantees across the entire pipeline

### 2. **Coordinated Partition Assignment**
- Flink coordinates partition assignment across parallel operator instances
- Uses Flink's internal parallelism model rather than just Kafka's consumer group protocol
- Ensures optimal load balancing and fault tolerance

### 3. **Unified Source API**
- Flink's unified source API abstracts bounded and unbounded streams
- Provides consistent behavior for batch and streaming workloads
- Supports advanced features like watermarks and event time processing

## Flink.NET Implementation

### Core Components

#### 1. `FlinkKafkaConsumerGroup`

Our implementation provides Apache Flink-style consumer group management:

```csharp
public class FlinkKafkaConsumerGroup : IDisposable
{
    // Key features:
    // - Checkpoint-based offset management
    // - Proper partition assignment coordination
    // - Enhanced failure recovery and rebalancing
    // - Flink-optimal configuration settings
}
```

**Key Features:**

- **Disabled Auto-commit**: Flink manages offsets through checkpoints, not Kafka auto-commit
- **Optimal Timeouts**: Configured for Flink's fault tolerance patterns (30s session, 10s heartbeat)
- **Cooperative Sticky Assignment**: Uses `CooperativeSticky` strategy for better rebalancing
- **Checkpoint Integration**: Offsets are committed only during successful checkpoints

#### 2. `KafkaSourceFunction<T>` with Checkpointing

Enhanced to implement `ICheckpointedFunction`:

```csharp
public class KafkaSourceFunction<T> : IUnifiedSource<T>, ICheckpointedFunction
{
    // Implements Apache Flink's exactly-once processing pattern
    void SnapshotState(long checkpointId, long checkpointTimestamp);
    void RestoreState(object state);
    void NotifyCheckpointComplete(long checkpointId);
}
```

#### 3. `ICheckpointedFunction` Interface

Provides Apache Flink's checkpointing contract:

```csharp
public interface ICheckpointedFunction
{
    /// <summary>
    /// Capture state for recovery (called during checkpoint)
    /// </summary>
    void SnapshotState(long checkpointId, long checkpointTimestamp);
    
    /// <summary>
    /// Restore from previous checkpoint
    /// </summary>
    void RestoreState(object state);
    
    /// <summary>
    /// Cleanup after successful checkpoint
    /// </summary>
    void NotifyCheckpointComplete(long checkpointId);
}
```

## Consumer Group Management Strategy

### 1. **Initialization Phase**

```csharp
// Configure consumer with Flink-optimal settings
var consumerGroup = new FlinkKafkaConsumerGroup(consumerConfig, logger);
await consumerGroup.InitializeAsync(topics);

// Restore offsets from checkpoint if recovering
if (hasCheckpointState)
{
    await consumerGroup.RestoreOffsetsAsync(checkpointOffsets);
}
```

### 2. **Processing Phase**

```csharp
// Consume with automatic offset tracking
var result = consumerGroup.ConsumeMessage(timeout);

// Process message and emit to downstream
await ctx.CollectAsync(deserializedMessage);

// Offsets are tracked automatically but not committed yet
```

### 3. **Checkpointing Phase**

```csharp
// During checkpoint, snapshot current state
public void SnapshotState(long checkpointId, long checkpointTimestamp)
{
    // Capture current consumer positions
    var assignment = consumerGroup.GetAssignment();
    foreach (var partition in assignment)
    {
        checkpointState[partition] = getCurrentOffset(partition);
    }
}

// After successful checkpoint, commit offsets
public void NotifyCheckpointComplete(long checkpointId)
{
    await consumerGroup.CommitCheckpointOffsetsAsync(checkpointId);
}
```

## Comparison with Standard Kafka Consumers

| Feature | Standard Kafka Consumer | Flink.NET Consumer Group |
|---------|------------------------|--------------------------|
| **Offset Management** | Auto-commit or manual commit | Checkpoint-based commit |
| **Failure Recovery** | Resume from last committed offset | Resume from last successful checkpoint |
| **Exactly-Once** | Requires complex coordination | Built-in with checkpointing |
| **Rebalancing** | Kafka's partition assignment | Flink's coordinated assignment |
| **State Management** | External state store needed | Integrated with Flink state |
| **Watermarks** | Manual implementation | Built-in support |

## Configuration Best Practices

### 1. **Consumer Configuration**

```csharp
var config = new ConsumerConfig
{
    BootstrapServers = "localhost:9092",
    GroupId = "flink-consumer-group",
    EnableAutoCommit = false,        // Critical: Let Flink manage offsets
    SessionTimeoutMs = 30000,        // 30s for Flink fault tolerance
    HeartbeatIntervalMs = 10000,     // 10s heartbeat
    MaxPollIntervalMs = 300000,      // 5min max processing time
    PartitionAssignmentStrategy = "CooperativeSticky"
};
```

### 2. **Error Handling**

```csharp
// Configure robust error handling
.SetErrorHandler((_, e) => 
{
    logger.LogError("Kafka consumer error: {Error}", e.Reason);
    // Let Flink's fault tolerance handle recovery
})
.SetPartitionsLostHandler((_, partitions) =>
{
    logger.LogWarning("Partitions lost: {Partitions}", partitions);
    // Clear state for lost partitions
});
```

### 3. **Monitoring and Observability**

```csharp
// Track metrics following Apache Flink patterns
metrics.RecordMessagesRead(partition, count);
metrics.RecordCheckpointDuration(checkpointId, duration);
metrics.RecordRebalanceEvents(consumer.Assignment.Count);
```

## Advanced Features

### 1. **Watermark Generation**

```csharp
// Extract event time from Kafka message timestamp
var eventTime = consumeResult.Message.Timestamp.UnixTimestampMs;
await ctx.CollectWithTimestampAsync(value, eventTime);

// Generate watermarks for late data handling
if (shouldGenerateWatermark(eventTime))
{
    ctx.EmitWatermark(new Watermark(eventTime - allowedLateness));
}
```

### 2. **Partition Assignment Strategies**

- **Range**: Assigns consecutive partitions (default)
- **RoundRobin**: Distributes partitions evenly
- **Sticky**: Minimizes partition movement during rebalancing
- **CooperativeSticky**: Flink's preferred strategy for minimal disruption

### 3. **State Backends Integration**

```csharp
// State is automatically managed by Flink's state backends
public void SnapshotState(long checkpointId, long checkpointTimestamp)
{
    // State is persisted to configured backend (RocksDB, filesystem, etc.)
    var state = new Dictionary<TopicPartition, long>();
    foreach (var partition in assignment)
    {
        state[partition] = getCommittedOffset(partition);
    }
    // Flink handles serialization and persistence
}
```

## Stress and Reliability Testing Integration

Our consumer group implementation is fully integrated with the stress and reliability test infrastructure:

### 1. **Stress Test Configuration**

```bash
# Environment variables for stress tests
export DOTNET_KAFKA_BOOTSTRAP_SERVERS="localhost:9092"
export SIMULATOR_KAFKA_TOPIC="flinkdotnet.sample.topic"
export SIMULATOR_NUM_MESSAGES="1000000"
```

### 2. **Reliability Test Validation**

The reliability tests validate:
- Exactly-once processing guarantees
- Proper offset management during failures
- Checkpoint consistency across restarts
- Consumer group rebalancing behavior

### 3. **Observability Integration**

Full integration with our Apache Flink 2.0-style observability framework:
- Metrics: Consumer lag, throughput, error rates
- Tracing: End-to-end message flow tracking  
- Logging: Structured logs with correlation IDs
- Health checks: Consumer group status monitoring

## Migration from Standard Kafka Consumers

### Before (Standard Kafka Consumer)

```csharp
var consumer = new ConsumerBuilder<string, string>(config)
    .SetValueDeserializer(Deserializers.Utf8)
    .Build();

consumer.Subscribe(topics);

while (true)
{
    var result = consumer.Consume();
    // Process message
    // Manual offset management required
    consumer.Commit(result);
}
```

### After (Flink.NET Consumer Group)

```csharp
var source = new KafkaSourceBuilder<string>()
    .BootstrapServers("localhost:9092")
    .GroupId("my-group")
    .Topic("my-topic")
    .ValueDeserializer(Deserializers.Utf8)
    .Build();

// Automatic integration with Flink's execution environment
env.AddSource(source)
   .Map(new MyProcessingFunction())
   .AddSink(new MySinkFunction());

// Checkpointing and offset management handled automatically
```

## Conclusion

Flink.NET's consumer group management provides the same level of reliability, consistency, and operational excellence as Apache Flink, while maintaining compatibility with the broader Kafka ecosystem. This implementation ensures:

- **Exactly-once processing guarantees**
- **Automatic failure recovery**
- **Optimal performance tuning**
- **Seamless integration with Flink's execution model**
- **Production-ready observability and monitoring**

The implementation follows Apache Flink's architectural patterns while adapting to .NET ecosystem conventions, providing a robust foundation for mission-critical streaming applications.