# Kafka Consumer Group TaskManager Load Distribution

This document explains how the Apache Flink-style consumer group management distributes load across TaskManagers and how to test it with the stress and reliability tests.

## Overview

FlinkDotNet implements Apache Flink 2.0's consumer group patterns for proper load distribution across multiple TaskManagers. The system includes:

1. **1-minute Kafka setup wait** - Ensures Kafka is fully ready before consumer initialization
2. **TaskManager load balancing** - Distributes Kafka partitions across available TaskManagers  
3. **Comprehensive logging** - Tracks which TaskManager gets which partitions and load distribution

## Architecture

### Consumer Group Management
- Uses `FlinkKafkaConsumerGroup` for Apache Flink-compatible consumer group handling
- Implements cooperative sticky partition assignment for minimal rebalancing disruption
- Disables Kafka auto-commit, manages offsets through Flink checkpointing
- Provides exactly-once processing guarantees

### TaskManager Distribution  
- **Topic: `flinkdotnet.sample.topic`** with 4 partitions (configured in AppHost)
- **TaskManagers: 20 instances** (5 in CI environments)
- **Load Distribution**: With 4 partitions and 20 TaskManagers, only 4 TaskManagers will be actively consuming at any time (1 per partition)
- **Idle TaskManagers**: The remaining 16 TaskManagers will be idle but ready for rebalancing if needed

## Configuration

### Enable Kafka Source Mode

To test TaskManager load distribution, set the environment variable:

```bash
export STRESS_TEST_USE_KAFKA_SOURCE=true
```

This switches FlinkJobSimulator from Redis-based message generation to real Kafka consumer group testing.

### Environment Variables

```bash
# Required for Kafka source mode
STRESS_TEST_USE_KAFKA_SOURCE=true

# Kafka configuration
SIMULATOR_KAFKA_TOPIC=flinkdotnet.sample.topic
SIMULATOR_KAFKA_CONSUMER_GROUP=flinkdotnet-stress-test-consumer-group

# Redis configuration (still used for sink)
SIMULATOR_REDIS_KEY_SINK_COUNTER=flinkdotnet:sample:processed_message_counter
SIMULATOR_NUM_MESSAGES=1000
```

## Load Distribution Logging

The system provides detailed logging for TaskManager load analysis:

### TaskManager Assignment Logging
```
📊 TASK MANAGER LOAD DISTRIBUTION: TaskManager TM-01 (PID: 12345) assigned 1 partitions: flinkdotnet.sample.topic [0]
📈 TaskManager TM-01: Topic 'flinkdotnet.sample.topic' partitions 0 (1 out of total available partitions)
⚖️ LOAD BALANCING: TaskManager TM-01 is now actively consuming 1 partitions - Load Status: ACTIVE
💤 LOAD BALANCING: TaskManager TM-02 has no partitions assigned - Load Status: IDLE
```

### Message Consumption Logging
```
📨 TaskManager TM-01: Consumed 100 messages from Kafka topic 'flinkdotnet.sample.topic' (partition: 0, offset: 99)
⚖️ LOAD BALANCE STATUS: TaskManager TM-01 currently assigned 1 partitions, consumed 100 total messages
```

### Rebalancing Logging
```
📤 TASK MANAGER REBALANCING: TaskManager TM-01 revoked 1 partitions: flinkdotnet.sample.topic [0] [100]
📉 TASK MANAGER FAULT TOLERANCE: TaskManager TM-02 lost 1 partitions: flinkdotnet.sample.topic [1] [200]
```

## Testing Instructions

### 1. Stress Test with Kafka Source

```bash
# Enable Kafka source mode
export STRESS_TEST_USE_KAFKA_SOURCE=true

# Run stress test
./scripts/run-stress-test.ps1
```

### 2. Reliability Test with Kafka Source

```bash
# Enable Kafka source mode  
export STRESS_TEST_USE_KAFKA_SOURCE=true

# Run reliability test
./scripts/run-reliability-test.ps1
```

### 3. Monitor TaskManager Load Distribution

Watch the logs for these key indicators:

1. **Active TaskManagers**: Should see 4 TaskManagers with "Load Status: ACTIVE"
2. **Idle TaskManagers**: Should see 16 TaskManagers with "Load Status: IDLE"
3. **Partition Assignment**: Each active TaskManager should get exactly 1 partition
4. **Message Flow**: Active TaskManagers should show message consumption progress

## Expected Behavior

### Normal Operation
- **4 active TaskManagers** (one per partition)
- **16 idle TaskManagers** (ready for failover)
- **Even message distribution** across the 4 partitions
- **No rebalancing** unless TaskManager failures occur

### Fault Tolerance Testing
To test rebalancing, you can:
1. Stop one of the active TaskManagers
2. Observe partition reassignment to another TaskManager
3. Verify exactly-once processing continues

### Performance Metrics
- **Startup Time**: Kafka wait adds ~60 seconds maximum to startup
- **Message Throughput**: Depends on Kafka configuration and message size
- **Rebalancing Time**: Should complete within session timeout (30 seconds)

## Troubleshooting

### No TaskManager Assignment
```
💤 LOAD BALANCING: TaskManager TM-XX has no partitions assigned - Load Status: IDLE
```
**Normal**: With 4 partitions and 20 TaskManagers, 16 will be idle

### Kafka Connection Issues
```
❌ TaskManager TM-XX: Kafka consume error: Broker: Unknown topic or partition
```
**Solution**: Ensure Kafka topics are created and Kafka is running

### Partition Rebalancing Issues
```
📤 TASK MANAGER REBALANCING: TaskManager TM-XX revoked 1 partitions
```
**Monitor**: Check if partitions get reassigned within session timeout

## Integration with Apache Flink Standards

This implementation follows Apache Flink 2.0 patterns:

1. **Consumer Group Protocol**: Uses Kafka's native consumer group protocol
2. **Checkpointing Integration**: Manages offsets through Flink checkpoints
3. **Fault Tolerance**: Automatic partition reassignment on failures
4. **Exactly-Once Processing**: Coordinates between Kafka commits and Flink checkpoints
5. **Load Balancing**: Uses cooperative sticky assignment strategy

The load distribution ensures optimal resource utilization while maintaining Apache Flink's reliability and fault tolerance guarantees.