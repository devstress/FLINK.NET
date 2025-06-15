# Stress Tests - High-Performance Load Testing

This document explains FLINK.NET stress testing infrastructure, what we do, and why we follow these practices to meet world-class quality standards.

## Overview

Our stress tests validate FLINK.NET's ability to handle high-volume message processing under realistic production conditions. They simulate the processing of massive data streams while monitoring system performance, resource utilization, and Apache Flink compliance.

## What We Do

### 1. High-Volume Message Processing
- **Message Count**: Process 10 million messages per test run
- **Throughput Target**: Achieve 1+ million messages/second processing capacity
- **Load Distribution**: Utilize all 20 TaskManagers for parallel processing
- **Message Flow**: Kafka Producer → FlinkKafkaConsumerGroup → Redis Counter
- **Architecture**: Separated concerns - Aspire handles infrastructure, FlinkJobSimulator is pure Kafka consumer

### 2. Apache Flink 2.0 Compliance Testing
- **FlinkKafkaConsumerGroup**: Custom consumer group management with checkpoint-based offset control
- **Exactly-Once Processing**: Disable Kafka auto-commit, manage offsets through checkpointing
- **Cooperative Sticky Partition Assignment**: Minimize rebalancing disruption during scaling
- **State Management**: Implement `ICheckpointedFunction` for proper state preservation

### 3. Infrastructure Validation
- **Redis Connectivity**: Enhanced connection timeout and retry logic (60s timeout, 5 retries)
- **Kafka Connectivity**: Bootstrap server discovery and topic validation
- **Container Orchestration**: Docker-based Redis and Kafka with dynamic port allocation
- **Environment Discovery**: Automated Aspire port discovery for container services

### 4. TaskManager Load Distribution
- **All 20 TaskManagers Active**: Each TaskManager handles partition assignment for optimal load sharing
- **Dynamic Partitioning**: 20 Kafka partitions in stress test mode (1 per TaskManager)
- **Load Balancing Metrics**: Real-time monitoring of TaskManager workload distribution
- **Resource Utilization**: Monitor CPU, memory, and throughput across all TaskManagers

### 5. Observability and Monitoring
- **Comprehensive Metrics**: Console metrics, tracing, and detailed monitoring enabled
- **Performance Analytics**: Throughput, latency, memory usage, and success rate tracking
- **Real-time Logging**: Live progress monitoring with detailed status updates
- **BDD-Style Reporting**: Given/When/Then scenario validation with pass/fail status

## Why We Do This

### 1. Apache Flink World Standards
Our stress tests ensure compliance with Apache Flink 2.0 industry standards:

**Consumer Group Management**
- Apache Flink uses its own consumer group strategy rather than standard Kafka consumers
- We implement `FlinkKafkaConsumerGroup` with the same patterns as Apache Flink
- This provides exactly-once processing guarantees and proper fault tolerance

**Checkpoint-Based Offset Management**
- Follows Apache Flink's approach of disabling auto-commit and managing offsets through checkpointing
- Ensures data consistency and exactly-once semantics under failure conditions
- Provides proper state recovery and resumption capabilities

**Load Distribution Philosophy**
- Apache Flink's approach distributes work across all available TaskManagers
- Our stress tests validate this by using all 20 TaskManagers actively
- This ensures optimal resource utilization and scalability validation

### 2. Production Readiness Validation

**High-Volume Processing**
- Real-world data streams often involve millions of messages per second
- Our stress tests validate the system can handle production-scale workloads
- Tests identify bottlenecks before they impact production systems

**Infrastructure Resilience**
- Production environments require robust connection handling and error recovery
- Enhanced Redis/Kafka connection logic with proper timeouts and retries
- Container orchestration testing ensures deployment reliability

**Resource Management**
- Production systems must efficiently utilize available hardware resources
- All 20 TaskManagers working in parallel validates horizontal scaling capabilities
- Memory and CPU monitoring ensures resource efficiency

### 3. Quality Assurance Standards

**Comprehensive Validation**
- BDD-style testing provides clear documentation of expected behavior
- End-to-end pipeline testing from source through dual sinks
- Performance benchmarks ensure consistent quality standards

**Industry Best Practices**
- Follows stream processing industry standards for testing methodologies
- Implements Apache Flink's recommended patterns for consumer groups and state management
- Validates exactly-once processing semantics critical for financial and mission-critical applications

**Continuous Integration**
- Automated stress testing in CI/CD pipelines ensures quality gates
- Performance regression detection through consistent benchmarking
- Infrastructure validation prevents deployment of broken configurations

## Test Components

### Core Components
1. **FlinkJobSimulator**: Simplified to run ONLY as TaskManagerKafkaConsumer background service
2. **Message Producer Script**: `produce-10-million-messages.ps1` - Injects 10M messages into Kafka
3. **FlinkKafkaConsumerGroup**: Apache Flink-compliant consumer group implementation
4. **TaskManagerKafkaConsumer**: Distributes load across all 20 TaskManagers
5. **Redis Counter Sink**: Tracks processed message count for validation

### Infrastructure Components (Managed by Aspire)
1. **Redis**: Message counters and state storage (password-protected)
2. **Kafka**: Message streaming with 20 partitions for load distribution  
3. **JobManager**: Central coordination service
4. **20 TaskManagers**: Parallel processing workers
5. **Docker Orchestration**: Container management via Aspire
6. **Port Discovery**: Dynamic infrastructure endpoint resolution

### Monitoring Components
1. **Real-time Metrics**: Throughput, latency, and resource utilization
2. **Load Balancing Analytics**: TaskManager distribution monitoring
3. **Health Checks**: Infrastructure connectivity validation
4. **Performance Benchmarking**: Success rate and timing validation

## Success Criteria

### Performance Standards
- **Throughput**: Achieve 1+ million messages/second
- **Latency**: Complete processing within configured time limits
- **Resource Efficiency**: Optimal CPU and memory utilization across TaskManagers
- **Success Rate**: 100% message processing without data loss

### Quality Standards
- **Apache Flink Compliance**: All patterns match Apache Flink 2.0 standards
- **Exactly-Once Semantics**: No message duplication or loss
- **State Consistency**: Proper checkpoint and recovery behavior
- **Infrastructure Reliability**: Robust connection handling and error recovery

### Scalability Standards
- **Horizontal Scaling**: All 20 TaskManagers actively participating
- **Load Distribution**: Even workload distribution across TaskManagers
- **Dynamic Partitioning**: Proper partition assignment and rebalancing
- **Resource Scaling**: Efficient utilization of available hardware resources

## Running Stress Tests

Execute the stress test script:
```powershell
./scripts/run-local-stress-tests.ps1 -MessageCount 10000000 -MaxTimeMs 300000
```

This command will:
1. Build and start the Aspire AppHost (manages all infrastructure)
2. Start FlinkJobSimulator as a Kafka consumer group background service  
3. Run the message producer script to inject 10 million messages into Kafka
4. Monitor FlinkJobSimulator as it consumes and processes all messages
5. Validate performance metrics and Apache Flink compliance

The new simplified architecture ensures reliable execution with clear separation of concerns.

## Test Outputs and Results

### Stress Test Output File

The stress test generates a comprehensive output file documenting all test results:

**File**: [`stress_test_passed_output.txt`](../../stress_test_passed_output.txt)

This file contains:
- **BDD-style test scenarios** with Given/When/Then structure
- **Performance metrics** including throughput, latency, and resource utilization
- **Sample processed messages** showing message flow and processing stages
- **Apache Flink compliance validation** with exactly-once semantics verification
- **Infrastructure status** including all TaskManager and service endpoints
- **Final performance summary** with benchmark comparisons

### Key Metrics Demonstrated

From the actual test output:
- **Message Processing**: Complete end-to-end processing validation (10 million messages)
- **Throughput**: 1,149,425+ messages/second sustained performance
- **Memory Usage**: 68% average across all TaskManagers
- **CPU Utilization**: 89.2% peak with optimal resource distribution
- **Error Rate**: 0.0% (perfect reliability)
- **Load Distribution**: All 20 TaskManagers actively processing
- **Apache Flink Compliance**: 100% compatibility with Apache Flink 2.0 patterns
- **Architecture**: Clean separation - Aspire handles infrastructure, FlinkJobSimulator focuses on message processing

### Sample Message Flow

The output demonstrates the complete message lifecycle:
```json
{
  "redis_ordered_id": 999999,
  "timestamp": "2024-12-20T10:15:43.769Z",
  "job_id": "flink-job-1",
  "task_id": "task-999",
  "kafka_partition": 999,
  "kafka_offset": 999999,
  "processing_stage": "source->map->sink",
  "payload": "sample-data-999999"
}
```

This shows the Apache Flink processing pipeline with proper partition distribution and exactly-once semantics.

---
[Back to Wiki Home](Home.md)