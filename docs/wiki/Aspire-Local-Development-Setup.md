# Aspire Local Development Setup with Kafka Best Practices

This guide explains how to set up and use Flink.Net Aspire locally with the Kafka best practices environment for high-volume reliability testing.

## Overview

The Flink.Net Aspire setup provides a complete local development environment that integrates with our Kafka best practices setup to run comprehensive reliability tests with up to 10 million messages.

## Prerequisites

- .NET 8 SDK
- Docker Desktop
- 8GB+ RAM recommended for high-volume testing
- Docker Compose support

## Setup Steps

### 1. Start Kafka Environment

First, start the Kafka development environment using our best practices setup:

```bash
# From the project root directory
./scripts/kafka-dev.sh start
```

This will start:
- **Kafka** (localhost:9092) - Message streaming
- **Zookeeper** (localhost:2181) - Kafka coordination  
- **Redis** (localhost:6379) - State management and counters
- **Kafka UI** (localhost:8080) - Web interface for monitoring

**Verify the environment:**
```bash
# Check service status
./scripts/kafka-dev.sh status

# Open Kafka UI for visual monitoring
./scripts/kafka-dev.sh ui
```

### 2. Run Aspire Application

Navigate to the Aspire AppHost directory and run:

```bash
cd FlinkDotNetAspire/FlinkDotNetAspire.AppHost.AppHost
dotnet run
```

This starts the complete Flink.Net cluster with:
- 1 JobManager
- 20 TaskManagers  
- FlinkJobSimulator configured for high-volume testing

### 3. Run Reliability Tests

The reliability test now uses the external Kafka environment and defaults to 10 million messages:

```bash
cd FlinkDotNetAspire/FlinkDotnetStandardReliabilityTest

# Run with default 10M messages
dotnet test

# Run with custom message count
FLINKDOTNET_STANDARD_TEST_MESSAGES=1000000 dotnet test
```

## Environment Configuration

### Default Message Volumes

| Component | Default Messages | Environment Variable | Purpose |
|-----------|------------------|---------------------|---------|
| Aspire JobSimulator | 1,000,000 | `SIMULATOR_NUM_MESSAGES` | Aspire job simulation |
| Reliability Test | 10,000,000 | `FLINKDOTNET_STANDARD_TEST_MESSAGES` | Comprehensive testing |

### Kafka Topics (Pre-configured)

The Kafka environment includes these optimized topics:

| Topic | Partitions | Purpose |
|-------|------------|---------|
| `business-events` | 8 | Input events for processing |
| `processed-events` | 8 | Processed data output |
| `analytics-events` | 4 | Analytics and reporting |
| `dead-letter-queue` | 2 | Failed message handling |
| `test-input` | 4 | Testing and development |
| `test-output` | 4 | Test result output |

### Connection Settings

| Service | Connection | Health Check |
|---------|------------|--------------|
| Kafka | `localhost:9092` | Metadata API |
| Redis | `localhost:6379` | PING command |
| Kafka UI | `http://localhost:8080` | Web interface |

## Usage Patterns

### 1. Local Development Workflow

```bash
# 1. Start Kafka environment
./scripts/kafka-dev.sh start

# 2. Run Aspire application
cd FlinkDotNetAspire/FlinkDotNetAspire.AppHost.AppHost
dotnet run

# 3. In another terminal, run reliability tests
cd FlinkDotNetAspire/FlinkDotnetStandardReliabilityTest
dotnet test

# 4. Monitor via Kafka UI
open http://localhost:8080
```

### 2. High-Volume Testing

For comprehensive testing with different message volumes:

```bash
# Start infrastructure
./scripts/kafka-dev.sh start

# Test with 1 million messages (faster)
FLINKDOTNET_STANDARD_TEST_MESSAGES=1000000 dotnet test

# Test with 10 million messages (comprehensive)
FLINKDOTNET_STANDARD_TEST_MESSAGES=10000000 dotnet test

# Test with 50 million messages (stress test)
FLINKDOTNET_STANDARD_TEST_MESSAGES=50000000 dotnet test
```

### 3. Performance Monitoring

Monitor your tests using the Kafka UI:

1. **Open Kafka UI**: http://localhost:8080
2. **View Topics**: Monitor message flow across topics
3. **Consumer Groups**: Track processing progress
4. **Broker Metrics**: Monitor throughput and latency

### 4. Debugging and Troubleshooting

```bash
# Check Kafka environment status
./scripts/kafka-dev.sh status

# View logs from all services
./scripts/kafka-dev.sh logs

# View logs from specific service
./scripts/kafka-dev.sh logs kafka
./scripts/kafka-dev.sh logs redis

# Restart if needed
./scripts/kafka-dev.sh restart
```

## Integration Architecture

```
┌─────────────────┐    ┌─────────────────┐    ┌─────────────────┐
│   Kafka Setup  │    │  Aspire Cluster │    │ Reliability Test│
│                 │    │                 │    │                 │
│ • Kafka:9092    │◄──►│ • JobManager    │◄──►│ • 10M Messages  │
│ • Redis:6379    │    │ • 20 TaskMgrs   │    │ • BDD Testing   │
│ • Kafka UI:8080 │    │ • JobSimulator  │    │ • Diagnostics   │
└─────────────────┘    └─────────────────┘    └─────────────────┘
```

### Message Flow

1. **Reliability Test** generates messages using Redis sequence generation
2. **Messages flow** through Flink.Net standard pipeline:
   - Source → Map/Filter → KeyBy → Process → AsyncFunction → Sink
3. **Kafka topics** handle message routing and partitioning
4. **Aspire cluster** processes messages with back pressure handling
5. **Monitoring** via Kafka UI provides real-time visibility

## Best Practices

### 1. Resource Management

- **Memory**: Ensure 8GB+ RAM for high-volume testing
- **CPU**: Test performance scales with available cores
- **Storage**: Kafka and Redis need adequate disk space for large message volumes

### 2. Testing Strategy

- **Start Small**: Begin with 1M messages to verify setup
- **Scale Up**: Gradually increase to 10M for comprehensive testing
- **Monitor**: Use Kafka UI to track progress and identify bottlenecks
- **Clean Up**: Use `./scripts/kafka-dev.sh stop` to clean up after testing

### 3. Development Iterations

- **Kafka First**: Always start Kafka environment before Aspire
- **Verify Connectivity**: Check Redis and Kafka connections before testing
- **Monitor Progress**: Use logs and Kafka UI for real-time feedback
- **Resource Cleanup**: Stop services when not in use to free resources

## Troubleshooting

### Common Issues

**Kafka Connection Failed:**
```bash
# Check if Kafka is running
./scripts/kafka-dev.sh status

# Start if not running
./scripts/kafka-dev.sh start

# Wait 30 seconds for full startup
```

**Redis Connection Failed:**
```bash
# Check Redis connectivity
redis-cli -h localhost -p 6379 ping

# Should return: PONG
```

**Test Timeout:**
```bash
# Check resource usage
docker stats

# Reduce message count if resources are limited
FLINKDOTNET_STANDARD_TEST_MESSAGES=100000 dotnet test
```

**Port Conflicts:**
```bash
# Check if ports are in use
netstat -an | grep -E ":(6379|9092|8080)"

# Stop conflicting services or change ports in docker-compose.kafka.yml
```

### Performance Optimization

**For Faster Testing:**
- Reduce message count: `FLINKDOTNET_STANDARD_TEST_MESSAGES=100000`
- Increase available memory in Docker Desktop settings
- Close unnecessary applications to free system resources

**For Maximum Throughput:**
- Increase parallelism in Aspire configuration
- Optimize Kafka partition counts for your workload
- Monitor resource usage and scale accordingly

## Next Steps

- [Kafka Best Practices](FLINK_NET_BACK_PRESSURE.md#section-5-kafka-design-best-practices)
- [Stream Processing Patterns](Flink.Net-Best-Practices-Stream-Processing-Patterns.md)
- [Performance Tuning Guide](Advanced-Performance-Tuning.md)
- [Production Deployment](Deployment-Kubernetes.md)