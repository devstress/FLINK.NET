# Aspire Local Development Setup with Kafka Best Practices

This guide explains how to set up and use Flink.Net Aspire locally with integrated Kafka best practices for high-volume reliability testing.

## Overview

The Flink.Net Aspire setup provides a complete local development environment with integrated Kafka infrastructure to run comprehensive reliability tests with up to 10 million messages.

## Prerequisites

- .NET 8 SDK
- Docker Desktop
- 8GB+ RAM recommended for high-volume testing
- Aspire tooling

If Docker is unavailable, install Kafka and Redis locally and run them on ports `9092` and `6379` before starting the individual services with `dotnet run`.

## Setup Steps

### 1. Start Complete Environment with Aspire

Start the complete development environment using Aspire:

```bash
# From the FlinkDotNetAspire AppHost directory
cd FlinkDotNetAspire/FlinkDotNetAspire.AppHost.AppHost
dotnet run
```

This will start:
- **Kafka** - Message streaming (dynamic port, check Aspire dashboard)
- **Zookeeper** - Kafka coordination (managed internally)
- **Kafka UI** - Web interface for monitoring (check Aspire dashboard for port)
- **Redis** - State management and counters (dynamic port)
- **Topic Initialization** - Automatically creates all required topics
- **JobManager** - Flink.Net job coordination
- **TaskManagers** (20 instances) - Flink.Net parallel processing
- **FlinkJobSimulator** - Simplified Kafka consumer group background service

**Verify the environment:**
```bash
# Access the Aspire dashboard (check console output for URL, typically http://localhost:15000)
# All services, endpoints, and logs are available through the dashboard
```

### 2. Monitor Services

This starts the complete Flink.Net cluster with:
- 1 JobManager
- 20 TaskManagers  
- FlinkJobSimulator configured as Kafka consumer group for real-world message processing

The Aspire dashboard provides access to all services and their endpoints. Navigate to the Kafka UI through the dashboard to monitor:

- Topic creation and partition distribution
- Message throughput and consumer lag
- Producer and consumer metrics
- Dead letter queue activity

### 3. Run Reliability Tests

The reliability test uses the Aspire-managed Kafka environment and defaults to 10 million messages:

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

### Kafka Topics (Auto-created)

The Aspire environment automatically creates these optimized topics:

| Topic | Partitions | Purpose |
|-------|------------|---------|
| `business-events` | 8 | Input events for processing |
| `processed-events` | 8 | Processed data output |
| `analytics-events` | 4 | Analytics and reporting |
| `dead-letter-queue` | 2 | Failed message handling |
| `test-input` | 4 | Testing and development |
| `test-output` | 4 | Test result output |
| `flinkdotnet.sample.topic` | 8 | Default sample topic |

### Connection Settings

| Service | Connection | Health Check |
|---------|------------|--------------|
| Kafka | Dynamic port (check Aspire dashboard) | Metadata API |
| Redis | Dynamic port (check Aspire dashboard) | PING command |
| Kafka UI | Dynamic port (check Aspire dashboard) | Web interface |

## Usage Patterns

### 1. Local Development Workflow

```bash
# 1. Start complete environment with Aspire
cd FlinkDotNetAspire/FlinkDotNetAspire.AppHost.AppHost
dotnet run

# 2. In another terminal, run reliability tests
cd FlinkDotNetAspire/FlinkDotnetStandardReliabilityTest
dotnet test

# 3. Monitor via Aspire dashboard (check console for URL)
# Access Kafka UI through the dashboard
```

### 2. High-Volume Testing

cd FlinkDotNetAspire/FlinkDotNetAspire.AppHost.AppHost
dotnet run

# Test with 1 million messages (faster)
cd FlinkDotNetAspire/FlinkDotnetStandardReliabilityTest
FLINKDOTNET_STANDARD_TEST_MESSAGES=1000000 dotnet test

# Test with 10 million messages (comprehensive)
FLINKDOTNET_STANDARD_TEST_MESSAGES=10000000 dotnet test

# Test with 50 million messages (stress test)
FLINKDOTNET_STANDARD_TEST_MESSAGES=50000000 dotnet test
```

### 3. Performance Monitoring

Monitor your tests using the Aspire dashboard and Kafka UI:

1. **Access Aspire Dashboard**: Check console output for URL (typically http://localhost:15000)
2. **Open Kafka UI**: Navigate to Kafka UI through the Aspire dashboard
3. **View Topics**: Monitor message flow across topics
4. **Consumer Groups**: Track processing progress
5. **Broker Metrics**: Monitor throughput and latency

### 4. Debugging and Troubleshooting

```bash
# Access Aspire dashboard for comprehensive service monitoring
# All logs, metrics, and service status available in one place

# Check individual service logs through the dashboard
# No need for separate commands - everything is integrated
```

## Integration Architecture

```
┌─────────────────┐    ┌─────────────────┐    ┌─────────────────┐
│  Aspire Setup   │    │  Aspire Cluster │    │ Reliability Test│
│                 │    │                 │    │                 │
│ • Kafka         │◄──►│ • JobManager    │◄──►│ • 10M Messages  │
│ • Redis         │    │ • 20 TaskMgrs   │    │ • BDD Testing   │
│ • Kafka UI      │    │ • JobSimulator  │    │ • Diagnostics   │
│ • Topic Init    │    │ • Auto Topics   │    │ • Monitoring    │
└─────────────────┘    └─────────────────┘    └─────────────────┘
```

### Message Flow

1. **Aspire** starts all infrastructure (Kafka, Redis, UI, topics)
2. **Reliability Test** generates messages using Redis sequence generation
3. **Messages flow** through Flink.Net standard pipeline:
   - Source → Map/Filter → KeyBy → Process → AsyncFunction → Sink
4. **Kafka topics** handle message routing and partitioning
5. **Aspire cluster** processes messages with back pressure handling
6. **Monitoring** via Aspire dashboard and Kafka UI provides real-time visibility

## Best Practices

### 1. Resource Management

- **Memory**: Ensure 8GB+ RAM for high-volume testing
- **CPU**: Test performance scales with available cores
- **Storage**: Kafka and Redis need adequate disk space for large message volumes

### 2. Testing Strategy

- **Start Small**: Begin with 1M messages to verify setup
- **Scale Up**: Gradually increase to 10M for comprehensive testing
- **Monitor**: Use Aspire dashboard and Kafka UI to track progress and identify bottlenecks
- **Clean Up**: Use Ctrl+C in Aspire to clean up after testing

### 3. Development Iterations

- **Single Command**: Aspire starts all required infrastructure
- **Integrated Monitoring**: All services monitored through one dashboard
- **Resource Cleanup**: Aspire manages container lifecycle automatically

## Troubleshooting

### Common Issues

**Services Connection Failed:**
```bash
# Check Aspire dashboard for service status
# All service health and connectivity shown in one place
# Restart individual services through the dashboard if needed
```

**Test Timeout:**
```bash
# Check resource usage through Aspire dashboard
# Or use traditional tools:
docker stats

# Reduce message count if resources are limited
FLINKDOTNET_STANDARD_TEST_MESSAGES=100000 dotnet test
```

**Port Conflicts:**
- Aspire handles dynamic port allocation to avoid conflicts
- Check Aspire dashboard for current port assignments
- No manual port management needed

### Performance Optimization

**For Faster Testing:**
- Reduce message count: `FLINKDOTNET_STANDARD_TEST_MESSAGES=100000`
- Increase available memory in Docker Desktop settings
- Close unnecessary applications to free system resources

**For Maximum Throughput:**
- Increase parallelism in Aspire configuration
- Optimize Kafka partition counts for your workload
- Monitor resource usage through Aspire dashboard and scale accordingly

## Next Steps

- [Kafka Best Practices](FLINK_NET_BACK_PRESSURE.md#section-5-kafka-design-best-practices)
- [Stream Processing Patterns](Flink.Net-Best-Practices-Stream-Processing-Patterns.md)
- [Performance Tuning Guide](Advanced-Performance-Tuning.md)
- [Production Deployment](Deployment-Kubernetes.md)