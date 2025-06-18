# Kafka Development Environment for Flink.Net

This setup provides a Kafka development environment optimized for Flink.Net stream processing applications, managed through .NET Aspire.

## Quick Start

1. **Start the Development Environment with Aspire:**
   ```bash
   cd FlinkDotNetAspire/FlinkDotNetAspire.AppHost.AppHost
   dotnet run
   ```

2. **Access Kafka UI:**
   Open the Aspire dashboard and navigate to the Kafka UI endpoint (typically [http://localhost:8080](http://localhost:8080))

3. **Kafka Bootstrap Servers:**
   Check the Aspire dashboard for the dynamically allocated Kafka port (typically `localhost:9092`)

## What's Included

- **Kafka Broker** - Main Kafka server (managed by Aspire)
- **Zookeeper** - Kafka coordination service (managed by Aspire)
- **Kafka UI** - Web interface for managing topics and monitoring
- **Topic Initialization** - Automatically creates topics following Flink.Net best practices:
  - `business-events` (8 partitions) - Input events
  - `processed-events` (8 partitions) - Processed data  
  - `analytics-events` (4 partitions) - Analytics output
  - `dead-letter-queue` (2 partitions) - Failed message handling
  - `test-input` & `test-output` (4 partitions each) - Testing
  - `flinkdotnet.sample.topic` (8 partitions) - Default sample topic
  - `flinkdotnet.sample.out.topic` (8 partitions) - FlinkJobSimulator output topic

## Management through Aspire

```bash
# Start the environment
cd FlinkDotNetAspire/FlinkDotNetAspire.AppHost.AppHost
dotnet run

# View all services and endpoints
# Use the Aspire dashboard at http://localhost:15000 (or dynamically allocated port)

# Monitor Kafka through Kafka UI
# Access via the Aspire dashboard or direct link (check dashboard for exact port)

# List topics and details
./scripts/kafka-dev.sh topics

# Monitor consumer groups
./scripts/kafka-dev.sh monitor

# Access services through Aspire Dashboard
# Visit http://localhost:15000 (or check console for Aspire dashboard URL)

# Monitor Kafka through Kafka UI
# Check Aspire dashboard for the Kafka UI endpoint
```

## Configuration Details

### Topic Configuration

Topics are automatically created following Apache Flink best practices:

| Topic | Partitions | Retention | Use Case |
|-------|------------|-----------|----------|
| business-events | 8 | 7 days | Raw input events |
| processed-events | 8 | 1 day | Validated/transformed events |
| analytics-events | 4 | 30 days | Aggregated analytics data |
| dead-letter-queue | 2 | 30 days | Failed message handling |
| test-input | 4 | 1 hour | Testing input |
| test-output | 4 | 1 hour | Testing output |
| flinkdotnet.sample.topic | 8 | 1 day | Default sample topic |

### Producer Configuration

Optimized for Flink.Net exactly-once semantics:
- Transactions enabled for exactly-once delivery
- Idempotence enabled to prevent duplicates
- Proper batching for throughput
- Compression for network efficiency

### Consumer Configuration

Optimized for Flink.Net stream processing:
- Automatic offset management disabled (Flink manages)
- Proper fetch sizes for throughput
- Session timeout and heartbeat tuned for stability

## Integration with Flink.Net

### Standard Pipeline Pattern

```csharp
// Kafka Source
var kafkaSource = KafkaSource<BusinessEvent>.Builder()
    .SetBootstrapServers("localhost:9092") // Check Aspire dashboard for exact port
    .SetTopics("business-events")
    .SetGroupId("flink-business-processor")
    .Build();

// Processing Pipeline
DataStream<BusinessEvent> sourceStream = env.FromSource(kafkaSource, 
    WatermarkStrategy.ForBoundedOutOfOrderness(Duration.OfSeconds(30)),
    "kafka-source");

var processedStream = sourceStream
    .Filter(event => event.IsValid)
    .KeyBy(event => event.CustomerId)
    .Window(TumblingEventTimeWindows.Of(Time.Minutes(5)))
    .Process(new BusinessEventProcessor());

// Kafka Sink
processedStream.SinkTo(KafkaSink<ProcessedEvent>.Builder()
    .SetBootstrapServers("localhost:9092") // Check Aspire dashboard for exact port
    .SetRecordSerializer(new JsonSerializationSchema<ProcessedEvent>())
    .SetDeliveryGuarantee(DeliveryGuarantee.ExactlyOnce)
    .Build());
```

## Troubleshooting

### Common Issues

1. **Port Conflicts:**
   - Aspire handles dynamic port allocation to avoid conflicts
   - Check the Aspire dashboard for current port assignments

2. **Memory Issues:**
   - Default JVM settings should work for development
   - Aspire manages container resources automatically

3. **Topic Creation Fails:**
   - Topics are created automatically by the kafka-init container
   - Check Aspire dashboard logs for the kafka-init container

4. **Consumer Lag:**
   - Monitor through Kafka UI (access via Aspire dashboard)
   - Check partition count and consumer parallelism

### Monitoring

- **Aspire Dashboard:** Primary monitoring interface for all services
- **Kafka UI:** Visual monitoring and management (accessed through Aspire dashboard)
- **Topic Metrics:** Monitor partition distribution and consumer lag
- **Consumer Groups:** Track processing progress and lag

## Aspire Integration

This Kafka environment is fully integrated with the Flink.Net Aspire setup:

### Unified Setup

```bash
# Start complete development environment
cd FlinkDotNetAspire/FlinkDotNetAspire.AppHost.AppHost
dotnet run

# This starts:
# - Redis
# - Kafka with Zookeeper
# - Kafka UI
# - Topic initialization
# - JobManager
# - 20 TaskManagers  
# - FlinkJobSimulator

# Run reliability tests with 10M messages (separate terminal)
cd FlinkDotNetAspire/FlinkDotnetStandardReliabilityTest
dotnet test
```

### Key Benefits

- **10 Million Message Testing**: Comprehensive reliability validation
- **Production-like Environment**: Managed Kafka infrastructure
- **Real-time Monitoring**: Kafka UI integration via Aspire dashboard
- **Best Practices Integration**: Follows Apache Flink recommended patterns
- **Unified Development**: Single command starts entire environment

For complete Aspire setup instructions, see [Aspire Local Development Setup](docs/wiki/Aspire-Local-Development-Setup.md).

## Production Considerations

This setup is for development only. For production:

1. **Use external Kafka cluster** with proper replication
2. **Configure security** (SSL, SASL, ACLs)
3. **Tune JVM settings** for your workload
4. **Set up monitoring** (JMX, Prometheus, etc.)
5. **Configure proper retention** based on your requirements
6. **Use dedicated Zookeeper ensemble** for high availability

## Documentation Links

- [Flink.Net Back Pressure Implementation](docs/wiki/FLINK_NET_BACK_PRESSURE.md) - Includes Kafka-specific best practices
- [Flink.Net Best Practices](docs/wiki/Flink.Net-Best-Practices-Stream-Processing-Patterns.md) - Standard vs custom pipeline patterns
- [Aspire Local Development Setup](docs/wiki/Aspire-Local-Development-Setup.md) - Complete development environment setup
- [Apache Kafka Documentation](https://kafka.apache.org/documentation/) - Official Kafka documentation