# Kafka Development Environment for Flink.Net

This directory contains Docker Compose configuration and helper scripts for setting up a Kafka development environment optimized for Flink.Net stream processing applications.

## Quick Start

1. **Start Kafka Environment:**
   ```bash
   ./scripts/kafka-dev.sh start
   ```

2. **Access Kafka UI:**
   Open [http://localhost:8080](http://localhost:8080) in your browser

3. **Kafka Bootstrap Servers:**
   `localhost:9092`

## What's Included

- **Kafka Broker** (localhost:9092) - Main Kafka server
- **Zookeeper** (localhost:2181) - Kafka coordination service  
- **Kafka UI** (localhost:8080) - Web interface for managing topics and monitoring
- **Pre-configured Topics** - Following Flink.Net best practices:
  - `business-events` (8 partitions) - Input events
  - `processed-events` (8 partitions) - Processed data  
  - `analytics-events` (4 partitions) - Analytics output
  - `dead-letter-queue` (2 partitions) - Failed message handling
  - `test-input` & `test-output` (4 partitions each) - Testing

## Management Commands

```bash
# Start the environment
./scripts/kafka-dev.sh start

# Stop the environment  
./scripts/kafka-dev.sh stop

# Restart everything
./scripts/kafka-dev.sh restart

# View service status
./scripts/kafka-dev.sh status

# View logs (all services or specific service)
./scripts/kafka-dev.sh logs
./scripts/kafka-dev.sh logs kafka

# Open Kafka UI
./scripts/kafka-dev.sh ui

# List topics and details
./scripts/kafka-dev.sh topics

# Monitor consumer groups
./scripts/kafka-dev.sh monitor

# Test connectivity
./scripts/kafka-dev.sh test

# Clean up (removes all data)
./scripts/kafka-dev.sh cleanup
```

## Configuration Details

### Topic Configuration

Topics are configured following Apache Flink best practices:

| Topic | Partitions | Retention | Use Case |
|-------|------------|-----------|----------|
| business-events | 8 | 7 days | Raw input events |
| processed-events | 8 | 1 day | Validated/transformed events |
| analytics-events | 4 | 30 days | Aggregated analytics data |
| dead-letter-queue | 2 | 30 days | Failed message handling |

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
    .SetBootstrapServers("localhost:9092")
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
    .SetBootstrapServers("localhost:9092")
    .SetRecordSerializer(new JsonSerializationSchema<ProcessedEvent>())
    .SetDeliveryGuarantee(DeliveryGuarantee.ExactlyOnce)
    .Build());
```

## Troubleshooting

### Common Issues

1. **Port Conflicts:**
   - Kafka: 9092, Zookeeper: 2181, Kafka UI: 8080
   - Stop conflicting services or change ports in docker-compose.kafka.yml

2. **Memory Issues:**
   - Default JVM settings should work for development
   - For production, tune KAFKA_HEAP_OPTS in docker-compose.kafka.yml

3. **Topic Creation Fails:**
   - Ensure Kafka is fully started before creating topics
   - Check logs: `./scripts/kafka-dev.sh logs kafka`

4. **Consumer Lag:**
   - Monitor with: `./scripts/kafka-dev.sh monitor`
   - Check partition count and consumer parallelism

### Monitoring

- **Kafka UI:** http://localhost:8080 - Visual monitoring and management
- **Topic Metrics:** Monitor partition distribution and consumer lag
- **Consumer Groups:** Track processing progress and lag

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
- [Apache Kafka Documentation](https://kafka.apache.org/documentation/) - Official Kafka documentation