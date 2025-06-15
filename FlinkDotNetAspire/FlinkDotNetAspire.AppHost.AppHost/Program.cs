// Existing using statements (implicit for DistributedApplication, Projects)

namespace FlinkDotNetAspire.AppHost.AppHost;

public static class Program
{
    public static async Task Main(string[] args)
    {
        var builder = DistributedApplication.CreateBuilder(args);

        // Add resources with Aspire's default dynamic port allocation
        // Don't force specific ports - let Aspire handle port management
        var redis = builder.AddRedis("redis")
            .PublishAsContainer(); // Ensure Redis is accessible from host
        
        // Use Aspire's built-in Kafka with custom configuration optimized for CI
        var kafka = builder.AddKafka("kafka")
            .WithEnvironment("KAFKA_AUTO_CREATE_TOPICS_ENABLE", "true")
            .WithEnvironment("KAFKA_NUM_PARTITIONS", "4") // Reduced for CI
            .WithEnvironment("KAFKA_DEFAULT_REPLICATION_FACTOR", "1")
            .WithEnvironment("KAFKA_LOG_RETENTION_HOURS", "1") // Reduced for CI
            .WithEnvironment("KAFKA_LOG_SEGMENT_BYTES", "104857600") // 100MB, reduced for CI
            .WithEnvironment("KAFKA_MESSAGE_MAX_BYTES", "1048576") // 1MB, reduced for CI
            .WithEnvironment("KAFKA_REPLICA_FETCH_MAX_BYTES", "1048576") // 1MB
            .WithEnvironment("KAFKA_GROUP_INITIAL_REBALANCE_DELAY_MS", "0")
            .WithEnvironment("KAFKA_OFFSETS_TOPIC_REPLICATION_FACTOR", "1") // CI compatibility
            .WithEnvironment("KAFKA_TRANSACTION_STATE_LOG_REPLICATION_FACTOR", "1") // CI compatibility
            .WithEnvironment("KAFKA_TRANSACTION_STATE_LOG_MIN_ISR", "1") // CI compatibility
            .WithEnvironment("KAFKA_LOG_FLUSH_INTERVAL_MESSAGES", "1000") // Faster flushing for CI
            .WithEnvironment("KAFKA_LOG_FLUSH_INTERVAL_MS", "1000") // Faster flushing
            .PublishAsContainer(); // Ensure Kafka is accessible from host

        // Add Kafka topic initialization container with better error handling and CI compatibility
        builder.AddContainer("kafka-init", "confluentinc/cp-kafka", "7.4.0")
            .WithArgs("bash", "-c", @"
                echo 'Waiting for Kafka to be ready...'
                max_attempts=30
                attempt=0
                until kafka-topics --bootstrap-server kafka:9092 --list >/dev/null 2>&1; do
                    attempt=$((attempt + 1))
                    if [ $attempt -ge $max_attempts ]; then
                        echo 'ERROR: Kafka failed to become ready after 30 attempts (150s)'
                        exit 1
                    fi
                    echo ""Kafka not ready yet, waiting... (attempt $attempt/$max_attempts)""
                    sleep 5
                done
                
                echo 'Kafka is ready! Creating topics for Flink.Net development...'
                
                # Create topics with CI-optimized settings (smaller segments, shorter retention)
                kafka-topics --create --if-not-exists --bootstrap-server kafka:9092 --topic business-events --partitions 4 --replication-factor 1 --config retention.ms=3600000 --config cleanup.policy=delete --config min.insync.replicas=1 --config segment.ms=60000
                kafka-topics --create --if-not-exists --bootstrap-server kafka:9092 --topic processed-events --partitions 4 --replication-factor 1 --config retention.ms=3600000 --config cleanup.policy=delete --config min.insync.replicas=1 --config segment.ms=60000
                kafka-topics --create --if-not-exists --bootstrap-server kafka:9092 --topic analytics-events --partitions 2 --replication-factor 1 --config retention.ms=3600000 --config cleanup.policy=delete --config min.insync.replicas=1 --config segment.ms=60000
                kafka-topics --create --if-not-exists --bootstrap-server kafka:9092 --topic dead-letter-queue --partitions 2 --replication-factor 1 --config retention.ms=3600000 --config cleanup.policy=delete --config min.insync.replicas=1 --config segment.ms=60000
                kafka-topics --create --if-not-exists --bootstrap-server kafka:9092 --topic test-input --partitions 2 --replication-factor 1 --config retention.ms=1800000 --config cleanup.policy=delete --config segment.ms=30000
                kafka-topics --create --if-not-exists --bootstrap-server kafka:9092 --topic test-output --partitions 2 --replication-factor 1 --config retention.ms=1800000 --config cleanup.policy=delete --config segment.ms=30000
                kafka-topics --create --if-not-exists --bootstrap-server kafka:9092 --topic flinkdotnet.sample.topic --partitions 4 --replication-factor 1 --config retention.ms=3600000 --config cleanup.policy=delete --config segment.ms=60000
                
                echo 'Kafka topics created successfully!'
                echo 'Topic list:'
                kafka-topics --list --bootstrap-server kafka:9092
                echo 'Kafka initialization completed successfully!'
            ")
            .WaitFor(kafka);

        // Add Kafka UI for visual monitoring and management - disabled in CI for performance
        var isCI = Environment.GetEnvironmentVariable("CI") == "true" || Environment.GetEnvironmentVariable("GITHUB_ACTIONS") == "true";
        if (!isCI)
        {
            builder.AddContainer("kafka-ui", "provectuslabs/kafka-ui", "latest")
                .WithEnvironment("KAFKA_CLUSTERS_0_NAME", "flink-development")
                .WithEnvironment("KAFKA_CLUSTERS_0_BOOTSTRAPSERVERS", "kafka:9092")
                .WithEnvironment("DYNAMIC_CONFIG_ENABLED", "true")
                .WithHttpEndpoint(8080, 8080, "ui")
                .PublishAsContainer()
                .WaitFor(kafka);
        }

        // Set up message count based on environment - smaller for CI
        var simulatorNumMessages = Environment.GetEnvironmentVariable("SIMULATOR_NUM_MESSAGES");
        if (string.IsNullOrEmpty(simulatorNumMessages))
        {
            // Default to smaller numbers in CI environments for faster execution
            simulatorNumMessages = isCI ? "1000" : "100000";
        }

        // Reduce TaskManager count in CI for resource efficiency
        var taskManagerCount = isCI ? 5 : 20;

        // Add JobManager (1 instance) - Using project for GitHub Actions compatibility
        var jobManager = builder.AddProject<Projects.FlinkDotNet_JobManager>("jobmanager")
            .WithEnvironment("DOTNET_ENVIRONMENT", "Development")
            .WithEnvironment("ASPNETCORE_ENVIRONMENT", "Development");

        // Add TaskManagers - Using projects for GitHub Actions compatibility
        for (int i = 1; i <= taskManagerCount; i++)
        {
            builder.AddProject<Projects.FlinkDotNet_TaskManager>($"taskmanager{i}")
                .WithEnvironment("TaskManagerId", $"TM-{i.ToString("D2")}")
                .WithEnvironment("DOTNET_ENVIRONMENT", "Development")
                .WithEnvironment("ASPNETCORE_ENVIRONMENT", "Development")
                .WithEnvironment("JOBMANAGER_GRPC_ADDRESS", jobManager.GetEndpoint("https"));
        }

        // Provide Redis and Kafka connection information to the FlinkJobSimulator
        // Wait for infrastructure to be ready before starting the simulator
        builder.AddProject<Projects.FlinkJobSimulator>("flinkjobsimulator")
            .WithReference(redis) // Makes "ConnectionStrings__redis" available
            .WithReference(kafka) // Makes "ConnectionStrings__kafka" available (or similar for bootstrap servers)
            .WithEnvironment("SIMULATOR_NUM_MESSAGES", simulatorNumMessages) // Use environment variable or default
            .WithEnvironment("SIMULATOR_REDIS_KEY_SINK_COUNTER", "flinkdotnet:sample:processed_message_counter") // Redis sink counter key
            .WithEnvironment("SIMULATOR_REDIS_KEY_GLOBAL_SEQUENCE", "flinkdotnet:global_sequence_id") // Redis global sequence key
            .WithEnvironment("SIMULATOR_KAFKA_TOPIC", "flinkdotnet.sample.topic") // Default Kafka topic
            .WithEnvironment("DOTNET_ENVIRONMENT", "Development")
            .WaitFor(redis);

        await builder.Build().RunAsync();
    }
}
