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
        
        // Use Aspire's built-in Kafka with custom configuration
        var kafka = builder.AddKafka("kafka")
            .WithEnvironment("KAFKA_AUTO_CREATE_TOPICS_ENABLE", "true")
            .WithEnvironment("KAFKA_NUM_PARTITIONS", "8")
            .WithEnvironment("KAFKA_DEFAULT_REPLICATION_FACTOR", "1")
            .WithEnvironment("KAFKA_LOG_RETENTION_HOURS", "168")
            .WithEnvironment("KAFKA_LOG_SEGMENT_BYTES", "1073741824")
            .WithEnvironment("KAFKA_MESSAGE_MAX_BYTES", "10485760")
            .WithEnvironment("KAFKA_REPLICA_FETCH_MAX_BYTES", "10485760")
            .WithEnvironment("KAFKA_GROUP_INITIAL_REBALANCE_DELAY_MS", "0")
            .PublishAsContainer(); // Ensure Kafka is accessible from host

        // Add Kafka topic initialization container
        builder.AddContainer("kafka-init", "confluentinc/cp-kafka", "7.4.0")
            .WithArgs("bash", "-c", @"
                echo 'Waiting for Kafka to be ready...'
                until kafka-topics --bootstrap-server kafka:9092 --list >/dev/null 2>&1; do
                    echo 'Kafka not ready yet, waiting...'
                    sleep 5
                done
                
                echo 'Creating Kafka topics for Flink.Net development...'
                
                kafka-topics --create --if-not-exists --bootstrap-server kafka:9092 --topic business-events --partitions 8 --replication-factor 1 --config retention.ms=604800000 --config cleanup.policy=delete --config min.insync.replicas=1
                kafka-topics --create --if-not-exists --bootstrap-server kafka:9092 --topic processed-events --partitions 8 --replication-factor 1 --config retention.ms=86400000 --config cleanup.policy=delete --config min.insync.replicas=1
                kafka-topics --create --if-not-exists --bootstrap-server kafka:9092 --topic analytics-events --partitions 4 --replication-factor 1 --config retention.ms=2592000000 --config cleanup.policy=delete --config min.insync.replicas=1
                kafka-topics --create --if-not-exists --bootstrap-server kafka:9092 --topic dead-letter-queue --partitions 2 --replication-factor 1 --config retention.ms=2592000000 --config cleanup.policy=delete --config min.insync.replicas=1
                kafka-topics --create --if-not-exists --bootstrap-server kafka:9092 --topic test-input --partitions 4 --replication-factor 1 --config retention.ms=3600000 --config cleanup.policy=delete
                kafka-topics --create --if-not-exists --bootstrap-server kafka:9092 --topic test-output --partitions 4 --replication-factor 1 --config retention.ms=3600000 --config cleanup.policy=delete
                kafka-topics --create --if-not-exists --bootstrap-server kafka:9092 --topic flinkdotnet.sample.topic --partitions 8 --replication-factor 1 --config retention.ms=86400000 --config cleanup.policy=delete
                
                echo 'Kafka topics created successfully!'
                kafka-topics --list --bootstrap-server kafka:9092
            ")
            .WaitFor(kafka);

        // Add Kafka UI for visual monitoring and management
        builder.AddContainer("kafka-ui", "provectuslabs/kafka-ui", "latest")
            .WithEnvironment("KAFKA_CLUSTERS_0_NAME", "flink-development")
            .WithEnvironment("KAFKA_CLUSTERS_0_BOOTSTRAPSERVERS", "kafka:9092")
            .WithEnvironment("DYNAMIC_CONFIG_ENABLED", "true")
            .WithHttpEndpoint(8080, 8080, "ui")
            .PublishAsContainer()
            .WaitFor(kafka);

        // Set up for 1 million message high throughput test
        var simulatorNumMessages = Environment.GetEnvironmentVariable("SIMULATOR_NUM_MESSAGES") ?? "1000000";

        // Add JobManager (1 instance) - Using project for GitHub Actions compatibility
        var jobManager = builder.AddProject<Projects.FlinkDotNet_JobManager>("jobmanager")
            .WithEnvironment("DOTNET_ENVIRONMENT", "Development")
            .WithEnvironment("ASPNETCORE_ENVIRONMENT", "Development");

        // Add TaskManagers (20 instances as per requirements) - Using projects for GitHub Actions compatibility
        for (int i = 1; i <= 20; i++)
        {
            builder.AddProject<Projects.FlinkDotNet_TaskManager>($"taskmanager{i}")
                .WithEnvironment("TaskManagerId", $"TM-{i.ToString("D2")}")
                .WithEnvironment("DOTNET_ENVIRONMENT", "Development")
                .WithEnvironment("ASPNETCORE_ENVIRONMENT", "Development")
                .WithEnvironment("JOBMANAGER_GRPC_ADDRESS", jobManager.GetEndpoint("https"));
        }

        // Provide Redis and Kafka connection information to the FlinkJobSimulator
        builder.AddProject<Projects.FlinkJobSimulator>("flinkjobsimulator")
            .WithReference(redis) // Makes "ConnectionStrings__redis" available
            .WithReference(kafka) // Makes "ConnectionStrings__kafka" available (or similar for bootstrap servers)
            .WithEnvironment("SIMULATOR_NUM_MESSAGES", simulatorNumMessages) // Use environment variable or 1M default
            .WithEnvironment("SIMULATOR_REDIS_KEY_SINK_COUNTER", "flinkdotnet:sample:processed_message_counter") // Redis sink counter key
            .WithEnvironment("SIMULATOR_REDIS_KEY_GLOBAL_SEQUENCE", "flinkdotnet:global_sequence_id") // Redis global sequence key
            .WithEnvironment("SIMULATOR_KAFKA_TOPIC", "flinkdotnet.sample.topic") // Default Kafka topic
            .WithEnvironment("DOTNET_ENVIRONMENT", "Development");

        await builder.Build().RunAsync();
    }
}
