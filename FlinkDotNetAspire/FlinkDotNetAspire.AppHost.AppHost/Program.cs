// Existing using statements (implicit for DistributedApplication, Projects)

namespace FlinkDotNetAspire.AppHost.AppHost;

public static class Program
{
    private static string GetKafkaInitializationScript()
    {
        var isStressTest = Environment.GetEnvironmentVariable("STRESS_TEST_MODE")?.ToLowerInvariant() == "true";
        var partitionCount = isStressTest ? 20 : 4; // Use 20 partitions for stress test to utilize all TaskManagers
        
        return $@"
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
                echo 'Configuration: Stress Test Mode: {isStressTest}, Partition Count: {partitionCount}'
                
                # Create topics with optimized settings for load distribution
                kafka-topics --create --if-not-exists --bootstrap-server kafka:9092 --topic business-events --partitions {partitionCount} --replication-factor 1 --config retention.ms=3600000 --config cleanup.policy=delete --config min.insync.replicas=1 --config segment.ms=60000
                kafka-topics --create --if-not-exists --bootstrap-server kafka:9092 --topic processed-events --partitions {partitionCount} --replication-factor 1 --config retention.ms=3600000 --config cleanup.policy=delete --config min.insync.replicas=1 --config segment.ms=60000
                kafka-topics --create --if-not-exists --bootstrap-server kafka:9092 --topic analytics-events --partitions 2 --replication-factor 1 --config retention.ms=3600000 --config cleanup.policy=delete --config min.insync.replicas=1 --config segment.ms=60000
                kafka-topics --create --if-not-exists --bootstrap-server kafka:9092 --topic dead-letter-queue --partitions 2 --replication-factor 1 --config retention.ms=3600000 --config cleanup.policy=delete --config min.insync.replicas=1 --config segment.ms=60000
                kafka-topics --create --if-not-exists --bootstrap-server kafka:9092 --topic test-input --partitions 2 --replication-factor 1 --config retention.ms=1800000 --config cleanup.policy=delete --config segment.ms=30000
                kafka-topics --create --if-not-exists --bootstrap-server kafka:9092 --topic test-output --partitions 2 --replication-factor 1 --config retention.ms=1800000 --config cleanup.policy=delete --config segment.ms=30000
                kafka-topics --create --if-not-exists --bootstrap-server kafka:9092 --topic flinkdotnet.sample.topic --partitions {partitionCount} --replication-factor 1 --config retention.ms=3600000 --config cleanup.policy=delete --config segment.ms=60000
                
                echo 'Kafka topics created successfully!'
                echo 'Topic list:'
                kafka-topics --list --bootstrap-server kafka:9092
                echo 'Kafka topic details:'
                kafka-topics --describe --bootstrap-server kafka:9092 --topic flinkdotnet.sample.topic
                echo 'Kafka initialization completed successfully!'
            ";
    }

    public static async Task Main(string[] args)
    {
        var builder = DistributedApplication.CreateBuilder(args);

        // Add Redis and Kafka infrastructure
        var redis = AddRedisInfrastructure(builder);
        var kafka = AddKafkaInfrastructure(builder);
        
        // Add Kafka initialization container
        var kafkaInit = AddKafkaInitialization(builder, kafka);
        
        // Add Kafka UI for local development only
        AddKafkaUIForLocalDevelopment(builder, kafka);

        // Configure Flink cluster based on environment
        var simulatorNumMessages = ConfigureFlinkCluster(builder);
        
        // Add and configure FlinkJobSimulator
        ConfigureFlinkJobSimulator(builder, redis, kafka, simulatorNumMessages, kafkaInit);

        await builder.Build().RunAsync();
    }

    private static IResourceBuilder<RedisResource> AddRedisInfrastructure(IDistributedApplicationBuilder builder)
    {
        // Set a fixed password for CI/CD consistency and authentication
        var redisPassword = "FlinkDotNet_Redis_CI_Password_2024";
        
        return builder.AddRedis("redis")
            .WithEnvironment("REDIS_PASSWORD", redisPassword)
            .WithEnvironment("REDIS_ARGS", "--requirepass " + redisPassword)
            .PublishAsContainer(); // Ensure Redis is accessible from host
    }

    private static IResourceBuilder<KafkaServerResource> AddKafkaInfrastructure(IDistributedApplicationBuilder builder)
    {
        var isCI = IsRunningInCI();
        return builder.AddKafka("kafka")
            .WithEnvironment("KAFKA_AUTO_CREATE_TOPICS_ENABLE", "true")
            .WithEnvironment("KAFKA_NUM_PARTITIONS", isCI ? "4" : "8") // Reduced for CI
            .WithEnvironment("KAFKA_DEFAULT_REPLICATION_FACTOR", "1")
            .WithEnvironment("KAFKA_LOG_RETENTION_HOURS", isCI ? "1" : "168") // Reduced for CI
            .WithEnvironment("KAFKA_LOG_SEGMENT_BYTES", isCI ? "104857600" : "1073741824") // 100MB for CI, 1GB for local
            .WithEnvironment("KAFKA_MESSAGE_MAX_BYTES", isCI ? "1048576" : "10485760") // 1MB for CI, 10MB for local
            .WithEnvironment("KAFKA_REPLICA_FETCH_MAX_BYTES", isCI ? "1048576" : "10485760") // 1MB for CI, 10MB for local
            .WithEnvironment("KAFKA_GROUP_INITIAL_REBALANCE_DELAY_MS", "0")
            .WithEnvironment("KAFKA_OFFSETS_TOPIC_REPLICATION_FACTOR", "1") // CI compatibility
            .WithEnvironment("KAFKA_TRANSACTION_STATE_LOG_REPLICATION_FACTOR", "1") // CI compatibility
            .WithEnvironment("KAFKA_TRANSACTION_STATE_LOG_MIN_ISR", "1") // CI compatibility
            .WithEnvironment("KAFKA_LOG_FLUSH_INTERVAL_MESSAGES", isCI ? "1000" : "10000") // Faster flushing for CI
            .WithEnvironment("KAFKA_LOG_FLUSH_INTERVAL_MS", isCI ? "1000" : "5000") // Faster flushing for CI
            .PublishAsContainer(); // Ensure Kafka is accessible from host
    }

    private static IResourceBuilder<ContainerResource> AddKafkaInitialization(IDistributedApplicationBuilder builder, IResourceBuilder<KafkaServerResource> kafka)
    {
        var kafkaInitScript = GetKafkaInitializationScript();
        return builder.AddContainer("kafka-init", "confluentinc/cp-kafka", "7.4.0")
            .WithArgs("bash", "-c", kafkaInitScript)
            .WaitFor(kafka);
    }

    private static void AddKafkaUIForLocalDevelopment(IDistributedApplicationBuilder builder, IResourceBuilder<KafkaServerResource> kafka)
    {
        var isCI = IsRunningInCI();
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
    }

    private static string ConfigureFlinkCluster(IDistributedApplicationBuilder builder)
    {
        var isCI = IsRunningInCI();
        var simulatorNumMessages = GetSimulatorMessageCount();
        var taskManagerCount = isCI ? 5 : 20; // Reduce TaskManager count in CI for resource efficiency

        // Add JobManager (1 instance)
        var jobManager = builder.AddProject<Projects.FlinkDotNet_JobManager>("jobmanager")
            .WithEnvironment("DOTNET_ENVIRONMENT", "Development")
            .WithEnvironment("ASPNETCORE_ENVIRONMENT", "Development");

        // Add TaskManagers
        for (int i = 1; i <= taskManagerCount; i++)
        {
            builder.AddProject<Projects.FlinkDotNet_TaskManager>($"taskmanager{i}")
                .WithEnvironment("TaskManagerId", $"TM-{i.ToString("D2")}")
                .WithEnvironment("DOTNET_ENVIRONMENT", "Development")
                .WithEnvironment("ASPNETCORE_ENVIRONMENT", "Development")
                .WithEnvironment("JOBMANAGER_GRPC_ADDRESS", jobManager.GetEndpoint("https"));
        }

        return simulatorNumMessages;
    }

    private static void ConfigureFlinkJobSimulator(IDistributedApplicationBuilder builder, 
        IResourceBuilder<RedisResource> redis, 
        IResourceBuilder<KafkaServerResource> kafka, 
        string simulatorNumMessages,
        IResourceBuilder<ContainerResource> kafkaInit)
    {
        // Set the Redis password to match the Redis infrastructure configuration
        var redisPassword = "FlinkDotNet_Redis_CI_Password_2024";
        
        // Check if we should use simplified mode
        var useSimplifiedMode = Environment.GetEnvironmentVariable("USE_SIMPLIFIED_MODE")?.ToLowerInvariant() == "true" ||
                               Environment.GetEnvironmentVariable("CI")?.ToLowerInvariant() == "true" ||
                               Environment.GetEnvironmentVariable("GITHUB_ACTIONS")?.ToLowerInvariant() == "true";
        
        // Check if we should use Kafka source for TaskManager load testing
        var useKafkaSource = Environment.GetEnvironmentVariable("STRESS_TEST_USE_KAFKA_SOURCE")?.ToLowerInvariant() == "true";
        
        Console.WriteLine($"🔍 APPHOST CONFIG: USE_SIMPLIFIED_MODE={Environment.GetEnvironmentVariable("USE_SIMPLIFIED_MODE")}");
        Console.WriteLine($"🔍 APPHOST CONFIG: CI={Environment.GetEnvironmentVariable("CI")}");
        Console.WriteLine($"🔍 APPHOST CONFIG: GITHUB_ACTIONS={Environment.GetEnvironmentVariable("GITHUB_ACTIONS")}");
        Console.WriteLine($"🔍 APPHOST CONFIG: Final useSimplifiedMode: {useSimplifiedMode}");
        Console.WriteLine($"🔍 APPHOST CONFIG: useKafkaSource: {useKafkaSource}");
        
        var flinkJobSimulator = builder.AddProject<Projects.FlinkJobSimulator>("flinkjobsimulator")
            .WithReference(redis) // Makes "ConnectionStrings__redis" available
            .WithEnvironment("SIMULATOR_NUM_MESSAGES", simulatorNumMessages)
            .WithEnvironment("SIMULATOR_REDIS_KEY_SINK_COUNTER", "flinkdotnet:sample:processed_message_counter")
            .WithEnvironment("SIMULATOR_REDIS_KEY_GLOBAL_SEQUENCE", "flinkdotnet:global_sequence_id")
            .WithEnvironment("SIMULATOR_KAFKA_TOPIC", "flinkdotnet.sample.topic")
            .WithEnvironment("SIMULATOR_REDIS_PASSWORD", redisPassword) // Add password for FlinkJobSimulator
            .WithEnvironment("DOTNET_ENVIRONMENT", "Development")
            .WaitFor(redis); // Always wait for Redis since we need it even in simplified mode
            
        // Only add Kafka dependencies if not in simplified mode
        if (!useSimplifiedMode)
        {
            Console.WriteLine("🔄 STANDARD MODE: Adding Kafka dependencies");
            flinkJobSimulator
                .WithReference(kafka) // Makes "ConnectionStrings__kafka" available for bootstrap servers
                .WaitFor(kafka) // Wait for Kafka to be ready
                .WaitFor(kafkaInit); // Wait for Kafka initialization (topics created) to complete
        }
        else
        {
            Console.WriteLine("🎯 SIMPLIFIED MODE: Skipping Kafka dependencies for reliable execution");
        }
            
        // Pass simplified mode flag if set
        var useSimplifiedModeEnv = Environment.GetEnvironmentVariable("USE_SIMPLIFIED_MODE");
        if (!string.IsNullOrEmpty(useSimplifiedModeEnv))
        {
            Console.WriteLine($"🎯 SIMPLIFIED MODE: Enabling simplified mode in FlinkJobSimulator: {useSimplifiedModeEnv}");
            flinkJobSimulator.WithEnvironment("USE_SIMPLIFIED_MODE", useSimplifiedModeEnv);
        }
            
        // Enable Kafka source mode for TaskManager load distribution testing if requested (only in non-simplified mode)
        if (useKafkaSource && !useSimplifiedMode)
        {
            Console.WriteLine("🔄 STRESS TEST CONFIG: Enabling Kafka source mode for TaskManager load distribution testing");
            flinkJobSimulator.WithEnvironment("SIMULATOR_USE_KAFKA_SOURCE", "true");
            flinkJobSimulator.WithEnvironment("SIMULATOR_KAFKA_CONSUMER_GROUP", "flinkdotnet-stress-test-consumer-group");
        }
        else if (useKafkaSource && useSimplifiedMode)
        {
            Console.WriteLine("🎯 SIMPLIFIED MODE: Kafka source mode requested but disabled due to simplified mode");
        }
    }

    private static bool IsRunningInCI()
    {
        return Environment.GetEnvironmentVariable("CI") == "true" || Environment.GetEnvironmentVariable("GITHUB_ACTIONS") == "true";
    }

    private static string GetSimulatorMessageCount()
    {
        var simulatorNumMessages = Environment.GetEnvironmentVariable("SIMULATOR_NUM_MESSAGES");
        if (string.IsNullOrEmpty(simulatorNumMessages))
        {
            // Default to 10 million messages for both CI and local
            simulatorNumMessages = "10000000";
        }
        return simulatorNumMessages;
    }
}
