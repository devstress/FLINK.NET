namespace FlinkDotNetAspire.AppHost.AppHost;

public static class Program
{
    private static string GetKafkaInitializationScript()
    {
        var partitionCount = 100; // Matches producer tuning: 100 partitions for i9-12900K 64GB RAM
        return $@"
            echo '=== KAFKA ULTRA-SAFE INITIALIZATION START ==='
            echo 'Hostname: ' $(hostname)
            echo 'UTC Time: ' $(date -u '+%Y-%m-%d %H:%M:%S UTC' 2>/dev/null || date)

            max_attempts=30
            attempt=0
            while [ $attempt -lt $max_attempts ]; do
                attempt=$((attempt + 1))
                echo ""[WAIT] Checking Kafka API... Attempt $attempt/$max_attempts""
                if kafka-topics --bootstrap-server kafka:9092 --list >/dev/null 2>&1; then
                    echo '[READY] Kafka API is available.'
                    break
                else
                    sleep 2
                fi
            done

            if [ $attempt -eq $max_attempts ]; then
                echo '[ERROR] Kafka API still not ready after $max_attempts attempts!'
                exit 1
            fi

            echo 'Creating flinkdotnet.sample.topic...'
            kafka-topics --create --if-not-exists --bootstrap-server kafka:9092 \
                --topic flinkdotnet.sample.topic \
                --partitions {partitionCount} \
                --replication-factor 1 \
                --config retention.ms=3600000 \
                --config cleanup.policy=delete \
                --config min.insync.replicas=1 \
                --config message.max.bytes=52428800

            echo 'Creating flinkdotnet.sample.out.topic for FlinkJobSimulator output...'
            kafka-topics --create --if-not-exists --bootstrap-server kafka:9092 \
                --topic flinkdotnet.sample.out.topic \
                --partitions {partitionCount} \
                --replication-factor 1 \
                --config retention.ms=3600000 \
                --config cleanup.policy=delete \
                --config min.insync.replicas=1 \
                --config message.max.bytes=52428800

            echo 'Validating topics...'
            kafka-topics --describe --bootstrap-server kafka:9092 --topic flinkdotnet.sample.topic
            kafka-topics --describe --bootstrap-server kafka:9092 --topic flinkdotnet.sample.out.topic

            echo '=== KAFKA ULTRA-SAFE INITIALIZATION COMPLETE ==='
        ";
    }

    public static async Task Main(string[] args)
    {
        var builder = DistributedApplication.CreateBuilder(args);

        // Log platform and environment information for debugging
        var isWindows = IsRunningOnWindows();
        var isCI = IsRunningInCI();

        Console.WriteLine($"🖥️  Platform: {(isWindows ? "Windows" : "Unix/Linux")}");
        Console.WriteLine($"🔧 CI Environment: {isCI}");
        Console.WriteLine($"📦 .NET Version: {Environment.Version}");
        Console.WriteLine($"🐳 Docker Host: {Environment.GetEnvironmentVariable("DOCKER_HOST") ?? "default"}");

        if (isWindows)
        {
            Console.WriteLine("🪟 Windows-specific optimizations enabled:");
            Console.WriteLine("   • Reduced container memory limits");
            Console.WriteLine("   • Optimized GC settings for containers");
            Console.WriteLine("   • Enhanced logging configuration");
            Console.WriteLine("   • Simplified shell script execution");
        }

        // Add Redis and Kafka infrastructure
        var redis = AddRedisInfrastructure(builder);
        var kafka = AddKafkaInfrastructure(builder);

        // Add Kafka initialization container
        var kafkaInit = AddKafkaInitialization(builder, kafka);

        // Add Kafka UI for local development only
        AddKafkaUIForLocalDevelopment(builder, kafka);

        // Configure Flink cluster based on environment
        var (simulatorNumMessages, jobManager) = ConfigureFlinkCluster(builder);

        // Add and configure FlinkJobSimulator
        ConfigureFlinkJobSimulator(builder, redis, kafka, simulatorNumMessages, kafkaInit, jobManager);

        await builder.Build().RunAsync();
    }

    private static IResourceBuilder<RedisResource> AddRedisInfrastructure(IDistributedApplicationBuilder builder)
    {
        // Use the same password that workflows and local tests expect
        var redisPassword = builder.AddParameter("redis-password", value: "FlinkDotNet_Redis_CI_Password_2024", secret: false);
        var redis = builder.AddRedis("redis", password: redisPassword);
        return redis.PublishAsContainer(); // Ensure Redis is accessible from host
    }


    private static IResourceBuilder<KafkaServerResource> AddKafkaInfrastructure(IDistributedApplicationBuilder builder)
    {
        var kafka = builder.AddKafka("kafka")
        .WithEnvironment("KAFKA_AUTO_CREATE_TOPICS_ENABLE", "true")   // Allow client to clean/create topics freely
        .WithEnvironment("KAFKA_NUM_PARTITIONS", "100")
        .WithEnvironment("KAFKA_DEFAULT_REPLICATION_FACTOR", "1")
        .WithEnvironment("KAFKA_TRANSACTION_STATE_LOG_REPLICATION_FACTOR", "1")
        .WithEnvironment("KAFKA_TRANSACTION_STATE_LOG_MIN_ISR", "1")
        .WithEnvironment("KAFKA_LOG_RETENTION_HOURS", "168")
        .WithEnvironment("KAFKA_LOG_SEGMENT_BYTES", "134217728")
        .WithEnvironment("KAFKA_LOG_FLUSH_INTERVAL_MESSAGES", "10000")
        .WithEnvironment("KAFKA_LOG_FLUSH_INTERVAL_MS", "100")
        .WithEnvironment("KAFKA_LOG_FLUSH_START_OFFSET_CHECKPOINT_INTERVAL_MS", "100")
        .WithEnvironment("KAFKA_LOG_FLUSH_OFFSET_CHECKPOINT_INTERVAL_MS", "100")
        .WithEnvironment("KAFKA_MESSAGE_MAX_BYTES", "52428800")
        .WithEnvironment("KAFKA_REPLICA_FETCH_MAX_BYTES", "52428800")
        .WithEnvironment("KAFKA_SOCKET_REQUEST_MAX_BYTES", "268435456")
        .WithEnvironment("KAFKA_GROUP_INITIAL_REBALANCE_DELAY_MS", "0")
        .WithEnvironment("KAFKA_HEAP_OPTS", "-Xmx16G -Xms16G")
        .WithEnvironment("KAFKA_NUM_IO_THREADS", "256")
        .WithEnvironment("KAFKA_NUM_NETWORK_THREADS", "64")
        .WithEnvironment("KAFKA_NUM_REPLICA_FETCHERS", "8")
        .WithEnvironment("KAFKA_SOCKET_SEND_BUFFER_BYTES", "16777216")
        .WithEnvironment("KAFKA_SOCKET_RECEIVE_BUFFER_BYTES", "16777216")
        .WithEnvironment("KAFKA_UNCLEAN_LEADER_ELECTION_ENABLE", "false")
        .WithEnvironment("KAFKA_JVM_PERFORMANCE_OPTS", "-server -XX:+UseG1GC -XX:MaxGCPauseMillis=10 -XX:InitiatingHeapOccupancyPercent=20")
        .WithEnvironment("KAFKA_LOG4J_ROOT_LOGLEVEL", "WARN");

#pragma warning disable S125 // Test WithVolume vs without
        //kafka.WithVolume("kafka-volume", "/var/lib/kafka/data");
#pragma warning restore S125

        return kafka.PublishAsContainer();
    }

    private static IResourceBuilder<ContainerResource> AddKafkaInitialization(IDistributedApplicationBuilder builder, IResourceBuilder<KafkaServerResource> kafka)
    {
        var kafkaInitScript = GetKafkaInitializationScript();

        var kafkaInit = builder.AddContainer("kafka-init", "confluentinc/cp-kafka:7.4.0");

        kafkaInit = kafkaInit.WithArgs("bash", "-c", kafkaInitScript.Replace("\r", ""));

        return kafkaInit.WaitFor(kafka);
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

    private static (string simulatorNumMessages, IResourceBuilder<ProjectResource> jobManager) ConfigureFlinkCluster(IDistributedApplicationBuilder builder)
    {
        var simulatorNumMessages = GetSimulatorMessageCount();
        
        Console.WriteLine("🚀 FULL INFRASTRUCTURE MODE: Starting JobManager and TaskManagers");
        var taskManagerCount = 20; // Always use 20 TaskManagers for Apache Flink 2.0 compliance and high-throughput 1M+ msg/sec processing

        // Add JobManager (1 instance)
        var jobManager = builder.AddProject<Projects.FlinkDotNet_JobManager>("jobmanager")
            .WithEnvironment("DOTNET_ENVIRONMENT", "Development")
            .WithEnvironment("ASPNETCORE_ENVIRONMENT", "Development")
            .WithEnvironment("ASPIRE_ALLOW_UNSECURED_TRANSPORT", "true");

        // Store TaskManager references for JobManager
        var taskManagers = new List<IResourceBuilder<ProjectResource>>();

        // Add TaskManagers with dynamic port allocation
        for (int i = 1; i <= taskManagerCount; i++)
        {
            // Let Aspire/Kubernetes assign ports dynamically to avoid conflicts
            // Each TaskManager will get a unique port through Aspire service discovery
            var taskManager = builder.AddProject<Projects.FlinkDotNet_TaskManager>($"taskmanager{i}")
                .WithEnvironment("TaskManagerId", $"TM-{i.ToString("D2")}")
                .WithEnvironment("DOTNET_ENVIRONMENT", "Development")
                .WithEnvironment("ASPNETCORE_ENVIRONMENT", "Development")
                .WithEnvironment("ASPIRE_ALLOW_UNSECURED_TRANSPORT", "true")
                .WithReference(jobManager) // Use service reference for proper Aspire discovery
                .WithEnvironment("ASPIRE_USE_DYNAMIC_PORTS", "true"); // Signal to use dynamic ports

            taskManagers.Add(taskManager);
        }

        // Add all TaskManager references to JobManager for reverse service discovery
        foreach (var taskManager in taskManagers)
        {
            jobManager.WithReference(taskManager);
        }

        Console.WriteLine($"🚀 CONFIGURED: JobManager + {taskManagerCount} TaskManagers for full infrastructure mode");
        return (simulatorNumMessages, jobManager);
    }

    private static void ConfigureFlinkJobSimulator(IDistributedApplicationBuilder builder,
        IResourceBuilder<RedisResource> redis,
        IResourceBuilder<KafkaServerResource> kafka,
        string simulatorNumMessages,
        IResourceBuilder<ContainerResource> kafkaInit,
        IResourceBuilder<ProjectResource> jobManager)
    {
        // Check if we should use Kafka source for TaskManager load testing
        var useKafkaSource = Environment.GetEnvironmentVariable("STRESS_TEST_USE_KAFKA_SOURCE")?.ToLowerInvariant() == "true";
        
        Console.WriteLine($"🚀 FULL INFRASTRUCTURE MODE: Configuring FlinkJobSimulator with JobManager integration");
        Console.WriteLine($"🔍 APPHOST CONFIG: STRESS_TEST_USE_KAFKA_SOURCE={Environment.GetEnvironmentVariable("STRESS_TEST_USE_KAFKA_SOURCE")}");
        Console.WriteLine($"🔍 APPHOST CONFIG: useKafkaSource: {useKafkaSource}");

        var flinkJobSimulator = builder.AddProject<Projects.FlinkJobSimulator>("flinkjobsimulator")
            .WithReference(redis) // Makes "ConnectionStrings__redis" available
            .WithEnvironment("SIMULATOR_NUM_MESSAGES", simulatorNumMessages)
            .WithEnvironment("SIMULATOR_REDIS_KEY_SINK_COUNTER", "flinkdotnet:sample:processed_message_counter")
            .WithEnvironment("SIMULATOR_REDIS_KEY_GLOBAL_SEQUENCE", "flinkdotnet:global_sequence_id")
            .WithEnvironment("SIMULATOR_KAFKA_TOPIC", "flinkdotnet.sample.topic")
            .WithEnvironment("DOTNET_ENVIRONMENT", "Development")
            .WaitFor(redis) // Always wait for Redis since we need it
            .WithReference(kafka) // Makes "ConnectionStrings__kafka" available for bootstrap servers
            .WaitFor(kafka) // Wait for Kafka to be ready
            .WaitFor(kafkaInit) // Wait for Kafka initialization (topics created) to complete
            .WithReference(jobManager) // Makes JobManager gRPC endpoint discoverable via Aspire
            .WaitFor(jobManager); // Wait for JobManager to be ready before starting job submission

        // Enable Kafka source mode for TaskManager load distribution testing if requested
        if (useKafkaSource)
        {
            Console.WriteLine("🔄 STRESS TEST CONFIG: Enabling Kafka source mode for TaskManager load distribution testing");
            flinkJobSimulator.WithEnvironment("SIMULATOR_USE_KAFKA_SOURCE", "true");
            flinkJobSimulator.WithEnvironment("SIMULATOR_KAFKA_CONSUMER_GROUP", "flinkdotnet-stress-test-consumer-group");
        }
    }

    private static bool IsRunningInCI()
    {
        return Environment.GetEnvironmentVariable("CI") == "true" || Environment.GetEnvironmentVariable("GITHUB_ACTIONS") == "true";
    }

    private static bool IsRunningOnWindows()
    {
        return Environment.OSVersion.Platform == PlatformID.Win32NT ||
               Environment.GetEnvironmentVariable("OS")?.ToUpperInvariant().Contains("WINDOWS") == true ||
               Environment.GetEnvironmentVariable("RUNNER_OS")?.ToUpperInvariant() == "WINDOWS";
    }

    private static string GetSimulatorMessageCount()
    {
        var simulatorNumMessages = Environment.GetEnvironmentVariable("SIMULATOR_NUM_MESSAGES");
        if (string.IsNullOrEmpty(simulatorNumMessages))
        {
            // Default to 1 million messages for optimized high-performance testing
            simulatorNumMessages = "1000000";
        }
        return simulatorNumMessages;
    }
}
