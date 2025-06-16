// Existing using statements (implicit for DistributedApplication, Projects)

namespace FlinkDotNetAspire.AppHost.AppHost;

public static class Program
{
    private static string GetKafkaInitializationScript()
    {
        var isStressTest = Environment.GetEnvironmentVariable("STRESS_TEST_MODE")?.ToLowerInvariant() == "true";
        var partitionCount = isStressTest ? 20 : 4; // Use 20 partitions for stress test to utilize all TaskManagers
        
        return $@"
                set -e  # Exit immediately if any command fails
                set -x  # Enable debug output to see what commands are being executed
                
                echo '=== KAFKA INITIALIZATION START ==='
                echo 'Configuration: Stress Test Mode: {isStressTest}, Partition Count: {partitionCount}'
                echo 'Container hostname:' $(hostname)
                echo 'Container IP:' $(hostname -i 2>/dev/null || echo 'IP not available')
                
                # Function to handle script exit and provide diagnostics
                cleanup_and_exit() {{
                    local exit_code=$1
                    echo ""=== KAFKA INITIALIZATION CLEANUP (exit code: $exit_code) ===""
                    
                    if [ $exit_code -eq 0 ]; then
                        echo 'SUCCESS: Kafka initialization completed successfully!'
                        echo 'Final topic verification:'
                        timeout 10 kafka-topics --list --bootstrap-server kafka:9092 2>/dev/null || echo 'Final verification failed'
                    else
                        echo 'ERROR: Kafka initialization failed!'
                        echo 'Diagnostics:'
                        echo 'Network status:' 
                        timeout 5 netstat -tuln 2>/dev/null || echo 'netstat not available'
                        echo 'Kafka process status:'
                        timeout 5 ps aux | grep kafka 2>/dev/null || echo 'ps not available'
                    fi
                    
                    echo '=== KAFKA INITIALIZATION END ==='
                    exit $exit_code
                }}
                
                # Set up trap to handle script termination
                trap 'cleanup_and_exit $?' EXIT
                
                # Test hostname resolution first
                echo 'Step 1: Testing hostname resolution...'
                if ! timeout 5 nslookup kafka >/dev/null 2>&1; then
                    echo 'WARNING: Cannot resolve kafka hostname'
                    echo 'Attempting to continue with IP resolution...'
                    # Try to get kafka container IP from /etc/hosts
                    if grep -q kafka /etc/hosts; then
                        echo 'Found kafka in /etc/hosts:'
                        grep kafka /etc/hosts
                    else
                        echo 'Kafka not found in /etc/hosts'
                    fi
                fi
                
                # Test basic connectivity to Kafka port
                echo 'Step 2: Testing Kafka port connectivity...'
                max_connectivity_attempts=20  # Reduced from 30 to fail faster
                connectivity_attempt=0
                
                until timeout 3 bash -c '</dev/tcp/kafka/9092' >/dev/null 2>&1; do
                    connectivity_attempt=$((connectivity_attempt + 1))
                    if [ $connectivity_attempt -ge $max_connectivity_attempts ]; then
                        echo ""ERROR: Cannot reach Kafka at kafka:9092 after $max_connectivity_attempts attempts""
                        echo 'Network troubleshooting:'
                        timeout 5 ping -c 2 kafka 2>/dev/null || echo 'Ping to kafka failed'
                        timeout 5 telnet kafka 9092 </dev/null 2>/dev/null || echo 'Telnet to kafka:9092 failed'
                        cleanup_and_exit 1
                    fi
                    echo ""Kafka port not reachable yet, waiting... (attempt $connectivity_attempt/$max_connectivity_attempts)""
                    sleep 3  # Reduced sleep time
                done
                
                echo 'SUCCESS: Kafka port is reachable!'
                
                # Test Kafka API readiness with enhanced error handling
                echo 'Step 3: Testing Kafka API readiness...'
                max_api_attempts=15  # Reduced from 20
                api_attempt=0
                
                while [ $api_attempt -lt $max_api_attempts ]; do
                    api_attempt=$((api_attempt + 1))
                    echo ""Testing Kafka API (attempt $api_attempt/$max_api_attempts)...""
                    
                    # Use timeout to prevent hanging and capture output
                    if api_output=$(timeout 15 kafka-topics --bootstrap-server kafka:9092 --list 2>&1); then
                        echo 'SUCCESS: Kafka API is ready!'
                        echo ""API test output: $api_output""
                        break
                    else
                        echo ""Kafka API test failed, output: $api_output""
                        
                        if [ $api_attempt -ge $max_api_attempts ]; then
                            echo ""ERROR: Kafka API failed to become ready after $max_api_attempts attempts""
                            cleanup_and_exit 1
                        fi
                        
                        echo ""Kafka API not ready yet, waiting... (attempt $api_attempt/$max_api_attempts)""
                        sleep 4
                    fi
                done
                
                echo 'Step 4: Creating topics for Flink.Net development...'
                
                # Function to create topic with error handling
                create_topic_safe() {{
                    local topic_name=$1
                    local partitions=$2
                    local retention_ms=$3
                    local segment_ms=$4
                    
                    echo ""Creating topic: $topic_name (partitions: $partitions)...""
                    
                    if topic_result=$(timeout 45 kafka-topics --create --if-not-exists --bootstrap-server kafka:9092 --topic ""$topic_name"" --partitions ""$partitions"" --replication-factor 1 --config retention.ms=""$retention_ms"" --config cleanup.policy=delete --config min.insync.replicas=1 --config segment.ms=""$segment_ms"" 2>&1); then
                        echo ""SUCCESS: Topic $topic_name created""
                        echo ""Result: $topic_result""
                    else
                        echo ""WARNING: Topic $topic_name creation may have failed""
                        echo ""Result: $topic_result""
                        
                        # Check if topic exists anyway (--if-not-exists might cause 'already exists' message)
                        if timeout 10 kafka-topics --describe --bootstrap-server kafka:9092 --topic ""$topic_name"" >/dev/null 2>&1; then
                            echo ""Topic $topic_name exists despite creation warning""
                        else
                            echo ""ERROR: Topic $topic_name does not exist after creation attempt""
                            cleanup_and_exit 1
                        fi
                    fi
                }}
                
                # Create all topics with enhanced error handling
                create_topic_safe 'business-events' '{partitionCount}' '3600000' '60000'
                create_topic_safe 'processed-events' '{partitionCount}' '3600000' '60000'
                create_topic_safe 'analytics-events' '2' '3600000' '60000'
                create_topic_safe 'dead-letter-queue' '2' '3600000' '60000'
                create_topic_safe 'test-input' '2' '1800000' '30000'
                create_topic_safe 'test-output' '2' '1800000' '30000'
                
                # Create the critical flinkdotnet.sample.topic with extra validation
                echo 'Step 5: Creating critical flinkdotnet.sample.topic...'
                create_topic_safe 'flinkdotnet.sample.topic' '{partitionCount}' '3600000' '60000'
                
                # Verify the critical topic exists
                echo 'Step 6: Verifying flinkdotnet.sample.topic exists...'
                if timeout 20 kafka-topics --describe --bootstrap-server kafka:9092 --topic flinkdotnet.sample.topic >/dev/null 2>&1; then
                    echo 'SUCCESS: flinkdotnet.sample.topic verified to exist!'
                    
                    # Get detailed topic information
                    echo 'Topic details:'
                    timeout 15 kafka-topics --describe --bootstrap-server kafka:9092 --topic flinkdotnet.sample.topic 2>/dev/null || echo 'Could not get topic details'
                else
                    echo 'ERROR: flinkdotnet.sample.topic verification failed!'
                    echo 'Available topics:'
                    timeout 15 kafka-topics --list --bootstrap-server kafka:9092 2>/dev/null || echo 'Could not list topics'
                    cleanup_and_exit 1
                fi
                
                echo 'Step 7: Final verification of all topics...'
                echo 'All topics created:'
                if topic_list=$(timeout 20 kafka-topics --list --bootstrap-server kafka:9092 2>&1); then
                    echo ""$topic_list""
                    
                    # Verify flinkdotnet.sample.topic is in the list
                    if echo ""$topic_list"" | grep -q 'flinkdotnet.sample.topic'; then
                        echo 'SUCCESS: flinkdotnet.sample.topic found in topic list!'
                    else
                        echo 'ERROR: flinkdotnet.sample.topic not found in topic list!'
                        cleanup_and_exit 1
                    fi
                else
                    echo ""WARNING: Could not list topics for final verification: $topic_list""
                fi
                
                # Success exit will be handled by trap
                exit 0
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
