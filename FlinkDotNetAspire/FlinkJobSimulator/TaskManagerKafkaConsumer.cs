using Confluent.Kafka;
using Confluent.Kafka.Admin;
using FlinkDotNet.Connectors.Sources.Kafka;
using FlinkDotNet.Core.Abstractions.Sources;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using StackExchange.Redis;

namespace FlinkJobSimulator
{
    /// <summary>
    /// Apache Flink-compliant TaskManager Kafka consumer that utilizes FlinkKafkaConsumerGroup
    /// for proper load distribution across all 20 TaskManagers. Follows Apache Flink 2.0 standards
    /// with checkpoint-based offset management and cooperative sticky partition assignment.
    /// </summary>
    public class TaskManagerKafkaConsumer : BackgroundService, ISourceContext<string>
    {
        private readonly IConfiguration _configuration;
        private readonly ILogger<TaskManagerKafkaConsumer> _logger;
        private readonly IDatabase _redisDatabase;
        private readonly string _kafkaTopic;
        private readonly string _outputTopic;
        private readonly string _redisSinkCounterKey;
        private readonly string _globalSequenceKey;
        private readonly string _taskManagerId;
        private FlinkKafkaConsumerGroup? _consumerGroup;
        private IProducer<Null, byte[]>? _producer;
        private long _messagesProcessed = 0;
        private readonly CancellationTokenSource _cancellationTokenSource = new();

        public TaskManagerKafkaConsumer(
            IConfiguration configuration, 
            ILogger<TaskManagerKafkaConsumer> logger,
            IDatabase redisDatabase)
        {
            _configuration = configuration ?? throw new ArgumentNullException(nameof(configuration));
            _logger = logger ?? throw new ArgumentNullException(nameof(logger));
            _redisDatabase = redisDatabase ?? throw new ArgumentNullException(nameof(redisDatabase));
            
            _kafkaTopic = _configuration["SIMULATOR_KAFKA_TOPIC"] ?? "flinkdotnet.sample.topic";
            _outputTopic = _kafkaTopic.EndsWith(".topic") ? _kafkaTopic.Replace(".topic", ".out.topic") : _kafkaTopic + ".out";
            _redisSinkCounterKey = _configuration["SIMULATOR_REDIS_KEY_SINK_COUNTER"] ?? "flinkdotnet:sample:processed_message_counter";
            _globalSequenceKey = _configuration["SIMULATOR_REDIS_KEY_GLOBAL_SEQUENCE"] ?? "flinkdotnet:global_sequence_id";
            _taskManagerId = Environment.GetEnvironmentVariable("TaskManagerId") ?? "TM-Unknown";
            
            _logger.LogInformation("TaskManagerKafkaConsumer initialized for TaskManager: {TaskManagerId}, Input: {InputTopic}, Output: {OutputTopic}", 
                _taskManagerId, _kafkaTopic, _outputTopic);
        }

        protected override async Task ExecuteAsync(CancellationToken stoppingToken)
        {
            _logger.LogInformation("🚀 TaskManager {TaskManagerId}: Starting Apache Flink 2.0-compliant Kafka consumption with automatic resumption", _taskManagerId);
            
            // Write consumer startup log to file for stress test monitoring
            await WriteConsumerStartupLogAsync();
            
            // Initialize Redis counters to indicate FlinkJobSimulator has started
            try
            {
                _logger.LogInformation("🔄 TaskManager {TaskManagerId}: Initializing Redis counters to indicate startup", _taskManagerId);
                
                // Initialize both Redis keys
                await _redisDatabase.StringSetAsync(_redisSinkCounterKey, 0);
                await _redisDatabase.StringSetAsync(_globalSequenceKey, 0);
                
                _logger.LogInformation("✅ TaskManager {TaskManagerId}: Redis counters initialized successfully - sink: {SinkKey}, global: {GlobalKey}", 
                    _taskManagerId, _redisSinkCounterKey, _globalSequenceKey);
                
                // Update startup log with Redis success
                await UpdateStartupLogAsync("REDIS_CONNECTED", "Redis counters initialized successfully");
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "❌ TaskManager {TaskManagerId}: Failed to initialize Redis counter", _taskManagerId);
                await UpdateStartupLogAsync("REDIS_FAILED", $"Redis initialization failed: {ex.Message}");
                throw new InvalidOperationException($"TaskManager {_taskManagerId}: Error during message consumption. Kafka topic: {_kafkaTopic}", ex);
            }
            
            try
            {
                _logger.LogInformation("🔄 TaskManager {TaskManagerId}: Starting FlinkKafkaConsumerGroup and producer initialization", _taskManagerId);
                
                // CRITICAL FIX: Enhanced producer-consumer coordination timing
                var forceResetToEarliest = _configuration["SIMULATOR_FORCE_RESET_TO_EARLIEST"] ?? "true";
                var shouldForceReset = string.Equals(forceResetToEarliest, "true", StringComparison.OrdinalIgnoreCase);
                
                if (shouldForceReset)
                {
                    // CRITICAL: Add coordination delay and verify messages exist
                    var delaySeconds = 20; // Increased delay for better coordination
                    _logger.LogInformation("⏳ TaskManager {TaskManagerId}: Adding {DelaySeconds}s coordination delay for producer-consumer sync (SIMULATOR_FORCE_RESET_TO_EARLIEST=true)", _taskManagerId, delaySeconds);
                    await Task.Delay(TimeSpan.FromSeconds(delaySeconds), stoppingToken);
                    
                    // CRITICAL FIX: Verify that messages actually exist before starting consumer
                    await VerifyMessagesAvailableInKafka();
                }
                
                await InitializeFlinkKafkaConsumerGroup();
                await InitializeHighPerformanceProducer();
                await UpdateStartupLogAsync("KAFKA_CONSUMING", "FlinkKafkaConsumerGroup and producer started with automatic resumption");
                
                // IMPORTANT: Mark FlinkJobSimulator as actually RUNNING
                _logger.LogInformation("🎯 TaskManager {TaskManagerId}: FlinkJobSimulator is now RUNNING and ready to process messages", _taskManagerId);
                await Program.WriteRunningStateLogAsync();
                await UpdateStartupLogAsync("FlinkJobSimulatorRunning", "FlinkJobSimulator is actively running and processing messages");
                
                _logger.LogInformation("🔄 TaskManager {TaskManagerId}: Starting message consumption with Apache Flink 2.0 patterns", _taskManagerId);
                await ConsumeMessagesWithFlinkPatterns(stoppingToken);
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "❌ TaskManager {_taskManagerId}: Error in Kafka consumption for topic '{_kafkaTopic}'", 
                    _taskManagerId, _kafkaTopic);
                await UpdateStartupLogAsync("KAFKA_FAILED", $"Kafka consumption failed: {ex.Message}");
                
                // Let FlinkKafkaConsumerGroup handle the recovery instead of heartbeat mode
                _logger.LogInformation("🔄 TaskManager {TaskManagerId}: FlinkKafkaConsumerGroup will handle automatic recovery", _taskManagerId);
                throw new InvalidOperationException($"TaskManager {_taskManagerId}: Error during message consumption. Kafka topic: {_kafkaTopic}", ex);
            }
            finally
            {
                // Mark as stopped when exiting
                await UpdateStartupLogAsync("FlinkJobSimulatorStartedByStop", "FlinkJobSimulator was stopped or exited");
                await CleanupResources();
            }
        }
        
        /// <summary>
        /// Find the project root directory by looking for .git directory
        /// </summary>
        private static string FindProjectRoot()
        {
            var currentDir = Directory.GetCurrentDirectory();
            var directory = new DirectoryInfo(currentDir);
            
            // Walk up the directory tree to find the .git directory
            while (directory != null)
            {
                if (Directory.Exists(Path.Combine(directory.FullName, ".git")))
                {
                    return directory.FullName;
                }
                directory = directory.Parent;
            }
            
            // If we can't find .git, fall back to current directory
            return currentDir;
        }
        
        /// <summary>
        /// Write consumer startup information to log file for stress test script to monitor
        /// </summary>
        private async Task WriteConsumerStartupLogAsync()
        {
            try
            {
                var logContent = $@"FLINKJOBSIMULATOR_CONSUMER_LOG
TaskManagerId: {_taskManagerId}
StartTime: {DateTime.UtcNow:yyyy-MM-dd HH:mm:ss} UTC
Topic: {_kafkaTopic}
RedisKey: {_redisSinkCounterKey}
Status: CONSUMER_STARTING
Message: TaskManager Kafka consumer is starting
";
                
                var projectRoot = FindProjectRoot();
                var logPath = Path.Combine(projectRoot, "flinkjobsimulator_consumer.log");
                await File.WriteAllTextAsync(logPath, logContent);
                _logger.LogInformation("📝 CONSUMER LOG: Written to {LogPath}", logPath);
            }
            catch (Exception ex)
            {
                _logger.LogWarning(ex, "⚠️ CONSUMER LOG: Failed to write consumer startup log");
            }
        }
        
        /// <summary>
        /// Update startup log with current status for stress test monitoring
        /// </summary>
        private async Task UpdateStartupLogAsync(string status, string message)
        {
            try
            {
                var logContent = $@"FLINKJOBSIMULATOR_STATUS_UPDATE
TaskManagerId: {_taskManagerId}
UpdateTime: {DateTime.UtcNow:yyyy-MM-dd HH:mm:ss} UTC
Status: {status}
Message: {message}
";
                
                var projectRoot = FindProjectRoot();
                var logPath = Path.Combine(projectRoot, "flinkjobsimulator_status.log");
                await File.WriteAllTextAsync(logPath, logContent);
                _logger.LogInformation("📝 STATUS LOG: {Status} - {Message}", status, message);
            }
            catch (Exception ex)
            {
                _logger.LogWarning(ex, "⚠️ STATUS LOG: Failed to write status update");
            }
        }

        private async Task InitializeFlinkKafkaConsumerGroup()
        {
            // CRITICAL: Use same Kafka discovery method as producer script to ensure compatibility
            string? bootstrapServers = await DiscoverKafkaBootstrapServersLikeProducerScript();
            
            if (string.IsNullOrEmpty(bootstrapServers))
            {
                // Fallback to environment variables if direct discovery fails
                bootstrapServers = _configuration["DOTNET_KAFKA_BOOTSTRAP_SERVERS"];
                if (string.IsNullOrEmpty(bootstrapServers))
                {
                    bootstrapServers = _configuration["ConnectionStrings__kafka"];
                    if (string.IsNullOrEmpty(bootstrapServers))
                    {
                        bootstrapServers = "localhost:9092";
                    }
                }
                
                _logger.LogWarning("⚠️ TaskManager {TaskManagerId}: Using fallback bootstrap servers: {BootstrapServers}", _taskManagerId, bootstrapServers);
            }
            else
            {
                _logger.LogInformation("✅ TaskManager {TaskManagerId}: Discovered Kafka bootstrap servers using producer script method: {BootstrapServers}", _taskManagerId, bootstrapServers);
            }

            // Fix IPv6 issue by forcing IPv4 localhost resolution
            bootstrapServers = bootstrapServers.Replace("localhost", "127.0.0.1");

            // PRODUCTION-GRADE APACHE FLINK PATTERN: Use consistent consumer group for continuous consumption
            // Unlike batch processing, continuous consumers should maintain group membership for proper coordination
            var baseGroupId = "flink-taskmanager-consumer-group";
            var forceResetToEarliest = _configuration["SIMULATOR_FORCE_RESET_TO_EARLIEST"] ?? "true";
            var shouldForceReset = string.Equals(forceResetToEarliest, "true", StringComparison.OrdinalIgnoreCase);
            
            // Use stable consumer group ID for production-grade continuous consumption
            // Only create unique ID if explicitly testing isolated consumption scenarios
            var consumerGroupId = baseGroupId;
            
            // PRODUCTION-GRADE PATTERN: Reset consumer group offsets if requested for fresh consumption
            if (shouldForceReset)
            {
                _logger.LogInformation("🔄 TaskManager {TaskManagerId}: Resetting consumer group offsets to earliest for fresh consumption", _taskManagerId);
                await ResetConsumerGroupOffsetsToEarliest(bootstrapServers, consumerGroupId);
            }
                
            _logger.LogInformation("🔄 TaskManager {TaskManagerId}: Using stable consumer group ID: {GroupId} for production-grade continuous consumption (ForceReset: {ForceReset})", 
                _taskManagerId, consumerGroupId, shouldForceReset);

            // CRITICAL FIX: Enhanced consumer configuration with Apache Flink 2.0 optimizations
            var consumerConfig = new ConsumerConfig
            {
                BootstrapServers = bootstrapServers,
                GroupId = consumerGroupId, 
                SecurityProtocol = SecurityProtocol.Plaintext,
                
                // CRITICAL: Ensure we start from earliest when resetting
                EnableAutoCommit = false, // Flink manages offsets through checkpoints
                AutoOffsetReset = AutoOffsetReset.Earliest, // Always start from beginning for new consumer groups
                
                // CRITICAL FIX: Optimize session timeouts for faster startup
                SessionTimeoutMs = 10000,  // Reduced from 30s to 10s for faster recovery
                HeartbeatIntervalMs = 3000, // Reduced from 10s to 3s for better responsiveness
                MaxPollIntervalMs = 300000, // Keep 5 minutes for processing
                
                // CRITICAL: Use cooperative sticky for better load balancing
                PartitionAssignmentStrategy = PartitionAssignmentStrategy.CooperativeSticky,
                
                // CRITICAL FIX: Performance settings optimized for high-throughput
                FetchMinBytes = 1024,      // Increased from 1 byte to reduce polling overhead
                FetchWaitMaxMs = 50,       // Reduced from 100ms for lower latency
                FetchMaxBytes = 52428800,  // 50MB max fetch
                MaxPartitionFetchBytes = 1048576, // 1MB per partition
                
                // CRITICAL: Network optimization
                SocketTimeoutMs = 10000,
                MetadataMaxAgeMs = 300000, // 5 minutes
            };

            _logger.LogInformation("🔄 TaskManager {TaskManagerId}: Initializing FlinkKafkaConsumerGroup with servers: {BootstrapServers}", 
                _taskManagerId, bootstrapServers);

            // CRITICAL FIX: Add topic verification before consumer initialization
            await VerifyTopicExistsAndHasMessages(bootstrapServers);

            // Simple initialization - FlinkKafkaConsumerGroup now handles resumption internally
            _consumerGroup = new FlinkKafkaConsumerGroup(consumerConfig, _logger);
            await _consumerGroup.InitializeAsync(new[] { _kafkaTopic });

            _logger.LogInformation("✅ TaskManager {TaskManagerId}: FlinkKafkaConsumerGroup initialized with built-in resumption", _taskManagerId);
            
            // CRITICAL FIX: Verify consumer group assignment after initialization
            await VerifyConsumerGroupAssignment();
        }
        
        /// <summary>
        /// Verify that the topic exists and has messages before starting consumption
        /// </summary>
        private async Task VerifyTopicExistsAndHasMessages(string bootstrapServers)
        {
            try
            {
                _logger.LogInformation("🔍 TaskManager {TaskManagerId}: Verifying topic {Topic} exists and has messages", _taskManagerId, _kafkaTopic);
                
                var adminConfig = new AdminClientConfig 
                { 
                    BootstrapServers = bootstrapServers,
                    SecurityProtocol = SecurityProtocol.Plaintext,
                    SocketTimeoutMs = 10000,
                    ApiVersionRequestTimeoutMs = 10000
                };
                
                using var admin = new AdminClientBuilder(adminConfig).Build();
                
                // Get topic metadata  
                var metadata = admin.GetMetadata(TimeSpan.FromSeconds(15));
                var topicMetadata = metadata.Topics.FirstOrDefault(t => t.Topic == _kafkaTopic);
                
                if (topicMetadata != null)
                {
                    _logger.LogInformation("✅ TaskManager {TaskManagerId}: Topic {Topic} found with {PartitionCount} partitions", 
                        _taskManagerId, _kafkaTopic, topicMetadata.Partitions.Count);
                    
                    // Log partition details
                    foreach (var partition in topicMetadata.Partitions)
                    {
                        _logger.LogInformation("  📍 Partition {PartitionId}: Leader={Leader}, Error={Error}", 
                            partition.PartitionId, partition.Leader, partition.Error?.Code ?? ErrorCode.NoError);
                    }
                    
                    // CRITICAL: Check if topic has any messages using a simple consumer
                    await CheckTopicMessageCount(bootstrapServers);
                }
                else
                {
                    _logger.LogError("❌ TaskManager {TaskManagerId}: Topic {Topic} not found in Kafka metadata!", _taskManagerId, _kafkaTopic);
                    throw new InvalidOperationException($"Topic {_kafkaTopic} does not exist");
                }
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "❌ TaskManager {TaskManagerId}: Failed to verify topic {Topic}", _taskManagerId, _kafkaTopic);
                throw;
            }
        }
        
        /// <summary>
        /// Check if the topic has messages by attempting to consume with a temporary consumer
        /// </summary>
        private async Task CheckTopicMessageCount(string bootstrapServers)
        {
            try
            {
                _logger.LogInformation("🔍 TaskManager {TaskManagerId}: Checking if topic {Topic} has messages", _taskManagerId, _kafkaTopic);
                
                var tempConsumerConfig = new ConsumerConfig
                {
                    BootstrapServers = bootstrapServers,
                    GroupId = $"temp-message-check-{Guid.NewGuid()}",
                    AutoOffsetReset = AutoOffsetReset.Earliest,
                    EnableAutoCommit = false,
                    SessionTimeoutMs = 6000,
                    SocketTimeoutMs = 5000
                };
                
                using var tempConsumer = new ConsumerBuilder<Ignore, byte[]>(tempConsumerConfig).Build();
                tempConsumer.Subscribe(_kafkaTopic);
                
                // Wait for assignment
                await Task.Delay(3000);
                
                // Try to consume a few messages
                int messageCount = 0;
                var timeout = TimeSpan.FromSeconds(10);
                var startTime = DateTime.UtcNow;
                
                while ((DateTime.UtcNow - startTime) < timeout && messageCount < 5)
                {
                    try
                    {
                        var result = tempConsumer.Consume(TimeSpan.FromSeconds(1));
                        if (result?.Message != null)
                        {
                            messageCount++;
                            _logger.LogInformation("  ✅ Found message {MessageCount} in partition {Partition} at offset {Offset}", 
                                messageCount, result.Partition.Value, result.Offset.Value);
                        }
                    }
                    catch (ConsumeException ex)
                    {
                        _logger.LogDebug("Consume attempt resulted in: {Error}", ex.Error.Reason);
                    }
                }
                
                if (messageCount > 0)
                {
                    _logger.LogInformation("✅ TaskManager {TaskManagerId}: Topic {Topic} has messages available - found {MessageCount} messages", 
                        _taskManagerId, _kafkaTopic, messageCount);
                }
                else
                {
                    _logger.LogWarning("⚠️ TaskManager {TaskManagerId}: Topic {Topic} appears to have no messages available for consumption", 
                        _taskManagerId, _kafkaTopic);
                }
            }
            catch (Exception ex)
            {
                _logger.LogWarning(ex, "⚠️ TaskManager {TaskManagerId}: Could not check topic message count for {Topic}", _taskManagerId, _kafkaTopic);
            }
        }
        
        /// <summary>
        /// Verify that the consumer group gets proper partition assignment
        /// </summary>
        private async Task VerifyConsumerGroupAssignment()
        {
            try
            {
                // Wait for consumer group rebalance to complete
                await Task.Delay(5000);
                
                var assignment = _consumerGroup!.GetAssignment();
                _logger.LogInformation("🔍 TaskManager {TaskManagerId}: Consumer group assignment verification:", _taskManagerId);
                _logger.LogInformation("  📊 Assigned {PartitionCount} partitions: {Partitions}", 
                    assignment.Count, string.Join(", ", assignment.Select(tp => $"{tp.Topic}:{tp.Partition}")));
                
                if (assignment.Count == 0)
                {
                    _logger.LogError("❌ TaskManager {TaskManagerId}: Consumer group has NO partition assignments - this will prevent message consumption", _taskManagerId);
                    throw new InvalidOperationException("Consumer group failed to get partition assignments");
                }
                else
                {
                    _logger.LogInformation("✅ TaskManager {TaskManagerId}: Consumer group has valid partition assignments", _taskManagerId);
                }
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "❌ TaskManager {TaskManagerId}: Failed to verify consumer group assignment", _taskManagerId);
                throw;
            }
        }

        private async Task InitializeHighPerformanceProducer()
        {
            // Multi-strategy Kafka bootstrap server discovery
            string? bootstrapServers = _configuration["DOTNET_KAFKA_BOOTSTRAP_SERVERS"];
            if (string.IsNullOrEmpty(bootstrapServers))
            {
                bootstrapServers = _configuration["ConnectionStrings__kafka"];
                if (string.IsNullOrEmpty(bootstrapServers))
                {
                    bootstrapServers = "localhost:9092";
                }
            }

            // Fix IPv6 issue by forcing IPv4 localhost resolution
            bootstrapServers = bootstrapServers.Replace("localhost", "127.0.0.1");

            // High-performance producer configuration based on produce-1-million-messages.ps1
            var producerConfig = new ProducerConfig
            {
                BootstrapServers = bootstrapServers,
                SecurityProtocol = SecurityProtocol.Plaintext,
                
                // High-performance settings from produce-1-million-messages.ps1
                Acks = Acks.None,                              // Maximum speed, no delivery confirmation
                LingerMs = 2,                                   // Micro-batching for performance  
                BatchSize = 524288,                             // 512KB batch size
                CompressionType = CompressionType.None,         // No compression for speed
                QueueBufferingMaxKbytes = 64 * 1024 * 1024,     // 64MB internal buffer
                QueueBufferingMaxMessages = 20_000_000,         // 20M message buffer capacity
                SocketTimeoutMs = 60000,                        // 60s socket timeout
                SocketKeepaliveEnable = true,                   // TCP keepalive
                EnableDeliveryReports = false,                  // No delivery reports for performance
                ClientId = $"flinkjobsim-producer-{_taskManagerId}"
            };

            _logger.LogInformation("🔄 TaskManager {TaskManagerId}: Initializing high-performance producer for output topic: {OutputTopic}", 
                _taskManagerId, _outputTopic);

            _producer = new ProducerBuilder<Null, byte[]>(producerConfig)
                .SetKeySerializer(Serializers.Null)
                .SetValueSerializer(Serializers.ByteArray)
                .Build();
            
            // Create output topic if needed
            await EnsureTopicExistsAsync(bootstrapServers, _outputTopic);

            _logger.LogInformation("✅ TaskManager {TaskManagerId}: High-performance producer initialized for topic: {OutputTopic}", 
                _taskManagerId, _outputTopic);
        }

        private async Task EnsureTopicExistsAsync(string bootstrapServers, string topicName)
        {
            try
            {
                var adminConfig = new AdminClientConfig 
                { 
                    BootstrapServers = bootstrapServers,
                    SecurityProtocol = SecurityProtocol.Plaintext,
                    SocketTimeoutMs = 10000,
                    ApiVersionRequestTimeoutMs = 10000
                };
                
                using var admin = new AdminClientBuilder(adminConfig).Build();
                
                var metadata = admin.GetMetadata(TimeSpan.FromSeconds(15));
                var topicExists = metadata.Topics.Any(t => t.Topic == topicName);
                
                if (!topicExists)
                {
                    _logger.LogInformation("🔄 TaskManager {TaskManagerId}: Creating output topic: {TopicName}", _taskManagerId, topicName);
                    
                    var topicSpec = new Confluent.Kafka.Admin.TopicSpecification
                    {
                        Name = topicName,
                        NumPartitions = 8,  // Multiple partitions for high throughput
                        ReplicationFactor = 1
                    };

                    await admin.CreateTopicsAsync(new[] { topicSpec });
                    _logger.LogInformation("✅ TaskManager {TaskManagerId}: Output topic created: {TopicName}", _taskManagerId, topicName);
                    
                    await Task.Delay(2000); // Wait for topic to be available
                }
                else
                {
                    _logger.LogInformation("✅ TaskManager {TaskManagerId}: Output topic already exists: {TopicName}", _taskManagerId, topicName);
                }
            }
            catch (Exception ex)
            {
                _logger.LogWarning(ex, "⚠️ TaskManager {TaskManagerId}: Could not create/verify output topic: {TopicName}", _taskManagerId, topicName);
            }
        }

        private async Task ConsumeMessagesWithFlinkPatterns(CancellationToken stoppingToken)
        {
            if (_consumerGroup == null)
                throw new InvalidOperationException("Consumer group not initialized");

            _logger.LogInformation("🔄 TaskManager {TaskManagerId}: Starting message consumption with Apache Flink 2.0 patterns and automatic resumption", _taskManagerId);
            
            // Add detailed debugging information
            _logger.LogInformation("🔍 TaskManager {TaskManagerId}: Consumer Configuration Debug Info:", _taskManagerId);
            _logger.LogInformation("  📋 Topic: {Topic}", _kafkaTopic);
            _logger.LogInformation("  📋 Consumer Group: {GroupId}", _consumerGroup.GetConsumerGroupId());
            _logger.LogInformation("  📋 Redis Counter Key: {CounterKey}", _redisSinkCounterKey);
            _logger.LogInformation("  📋 Global Sequence Key: {GlobalKey}", _globalSequenceKey);
            
            // Log bootstrap servers being used for comparison with producer
            var bootstrapServers = _configuration["DOTNET_KAFKA_BOOTSTRAP_SERVERS"] ?? 
                                 _configuration["ConnectionStrings__kafka"] ?? 
                                 "localhost:9092";
            _logger.LogInformation("  📋 Bootstrap Servers: {BootstrapServers}", bootstrapServers);
            
            // CRITICAL FIX: Enhanced consumer group debugging
            await LogDetailedConsumerGroupStatus();
            
            var consumptionContext = new ConsumptionContext(DateTime.UtcNow);
            var consecutiveNullResults = 0;
            var maxConsecutiveNulls = 60; // Allow 60 consecutive null results before enhanced logging
            var lastProgressLogTime = DateTime.UtcNow;

            while (!stoppingToken.IsCancellationRequested)
            {
                try
                {
                    // CRITICAL FIX: Enhanced timeout and polling strategy
                    var consumeResult = _consumerGroup.ConsumeMessage(TimeSpan.FromMilliseconds(2000)); // Increased to 2s
                    
                    if (consumeResult?.Message != null)
                    {
                        // Reset null counter on successful consumption
                        consecutiveNullResults = 0;
                        await ProcessConsumeResult(consumeResult, consumptionContext, stoppingToken);
                    }
                    else
                    {
                        consecutiveNullResults++;
                        
                        // CRITICAL FIX: Enhanced null result debugging
                        if (consecutiveNullResults >= maxConsecutiveNulls)
                        {
                            _logger.LogWarning("⚠️ TaskManager {TaskManagerId}: {ConsecutiveNulls} consecutive null results - diagnosing consumption issues", 
                                _taskManagerId, consecutiveNullResults);
                            
                            await DiagnoseConsumptionIssues();
                            consecutiveNullResults = 0; // Reset counter after diagnosis
                        }
                        else if ((DateTime.UtcNow - lastProgressLogTime).TotalSeconds >= 30)
                        {
                            _logger.LogInformation("🔍 TaskManager {TaskManagerId}: No messages received in last 30s (null results: {NullCount}) - consumer assignment: {AssignmentCount} partitions", 
                                _taskManagerId, consecutiveNullResults, _consumerGroup?.GetAssignment().Count ?? 0);
                            lastProgressLogTime = DateTime.UtcNow;
                        }
                        
                        // Brief pause to prevent tight polling
                        await Task.Delay(100, stoppingToken);
                    }
                    
                    // Check if consumer group is in recovery mode
                    if (_consumerGroup.IsInRecoveryMode())
                    {
                        var failureCount = _consumerGroup.GetConsecutiveFailureCount();
                        _logger.LogWarning("⚠️ TaskManager {TaskManagerId}: FlinkKafkaConsumerGroup in recovery mode (failures: {FailureCount})", 
                            _taskManagerId, failureCount);
                        
                        // Brief pause to allow recovery
                        await Task.Delay(TimeSpan.FromSeconds(2), stoppingToken);
                    }
                }
                catch (ConsumeException ex)
                {
                    _logger.LogWarning(ex, "🔄 TaskManager {TaskManagerId}: ConsumeException - will be handled by FlinkKafkaConsumerGroup: {Error}",
                        _taskManagerId, ex.Error.Reason);
                    await Task.Delay(TimeSpan.FromSeconds(1), stoppingToken);
                }
                catch (OperationCanceledException ex)
                {
                    _logger.LogInformation(ex, "🛑 TaskManager {TaskManagerId}: Consumption cancelled", _taskManagerId);
                    break;
                }
                catch (Exception ex)
                {
                    _logger.LogError(ex, "❌ TaskManager {TaskManagerId}: Unexpected error during consumption", _taskManagerId);
                    await Task.Delay(TimeSpan.FromSeconds(5), stoppingToken);
                }
            }
            
            LogFinalConsumptionStats(consumptionContext.StartTime);
        }
        
        /// <summary>
        /// Enhanced debugging for consumption issues
        /// </summary>
        private async Task DiagnoseConsumptionIssues()
        {
            try
            {
                _logger.LogInformation("🔍 TaskManager {TaskManagerId}: DIAGNOSING CONSUMPTION ISSUES", _taskManagerId);
                
                // Check consumer group assignment
                var assignment = _consumerGroup!.GetAssignment();
                _logger.LogInformation("  📊 Current assignment: {PartitionCount} partitions: {Partitions}", 
                    assignment.Count, string.Join(", ", assignment.Select(tp => $"{tp.Topic}:{tp.Partition}")));
                
                if (assignment.Count == 0)
                {
                    _logger.LogError("  ❌ NO PARTITION ASSIGNMENTS - Consumer group is not assigned to any partitions!");
                    
                    // Try to trigger rebalance by re-subscribing
                    _logger.LogInformation("  🔄 Attempting to trigger rebalance...");
                    // Note: FlinkKafkaConsumerGroup handles this internally
                }
                
                // Check current offsets
                var checkpointState = _consumerGroup.GetCheckpointState();
                _logger.LogInformation("  💾 Current checkpoint state: {OffsetCount} tracked offsets", checkpointState.Count);
                foreach (var kvp in checkpointState)
                {
                    _logger.LogInformation("    📍 {Topic}:{Partition} -> Offset {Offset}", 
                        kvp.Key.Topic, kvp.Key.Partition, kvp.Value);
                }
                
                // Check if consumer group is in recovery mode
                if (_consumerGroup.IsInRecoveryMode())
                {
                    var failures = _consumerGroup.GetConsecutiveFailureCount();
                    _logger.LogWarning("  ⚠️ Consumer group is in RECOVERY MODE with {FailureCount} consecutive failures", failures);
                }
                
                // Additional diagnostic: Check Redis connectivity
                try
                {
                    var redisTest = await _redisDatabase.PingAsync();
                    _logger.LogInformation("  ✅ Redis connectivity: {Latency}ms", redisTest.TotalMilliseconds);
                }
                catch (Exception ex)
                {
                    _logger.LogError(ex, "  ❌ Redis connectivity failed");
                }
                
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "❌ TaskManager {TaskManagerId}: Failed to diagnose consumption issues", _taskManagerId);
            }
        }
        
        /// <summary>
        /// Enhanced consumer group status logging
        /// </summary>
        private async Task LogDetailedConsumerGroupStatus()
        {
            try
            {
                _logger.LogInformation("🔍 TaskManager {TaskManagerId}: DETAILED CONSUMER GROUP STATUS", _taskManagerId);
                
                // Wait a moment for initialization to complete
                await Task.Delay(2000);
                
                // Get current assignment
                var assignment = _consumerGroup!.GetAssignment();
                _logger.LogInformation("📊 Consumer assignment: {PartitionCount} partitions: {Partitions}", 
                    assignment.Count, string.Join(", ", assignment.Select(tp => $"{tp.Topic}:{tp.Partition}")));
                
                // Get current checkpoint state
                var checkpointState = _consumerGroup.GetCheckpointState();
                _logger.LogInformation("💾 Current checkpoint state: {OffsetCount} tracked offsets", checkpointState.Count);
                foreach (var kvp in checkpointState)
                {
                    _logger.LogInformation("  📍 {Topic}:{Partition} -> Offset {Offset}", 
                        kvp.Key.Topic, kvp.Key.Partition, kvp.Value);
                }
                
                // Log consumer group ID
                var consumerGroupId = _consumerGroup.GetConsumerGroupId();
                _logger.LogInformation("👥 Consumer group ID: {ConsumerGroupId}", consumerGroupId);
                
                // CRITICAL: Verify topic exists and check basic metadata
                await LogEnhancedTopicStatus();
                
            }
            catch (Exception ex)
            {
                _logger.LogWarning(ex, "⚠️ Failed to log detailed consumer group status");
            }
        }
        
        /// <summary>
        /// Enhanced topic status logging
        /// </summary>
        private async Task LogEnhancedTopicStatus()
        {
            try
            {
                _logger.LogInformation("🔍 TaskManager {TaskManagerId}: ENHANCED TOPIC STATUS CHECK", _taskManagerId);
                
                // Multi-strategy Kafka bootstrap server discovery
                string? bootstrapServers = _configuration["DOTNET_KAFKA_BOOTSTRAP_SERVERS"];
                if (string.IsNullOrEmpty(bootstrapServers))
                {
                    bootstrapServers = _configuration["ConnectionStrings__kafka"];
                    if (string.IsNullOrEmpty(bootstrapServers))
                    {
                        bootstrapServers = "localhost:9092";
                    }
                }
                
                bootstrapServers = bootstrapServers.Replace("localhost", "127.0.0.1");
                _logger.LogInformation("  🔗 Using bootstrap servers: {BootstrapServers}", bootstrapServers);
                
                var adminConfig = new AdminClientConfig 
                { 
                    BootstrapServers = bootstrapServers,
                    SecurityProtocol = SecurityProtocol.Plaintext,
                    SocketTimeoutMs = 10000
                };
                
                using var admin = new AdminClientBuilder(adminConfig).Build();
                var metadata = admin.GetMetadata(TimeSpan.FromSeconds(10));
                
                _logger.LogInformation("  📊 Kafka cluster: {BrokerCount} brokers, {TopicCount} topics", 
                    metadata.Brokers.Count, metadata.Topics.Count);
                
                var topicMetadata = metadata.Topics.FirstOrDefault(t => t.Topic == _kafkaTopic);
                if (topicMetadata != null)
                {
                    _logger.LogInformation("  ✅ Topic {Topic} found with {PartitionCount} partitions", 
                        _kafkaTopic, topicMetadata.Partitions.Count);
                    
                    foreach (var partition in topicMetadata.Partitions)
                    {
                        var partitionInfo = $"Partition {partition.PartitionId}: Leader={partition.Leader}, Error={partition.Error?.Code ?? ErrorCode.NoError}";
                        _logger.LogInformation("    📍 {PartitionInfo}", partitionInfo);
                    }
                }
                else
                {
                    _logger.LogError("  ❌ Topic {Topic} NOT FOUND in Kafka metadata!", _kafkaTopic);
                    
                    // List all available topics for debugging
                    _logger.LogInformation("  📋 Available topics:");
                    foreach (var topic in metadata.Topics)
                    {
                        _logger.LogInformation("    - {TopicName}", topic.Topic);
                    }
                }
                
            }
            catch (Exception ex)
            {
                _logger.LogWarning(ex, "⚠️ Failed to log enhanced topic status");
            }
        }

        private async Task ProcessConsumeResult(ConsumeResult<Ignore, byte[]>? consumeResult, ConsumptionContext context, CancellationToken stoppingToken)
        {
            if (consumeResult?.Message != null)
            {
                await ProcessMessageWithFlinkPatterns(consumeResult);
                CheckAndLogProgress(consumeResult, context);
            }
            else
            {
                // Log debugging info when no message is received
                if ((DateTime.UtcNow - context.LastLogTime).TotalSeconds >= 30)
                {
                    _logger.LogInformation("🔍 TaskManager {TaskManagerId}: No messages received in last 30s - consumer assignment: {AssignmentCount} partitions", 
                        _taskManagerId, _consumerGroup?.GetAssignment().Count ?? 0);
                    
                    // Check consumer group status periodically
                    LogConsumerGroupStatus();
                    context.LastLogTime = DateTime.UtcNow;
                }
                
                await Task.Delay(100, stoppingToken);
            }
        }

        private void CheckAndLogProgress(ConsumeResult<Ignore, byte[]> consumeResult, ConsumptionContext context)
        {
            // Reduced logging frequency for better performance
            if ((DateTime.UtcNow - context.LastLogTime).TotalSeconds >= 60)
            {
                var currentProcessedCount = Interlocked.Read(ref _messagesProcessed);
                var messagesInPeriod = currentProcessedCount - context.LastProcessedCount;
                var elapsed = DateTime.UtcNow - context.StartTime;
                var totalRate = currentProcessedCount / elapsed.TotalSeconds;
                
                _logger.LogInformation("📊 TaskManager {TaskManagerId}: Processed {TotalMessages} messages " +
                                     "(+{PeriodMessages} in last 60s) Rate: {Rate:F1} msg/s " +
                                     "Partition: {Partition}",
                                     _taskManagerId, currentProcessedCount, messagesInPeriod, totalRate,
                                     consumeResult.Partition.Value);
                
                context.LastLogTime = DateTime.UtcNow;
                context.LastProcessedCount = currentProcessedCount;
            }
        }

        private void LogFinalConsumptionStats(DateTime startTime)
        {
            var finalElapsed = DateTime.UtcNow - startTime;
            var finalCount = Interlocked.Read(ref _messagesProcessed);
            var finalRate = finalCount / finalElapsed.TotalSeconds;
            
            _logger.LogInformation("🏁 TaskManager {TaskManagerId}: Consumption completed. " +
                                 "Processed {TotalMessages} messages in {Duration:F1}s " +
                                 "Final rate: {Rate:F1} msg/s",
                                 _taskManagerId, finalCount, finalElapsed.TotalSeconds, finalRate);
        }

        /// <summary>
        /// Verify that messages actually exist in Kafka before starting the consumer
        /// This is critical to ensure the consumer isn't starting before the producer finishes
        /// </summary>
        private async Task VerifyMessagesAvailableInKafka()
        {
            try
            {
                _logger.LogInformation("🔍 TaskManager {TaskManagerId}: Verifying messages are available in Kafka topic {Topic}", _taskManagerId, _kafkaTopic);
                
                // Discover bootstrap servers using same method as consumer will use
                string? bootstrapServers = await DiscoverKafkaBootstrapServersLikeProducerScript();
                if (string.IsNullOrEmpty(bootstrapServers))
                {
                    bootstrapServers = _configuration["DOTNET_KAFKA_BOOTSTRAP_SERVERS"] ?? 
                                     _configuration["ConnectionStrings__kafka"] ?? 
                                     "localhost:9092";
                }
                bootstrapServers = bootstrapServers.Replace("localhost", "127.0.0.1");
                
                var maxAttempts = 5;
                var retryDelay = TimeSpan.FromSeconds(5);
                
                for (int attempt = 1; attempt <= maxAttempts; attempt++)
                {
                    try
                    {
                        _logger.LogInformation("🔍 TaskManager {TaskManagerId}: Message verification attempt {Attempt}/{MaxAttempts}", _taskManagerId, attempt, maxAttempts);
                        
                        var tempConsumerConfig = new ConsumerConfig
                        {
                            BootstrapServers = bootstrapServers,
                            GroupId = $"message-verification-{Guid.NewGuid()}",
                            AutoOffsetReset = AutoOffsetReset.Earliest,
                            EnableAutoCommit = false,
                            SessionTimeoutMs = 6000,
                            SocketTimeoutMs = 5000,
                            FetchWaitMaxMs = 1000,
                            MetadataMaxAgeMs = 30000
                        };
                        
                        using var tempConsumer = new ConsumerBuilder<Ignore, byte[]>(tempConsumerConfig).Build();
                        tempConsumer.Subscribe(_kafkaTopic);
                        
                        // Wait for consumer to get assignment
                        await Task.Delay(3000);
                        
                        // Check assignment
                        var assignment = tempConsumer.Assignment;
                        _logger.LogInformation("🔍 TaskManager {TaskManagerId}: Verification consumer assigned to {PartitionCount} partitions", _taskManagerId, assignment.Count);
                        
                        if (assignment.Count == 0)
                        {
                            _logger.LogWarning("⚠️ TaskManager {TaskManagerId}: Verification consumer has no partition assignments on attempt {Attempt}", _taskManagerId, attempt);
                            if (attempt < maxAttempts)
                            {
                                await Task.Delay(retryDelay);
                                continue;
                            }
                            else
                            {
                                throw new InvalidOperationException("Verification consumer failed to get partition assignments");
                            }
                        }
                        
                        // Try to consume messages with increased timeout
                        int messageCount = 0;
                        var verificationTimeout = TimeSpan.FromSeconds(15);
                        var startTime = DateTime.UtcNow;
                        
                        _logger.LogInformation("🔍 TaskManager {TaskManagerId}: Starting message verification scan (timeout: {Timeout}s)", _taskManagerId, verificationTimeout.TotalSeconds);
                        
                        while ((DateTime.UtcNow - startTime) < verificationTimeout)
                        {
                            try
                            {
                                var result = tempConsumer.Consume(TimeSpan.FromSeconds(2));
                                if (result?.Message != null)
                                {
                                    messageCount++;
                                    _logger.LogInformation("  ✅ TaskManager {TaskManagerId}: Found message {MessageCount} in partition {Partition} at offset {Offset}", 
                                        _taskManagerId, messageCount, result.Partition.Value, result.Offset.Value);
                                    
                                    // Found messages, we can proceed
                                    if (messageCount >= 3)
                                    {
                                        _logger.LogInformation("✅ TaskManager {TaskManagerId}: Message verification SUCCESSFUL - found {MessageCount} messages in topic {Topic}", 
                                            _taskManagerId, messageCount, _kafkaTopic);
                                        return;
                                    }
                                }
                                else
                                {
                                    // Brief pause between consume attempts
                                    await Task.Delay(500);
                                }
                            }
                            catch (ConsumeException ex)
                            {
                                _logger.LogDebug("TaskManager {TaskManagerId}: Consume attempt resulted in: {Error}", _taskManagerId, ex.Error.Reason);
                            }
                        }
                        
                        if (messageCount > 0)
                        {
                            _logger.LogInformation("✅ TaskManager {TaskManagerId}: Message verification completed - found {MessageCount} messages (partial verification)", 
                                _taskManagerId, messageCount);
                            return;
                        }
                        else
                        {
                            _logger.LogWarning("⚠️ TaskManager {TaskManagerId}: No messages found in verification attempt {Attempt}/{MaxAttempts}", _taskManagerId, attempt, maxAttempts);
                            
                            if (attempt < maxAttempts)
                            {
                                _logger.LogInformation("⏳ TaskManager {TaskManagerId}: Waiting {Delay}s before retry...", _taskManagerId, retryDelay.TotalSeconds);
                                await Task.Delay(retryDelay);
                            }
                        }
                    }
                    catch (Exception ex)
                    {
                        _logger.LogWarning(ex, "⚠️ TaskManager {TaskManagerId}: Exception during message verification attempt {Attempt}", _taskManagerId, attempt);
                        
                        if (attempt < maxAttempts)
                        {
                            await Task.Delay(retryDelay);
                        }
                    }
                }
                
                // If we get here, no messages were found after all attempts
                _logger.LogError("❌ TaskManager {TaskManagerId}: Message verification FAILED - no messages found in topic {Topic} after {MaxAttempts} attempts", 
                    _taskManagerId, _kafkaTopic, maxAttempts);
                
                // Log additional diagnostic information
                await LogKafkaTopicDiagnostics(bootstrapServers);
                
                throw new InvalidOperationException($"No messages found in topic {_kafkaTopic} during verification - producer may not have completed or topic may be empty");
                
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "❌ TaskManager {TaskManagerId}: Failed to verify messages in Kafka topic {Topic}", _taskManagerId, _kafkaTopic);
                throw;
            }
        }
        
        /// <summary>
        /// Log detailed Kafka topic diagnostics for troubleshooting
        /// </summary>
        private async Task LogKafkaTopicDiagnostics(string bootstrapServers)
        {
            try
            {
                _logger.LogInformation("🔍 TaskManager {TaskManagerId}: Performing Kafka topic diagnostics", _taskManagerId);
                
                var adminConfig = new AdminClientConfig 
                { 
                    BootstrapServers = bootstrapServers,
                    SecurityProtocol = SecurityProtocol.Plaintext,
                    SocketTimeoutMs = 10000,
                    ApiVersionRequestTimeoutMs = 10000
                };
                
                using var admin = new AdminClientBuilder(adminConfig).Build();
                
                // Get cluster metadata
                var metadata = admin.GetMetadata(TimeSpan.FromSeconds(15));
                _logger.LogInformation("  📊 Kafka cluster: {BrokerCount} brokers, {TopicCount} total topics", 
                    metadata.Brokers.Count, metadata.Topics.Count);
                
                // Find our specific topic
                var topicMetadata = metadata.Topics.FirstOrDefault(t => t.Topic == _kafkaTopic);
                if (topicMetadata != null)
                {
                    _logger.LogInformation("  ✅ Topic {Topic} exists with {PartitionCount} partitions", 
                        _kafkaTopic, topicMetadata.Partitions.Count);
                    
                    foreach (var partition in topicMetadata.Partitions)
                    {
                        var partitionInfo = $"Partition {partition.PartitionId}: Leader={partition.Leader}, Error={partition.Error?.Code ?? ErrorCode.NoError}";
                        _logger.LogInformation("    📍 {PartitionInfo}", partitionInfo);
                    }
                }
                else
                {
                    _logger.LogError("  ❌ Topic {Topic} NOT FOUND in cluster metadata!", _kafkaTopic);
                    
                    // List available topics
                    _logger.LogInformation("  📋 Available topics in cluster:");
                    foreach (var topic in metadata.Topics.Take(10)) // Show first 10 topics
                    {
                        _logger.LogInformation("    - {TopicName} ({PartitionCount} partitions)", topic.Topic, topic.Partitions.Count);
                    }
                    
                    if (metadata.Topics.Count > 10)
                    {
                        _logger.LogInformation("    ... and {RemainingCount} more topics", metadata.Topics.Count - 10);
                    }
                }
                
            }
            catch (Exception ex)
            {
                _logger.LogWarning(ex, "⚠️ TaskManager {TaskManagerId}: Failed to perform Kafka diagnostics", _taskManagerId);
            }
        }

        /// <summary>
        /// Reset consumer group offsets to earliest position to ensure we consume all available messages
        /// </summary>
        private async Task ResetConsumerGroupOffsetsToEarliest(string bootstrapServers, string consumerGroupId)
        {
            try
            {
                _logger.LogInformation("🔄 TaskManager {TaskManagerId}: Resetting consumer group '{GroupId}' offsets to earliest", _taskManagerId, consumerGroupId);
                
                // Create temporary consumer to perform offset reset
                var resetConsumerConfig = new ConsumerConfig
                {
                    BootstrapServers = bootstrapServers,
                    GroupId = consumerGroupId,
                    SecurityProtocol = SecurityProtocol.Plaintext,
                    SessionTimeoutMs = 10000,
                    EnableAutoCommit = false
                };
                
                using var resetConsumer = new ConsumerBuilder<Ignore, byte[]>(resetConsumerConfig).Build();
                
                // Subscribe to get partition assignment
                resetConsumer.Subscribe(_kafkaTopic);
                
                // Wait for assignment
                var maxWaitTime = TimeSpan.FromSeconds(15);
                var startTime = DateTime.UtcNow;
                List<TopicPartition> assignment = new();
                
                while ((DateTime.UtcNow - startTime) < maxWaitTime && assignment.Count == 0)
                {
                    try
                    {
                        var result = resetConsumer.Consume(TimeSpan.FromMilliseconds(1000));
                        assignment = resetConsumer.Assignment.ToList();
                        if (assignment.Count > 0) break;
                    }
                    catch (ConsumeException)
                    {
                        // Expected during assignment
                    }
                    await Task.Delay(500);
                }
                
                if (assignment.Count > 0)
                {
                    _logger.LogInformation("✅ TaskManager {TaskManagerId}: Got assignment for reset - {PartitionCount} partitions", _taskManagerId, assignment.Count);
                    
                    // Get earliest offsets for all assigned partitions
                    var partitionsWithEarliestOffsets = new List<TopicPartitionOffset>();
                    foreach (var partition in assignment)
                    {
                        try
                        {
                            var watermarks = resetConsumer.QueryWatermarkOffsets(partition, TimeSpan.FromSeconds(5));
                            var earliestOffset = watermarks.Low;
                            partitionsWithEarliestOffsets.Add(new TopicPartitionOffset(partition, earliestOffset));
                            _logger.LogInformation("📍 TaskManager {TaskManagerId}: Partition {Partition} earliest offset: {Offset}", 
                                _taskManagerId, partition.Partition, earliestOffset);
                        }
                        catch (Exception ex)
                        {
                            _logger.LogWarning(ex, "⚠️ TaskManager {TaskManagerId}: Could not get watermarks for partition {Partition}, using Offset.Beginning", 
                                _taskManagerId, partition.Partition);
                            partitionsWithEarliestOffsets.Add(new TopicPartitionOffset(partition, Offset.Beginning));
                        }
                    }
                    
                    // Commit earliest offsets
                    if (partitionsWithEarliestOffsets.Count > 0)
                    {
                        resetConsumer.Commit(partitionsWithEarliestOffsets);
                        _logger.LogInformation("✅ TaskManager {TaskManagerId}: Successfully reset {PartitionCount} partition offsets to earliest", 
                            _taskManagerId, partitionsWithEarliestOffsets.Count);
                    }
                }
                else
                {
                    _logger.LogWarning("⚠️ TaskManager {TaskManagerId}: No partitions assigned for offset reset within timeout", _taskManagerId);
                }
            }
            catch (Exception ex)
            {
                _logger.LogWarning(ex, "⚠️ TaskManager {TaskManagerId}: Failed to reset consumer group offsets - will rely on AutoOffsetReset", _taskManagerId);
            }
        }

        /// <summary>
        /// Discover Kafka bootstrap servers using the exact same method as the producer script
        /// to ensure both producer and consumer connect to the same Kafka instance
        /// </summary>
        private async Task<string?> DiscoverKafkaBootstrapServersLikeProducerScript()
        {
            try
            {
                _logger.LogInformation("🔍 TaskManager {TaskManagerId}: Discovering Kafka bootstrap servers using producer script method", _taskManagerId);
                
                // CRITICAL FIX: Add retry logic since producer and consumer may run at different times
                var maxAttempts = 3;
                var retryDelay = TimeSpan.FromSeconds(2);
                
                for (int attempt = 1; attempt <= maxAttempts; attempt++)
                {
                    try
                    {
                        // Use the exact same logic as produce-1-million-messages.ps1
                        var process = new System.Diagnostics.Process
                        {
                            StartInfo = new System.Diagnostics.ProcessStartInfo
                            {
                                FileName = "docker",
                                Arguments = "ps --filter \"name=kafka\" --format \"{{.Ports}}\"",
                                RedirectStandardOutput = true,
                                RedirectStandardError = true,
                                UseShellExecute = false,
                                CreateNoWindow = true
                            }
                        };
                        
                        process.Start();
                        var output = await process.StandardOutput.ReadToEndAsync();
                        var error = await process.StandardError.ReadToEndAsync();
                        await process.WaitForExitAsync();
                        
                        if (process.ExitCode != 0 || string.IsNullOrWhiteSpace(output))
                        {
                            _logger.LogWarning("⚠️ TaskManager {TaskManagerId}: Docker ps attempt {Attempt}/{MaxAttempts} failed. Exit code: {ExitCode}, Error: {Error}", 
                                _taskManagerId, attempt, maxAttempts, process.ExitCode, error);
                            
                            if (attempt < maxAttempts)
                            {
                                await Task.Delay(retryDelay);
                                continue;
                            }
                            return null;
                        }
                        
                        _logger.LogInformation("🔍 TaskManager {TaskManagerId}: Docker ps output (attempt {Attempt}): {Output}", _taskManagerId, attempt, output.Trim());
                        
                        // Parse ports using same logic as producer script
                        var portsArray = output.Split(new[] { ' ', '\t', '\n', '\r' }, StringSplitOptions.RemoveEmptyEntries);
                        var matchingPorts = portsArray.Where(portMapping => 
                            System.Text.RegularExpressions.Regex.IsMatch(portMapping, @"127\.0\.0\.1:(\d+)->9092/tcp"));
                        
                        foreach (var portMapping in matchingPorts)
                        {
                            // Match pattern: 127.0.0.1:PORT->9092/tcp
                            var match = System.Text.RegularExpressions.Regex.Match(portMapping, @"127\.0\.0\.1:(\d+)->9092/tcp");
                            if (match.Success)
                            {
                                var hostPort = match.Groups[1].Value;
                                var bootstrapServers = $"127.0.0.1:{hostPort}";
                                
                                // CRITICAL FIX: Verify connectivity before returning
                                if (await VerifyKafkaConnectivity(bootstrapServers))
                                {
                                    _logger.LogInformation("✅ TaskManager {TaskManagerId}: Verified Kafka bootstrap server: {BootstrapServers}", _taskManagerId, bootstrapServers);
                                    return bootstrapServers;
                                }
                                else
                                {
                                    _logger.LogWarning("⚠️ TaskManager {TaskManagerId}: Discovered server {BootstrapServers} but connectivity verification failed", _taskManagerId, bootstrapServers);
                                }
                            }
                        }
                        
                        _logger.LogWarning("⚠️ TaskManager {TaskManagerId}: Unable to parse valid Kafka 9092 port mapping from: {Output}", _taskManagerId, output);
                        
                        if (attempt < maxAttempts)
                        {
                            await Task.Delay(retryDelay);
                        }
                    }
                    catch (Exception ex)
                    {
                        _logger.LogWarning(ex, "⚠️ TaskManager {TaskManagerId}: Exception during discovery attempt {Attempt}/{MaxAttempts}", _taskManagerId, attempt, maxAttempts);
                        if (attempt < maxAttempts)
                        {
                            await Task.Delay(retryDelay);
                        }
                    }
                }
                
                return null;
                
            }
            catch (Exception ex)
            {
                _logger.LogWarning(ex, "⚠️ TaskManager {TaskManagerId}: Fatal exception during Kafka bootstrap server discovery", _taskManagerId);
                return null;
            }
        }
        
        /// <summary>
        /// Verify that we can actually connect to the discovered Kafka bootstrap server
        /// </summary>
        private async Task<bool> VerifyKafkaConnectivity(string bootstrapServers)
        {
            try
            {
                var adminConfig = new AdminClientConfig 
                { 
                    BootstrapServers = bootstrapServers,
                    SecurityProtocol = SecurityProtocol.Plaintext,
                    SocketTimeoutMs = 5000,
                    ApiVersionRequestTimeoutMs = 5000
                };
                
                using var admin = new AdminClientBuilder(adminConfig).Build();
                var metadata = admin.GetMetadata(TimeSpan.FromSeconds(10));
                
                _logger.LogInformation("🔍 TaskManager {TaskManagerId}: Kafka connectivity verified - {BrokerCount} brokers, {TopicCount} topics", 
                    _taskManagerId, metadata.Brokers.Count, metadata.Topics.Count);
                
                return metadata.Brokers.Count > 0;
            }
            catch (Exception ex)
            {
                _logger.LogWarning(ex, "⚠️ TaskManager {TaskManagerId}: Kafka connectivity verification failed for {BootstrapServers}", _taskManagerId, bootstrapServers);
                return false;
            }
        }
        
        private async Task LogTopicMessageAvailability()
        {
            try
            {
                _logger.LogInformation("🔍 TaskManager {TaskManagerId}: Checking topic message availability", _taskManagerId);
                
                // Multi-strategy Kafka bootstrap server discovery
                string? bootstrapServers = _configuration["DOTNET_KAFKA_BOOTSTRAP_SERVERS"];
                if (string.IsNullOrEmpty(bootstrapServers))
                {
                    bootstrapServers = _configuration["ConnectionStrings__kafka"];
                }
                if (string.IsNullOrEmpty(bootstrapServers))
                {
                    bootstrapServers = "localhost:9092";
                }
                
                // Fix IPv6 issue by forcing IPv4 localhost resolution
                bootstrapServers = bootstrapServers.Replace("localhost", "127.0.0.1");
                
                var adminConfig = new AdminClientConfig 
                { 
                    BootstrapServers = bootstrapServers,
                    SecurityProtocol = SecurityProtocol.Plaintext,
                    SocketTimeoutMs = 10000
                };
                
                using var admin = new AdminClientBuilder(adminConfig).Build();
                var metadata = admin.GetMetadata(_kafkaTopic, TimeSpan.FromSeconds(10));
                
                var topicMetadata = metadata.Topics.FirstOrDefault(t => t.Topic == _kafkaTopic);
                if (topicMetadata != null)
                {
                    _logger.LogInformation("📊 Topic {Topic} found with {PartitionCount} partitions", 
                        _kafkaTopic, topicMetadata.Partitions.Count);
                    
                    foreach (var partition in topicMetadata.Partitions)
                    {
                        _logger.LogInformation("  📍 Partition {PartitionId}: Status = {PartitionError}", 
                            partition.PartitionId, partition.Error?.Code ?? ErrorCode.NoError);
                    }
                }
                else
                {
                    _logger.LogWarning("❌ Topic {Topic} not found in metadata!", _kafkaTopic);
                }
            }
            catch (Exception ex)
            {
                _logger.LogWarning(ex, "⚠️ TaskManager {TaskManagerId}: Failed to check topic availability", _taskManagerId);
            }
        }
        
        private void LogConsumerGroupStatus()
        {
            try
            {
                _logger.LogInformation("🔍 TaskManager {TaskManagerId}: Logging consumer group status for debugging", _taskManagerId);
                
                // Get current assignment
                var assignment = _consumerGroup!.GetAssignment();
                _logger.LogInformation("📊 Consumer assignment: {PartitionCount} partitions: {Partitions}", 
                    assignment.Count, string.Join(", ", assignment.Select(tp => $"{tp.Topic}:{tp.Partition}")));
                
                // Get current checkpoint state
                var checkpointState = _consumerGroup.GetCheckpointState();
                _logger.LogInformation("💾 Current checkpoint state: {OffsetCount} tracked offsets", checkpointState.Count);
                foreach (var kvp in checkpointState)
                {
                    _logger.LogInformation("  📍 {Topic}:{Partition} -> Offset {Offset}", 
                        kvp.Key.Topic, kvp.Key.Partition, kvp.Value);
                }
                
                // Log consumer group ID
                var consumerGroupId = _consumerGroup.GetConsumerGroupId();
                _logger.LogInformation("👥 Consumer group ID: {ConsumerGroupId}", consumerGroupId);
                
                // Additional debug: Check topic partition high water marks
                LogTopicPartitionStatus();
            }
            catch (Exception ex)
            {
                _logger.LogWarning(ex, "⚠️ Failed to log consumer group status");
            }
        }
        
        private void LogTopicPartitionStatus()
        {
            try
            {
                // Multi-strategy Kafka bootstrap server discovery
                string? bootstrapServers = _configuration["DOTNET_KAFKA_BOOTSTRAP_SERVERS"];
                if (string.IsNullOrEmpty(bootstrapServers))
                {
                    bootstrapServers = _configuration["ConnectionStrings__kafka"];
                    if (string.IsNullOrEmpty(bootstrapServers))
                    {
                        bootstrapServers = "localhost:9092";
                    }
                }
                
                bootstrapServers = bootstrapServers.Replace("localhost", "127.0.0.1");
                
                // Get metadata for the topic using the admin client
                var adminConfig = new AdminClientConfig 
                { 
                    BootstrapServers = bootstrapServers,
                    SecurityProtocol = SecurityProtocol.Plaintext,
                    SocketTimeoutMs = 10000
                };
                
                using var admin = new AdminClientBuilder(adminConfig).Build();
                var metadata = admin.GetMetadata(TimeSpan.FromSeconds(10));
                var topicMetadata = metadata.Topics.FirstOrDefault(t => t.Topic == _kafkaTopic);
                
                if (topicMetadata != null)
                {
                    _logger.LogInformation("📊 Topic {Topic} metadata: {PartitionCount} partitions", 
                        _kafkaTopic, topicMetadata.Partitions.Count);
                    
                    // Use kafka-console-consumer to get actual message counts as watermarks API is unreliable
                    _logger.LogInformation("📊 Topic {Topic} ready for consumption (watermark API shows 'Unset' due to Kafka client limitations)", _kafkaTopic);
                    _logger.LogInformation("🔄 Consumer group will start from earliest due to AutoOffsetReset = Earliest configuration");
                }
                else
                {
                    _logger.LogWarning("⚠️ Topic {Topic} not found in metadata", _kafkaTopic);
                }
            }
            catch (Exception ex)
            {
                _logger.LogWarning(ex, "⚠️ Failed to log topic partition status");
            }
        }

        private sealed class ConsumptionContext
        {
            public DateTime StartTime { get; }
            public DateTime LastLogTime { get; set; }
            public long LastProcessedCount { get; set; }

            public ConsumptionContext(DateTime startTime)
            {
                StartTime = startTime;
                LastLogTime = startTime;
                LastProcessedCount = 0L;
            }
        }

        private async Task ProcessMessageWithFlinkPatterns(ConsumeResult<Ignore, byte[]> consumeResult)
        {
            try
            {
                // Convert bytes to string (Apache Flink standard deserialization)
                var messageContent = System.Text.Encoding.UTF8.GetString(consumeResult.Message.Value);
                
                // Enhanced logging for initial messages and milestones  
                var currentCount = Interlocked.Read(ref _messagesProcessed);
                if (currentCount < 50 || currentCount % 10000 == 0)
                {
                    _logger.LogInformation("🔄 KAFKA CONSUME: TaskManager {TaskManagerId} consumed message from " +
                                           "topic: {Topic}, partition: {Partition}, offset: {Offset}",
                                           _taskManagerId, consumeResult.Topic, consumeResult.Partition.Value, 
                                           consumeResult.Offset.Value);
                }
                
                // Process the message using Apache Flink sink patterns
                await ProcessThroughFlinkSinkFunction();
                
                // Increment processed message counter
                Interlocked.Increment(ref _messagesProcessed);
                
                // Produce to output topic with high performance
                await ProduceToOutputTopic(messageContent);
                
                // Update Redis counters using Apache Flink sink pattern
                await UpdateRedisCountersWithFlinkPatterns();
                
                // Periodic checkpoint simulation (Apache Flink pattern) - less frequent for performance
                if (_messagesProcessed % 5000 == 0)
                {
                    await SimulateFlinkCheckpoint();
                }
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "❌ TaskManager {TaskManagerId}: Error processing message from " +
                               "partition {Partition}, offset {Offset}",
                               _taskManagerId, consumeResult.Partition.Value, consumeResult.Offset.Value);
                // Continue processing other messages
            }
        }

        private async Task ProduceToOutputTopic(string messageContent)
        {
            if (_producer == null)
            {
                // Producer not available, log once but don't spam
                var currentCount = Interlocked.Read(ref _messagesProcessed);
                if (currentCount <= 5)
                {
                    _logger.LogWarning("⚠️ TaskManager {TaskManagerId}: Producer not available for output topic: {OutputTopic}", 
                        _taskManagerId, _outputTopic);
                }
                return;
            }

            try
            {
                // Create processed message (could add transformation logic here)
                var processedMessage = $"PROCESSED:{messageContent}:{DateTimeOffset.UtcNow.ToUnixTimeMilliseconds()}";
                var messageBytes = System.Text.Encoding.UTF8.GetBytes(processedMessage);
                
                // High-performance produce (asynchronous as recommended by SonarQube)
                var message = new Message<Null, byte[]> { Value = messageBytes };
                await _producer.ProduceAsync(_outputTopic, message);
                
                // Log success for initial messages and milestones  
                var currentCount = Interlocked.Read(ref _messagesProcessed);
                if (currentCount <= 50 || currentCount % 10000 == 0)
                {
                    _logger.LogInformation("✅ TaskManager {TaskManagerId}: Produced message to output topic: {OutputTopic}", 
                        _taskManagerId, _outputTopic);
                }
            }
            catch (Exception ex)
            {
                _logger.LogWarning(ex, "⚠️ TaskManager {TaskManagerId}: Failed to produce to output topic: {OutputTopic}", 
                    _taskManagerId, _outputTopic);
            }
            
            // Make it async for compatibility but no actual async work
            await Task.CompletedTask;
        }

        private async Task UpdateRedisCountersWithFlinkPatterns()
        {
            try
            {
                // Update both Redis counters efficiently
                var pipeline = _redisDatabase.CreateBatch();
                var sinkCounterTask = pipeline.StringIncrementAsync(_redisSinkCounterKey);
                var globalSequenceTask = pipeline.StringIncrementAsync(_globalSequenceKey);
                
                pipeline.Execute();
                
                await sinkCounterTask;
                var newGlobalCount = await globalSequenceTask;
                
                // Enhanced logging for initial messages and milestones  
                var currentCount = Interlocked.Read(ref _messagesProcessed);
                if (currentCount <= 50 || currentCount % 10000 == 0)
                {
                    _logger.LogInformation("✅ REDIS SUCCESS: TaskManager {TaskManagerId} updated counters - " +
                                   "sink: {SinkKey}, global: {GlobalKey} = {GlobalCount}",
                                   _taskManagerId, _redisSinkCounterKey, _globalSequenceKey, newGlobalCount);
                }
            }
            catch (Exception ex)
            {
                _logger.LogWarning(ex, "⚠️ TaskManager {TaskManagerId}: Failed to update Redis counters", _taskManagerId);
                // Continue processing - Redis update failures shouldn't stop message processing
            }
        }

        private static async Task ProcessThroughFlinkSinkFunction()
        {
            // Simulate Apache Flink sink function processing
            // In a real implementation, this would go through the actual sink function
            await Task.Delay(1); // Minimal processing simulation
        }



        private async Task SimulateFlinkCheckpoint()
        {
            try
            {
                // Simulate Apache Flink checkpoint commit
                var checkpointId = DateTimeOffset.UtcNow.ToUnixTimeMilliseconds();
                await _consumerGroup!.CommitCheckpointOffsetsAsync(checkpointId);
                
                _logger.LogDebug("🔄 TaskManager {TaskManagerId}: Simulated Flink checkpoint {CheckpointId} " +
                               "after {MessagesProcessed} messages",
                               _taskManagerId, checkpointId, _messagesProcessed);
            }
            catch (Exception ex)
            {
                _logger.LogWarning(ex, "⚠️ TaskManager {TaskManagerId}: Checkpoint simulation failed", _taskManagerId);
            }
        }

        private async Task CleanupResources()
        {
            try
            {
                _logger.LogInformation("🔄 TaskManager {TaskManagerId}: Cleaning up resources", _taskManagerId);
                
                // Flush and dispose producer first
                if (_producer != null)
                {
                    _logger.LogInformation("🔄 TaskManager {TaskManagerId}: Flushing producer", _taskManagerId);
                    _producer.Flush(TimeSpan.FromSeconds(10));
                    _producer.Dispose();
                }
                
                _consumerGroup?.Dispose();
                _cancellationTokenSource.Dispose();
                
                _logger.LogInformation("✅ TaskManager {TaskManagerId}: Resource cleanup completed", _taskManagerId);
            }
            catch (Exception ex)
            {
                _logger.LogWarning(ex, "⚠️ TaskManager {TaskManagerId}: Error during cleanup", _taskManagerId);
            }
            
            await Task.CompletedTask;
        }

        public override void Dispose()
        {
            _producer?.Dispose();
            _consumerGroup?.Dispose();
            _cancellationTokenSource.Dispose();
            base.Dispose();
        }

        // ISourceContext<string> implementation for Apache Flink compatibility
        public void Collect(string record)
        {
            // This would be used in a real Flink pipeline
        }

        public Task CollectAsync(string record)
        {
            // This would be used in a real Flink pipeline
            return Task.CompletedTask;
        }

        public void CollectWithTimestamp(string record, long timestamp)
        {
            // This would be used in a real Flink pipeline with event time
        }

        public Task CollectWithTimestampAsync(string record, long timestamp)
        {
            // This would be used in a real Flink pipeline with event time
            return Task.CompletedTask;
        }

        public void EmitWatermark(FlinkDotNet.Core.Abstractions.Windowing.Watermark watermark)
        {
            // This would be used for watermark generation in event time processing
        }

        public static Task EmitWatermarkAsync(long timestamp)
        {
            // This would be used for watermark generation in event time processing
            return Task.CompletedTask;
        }

        public long ProcessingTime => DateTimeOffset.UtcNow.ToUnixTimeMilliseconds();

        public static long CurrentProcessingTimeMillis() => DateTimeOffset.UtcNow.ToUnixTimeMilliseconds();
    }
}
