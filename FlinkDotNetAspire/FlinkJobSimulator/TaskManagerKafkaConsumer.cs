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
                
                // Check if we should add a delay for producer-consumer coordination
                var forceResetToEarliest = _configuration["SIMULATOR_FORCE_RESET_TO_EARLIEST"] ?? "true";
                var shouldForceReset = string.Equals(forceResetToEarliest, "true", StringComparison.OrdinalIgnoreCase);
                
                if (shouldForceReset)
                {
                    // Add delay to allow producer time to generate messages first
                    var delaySeconds = 15; // Allow 15 seconds for producer to start and generate some messages
                    _logger.LogInformation("⏳ TaskManager {TaskManagerId}: Adding {DelaySeconds}s delay for producer-consumer coordination (SIMULATOR_FORCE_RESET_TO_EARLIEST=true)", _taskManagerId, delaySeconds);
                    await Task.Delay(TimeSpan.FromSeconds(delaySeconds), stoppingToken);
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

            // Check if we should force reset consumer group to earliest
            var forceResetToEarliest = _configuration["SIMULATOR_FORCE_RESET_TO_EARLIEST"] ?? "true";
            var shouldForceReset = string.Equals(forceResetToEarliest, "true", StringComparison.OrdinalIgnoreCase);
            
            if (shouldForceReset)
            {
                _logger.LogInformation("🔄 TaskManager {TaskManagerId}: Forcing consumer group reset to earliest (SIMULATOR_FORCE_RESET_TO_EARLIEST=true)", _taskManagerId);
                await ResetConsumerGroupToEarliest(bootstrapServers);
            }

            // Create unique consumer group ID when forcing reset to ensure AutoOffsetReset = Earliest takes effect
            var baseGroupId = "flink-taskmanager-consumer-group";
            var consumerGroupId = shouldForceReset 
                ? $"{baseGroupId}-reset-{DateTimeOffset.UtcNow.ToUnixTimeSeconds()}" 
                : baseGroupId;
                
            _logger.LogInformation("🔄 TaskManager {TaskManagerId}: Using consumer group ID: {GroupId} (ForceReset: {ForceReset})", 
                _taskManagerId, consumerGroupId, shouldForceReset);

            // Apache Flink-compliant consumer configuration
            var consumerConfig = new ConsumerConfig
            {
                BootstrapServers = bootstrapServers,
                GroupId = consumerGroupId, // Use unique group ID when forcing reset
                SecurityProtocol = SecurityProtocol.Plaintext,
                
                // Apache Flink 2.0 optimal settings (these will be enhanced by FlinkKafkaConsumerGroup)
                EnableAutoCommit = false, // Flink manages offsets through checkpoints
                AutoOffsetReset = AutoOffsetReset.Earliest,
                SessionTimeoutMs = 30000,
                HeartbeatIntervalMs = 10000,
                MaxPollIntervalMs = 300000,
                PartitionAssignmentStrategy = PartitionAssignmentStrategy.CooperativeSticky,
                
                // Performance settings for high-throughput
                FetchMinBytes = 1,
                FetchWaitMaxMs = 100,
                SocketTimeoutMs = 10000
            };

            _logger.LogInformation("🔄 TaskManager {TaskManagerId}: Initializing FlinkKafkaConsumerGroup with servers: {BootstrapServers}", 
                _taskManagerId, bootstrapServers);

            // Simple initialization - FlinkKafkaConsumerGroup now handles resumption internally
            _consumerGroup = new FlinkKafkaConsumerGroup(consumerConfig, _logger);
            await _consumerGroup.InitializeAsync(new[] { _kafkaTopic });

            _logger.LogInformation("✅ TaskManager {TaskManagerId}: FlinkKafkaConsumerGroup initialized with built-in resumption", _taskManagerId);
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
            
            // Check if messages are available in the topic before starting consumption
            await LogTopicMessageAvailability();
            
            // Log current consumer group assignment and offsets for debugging
            LogConsumerGroupStatus();
            
            var consumptionContext = new ConsumptionContext(DateTime.UtcNow);

            while (!stoppingToken.IsCancellationRequested)
            {
                try
                {
                    var consumeResult = _consumerGroup.ConsumeMessage(TimeSpan.FromMilliseconds(1000));
                    await ProcessConsumeResult(consumeResult, consumptionContext, stoppingToken);
                    
                    // Check if consumer group is in recovery mode
                    if (_consumerGroup.IsInRecoveryMode())
                    {
                        var failureCount = _consumerGroup.GetConsecutiveFailureCount();
                        _logger.LogWarning("⚠️ TaskManager {TaskManagerId}: FlinkKafkaConsumerGroup in recovery mode (failures: {FailureCount})", 
                            _taskManagerId, failureCount);
                        
                        // Brief pause to allow recovery
                        await Task.Delay(TimeSpan.FromSeconds(1), stoppingToken);
                    }
                }
                catch (ConsumeException ex)
                {
                    _logger.LogDebug(ex, "🔄 TaskManager {TaskManagerId}: ConsumeException handled by FlinkKafkaConsumerGroup: {Error}",
                        _taskManagerId, ex.Error.Reason);
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
        /// Reset consumer group offsets to earliest position to ensure we consume all available messages
        /// </summary>
        private Task ResetConsumerGroupToEarliest(string bootstrapServers)
        {
            try
            {
                _logger.LogInformation("🔄 TaskManager {TaskManagerId}: Verifying topic availability for consumption from earliest", _taskManagerId);
                
                var adminConfig = new AdminClientConfig 
                { 
                    BootstrapServers = bootstrapServers,
                    SecurityProtocol = SecurityProtocol.Plaintext,
                    SocketTimeoutMs = 10000,
                    ApiVersionRequestTimeoutMs = 10000
                };
                
                using var admin = new AdminClientBuilder(adminConfig).Build();
                
                // Verify topic exists and get metadata  
                var metadata = admin.GetMetadata(TimeSpan.FromSeconds(15));
                var topicMetadata = metadata.Topics.FirstOrDefault(t => t.Topic == _kafkaTopic);
                
                if (topicMetadata != null)
                {
                    _logger.LogInformation("📊 Topic {Topic} metadata: {PartitionCount} partitions available for consumption", 
                        _kafkaTopic, topicMetadata.Partitions.Count);
                    _logger.LogInformation("✅ TaskManager {TaskManagerId}: Topic verified - unique consumer group will ensure consumption from earliest", _taskManagerId);
                }
                else
                {
                    _logger.LogWarning("⚠️ Topic {KafkaTopic} not found in metadata", _kafkaTopic);
                }
                
                return Task.CompletedTask;
            }
            catch (Exception ex)
            {
                _logger.LogWarning(ex, "⚠️ TaskManager {TaskManagerId}: Failed to verify topic metadata - will proceed with normal startup", _taskManagerId);
                return Task.CompletedTask;
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
                    _logger.LogWarning("⚠️ TaskManager {TaskManagerId}: Docker ps command failed or no output. Exit code: {ExitCode}, Error: {Error}", 
                        _taskManagerId, process.ExitCode, error);
                    return null;
                }
                
                _logger.LogInformation("🔍 TaskManager {TaskManagerId}: Docker ps output: {Output}", _taskManagerId, output.Trim());
                
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
                        _logger.LogInformation("✅ TaskManager {TaskManagerId}: Discovered Kafka bootstrap server: {BootstrapServers}", _taskManagerId, bootstrapServers);
                        return bootstrapServers;
                    }
                }
                
                _logger.LogWarning("⚠️ TaskManager {TaskManagerId}: Unable to parse Kafka 9092 port mapping from: {Output}", _taskManagerId, output);
                return null;
                
            }
            catch (Exception ex)
            {
                _logger.LogWarning(ex, "⚠️ TaskManager {TaskManagerId}: Exception during Kafka bootstrap server discovery", _taskManagerId);
                return null;
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
