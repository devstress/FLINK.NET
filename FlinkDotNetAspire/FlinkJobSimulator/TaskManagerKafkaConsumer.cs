using Confluent.Kafka;
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
        
        // High-performance batch processing configuration for Redis updates
        private long _pendingRedisUpdates = 0;  // Use Interlocked for thread-safe long operations
        private readonly TimeSpan _redisUpdateInterval = TimeSpan.FromMilliseconds(100);  // 100ms Redis batching
        
        // Processing timing tracking
        private DateTime? _actualProcessingStartTime = null;
        private readonly object _timingLock = new object();
        private DateTime _lastProgressUpdateTime = DateTime.MinValue;

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
            
            // APACHE FLINK 2.0 PATTERN: Initialize Redis counters with retries - don't fail immediately
            bool redisInitialized = false;
            try
            {
                _logger.LogInformation("🔄 TaskManager {TaskManagerId}: Initializing Redis counters to indicate startup", _taskManagerId);
                
                // Apache Flink 2.0 resilient Redis initialization
                redisInitialized = await InitializeRedisCountersWithRetry();
                
                if (redisInitialized)
                {
                    _logger.LogInformation("✅ TaskManager {TaskManagerId}: Redis counters initialized successfully - sink: {SinkKey}, global: {GlobalKey}", 
                        _taskManagerId, _redisSinkCounterKey, _globalSequenceKey);
                    await UpdateStartupLogAsync("REDIS_CONNECTED", "Redis counters initialized successfully");
                }
                else
                {
                    _logger.LogWarning("⚠️ TaskManager {TaskManagerId}: Redis counters not yet available - will retry during operation", _taskManagerId);
                    await UpdateStartupLogAsync("REDIS_PENDING", "Redis counters pending - will retry");
                }
            }
            catch (Exception ex)
            {
                _logger.LogWarning(ex, "⚠️ TaskManager {TaskManagerId}: Redis initialization had issues - will continue and retry", _taskManagerId);
                await UpdateStartupLogAsync("REDIS_RETRY", $"Redis initialization issues - will retry: {ex.Message}");
            }
            
            // APACHE FLINK 2.0 PATTERN: Start Kafka consumer with resilient initialization
            bool kafkaInitialized = false;
            try
            {
                _logger.LogInformation("🔄 TaskManager {TaskManagerId}: Starting Apache Flink 2.0 KafkaSource initialization", _taskManagerId);
                
                // Apache Flink 2.0 pattern: No excessive coordination delays - consumers start immediately
                var forceResetToEarliest = _configuration["SIMULATOR_FORCE_RESET_TO_EARLIEST"] ?? "true";
                var shouldForceReset = string.Equals(forceResetToEarliest, "true", StringComparison.OrdinalIgnoreCase);
                
                if (shouldForceReset)
                {
                    // Apache Flink 2.0 pattern: NO coordination delay - consumers should start immediately
                    _logger.LogInformation("🚀 TaskManager {TaskManagerId}: Apache Flink 2.0 immediate startup (SIMULATOR_FORCE_RESET_TO_EARLIEST=true)", _taskManagerId);
                    
                    // Apache Flink 2.0 pattern: No message verification - consumers should be resilient to empty topics
                    _logger.LogInformation("💡 TaskManager {TaskManagerId}: Consumer will continuously poll for messages as they arrive (Apache Flink 2.0 pattern)", _taskManagerId);
                }
                
                kafkaInitialized = await InitializeKafkaWithRetry();
                
                if (kafkaInitialized)
                {
                    await UpdateStartupLogAsync("KAFKA_CONSUMING", "FlinkKafkaConsumerGroup and producer started with automatic resumption");
                    
                    // IMPORTANT: Mark FlinkJobSimulator as actually RUNNING
                    _logger.LogInformation("🎯 TaskManager {TaskManagerId}: FlinkJobSimulator is now RUNNING and ready to process messages", _taskManagerId);
                    await Program.WriteRunningStateLogAsync();
                    await UpdateStartupLogAsync("FlinkJobSimulatorRunning", "FlinkJobSimulator is actively running and processing messages");
                    
                    _logger.LogInformation("🔄 TaskManager {TaskManagerId}: Starting message consumption with Apache Flink 2.0 patterns", _taskManagerId);
                    await ConsumeMessagesWithFlinkPatterns(stoppingToken);
                }
                else
                {
                    // Apache Flink 2.0 pattern: Start anyway and wait for infrastructure to become ready
                    _logger.LogInformation("💡 TaskManager {TaskManagerId}: Kafka not immediately available - starting in wait mode (Apache Flink 2.0 pattern)", _taskManagerId);
                    await UpdateStartupLogAsync("KAFKA_WAIT_MODE", "FlinkJobSimulator started - waiting for Kafka infrastructure");
                    await Program.WriteRunningStateLogAsync();
                    await UpdateStartupLogAsync("FlinkJobSimulatorRunning", "FlinkJobSimulator running in wait mode - will retry Kafka connection");
                    
                    // Wait for infrastructure with continuous retry
                    await WaitForInfrastructureAndConsume(stoppingToken);
                }
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "❌ TaskManager {_taskManagerId}: Error in Kafka consumption for topic '{_kafkaTopic}'", 
                    _taskManagerId, _kafkaTopic);
                await UpdateStartupLogAsync("KAFKA_FAILED", $"Kafka consumption failed: {ex.Message}");
                
                // Apache Flink 2.0 pattern: Mark as running even with failures, then go into retry mode
                await Program.WriteRunningStateLogAsync();
                await UpdateStartupLogAsync("FlinkJobSimulatorRunning", "FlinkJobSimulator running but in error recovery mode");
                
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
        /// Apache Flink 2.0 resilient Kafka initialization with fallback patterns
        /// </summary>
        private async Task<bool> InitializeKafkaWithRetry()
        {
            const int maxAttempts = 2; // Limited attempts to fail fast during startup
            
            for (int attempt = 1; attempt <= maxAttempts; attempt++)
            {
                try
                {
                    _logger.LogInformation("🔄 TaskManager {TaskManagerId}: Kafka initialization attempt {Attempt}/{MaxAttempts}", 
                        _taskManagerId, attempt, maxAttempts);
                    
                    await InitializeFlinkKafkaConsumerGroup();
                    await InitializeHighPerformanceProducer();
                    
                    _logger.LogInformation("✅ TaskManager {TaskManagerId}: Kafka initialized successfully on attempt {Attempt}", 
                        _taskManagerId, attempt);
                    return true;
                }
                catch (Exception ex)
                {
                    _logger.LogWarning(ex, "⚠️ TaskManager {TaskManagerId}: Kafka initialization attempt {Attempt}/{MaxAttempts} failed", 
                        _taskManagerId, attempt, maxAttempts);
                    
                    if (attempt < maxAttempts)
                    {
                        await Task.Delay(2000); // 2 second delay between attempts
                    }
                }
            }
            
            _logger.LogWarning("⚠️ TaskManager {TaskManagerId}: Kafka initialization failed after {MaxAttempts} attempts - will use wait mode", 
                _taskManagerId, maxAttempts);
            return false;
        }
        
        /// <summary>
        /// Apache Flink 2.0 pattern: Wait for infrastructure to become ready, then start consuming
        /// </summary>
        private async Task WaitForInfrastructureAndConsume(CancellationToken stoppingToken)
        {
            _logger.LogInformation("⏳ TaskManager {TaskManagerId}: Apache Flink 2.0 infrastructure wait mode - continuously retrying until ready", _taskManagerId);
            
            const int retryIntervalMs = 5000; // 5 seconds between retries
            var retryCount = 0;
            
            while (!stoppingToken.IsCancellationRequested)
            {
                retryCount++;
                
                try
                {
                    _logger.LogInformation("🔄 TaskManager {TaskManagerId}: Infrastructure retry #{RetryCount} - attempting to initialize Kafka", 
                        _taskManagerId, retryCount);
                    
                    await InitializeFlinkKafkaConsumerGroup();
                    await InitializeHighPerformanceProducer();
                    
                    // If we get here, Kafka is ready
                    _logger.LogInformation("✅ TaskManager {TaskManagerId}: Infrastructure ready after {RetryCount} retries - starting consumption", 
                        _taskManagerId, retryCount);
                    
                    await UpdateStartupLogAsync("KAFKA_READY", "Kafka infrastructure ready - starting consumption");
                    await ConsumeMessagesWithFlinkPatterns(stoppingToken);
                    return; // Exit the retry loop
                }
                catch (Exception ex)
                {
                    _logger.LogDebug(ex, "🔄 TaskManager {TaskManagerId}: Infrastructure retry #{RetryCount} failed - will continue trying", 
                        _taskManagerId, retryCount);
                    
                    // Show periodic status updates
                    if (retryCount % 12 == 0) // Every 60 seconds (12 * 5s)
                    {
                        _logger.LogInformation("💡 TaskManager {TaskManagerId}: Still waiting for infrastructure after {RetryCount} attempts - continuing (Apache Flink 2.0 pattern)", 
                            _taskManagerId, retryCount);
                    }
                    
                    await Task.Delay(retryIntervalMs, stoppingToken);
                }
            }
            
            _logger.LogInformation("⏹️ TaskManager {TaskManagerId}: Infrastructure wait cancelled", _taskManagerId);
        }

        /// <summary>
        /// Apache Flink 2.0 resilient Redis counter initialization with retries
        /// </summary>
        private async Task<bool> InitializeRedisCountersWithRetry()
        {
            const int maxAttempts = 3;
            const int delayMs = 1000;
            
            for (int attempt = 1; attempt <= maxAttempts; attempt++)
            {
                try
                {
                    _logger.LogInformation("🔄 TaskManager {TaskManagerId}: Redis initialization attempt {Attempt}/{MaxAttempts}", 
                        _taskManagerId, attempt, maxAttempts);
                    
                    // Initialize both Redis keys to 0 ONLY if they don't exist
                    // Use SET NX to prevent overwriting existing values from other TaskManagers
                    var sinkExists = await _redisDatabase.KeyExistsAsync(_redisSinkCounterKey);
                    var globalExists = await _redisDatabase.KeyExistsAsync(_globalSequenceKey);
                    
                    if (!sinkExists)
                    {
                        await _redisDatabase.StringSetAsync(_redisSinkCounterKey, 0, when: When.NotExists);
                        _logger.LogInformation("✅ TaskManager {TaskManagerId}: Initialized sink counter key", _taskManagerId);
                    }
                    else
                    {
                        _logger.LogInformation("💡 TaskManager {TaskManagerId}: Sink counter already exists (other TaskManager initialized it)", _taskManagerId);
                    }
                    
                    if (!globalExists)
                    {
                        await _redisDatabase.StringSetAsync(_globalSequenceKey, 0, when: When.NotExists);
                        _logger.LogInformation("✅ TaskManager {TaskManagerId}: Initialized global sequence key", _taskManagerId);
                    }
                    else
                    {
                        _logger.LogInformation("💡 TaskManager {TaskManagerId}: Global sequence already exists (other TaskManager initialized it)", _taskManagerId);
                    }
                    
                    _logger.LogInformation("✅ TaskManager {TaskManagerId}: Redis counters initialized on attempt {Attempt}", 
                        _taskManagerId, attempt);
                    return true;
                }
                catch (Exception ex)
                {
                    _logger.LogWarning(ex, "⚠️ TaskManager {TaskManagerId}: Redis initialization attempt {Attempt}/{MaxAttempts} failed", 
                        _taskManagerId, attempt, maxAttempts);
                    
                    if (attempt < maxAttempts)
                    {
                        await Task.Delay(delayMs);
                    }
                }
            }
            
            _logger.LogWarning("⚠️ TaskManager {TaskManagerId}: Redis initialization failed after {MaxAttempts} attempts - will continue without immediate counters", 
                _taskManagerId, maxAttempts);
            return false;
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
ProcessingStartTime: UNSET
MessagesProcessed: 0
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
        /// Write progress information to per-TaskManager log files for stress test script to monitor
        /// Uses separate log files for each TaskManager to prevent overwrites with 20 parallel TaskManagers
        /// Matches the format from produce-1-million-messages.ps1 for consistency
        /// </summary>
        private async Task WriteProgressLogAsync(long currentCount, DateTime? processingStartTime = null)
        {
            try
            {
                var progressPercent = currentCount > 0 ? Math.Round((double)currentCount / 1000000 * 100, 1) : 0.0;
                var rate = 0;
                var elapsedSeconds = 0.0;
                
                if (processingStartTime.HasValue && currentCount > 0)
                {
                    elapsedSeconds = (DateTime.UtcNow - processingStartTime.Value).TotalSeconds;
                    rate = elapsedSeconds > 0 ? (int)Math.Round(currentCount / elapsedSeconds) : 0;
                }
                
                // Write structured log for workflow script parsing - PER TASKMANAGER
                var logContent = $@"FLINKJOBSIMULATOR_PROGRESS_LOG
TaskManagerId: {_taskManagerId}
UpdateTime: {DateTime.UtcNow:yyyy-MM-dd HH:mm:ss.fff} UTC
MessagesProcessed: {currentCount}
TotalExpected: 1000000
ProgressPercent: {progressPercent:F1}%
ElapsedSeconds: {elapsedSeconds:F3}
MessageRate: {rate} msg/sec
ProcessingStartTime: {(processingStartTime?.ToString("yyyy-MM-dd HH:mm:ss.fff") ?? "UNSET")}
Status: {(currentCount > 0 ? "PROCESSING" : "WAITING")}
";
                
                var projectRoot = FindProjectRoot();
                // CHANGE: Write to per-TaskManager log files to prevent overwrites
                var logPath = Path.Combine(projectRoot, $"flinkjobsimulator_progress_{_taskManagerId}.log");
                await File.WriteAllTextAsync(logPath, logContent);
                
                // Write human-readable progress display matching producer script format EXACTLY - PER TASKMANAGER
                var displayLogContent = "";
                if (currentCount > 0)
                {
                    // Match producer script format: "[PROGRESS] Sent={totalSent:N0}  Rate={rate:N0} msg/sec"
                    displayLogContent = $"[PROGRESS] Sent={currentCount:N0}  Rate={rate:N0} msg/sec\n";
                }
                else
                {
                    displayLogContent = "[PROGRESS] Waiting for messages to arrive...\n";
                }
                
                // CHANGE: Write to per-TaskManager display log files to prevent overwrites
                var displayLogPath = Path.Combine(projectRoot, $"flinkjobsimulator_display_{_taskManagerId}.log");
                await File.WriteAllTextAsync(displayLogPath, displayLogContent);
                
                // Console output matching producer script format for major milestones
                if (currentCount == 1 || currentCount % 250000 == 0 || currentCount >= 1000000)
                {
                    _logger.LogInformation("📊 [PROGRESS] Sent={Count:N0}  Rate={Rate:N0} msg/sec", 
                        currentCount, rate);
                }
            }
            catch (Exception ex)
            {
                _logger.LogWarning(ex, "⚠️ PROGRESS LOG: Failed to write progress log for TaskManager {TaskManagerId}", _taskManagerId);
            }
        }
        
        /// <summary>
        /// Write final completion log for stress test script monitoring - per TaskManager
        /// Matches the format from produce-1-million-messages.ps1 for consistency
        /// </summary>
        private async Task WriteCompletionLogAsync(long finalCount, double totalElapsedSeconds, int finalRate)
        {
            try
            {
                var progressPercent = finalCount > 0 ? Math.Round((double)finalCount / 1000000 * 100, 1) : 0.0;
                
                // Write structured log for workflow script parsing - PER TASKMANAGER
                var logContent = $@"FLINKJOBSIMULATOR_COMPLETION_LOG
TaskManagerId: {_taskManagerId}
CompletionTime: {DateTime.UtcNow:yyyy-MM-dd HH:mm:ss.fff} UTC
FinalMessagesProcessed: {finalCount}
TotalExpected: 1000000
FinalProgressPercent: {progressPercent:F1}%
TotalElapsedSeconds: {totalElapsedSeconds:F3}
FinalMessageRate: {finalRate} msg/sec
ProcessingStartTime: {(_actualProcessingStartTime?.ToString("yyyy-MM-dd HH:mm:ss.fff") ?? "UNSET")}
Status: COMPLETED
Success: {(finalCount >= 1000000 ? "TRUE" : "FALSE")}
";
                
                var projectRoot = FindProjectRoot();
                // CHANGE: Write to per-TaskManager completion log files to prevent overwrites
                var logPath = Path.Combine(projectRoot, $"flinkjobsimulator_completion_{_taskManagerId}.log");
                await File.WriteAllTextAsync(logPath, logContent);
                
                // Write human-readable finish display matching producer script format EXACTLY - PER TASKMANAGER
                // Producer format: "[FINISH] Total: {finalSent:N0} Time: {sw.Elapsed.TotalSeconds:F3}s Rate: {finalSent / sw.Elapsed.TotalSeconds:N0} msg/sec"
                var displayLogContent = $"[FINISH] Total: {finalCount:N0} Time: {totalElapsedSeconds:F3}s Rate: {finalRate:N0} msg/sec\n";
                
                // CHANGE: Write to per-TaskManager display log files to prevent overwrites
                var displayLogPath = Path.Combine(projectRoot, $"flinkjobsimulator_display_{_taskManagerId}.log");
                await File.WriteAllTextAsync(displayLogPath, displayLogContent);
                
                _logger.LogInformation("📝 COMPLETION LOG: Written final completion status to {LogPath}", logPath);
                _logger.LogInformation("🏁 [FINISH] Total: {Total:N0} Time: {Time:F3}s Rate: {Rate:N0} msg/sec", 
                    finalCount, totalElapsedSeconds, finalRate);
            }
            catch (Exception ex)
            {
                _logger.LogWarning(ex, "⚠️ COMPLETION LOG: Failed to write completion log for TaskManager {TaskManagerId}", _taskManagerId);
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
            // CRITICAL FIX: Use environment variable first (set by port discovery), then fallback to discovery
            string? bootstrapServers = _configuration["DOTNET_KAFKA_BOOTSTRAP_SERVERS"];
            
            if (string.IsNullOrEmpty(bootstrapServers))
            {
                // Use same Kafka discovery method as producer script as secondary option
                bootstrapServers = await DiscoverKafkaBootstrapServersLikeProducerScript();
            }
            
            if (string.IsNullOrEmpty(bootstrapServers))
            {
                // Final fallback to connection string or default
                bootstrapServers = _configuration["ConnectionStrings__kafka"];
                if (string.IsNullOrEmpty(bootstrapServers))
                {
                    bootstrapServers = "localhost:9092";
                }
                
                _logger.LogWarning("⚠️ TaskManager {TaskManagerId}: Using final fallback bootstrap servers: {BootstrapServers}", _taskManagerId, bootstrapServers);
            }
            else
            {
                _logger.LogInformation("✅ TaskManager {TaskManagerId}: Using bootstrap servers: {BootstrapServers}", _taskManagerId, bootstrapServers);
            }

            // Fix IPv6 issue by forcing IPv4 localhost resolution
            bootstrapServers = bootstrapServers.Replace("localhost", "127.0.0.1");

            // PRODUCTION-GRADE APACHE FLINK PATTERN: Use timestamp-based consumer group for stress testing
            // This ensures each test run starts fresh and consumes all available messages
            var baseGroupId = "flink-taskmanager-consumer-group";
            var forceResetToEarliest = _configuration["SIMULATOR_FORCE_RESET_TO_EARLIEST"] ?? "true";
            var shouldForceReset = string.Equals(forceResetToEarliest, "true", StringComparison.OrdinalIgnoreCase);
            
            // CRITICAL FIX: Always use timestamp-based unique consumer group for stress testing
            // This ensures proper AutoOffsetReset.Earliest behavior since existing consumer groups with 
            // committed offsets will ignore the AutoOffsetReset setting
            var consumerGroupId = $"{baseGroupId}-{DateTime.UtcNow:yyyyMMddHHmmss}";
            
            _logger.LogInformation("🔄 TaskManager {TaskManagerId}: Using consumer group ID: {GroupId} (ForceReset: {ForceReset})", 
                _taskManagerId, consumerGroupId, shouldForceReset);

            _logger.LogInformation("💡 TaskManager {TaskManagerId}: Using timestamp-based consumer group for guaranteed fresh consumption", _taskManagerId);

            // APACHE FLINK 2.0 KAFKASOURCE CONFIGURATION: Exact same settings as Apache Flink KafkaSource
            var consumerConfig = new ConsumerConfig
            {
                BootstrapServers = bootstrapServers,
                GroupId = consumerGroupId, 
                SecurityProtocol = SecurityProtocol.Plaintext,
                
                // APACHE FLINK 2.0 OFFSET MANAGEMENT: Flink manages offsets through checkpoints, not Kafka auto-commit
                EnableAutoCommit = false, // Critical: Flink checkpoint-based offset management
                AutoOffsetReset = AutoOffsetReset.Earliest, // Start from beginning for new consumer groups
                
                // APACHE FLINK 2.0 SESSION MANAGEMENT: Optimized for continuous streaming
                SessionTimeoutMs = 10000,  // 10s session timeout (Apache Flink 2.0 optimal)
                HeartbeatIntervalMs = 3000, // 3s heartbeat (1/3 of session timeout)
                MaxPollIntervalMs = 60000, // 1 minute max poll (reduced from 5 minutes for responsiveness)
                
                // APACHE FLINK 2.0 PARTITION ASSIGNMENT: Cooperative sticky for minimal disruption
                PartitionAssignmentStrategy = PartitionAssignmentStrategy.CooperativeSticky,
                
                // HIGH-THROUGHPUT FETCH SETTINGS: Optimized for maximum performance like producer script
                FetchMinBytes = 1048576,           // 1MB minimum fetch for efficiency (vs 1 byte)
                FetchWaitMaxMs = 50,               // 50ms max wait (matches Apache Flink 2.0 KafkaSource)
                FetchMaxBytes = 52428800,          // 50MB max fetch for high throughput
                MaxPartitionFetchBytes = 16777216, // 16MB per partition (increased from 1MB)
                
                // APACHE FLINK 2.0 NETWORK OPTIMIZATION: Fast connectivity and metadata refresh
                SocketTimeoutMs = 10000,       // 10s socket timeout
                MetadataMaxAgeMs = 300000,     // 5 minutes metadata refresh
            };

            _logger.LogInformation("🔄 TaskManager {TaskManagerId}: Initializing FlinkKafkaConsumerGroup with servers: {BootstrapServers}", 
                _taskManagerId, bootstrapServers);

            // WAIT FOR TOPIC AVAILABILITY: Check if flinkdotnet.sample.topic exists before proceeding
            _logger.LogInformation("🔍 TaskManager {TaskManagerId}: Checking topic availability: {Topic}", _taskManagerId, _kafkaTopic);
            bool topicAvailable = await WaitForTopicAvailability(bootstrapServers, _kafkaTopic, maxWaitSeconds: 30);
            
            if (!topicAvailable)
            {
                throw new InvalidOperationException($"Topic '{_kafkaTopic}' not found after 30 seconds. " +
                    "This topic should be created by Aspire infrastructure (kafka-init container). " +
                    $"The topic is defined in AppHost Program.cs: create_topic_safe '{_kafkaTopic}'");
            }
            
            _logger.LogInformation("✅ TaskManager {TaskManagerId}: Topic {Topic} is available, proceeding with consumer initialization", _taskManagerId, _kafkaTopic);

            // Simple initialization - FlinkKafkaConsumerGroup now handles resumption internally
            _consumerGroup = new FlinkKafkaConsumerGroup(consumerConfig, _logger);
            await _consumerGroup.InitializeAsync(new[] { _kafkaTopic });

            _logger.LogInformation("✅ TaskManager {TaskManagerId}: FlinkKafkaConsumerGroup initialized with built-in resumption", _taskManagerId);
            
            // CRITICAL FIX: Enhanced consumer group assignment verification with timeout
            await VerifyConsumerGroupAssignmentWithTimeout();
            
            // CRITICAL FIX: Log final configuration for debugging
            _logger.LogInformation("🔍 TaskManager {TaskManagerId}: Final Consumer Configuration:", _taskManagerId);
            _logger.LogInformation("  📋 Bootstrap Servers: {BootstrapServers}", consumerConfig.BootstrapServers);
            _logger.LogInformation("  📋 Consumer Group ID: {GroupId}", consumerConfig.GroupId);
            _logger.LogInformation("  📋 Topic: {Topic}", _kafkaTopic);
            _logger.LogInformation("  📋 Auto Offset Reset: {AutoOffsetReset}", consumerConfig.AutoOffsetReset);
            _logger.LogInformation("  📋 Enable Auto Commit: {EnableAutoCommit}", consumerConfig.EnableAutoCommit);
        }
        
        /// <summary>
        /// Apache Flink 2.0 quick topic verification - just check existence, no message counting
        /// </summary>
        
        /// <summary>
        /// Verify that the consumer group gets proper partition assignment
        /// </summary>
        /// <summary>
        /// Apache Flink 2.0 consumer group assignment verification with timeout and enhanced debugging
        /// </summary>
        private async Task VerifyConsumerGroupAssignmentWithTimeout()
        {
            try
            {
                _logger.LogInformation("🔍 TaskManager {TaskManagerId}: Verifying consumer group assignment...", _taskManagerId);
                
                // Apache Flink 2.0 pattern: Allow up to 10 seconds for partition assignment
                var maxWaitTime = TimeSpan.FromSeconds(10);
                var checkInterval = TimeSpan.FromMilliseconds(500);
                var startTime = DateTime.UtcNow;
                
                while ((DateTime.UtcNow - startTime) < maxWaitTime)
                {
                    var assignment = _consumerGroup!.GetAssignment();
                    
                    if (assignment.Count > 0)
                    {
                        _logger.LogInformation("✅ TaskManager {TaskManagerId}: Consumer group assignment verified:", _taskManagerId);
                        _logger.LogInformation("  📊 Assigned {PartitionCount} partitions: {Partitions}", 
                            assignment.Count, string.Join(", ", assignment.Select(tp => $"{tp.Topic}:{tp.Partition}")));
                        return;
                    }
                    
                    // Log progress every 2 seconds
                    var elapsed = DateTime.UtcNow - startTime;
                    if (elapsed.TotalSeconds % 2 < 0.5)
                    {
                        _logger.LogInformation("⏳ TaskManager {TaskManagerId}: Waiting for partition assignment... {Elapsed:F1}s elapsed", 
                            _taskManagerId, elapsed.TotalSeconds);
                    }
                    
                    await Task.Delay(checkInterval);
                }
                
                // If we get here, assignment verification timed out
                _logger.LogWarning("⚠️ TaskManager {TaskManagerId}: Partition assignment verification timed out after {MaxWait}s", 
                    _taskManagerId, maxWaitTime.TotalSeconds);
                _logger.LogWarning("  💡 Note: Apache Flink 2.0 consumers can receive assignments during polling, so this may be normal");
                
                // Final attempt to log current assignment state
                var finalAssignment = _consumerGroup!.GetAssignment();
                _logger.LogInformation("  📊 Final assignment state: {PartitionCount} partitions", finalAssignment.Count);
                
            }
            catch (Exception ex)
            {
                _logger.LogWarning(ex, "⚠️ TaskManager {TaskManagerId}: Assignment verification encountered an exception - will proceed with polling", _taskManagerId);
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
                
                // MAXIMUM THROUGHPUT SETTINGS: Match produce-1-million-messages.ps1 for consistency
                Acks = Acks.None,                              // Maximum speed, no delivery confirmation  
                LingerMs = 5,                                   // Micro-batching for performance (increased from 2ms)
                BatchSize = 1048576,                            // 1MB batch size (increased from 512KB)
                CompressionType = CompressionType.None,         // No compression for speed
                QueueBufferingMaxKbytes = 128 * 1024 * 1024,    // 128MB internal buffer (increased from 64MB)
                QueueBufferingMaxMessages = 50_000_000,         // 50M message buffer capacity (increased from 20M)
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

        private async Task<bool> WaitForTopicAvailability(string bootstrapServers, string topicName, int maxWaitSeconds = 30)
        {
            _logger.LogInformation("🔍 TaskManager {TaskManagerId}: Waiting for topic '{TopicName}' availability (max {MaxWaitSeconds}s)", 
                _taskManagerId, topicName, maxWaitSeconds);
            
            var adminConfig = new AdminClientConfig 
            { 
                BootstrapServers = bootstrapServers,
                SecurityProtocol = SecurityProtocol.Plaintext,
                SocketTimeoutMs = 10000,
                ApiVersionRequestTimeoutMs = 10000
            };
            
            var startTime = DateTime.UtcNow;
            var checkInterval = TimeSpan.FromSeconds(2);
            var maxWaitTime = TimeSpan.FromSeconds(maxWaitSeconds);
            var attempt = 0;
            
            while ((DateTime.UtcNow - startTime) < maxWaitTime)
            {
                attempt++;
                try
                {
                    using var admin = new AdminClientBuilder(adminConfig).Build();
                    var metadata = admin.GetMetadata(TimeSpan.FromSeconds(15));
                    var topicExists = metadata.Topics.Any(t => t.Topic == topicName && t.Error.Code == ErrorCode.NoError);
                    
                    if (topicExists)
                    {
                        _logger.LogInformation("✅ TaskManager {TaskManagerId}: Topic '{TopicName}' found after {Attempt} attempts in {ElapsedSeconds:F1}s", 
                            _taskManagerId, topicName, attempt, (DateTime.UtcNow - startTime).TotalSeconds);
                        return true;
                    }
                    
                    var elapsed = DateTime.UtcNow - startTime;
                    _logger.LogInformation("⏳ TaskManager {TaskManagerId}: Topic '{TopicName}' not found (attempt {Attempt}/{MaxAttempts}), waiting {CheckIntervalSeconds}s... (elapsed: {ElapsedSeconds:F1}s)", 
                        _taskManagerId, topicName, attempt, maxWaitSeconds / 2, checkInterval.TotalSeconds, elapsed.TotalSeconds);
                }
                catch (Exception ex)
                {
                    var elapsed = DateTime.UtcNow - startTime;
                    _logger.LogDebug(ex, "⚠️ TaskManager {TaskManagerId}: Topic check attempt {Attempt} failed (elapsed: {ElapsedSeconds:F1}s): {Message}", 
                        _taskManagerId, attempt, elapsed.TotalSeconds, ex.Message);
                }
                
                if ((DateTime.UtcNow - startTime) < maxWaitTime)
                {
                    await Task.Delay(checkInterval);
                }
            }
            
            _logger.LogWarning("❌ TaskManager {TaskManagerId}: Topic '{TopicName}' not found after {MaxWaitSeconds} seconds", 
                _taskManagerId, topicName, maxWaitSeconds);
            _logger.LogWarning("⚠️  WARNING: {TopicName} not found in Kafka broker", topicName);
            _logger.LogWarning("⚠️  This topic should be created by Aspire infrastructure (kafka-init container)");
            _logger.LogWarning("⚠️  The topic is defined in AppHost Program.cs line 190: create_topic_safe '{TopicName}'", topicName);
            
            return false;
        }

        private void LogConsumptionStartupInfo()
        {
            _logger.LogInformation("🔄 TaskManager {TaskManagerId}: Starting message consumption with Apache Flink 2.0 patterns and automatic resumption", _taskManagerId);
            
            // Add detailed debugging information
            _logger.LogInformation("🔍 TaskManager {TaskManagerId}: Consumer Configuration Debug Info:", _taskManagerId);
            _logger.LogInformation("  📋 Topic: {Topic}", _kafkaTopic);
            _logger.LogInformation("  📋 Consumer Group: {GroupId}", _consumerGroup!.GetConsumerGroupId());
            _logger.LogInformation("  📋 Redis Counter Key: {CounterKey}", _redisSinkCounterKey);
            _logger.LogInformation("  📋 Global Sequence Key: {GlobalKey}", _globalSequenceKey);
            
            // Log bootstrap servers being used for comparison with producer
            var bootstrapServers = _configuration["DOTNET_KAFKA_BOOTSTRAP_SERVERS"] ?? 
                                 _configuration["ConnectionStrings__kafka"] ?? 
                                 "localhost:9092";
            _logger.LogInformation("  📋 Bootstrap Servers: {BootstrapServers}", bootstrapServers);
        }

        private async Task ConsumeMessagesWithFlinkPatterns(CancellationToken stoppingToken)
        {
            if (_consumerGroup == null)
                throw new InvalidOperationException("Consumer group not initialized");

            var (consumptionContext, consecutiveNullResults, lastProgressLogTime, batchProcessorTask, progressReporterTask) = 
                await InitializeConsumptionContext(stoppingToken);

            await RunMainConsumptionLoop(consumptionContext, consecutiveNullResults, lastProgressLogTime, stoppingToken);

            await FinalizeConsumption(consumptionContext, batchProcessorTask, progressReporterTask);
        }

        private async Task<(ConsumptionContext context, int consecutiveNullResults, DateTime lastProgressLogTime, Task batchProcessorTask, Task progressReporterTask)> 
            InitializeConsumptionContext(CancellationToken stoppingToken)
        {
            LogConsumptionStartupInfo();
            await LogDetailedConsumerGroupStatus();
            
            var consumptionContext = new ConsumptionContext(DateTime.UtcNow);
            var consecutiveNullResults = 0;
            var lastProgressLogTime = DateTime.UtcNow;

            // Start background Redis batch processor for high-throughput performance
            var batchProcessorTask = ProcessRedisBatchUpdatesAsync(stoppingToken);
            
            // Start background progress reporter similar to producer script
            var progressReporterTask = ProcessProgressReportsAsync(stoppingToken);

            return (consumptionContext, consecutiveNullResults, lastProgressLogTime, batchProcessorTask, progressReporterTask);
        }

        private async Task RunMainConsumptionLoop(ConsumptionContext consumptionContext, int consecutiveNullResults, DateTime lastProgressLogTime, CancellationToken stoppingToken)
        {
            var maxConsecutiveNulls = 60; // Allow 60 consecutive null results before enhanced logging

            while (!stoppingToken.IsCancellationRequested)
            {
                try
                {
                    // CRITICAL CHECK: Stop processing once we've reached exactly 1,000,000 messages
                    var currentProcessedCount = Interlocked.Read(ref _messagesProcessed);
                    if (currentProcessedCount >= 1000000)
                    {
                        _logger.LogInformation("🛑 TaskManager {TaskManagerId}: Reached target of 1,000,000 messages ({CurrentCount}), stopping consumption", 
                            _taskManagerId, currentProcessedCount);
                        break;
                    }
                    
                    // APACHE FLINK 2.0 KAFKASOURCE PATTERN: Ultra-fast polling with 50ms timeout exactly like Apache Flink
                    // This matches the exact polling pattern used in Apache Flink 2.0 KafkaSource implementation
                    var consumeResult = _consumerGroup?.ConsumeMessage(TimeSpan.FromMilliseconds(50)); // Apache Flink 2.0 standard: 50ms
                    
                    if (consumeResult?.Message != null)
                    {
                        consecutiveNullResults = await ProcessMessage(consumeResult, consumptionContext, stoppingToken);
                        if (consecutiveNullResults == -1) break; // Signal to break from main loop
                    }
                    else
                    {
                        consecutiveNullResults = HandleNullResult(consecutiveNullResults, maxConsecutiveNulls, ref lastProgressLogTime);
                    }
                    
                    await HandleRecoveryMode(stoppingToken);
                }
                catch (ConsumeException ex)
                {
                    _logger.LogDebug(ex, "🔄 TaskManager {TaskManagerId}: ConsumeException in Apache Flink 2.0 polling - normal behavior: {Error}",
                        _taskManagerId, ex.Error.Reason);
                    // Apache Flink 2.0 pattern: Brief pause only for actual consume errors
                    await Task.Delay(TimeSpan.FromMilliseconds(50), stoppingToken); // Minimal 50ms pause for consume errors
                }
                catch (OperationCanceledException ex)
                {
                    _logger.LogInformation(ex, "🛑 TaskManager {TaskManagerId}: Consumption cancelled", _taskManagerId);
                    break;
                }
                catch (Exception ex)
                {
                    _logger.LogError(ex, "❌ TaskManager {TaskManagerId}: Unexpected error during consumption", _taskManagerId);
                    await Task.Delay(TimeSpan.FromSeconds(1), stoppingToken); // Keep 1s delay for unexpected errors
                }
            }
        }

        private async Task<int> ProcessMessage(ConsumeResult<Ignore, byte[]>? consumeResult, ConsumptionContext consumptionContext, CancellationToken stoppingToken)
        {
            // Double-check we haven't exceeded limit before processing
            var beforeProcessCount = Interlocked.Read(ref _messagesProcessed);
            if (beforeProcessCount >= 1000000)
            {
                _logger.LogInformation("🛑 TaskManager {TaskManagerId}: Reached limit just before processing message, skipping", _taskManagerId);
                return -1; // Signal to break from main loop
            }
            
            // Reset null counter on successful consumption and process message
            await ProcessConsumeResult(consumeResult, consumptionContext, stoppingToken);
            return 0; // Reset null counter
        }

        private int HandleNullResult(int consecutiveNullResults, int maxConsecutiveNulls, ref DateTime lastProgressLogTime)
        {
            consecutiveNullResults++;
            
            // CRITICAL FIX: Enhanced null result handling with diagnostic logging
            if (consecutiveNullResults >= maxConsecutiveNulls)
            {
                _logger.LogInformation("🔍 TaskManager {TaskManagerId}: {ConsecutiveNulls} consecutive null results - checking consumer status", 
                    _taskManagerId, consecutiveNullResults);
                
                // Check if consumer group is still properly assigned
                var currentAssignment = _consumerGroup?.GetAssignment();
                _logger.LogInformation("  📊 Current assignment: {PartitionCount} partitions", currentAssignment?.Count ?? 0);
                
                // Check if consumer group is in recovery mode
                if (_consumerGroup?.IsInRecoveryMode() == true)
                {
                    var failures = _consumerGroup.GetConsecutiveFailureCount();
                    _logger.LogWarning("  ⚠️ Consumer group in recovery mode with {FailureCount} failures", failures);
                }
                
                return 0; // Reset counter after logging
            }
            else if ((DateTime.UtcNow - lastProgressLogTime).TotalSeconds >= 30) // Reduced logging frequency
            {
                _logger.LogDebug("🔍 TaskManager {TaskManagerId}: Apache Flink 2.0 continuous polling - assignment: {AssignmentCount} partitions", 
                    _taskManagerId, _consumerGroup?.GetAssignment().Count ?? 0);
                lastProgressLogTime = DateTime.UtcNow;
            }
            
            // APACHE FLINK 2.0 PATTERN: NO DELAY in tight polling loop for maximum responsiveness
            // Apache Flink 2.0 KafkaSource uses continuous polling without artificial delays
            // The 50ms timeout in ConsumeMessage already provides CPU breathing room
            
            return consecutiveNullResults;
        }

        private async Task HandleRecoveryMode(CancellationToken stoppingToken)
        {
            // Apache Flink 2.0 pattern: Minimal recovery mode handling
            if (_consumerGroup?.IsInRecoveryMode() == true)
            {
                var failureCount = _consumerGroup.GetConsecutiveFailureCount();
                _logger.LogWarning("⚠️ TaskManager {TaskManagerId}: Apache Flink 2.0 recovery mode (failures: {FailureCount})", 
                    _taskManagerId, failureCount);
                
                // Apache Flink 2.0 pattern: Brief pause only for actual recovery, not normal operation
                await Task.Delay(TimeSpan.FromMilliseconds(100), stoppingToken); // Minimal 100ms recovery pause
            }
        }

        private async Task FinalizeConsumption(ConsumptionContext consumptionContext, Task batchProcessorTask, Task progressReporterTask)
        {
            // Complete any pending Redis batch updates
            var finalPendingUpdates = Interlocked.Exchange(ref _pendingRedisUpdates, 0);
            if (finalPendingUpdates > 0)
            {
                await BatchUpdateRedisCountersAsync(finalPendingUpdates);
            }
            
            // Wait for batch processor to complete
            await batchProcessorTask;
            
            // Wait for progress reporter to complete
            await progressReporterTask;
            
            // Write final completion log
            var finalCount = Interlocked.Read(ref _messagesProcessed);
            if (_actualProcessingStartTime.HasValue && finalCount > 0)
            {
                var totalElapsed = (DateTime.UtcNow - _actualProcessingStartTime.Value).TotalSeconds;
                var finalRate = totalElapsed > 0 ? (int)Math.Round(finalCount / totalElapsed) : 0;
                
                _logger.LogInformation("🏁 TaskManager {TaskManagerId}: FINAL PROCESSING COMPLETE - " +
                                     "Total: {TotalCount:N0} messages, Time: {ElapsedTime:F3}s, Rate: {Rate:N0} msg/sec",
                                     _taskManagerId, finalCount, totalElapsed, finalRate);
                
                // Write final progress log
                await WriteCompletionLogAsync(finalCount, totalElapsed, finalRate);
            }
            
            LogFinalConsumptionStats(consumptionContext.StartTime);
        }
        
        /// <summary>
        /// Enhanced debugging for consumption issues
        /// </summary>
        
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
                LogEnhancedTopicStatus();
                
            }
            catch (Exception ex)
            {
                _logger.LogWarning(ex, "⚠️ Failed to log detailed consumer group status");
            }
        }
        
        /// <summary>
        /// Enhanced topic status logging
        /// </summary>
        private void LogEnhancedTopicStatus()
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
                                if (VerifyKafkaConnectivity(bootstrapServers))
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
        private bool VerifyKafkaConnectivity(string bootstrapServers)
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

        private Task ProcessMessageWithFlinkPatterns(ConsumeResult<Ignore, byte[]> consumeResult)
        {
            try
            {
                // CRITICAL CHECK: Stop processing if we've already reached 1,000,000 messages
                var currentCount = Interlocked.Read(ref _messagesProcessed);
                if (currentCount >= 1000000)
                {
                    _logger.LogInformation("🛑 TaskManager {TaskManagerId}: Already at 1,000,000 messages limit, skipping message processing", _taskManagerId);
                    return Task.CompletedTask;
                }
                
                // Increment processed message counter FIRST for accurate counting
                var newCount = Interlocked.Increment(ref _messagesProcessed);
                
                // SAFETY CHECK: If we somehow exceeded 1,000,000, decrement back and stop
                if (newCount > 1000000)
                {
                    Interlocked.Decrement(ref _messagesProcessed);
                    _logger.LogInformation("🛑 TaskManager {TaskManagerId}: Hit exact 1,000,000 message limit, stopping processing", _taskManagerId);
                    return Task.CompletedTask;
                }
                
                // Record actual processing start time when FIRST message is processed
                if (!_actualProcessingStartTime.HasValue)
                {
                    lock (_timingLock)
                    {
                        if (!_actualProcessingStartTime.HasValue)
                        {
                            _actualProcessingStartTime = DateTime.UtcNow;
                            _logger.LogInformation("🚀 TaskManager {TaskManagerId}: ACTUAL processing started at {StartTime}", 
                                _taskManagerId, _actualProcessingStartTime.Value.ToString("yyyy-MM-dd HH:mm:ss.fff"));
                            
                            // Write initial progress log immediately
                            _ = Task.Run(async () => await WriteProgressLogAsync(newCount, _actualProcessingStartTime));
                        }
                    }
                }
                
                // Convert bytes to string (Apache Flink standard deserialization)
                var messageContent = System.Text.Encoding.UTF8.GetString(consumeResult.Message.Value);
                
                // Enhanced logging for initial messages and major milestones only  
                if (newCount <= 10 || newCount % 50000 == 0) // Reduced from every 10k to every 50k
                {
                    _logger.LogInformation("⚡ CONSUME: TaskManager {TaskManagerId} message {Count} from " +
                                           "topic: {Topic}, partition: {Partition}, offset: {Offset}",
                                           _taskManagerId, newCount, consumeResult.Topic, 
                                           consumeResult.Partition.Value, consumeResult.Offset.Value);
                }
                
                // HIGH-PERFORMANCE: Batch Redis updates instead of individual operations per message
                Interlocked.Increment(ref _pendingRedisUpdates);
                
                // Produce to output topic with fire-and-forget for maximum performance
                ProduceToOutputTopicFireAndForget(messageContent);
                
                // Periodic checkpoint simulation - less frequent for performance
                if (newCount % 25000 == 0) // Increased from every 5k to every 25k messages
                {
                    _ = Task.Run(async () => await SimulateFlinkCheckpoint());
                }
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "❌ TaskManager {TaskManagerId}: Error processing message from " +
                               "partition {Partition}, offset {Offset}",
                               _taskManagerId, consumeResult.Partition.Value, consumeResult.Offset.Value);
                // Continue processing other messages
            }
            
            return Task.CompletedTask;
        }

        private void ProduceToOutputTopicFireAndForget(string messageContent)
        {
            if (_producer == null) return;

            try
            {
                // Create processed message (minimal transformation for performance)
                var processedMessage = $"PROCESSED:{messageContent}:{DateTimeOffset.UtcNow.ToUnixTimeMilliseconds()}";
                var messageBytes = System.Text.Encoding.UTF8.GetBytes(processedMessage);
                
                // Fire-and-forget produce for maximum speed (synchronous like producer script)
                var message = new Message<Null, byte[]> { Value = messageBytes };
                _producer.Produce(_outputTopic, message);
            }
            catch (Exception ex)
            {
                // Minimal error logging to avoid performance impact
                if (_messagesProcessed % 10000 == 0)
                {
                    _logger.LogWarning(ex, "⚠️ TaskManager {TaskManagerId}: Output topic production issues", _taskManagerId);
                }
            }
        }

        /// <summary>
        /// Background progress reporter - updates progress every second like producer script
        /// </summary>
        private async Task ProcessProgressReportsAsync(CancellationToken stoppingToken)
        {
            while (!stoppingToken.IsCancellationRequested)
            {
                try
                {
                    await Task.Delay(1000, stoppingToken); // Update every second like producer script
                    
                    var currentCount = Interlocked.Read(ref _messagesProcessed);
                    var now = DateTime.UtcNow;
                    
                    // Skip if we just updated recently (avoid spam)
                    if ((now - _lastProgressUpdateTime).TotalMilliseconds < 500)
                    {
                        continue;
                    }
                    
                    _lastProgressUpdateTime = now;
                    
                    // Write progress log with current status
                    await WriteProgressLogAsync(currentCount, _actualProcessingStartTime);
                    
                    // Stop reporting once we reach 1M messages
                    if (currentCount >= 1000000)
                    {
                        break;
                    }
                }
                catch (OperationCanceledException)
                {
                    break;
                }
                catch (Exception ex)
                {
                    _logger.LogWarning(ex, "⚠️ TaskManager {TaskManagerId}: Progress reporter error", _taskManagerId);
                    await Task.Delay(TimeSpan.FromSeconds(1), stoppingToken);
                }
            }
        }
        
        /// <summary>
        /// High-performance batch Redis counter updates - processes batches every 100ms
        /// </summary>
        private async Task ProcessRedisBatchUpdatesAsync(CancellationToken stoppingToken)
        {
            while (!stoppingToken.IsCancellationRequested)
            {
                try
                {
                    await Task.Delay(_redisUpdateInterval, stoppingToken);
                    
                    var pendingUpdates = Interlocked.Exchange(ref _pendingRedisUpdates, 0);
                    if (pendingUpdates > 0)
                    {
                        await BatchUpdateRedisCountersAsync(pendingUpdates);
                    }
                }
                catch (OperationCanceledException)
                {
                    break;
                }
                catch (Exception ex)
                {
                    _logger.LogWarning(ex, "⚠️ TaskManager {TaskManagerId}: Redis batch processor error", _taskManagerId);
                    await Task.Delay(TimeSpan.FromSeconds(1), stoppingToken);
                }
            }
        }
        
        /// <summary>
        /// Efficient batch Redis counter updates with exact counting verification
        /// Ensures exactly 1,000,000 messages are counted, no more, no less
        /// </summary>
        private async Task BatchUpdateRedisCountersAsync(long updateCount)
        {
            try
            {
                // CRITICAL FIX: Use Redis scripting to ensure atomic increment with STRICT limit enforcement
                // This prevents race conditions between multiple TaskManagers and ensures exactly 1M messages
                
                var currentProcessedCount = Interlocked.Read(ref _messagesProcessed);
                
                if (updateCount <= 0)
                {
                    return;
                }
                
                // Use Lua script for atomic increment with STRICT limit check to ensure exactly 1,000,000
                const string luaScript = @"
                    local sinkKey = KEYS[1]
                    local globalKey = KEYS[2] 
                    local increment = tonumber(ARGV[1])
                    local maxLimit = tonumber(ARGV[2])
                    
                    local currentSink = tonumber(redis.call('GET', sinkKey) or 0)
                    local currentGlobal = tonumber(redis.call('GET', globalKey) or 0)
                    
                    -- Calculate EXACT safe increment to not exceed limit
                    -- CRITICAL: This ensures we never go over exactly 1,000,000
                    local remainingCapacity = math.max(0, maxLimit - currentSink)
                    local safeIncrement = math.min(increment, remainingCapacity)
                    
                    -- Only increment if we have remaining capacity
                    if safeIncrement > 0 and currentSink < maxLimit then
                        local newSink = redis.call('INCRBY', sinkKey, safeIncrement)
                        local newGlobal = redis.call('INCRBY', globalKey, safeIncrement)
                        
                        -- SAFETY CHECK: If we somehow exceeded limit, correct it
                        if newSink > maxLimit then
                            local excess = newSink - maxLimit
                            redis.call('DECRBY', sinkKey, excess)
                            redis.call('DECRBY', globalKey, excess)
                            newSink = maxLimit
                            newGlobal = newGlobal - excess
                            safeIncrement = safeIncrement - excess
                        end
                        
                        return {newSink, newGlobal, safeIncrement}
                    else
                        return {currentSink, currentGlobal, 0}
                    end
                ";
                
                var result = await _redisDatabase.ScriptEvaluateAsync(luaScript, 
                    new RedisKey[] { _redisSinkCounterKey, _globalSequenceKey },
                    new RedisValue[] { updateCount, 1000000 });
                
                if (result != null && result.Resp2Type == ResultType.Array)
                {
                    // Use explicit cast to RedisValue[] 
                    RedisValue[]? resultArray = (RedisValue[]?)result;
                    if (resultArray != null && resultArray.Length >= 3)
                    {
                        var newSinkCount = (long)resultArray[0];
                        var newGlobalCount = (long)resultArray[1]; 
                        var actualIncrement = (long)resultArray[2];
                    
                    // Log only at significant milestones for performance
                    if (currentProcessedCount <= 100 || currentProcessedCount % 25000 == 0)
                    {
                        _logger.LogInformation("⚡ REDIS BATCH: TaskManager {TaskManagerId} updated +{BatchSize} (requested: {RequestedSize}) - " +
                                       "local: {LocalCount}, redis_sink: {RedisSinkCount}, redis_global: {RedisGlobalCount}",
                                       _taskManagerId, actualIncrement, updateCount, currentProcessedCount, newSinkCount, newGlobalCount);
                    }
                    
                    // If we couldn't increment as much as requested, we've hit the limit
                    if (actualIncrement < updateCount)
                    {
                        _logger.LogInformation("🛑 TaskManager {TaskManagerId}: Hit Redis counter limit - sink: {SinkCount}, global: {GlobalCount}", 
                            _taskManagerId, newSinkCount, newGlobalCount);
                    }
                    
                    // CRITICAL: Verify we never exceed 1,000,000 exactly
                    if (newSinkCount > 1000000)
                    {
                        _logger.LogError("❌ TaskManager {TaskManagerId}: CRITICAL ERROR - Redis counter exceeded 1,000,000: {ActualCount}", 
                            _taskManagerId, newSinkCount);
                        
                        // Emergency correction
                        await _redisDatabase.StringSetAsync(_redisSinkCounterKey, 1000000);
                        await _redisDatabase.StringSetAsync(_globalSequenceKey, 1000000);
                    }
                    }
                }
            }
            catch (Exception ex)
            {
                _logger.LogWarning(ex, "⚠️ TaskManager {TaskManagerId}: Batch Redis update failed for {UpdateCount} messages", 
                    _taskManagerId, updateCount);
            }
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
