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
        private readonly string _redisSinkCounterKey;
        private readonly string _taskManagerId;
        private FlinkKafkaConsumerGroup? _consumerGroup;
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
            _redisSinkCounterKey = _configuration["SIMULATOR_REDIS_KEY_SINK_COUNTER"] ?? "flinkdotnet:sample:processed_message_counter";
            _taskManagerId = Environment.GetEnvironmentVariable("TaskManagerId") ?? "TM-Unknown";
            
            _logger.LogInformation("TaskManagerKafkaConsumer initialized for TaskManager: {TaskManagerId}", _taskManagerId);
        }

        protected override async Task ExecuteAsync(CancellationToken stoppingToken)
        {
            _logger.LogInformation("🚀 TaskManager {TaskManagerId}: Starting Apache Flink-compliant Kafka consumption", _taskManagerId);
            
            // Write consumer startup log to file for stress test monitoring
            await WriteConsumerStartupLogAsync();
            
            // Initialize Redis counter to indicate FlinkJobSimulator has started
            try
            {
                _logger.LogInformation("🔄 TaskManager {TaskManagerId}: Initializing Redis counter to indicate startup", _taskManagerId);
                await _redisDatabase.StringSetAsync(_redisSinkCounterKey, 0);
                _logger.LogInformation("✅ TaskManager {TaskManagerId}: Redis counter initialized successfully", _taskManagerId);
                
                // Update startup log with Redis success
                await UpdateStartupLogAsync("REDIS_CONNECTED", "Redis counter initialized successfully");
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "❌ TaskManager {TaskManagerId}: Failed to initialize Redis counter", _taskManagerId);
                await UpdateStartupLogAsync("REDIS_FAILED", $"Redis initialization failed: {ex.Message}");
                throw;
            }
            
            try
            {
                _logger.LogInformation("🔄 TaskManager {TaskManagerId}: Starting Kafka consumer group initialization", _taskManagerId);
                await InitializeFlinkKafkaConsumerGroup();
                await UpdateStartupLogAsync("KAFKA_CONSUMING", "Kafka consumer group started successfully");
                
                _logger.LogInformation("🔄 TaskManager {TaskManagerId}: Starting message consumption", _taskManagerId);
                await ConsumeMessagesWithFlinkPatterns(stoppingToken);
            }
            catch (InvalidOperationException ex) when (ex.Message.Contains("Failed to connect to Kafka"))
            {
                // Kafka connection timeout - log detailed error and keep service alive for Aspire
                _logger.LogError(ex, "❌ TaskManager {TaskManagerId}: Kafka connection failed after retry timeout for topic '{Topic}'", 
                    _taskManagerId, _kafkaTopic);
                await UpdateStartupLogAsync("KAFKA_CONNECTION_TIMEOUT", $"Kafka connection timeout: {ex.Message}");
                
                // Keep the service alive but not consuming - Aspire will restart if needed
                _logger.LogWarning("💓 TaskManager {TaskManagerId}: Entering heartbeat mode due to Kafka connection failure", _taskManagerId);
                await KeepServiceAliveAsync(stoppingToken);
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "❌ TaskManager {TaskManagerId}: Unexpected error in Kafka consumption for topic '{Topic}'", 
                    _taskManagerId, _kafkaTopic);
                await UpdateStartupLogAsync("KAFKA_FAILED", $"Kafka consumption failed: {ex.Message}");
                throw new InvalidOperationException($"TaskManager {_taskManagerId} failed during Kafka consumption", ex);
            }
            finally
            {
                await CleanupResources();
            }
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
                
                var logPath = Path.Combine(Directory.GetCurrentDirectory(), "flinkjobsimulator_consumer.log");
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
                
                var logPath = Path.Combine(Directory.GetCurrentDirectory(), "flinkjobsimulator_status.log");
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

            // Apache Flink-compliant consumer configuration
            var consumerConfig = new ConsumerConfig
            {
                BootstrapServers = bootstrapServers,
                GroupId = "flink-taskmanager-consumer-group", // Unified consumer group for all TaskManagers
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

            // Retry logic for 2 minutes until able to connect to Kafka
            await InitializeWithRetryAsync(consumerConfig, TimeSpan.FromMinutes(2));

            _logger.LogInformation("✅ TaskManager {TaskManagerId}: FlinkKafkaConsumerGroup initialized successfully", _taskManagerId);
        }

        /// <summary>
        /// Initialize FlinkKafkaConsumerGroup with retry logic for 2 minutes until able to connect.
        /// Catches Kafka exceptions and logs warnings while retrying.
        /// </summary>
        private async Task InitializeWithRetryAsync(ConsumerConfig consumerConfig, TimeSpan retryTimeout)
        {
            var startTime = DateTime.UtcNow;
            var retryCount = 0;
            var retryInterval = TimeSpan.FromSeconds(10); // Retry every 10 seconds
            
            while ((DateTime.UtcNow - startTime) < retryTimeout)
            {
                retryCount++;
                var elapsed = DateTime.UtcNow - startTime;
                
                try
                {
                    _logger.LogInformation("🔄 TaskManager {TaskManagerId}: Kafka connection attempt {RetryCount} " +
                                         "(elapsed: {Elapsed:mm\\:ss})", _taskManagerId, retryCount, elapsed);
                    
                    _consumerGroup = new FlinkKafkaConsumerGroup(consumerConfig, _logger);
                    await _consumerGroup.InitializeAsync(new[] { _kafkaTopic });
                    
                    _logger.LogInformation("✅ TaskManager {TaskManagerId}: Kafka connection successful on attempt {RetryCount} " +
                                         "after {Elapsed:mm\\:ss}", _taskManagerId, retryCount, elapsed);
                    return; // Success! Exit retry loop
                }
                catch (Exception ex) when (IsKafkaConnectionException(ex))
                {
                    _logger.LogWarning("⚠️ TaskManager {TaskManagerId}: Kafka connection failed on attempt {RetryCount} " +
                                     "after {Elapsed:mm\\:ss} - {ErrorType}: {ErrorMessage}", 
                                     _taskManagerId, retryCount, elapsed, ex.GetType().Name, ex.Message);
                    
                    // Clean up failed consumer group attempt
                    _consumerGroup?.Dispose();
                    _consumerGroup = null;
                    
                    // Check if we should continue retrying
                    var timeRemaining = retryTimeout - elapsed;
                    if (timeRemaining <= TimeSpan.Zero)
                    {
                        _logger.LogError("❌ TaskManager {TaskManagerId}: Kafka connection timeout after {RetryCount} attempts " +
                                       "over {Elapsed:mm\\:ss}. Final error: {ErrorMessage}", 
                                       _taskManagerId, retryCount, elapsed, ex.Message);
                        throw new InvalidOperationException(
                            $"Failed to connect to Kafka after {retryCount} attempts over {elapsed:mm\\:ss}. " +
                            $"Bootstrap servers: {consumerConfig.BootstrapServers}. " +
                            $"Last error: {ex.Message}", ex);
                    }
                    
                    // Log next retry info
                    var nextRetryIn = TimeSpan.FromSeconds(Math.Min(retryInterval.TotalSeconds, timeRemaining.TotalSeconds));
                    _logger.LogInformation("🔄 TaskManager {TaskManagerId}: Retrying Kafka connection in {RetryDelay:ss}s " +
                                         "(time remaining: {TimeRemaining:mm\\:ss})", 
                                         _taskManagerId, nextRetryIn, timeRemaining);
                    
                    // Wait before next retry
                    await Task.Delay(nextRetryIn);
                }
                catch (Exception ex)
                {
                    // Non-Kafka exceptions (like Redis issues) should not be retried here
                    _logger.LogError(ex, "❌ TaskManager {TaskManagerId}: Non-Kafka error during initialization: {ErrorMessage}", 
                                   _taskManagerId, ex.Message);
                    throw;
                }
            }
            
            // This should not be reached due to the timeout check above, but included for safety
            throw new InvalidOperationException(
                $"Kafka connection timeout after {retryCount} attempts over {retryTimeout:mm\\:ss}. " +
                $"Bootstrap servers: {consumerConfig.BootstrapServers}");
        }

        /// <summary>
        /// Determine if an exception is related to Kafka connectivity and should be retried.
        /// </summary>
        private static bool IsKafkaConnectionException(Exception ex)
        {
            // Check for Kafka-specific exceptions that indicate connection issues
            return ex is KafkaException ||
                   ex is ConsumeException ||
                   ex.GetType().Name.StartsWith("ProduceException") ||
                   ex.Message.Contains("broker", StringComparison.OrdinalIgnoreCase) ||
                   ex.Message.Contains("kafka", StringComparison.OrdinalIgnoreCase) ||
                   ex.Message.Contains("bootstrap", StringComparison.OrdinalIgnoreCase) ||
                   ex.Message.Contains("connection", StringComparison.OrdinalIgnoreCase) ||
                   ex.Message.Contains("timeout", StringComparison.OrdinalIgnoreCase) ||
                   ex.Message.Contains("ApiVersion", StringComparison.OrdinalIgnoreCase) ||
                   ex.Message.Contains("security.protocol", StringComparison.OrdinalIgnoreCase) ||
                   ex.InnerException != null && IsKafkaConnectionException(ex.InnerException);
        }

        /// <summary>
        /// Keep the service alive with heartbeat when Kafka connection fails.
        /// This prevents Aspire from considering the service failed while allowing restart mechanisms to work.
        /// </summary>
        private async Task KeepServiceAliveAsync(CancellationToken stoppingToken)
        {
            _logger.LogInformation("💓 TaskManager {TaskManagerId}: Starting heartbeat mode (Kafka unavailable)", _taskManagerId);
            
            var heartbeatCount = 0;
            try
            {
                while (!stoppingToken.IsCancellationRequested && heartbeatCount < 360) // 360 * 10s = 1 hour max
                {
                    heartbeatCount++;
                    await Task.Delay(TimeSpan.FromSeconds(10), stoppingToken);
                    
                    _logger.LogInformation("💓 TaskManager {TaskManagerId}: Heartbeat {Count} - Kafka unavailable, service alive", 
                        _taskManagerId, heartbeatCount);
                    
                    // Periodically try to reconnect to Kafka
                    if (heartbeatCount % 6 == 0) // Every 60 seconds
                    {
                        _logger.LogInformation("🔄 TaskManager {TaskManagerId}: Attempting Kafka reconnection (heartbeat {Count})", 
                            _taskManagerId, heartbeatCount);
                        
                        try
                        {
                            await InitializeFlinkKafkaConsumerGroup();
                            _logger.LogInformation("✅ TaskManager {TaskManagerId}: Kafka reconnection successful, resuming consumption", 
                                _taskManagerId);
                            
                            // If reconnection successful, resume normal consumption
                            await ConsumeMessagesWithFlinkPatterns(stoppingToken);
                            return;
                        }
                        catch (Exception ex)
                        {
                            _logger.LogWarning("⚠️ TaskManager {TaskManagerId}: Kafka reconnection failed: {ErrorMessage}", 
                                _taskManagerId, ex.Message);
                            // Continue heartbeat mode
                        }
                    }
                }
                
                _logger.LogWarning("💓 TaskManager {TaskManagerId}: Heartbeat mode timeout reached, service will exit", _taskManagerId);
            }
            catch (OperationCanceledException)
            {
                _logger.LogInformation("💓 TaskManager {TaskManagerId}: Heartbeat mode cancelled", _taskManagerId);
            }
        }

        private async Task ConsumeMessagesWithFlinkPatterns(CancellationToken stoppingToken)
        {
            if (_consumerGroup == null)
                throw new InvalidOperationException("Consumer group not initialized");

            _logger.LogInformation("🔄 TaskManager {TaskManagerId}: Starting message consumption with Apache Flink patterns", _taskManagerId);
            
            var consumptionContext = new ConsumptionContext(DateTime.UtcNow);

            while (!stoppingToken.IsCancellationRequested)
            {
                try
                {
                    var consumeResult = _consumerGroup.ConsumeMessage(TimeSpan.FromMilliseconds(1000));
                    await ProcessConsumeResult(consumeResult, consumptionContext, stoppingToken);
                }
                catch (ConsumeException ex)
                {
                    if (!await HandleConsumeException(ex, stoppingToken))
                        break;
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
                await Task.Delay(100, stoppingToken);
            }
        }

        private void CheckAndLogProgress(ConsumeResult<Ignore, byte[]> consumeResult, ConsumptionContext context)
        {
            if ((DateTime.UtcNow - context.LastLogTime).TotalSeconds >= 30)
            {
                var currentProcessedCount = Interlocked.Read(ref _messagesProcessed);
                var messagesInPeriod = currentProcessedCount - context.LastProcessedCount;
                var elapsed = DateTime.UtcNow - context.StartTime;
                var totalRate = currentProcessedCount / elapsed.TotalSeconds;
                
                _logger.LogInformation("📊 TaskManager {TaskManagerId}: Processed {TotalMessages} messages " +
                                     "(+{PeriodMessages} in last 30s) Rate: {Rate:F1} msg/s " +
                                     "Partition: {Partition}",
                                     _taskManagerId, currentProcessedCount, messagesInPeriod, totalRate,
                                     consumeResult.Partition.Value);
                
                context.LastLogTime = DateTime.UtcNow;
                context.LastProcessedCount = currentProcessedCount;
            }
        }

        private async Task<bool> HandleConsumeException(ConsumeException ex, CancellationToken stoppingToken)
        {
            _logger.LogWarning(ex, "⚠️ TaskManager {TaskManagerId}: Consume exception: {Error}", 
                _taskManagerId, ex.Error.Reason);
            
            if (ex.Error.IsFatal)
            {
                _logger.LogError("💥 TaskManager {TaskManagerId}: Fatal Kafka error, stopping consumption", _taskManagerId);
                return false;
            }
            
            await Task.Delay(TimeSpan.FromSeconds(2), stoppingToken);
            return true;
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
                
                // 🔄 ENHANCED KAFKA LOGGING: Log consumption for monitoring
                if (_messagesProcessed < 10 || _messagesProcessed % 10000 == 0)
                {
                    _logger.LogDebug("🔄 KAFKA CONSUME: TaskManager {TaskManagerId} consumed message from " +
                                   "topic: {Topic}, partition: {Partition}, offset: {Offset}, message: {Message}",
                                   _taskManagerId, consumeResult.Topic, consumeResult.Partition.Value, 
                                   consumeResult.Offset.Value, messageContent);
                }
                
                // Process the message using Apache Flink sink patterns
                await ProcessThroughFlinkSinkFunction();
                
                // Increment processed message counter
                Interlocked.Increment(ref _messagesProcessed);
                
                // Update Redis counter using Apache Flink sink pattern
                await UpdateRedisCounterWithFlinkPatterns(messageContent);
                
                // Periodic checkpoint simulation (Apache Flink pattern)
                if (_messagesProcessed % 1000 == 0)
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

        private static async Task ProcessThroughFlinkSinkFunction()
        {
            // Simulate Apache Flink sink function processing
            // In a real implementation, this would go through the actual sink function
            await Task.Delay(1); // Minimal processing simulation
        }

        private async Task UpdateRedisCounterWithFlinkPatterns(string messageContent)
        {
            try
            {
                // 🔄 ENHANCED REDIS LOGGING: Log Redis communication attempts  
                if (_messagesProcessed < 10 || _messagesProcessed % 10000 == 0)
                {
                    _logger.LogDebug("🔄 REDIS SINK: TaskManager {TaskManagerId} updating Redis counter " +
                                   "key: {RedisKey}, message: {Message}",
                                   _taskManagerId, _redisSinkCounterKey, messageContent);
                }
                
                var newCount = await _redisDatabase.StringIncrementAsync(_redisSinkCounterKey);
                
                // ✅ ENHANCED REDIS LOGGING: Log successful Redis operations
                if (_messagesProcessed < 10 || _messagesProcessed % 10000 == 0)
                {
                    _logger.LogDebug("✅ REDIS SUCCESS: TaskManager {TaskManagerId} incremented Redis counter " +
                                   "to {NewCount}",
                                   _taskManagerId, newCount);
                }
            }
            catch (Exception ex)
            {
                _logger.LogWarning(ex, "⚠️ TaskManager {TaskManagerId}: Failed to update Redis counter", _taskManagerId);
                // Continue processing - Redis update failures shouldn't stop message processing
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