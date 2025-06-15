using System;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;
using Confluent.Kafka;
using FlinkDotNet.Connectors.Sources.Kafka;
using FlinkDotNet.Core.Abstractions.Context;
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
            
            try
            {
                await InitializeFlinkKafkaConsumerGroup();
                await ConsumeMessagesWithFlinkPatterns(stoppingToken);
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "❌ TaskManager {TaskManagerId}: Error in Kafka consumption", _taskManagerId);
                throw;
            }
            finally
            {
                await CleanupResources();
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

            _consumerGroup = new FlinkKafkaConsumerGroup(consumerConfig, _logger);
            await _consumerGroup.InitializeAsync(new[] { _kafkaTopic });

            _logger.LogInformation("✅ TaskManager {TaskManagerId}: FlinkKafkaConsumerGroup initialized successfully", _taskManagerId);
        }

        private async Task ConsumeMessagesWithFlinkPatterns(CancellationToken stoppingToken)
        {
            if (_consumerGroup == null)
                throw new InvalidOperationException("Consumer group not initialized");

            _logger.LogInformation("🔄 TaskManager {TaskManagerId}: Starting message consumption with Apache Flink patterns", _taskManagerId);
            
            var startTime = DateTime.UtcNow;
            var lastLogTime = DateTime.UtcNow;
            var lastProcessedCount = 0L;

            while (!stoppingToken.IsCancellationRequested)
            {
                try
                {
                    // Use FlinkKafkaConsumerGroup for Apache Flink-compliant consumption
                    var consumeResult = _consumerGroup.ConsumeMessage(TimeSpan.FromMilliseconds(1000));
                    
                    if (consumeResult?.Message != null)
                    {
                        await ProcessMessageWithFlinkPatterns(consumeResult);
                        
                        // Periodic logging for load balancing visibility
                        if ((DateTime.UtcNow - lastLogTime).TotalSeconds >= 30)
                        {
                            var currentProcessedCount = Interlocked.Read(ref _messagesProcessed);
                            var messagesInPeriod = currentProcessedCount - lastProcessedCount;
                            var elapsed = DateTime.UtcNow - startTime;
                            var totalRate = currentProcessedCount / elapsed.TotalSeconds;
                            
                            _logger.LogInformation("📊 TaskManager {TaskManagerId}: Processed {TotalMessages} messages " +
                                                 "(+{PeriodMessages} in last 30s) Rate: {Rate:F1} msg/s " +
                                                 "Partition: {Partition}",
                                                 _taskManagerId, currentProcessedCount, messagesInPeriod, totalRate,
                                                 consumeResult.Partition.Value);
                            
                            lastLogTime = DateTime.UtcNow;
                            lastProcessedCount = currentProcessedCount;
                        }
                    }
                    else
                    {
                        // No message available, small delay to prevent busy waiting
                        await Task.Delay(100, stoppingToken);
                    }
                }
                catch (ConsumeException ex)
                {
                    _logger.LogWarning(ex, "⚠️ TaskManager {TaskManagerId}: Consume exception: {Error}", 
                        _taskManagerId, ex.Error.Reason);
                    
                    if (ex.Error.IsFatal)
                    {
                        _logger.LogError("💥 TaskManager {TaskManagerId}: Fatal Kafka error, stopping consumption", _taskManagerId);
                        break;
                    }
                    
                    // Non-fatal error, continue with exponential backoff
                    await Task.Delay(TimeSpan.FromSeconds(2), stoppingToken);
                }
                catch (OperationCanceledException)
                {
                    _logger.LogInformation("🛑 TaskManager {TaskManagerId}: Consumption cancelled", _taskManagerId);
                    break;
                }
                catch (Exception ex)
                {
                    _logger.LogError(ex, "❌ TaskManager {TaskManagerId}: Unexpected error during consumption", _taskManagerId);
                    await Task.Delay(TimeSpan.FromSeconds(5), stoppingToken);
                }
            }
            
            var finalElapsed = DateTime.UtcNow - startTime;
            var finalCount = Interlocked.Read(ref _messagesProcessed);
            var finalRate = finalCount / finalElapsed.TotalSeconds;
            
            _logger.LogInformation("🏁 TaskManager {TaskManagerId}: Consumption completed. " +
                                 "Processed {TotalMessages} messages in {Duration:F1}s " +
                                 "Final rate: {Rate:F1} msg/s",
                                 _taskManagerId, finalCount, finalElapsed.TotalSeconds, finalRate);
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
                await ProcessThroughFlinkSinkFunction(messageContent);
                
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

        private async Task ProcessThroughFlinkSinkFunction(string messageContent)
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

        public void CollectWithTimestamp(string record, long timestampMillis)
        {
            // This would be used in a real Flink pipeline with event time
        }

        public Task CollectWithTimestampAsync(string record, long timestampMillis)
        {
            // This would be used in a real Flink pipeline with event time
            return Task.CompletedTask;
        }

        public void EmitWatermark(FlinkDotNet.Core.Abstractions.Windowing.Watermark watermark)
        {
            // This would be used for watermark generation in event time processing
        }

        public Task EmitWatermarkAsync(long timestampMillis)
        {
            // This would be used for watermark generation in event time processing
            return Task.CompletedTask;
        }

        public long ProcessingTime => DateTimeOffset.UtcNow.ToUnixTimeMilliseconds();

        public long CurrentProcessingTimeMillis() => DateTimeOffset.UtcNow.ToUnixTimeMilliseconds();
    }
}