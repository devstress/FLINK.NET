using FlinkDotNet.Core.Abstractions.Sources;
using FlinkDotNet.Core.Abstractions.Checkpointing;
using Confluent.Kafka;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.Logging;
using StackExchange.Redis;

namespace FlinkDotNet.TaskManager.Operators
{
    /// <summary>
    /// Combined Kafka-to-Redis operator for TaskManager execution.
    /// Consumes from Kafka and writes directly to Redis for simplified architecture.
    /// </summary>
    public class KafkaToRedisOperator : ISourceFunction<string>, ICheckpointedFunction
    {
        private readonly string _topic;
        private readonly string _consumerGroupId;
        private readonly string _redisSinkCounterKey;
        private readonly string _globalSequenceKey;
        private readonly int _expectedMessages;
        private readonly string _taskManagerId;
        private readonly ILogger<KafkaToRedisOperator>? _logger;
        
        private IConsumer<Ignore, string>? _consumer;
        private IConnectionMultiplexer? _redisConnection;
        private IDatabase? _redisDatabase;
        private volatile bool _isRunning = true;
        private long _messagesProcessed = 0;
        private readonly Dictionary<TopicPartition, long> _checkpointState;
        private readonly object _checkpointLock = new object();

        public KafkaToRedisOperator(
            string topic, 
            string consumerGroupId, 
            string redisSinkCounterKey,
            string globalSequenceKey,
            int expectedMessages,
            string taskManagerId, 
            ILogger<KafkaToRedisOperator>? logger = null)
        {
            _topic = topic ?? throw new ArgumentNullException(nameof(topic));
            _consumerGroupId = consumerGroupId ?? throw new ArgumentNullException(nameof(consumerGroupId));
            _redisSinkCounterKey = redisSinkCounterKey ?? throw new ArgumentNullException(nameof(redisSinkCounterKey));
            _globalSequenceKey = globalSequenceKey ?? throw new ArgumentNullException(nameof(globalSequenceKey));
            _expectedMessages = expectedMessages;
            _taskManagerId = taskManagerId ?? "Unknown";
            _logger = logger;
            _checkpointState = new Dictionary<TopicPartition, long>();
            
            _logger?.LogInformation("KafkaToRedisOperator initialized for TaskManager {TaskManagerId}, Topic: {Topic}, ConsumerGroup: {ConsumerGroup}", 
                _taskManagerId, _topic, _consumerGroupId);
        }

        public void Run(ISourceContext<string> ctx)
        {
            RunAsync(ctx, CancellationToken.None).GetAwaiter().GetResult();
        }

        public async Task RunAsync(ISourceContext<string> ctx, CancellationToken cancellationToken = default)
        {
            try
            {
                _logger?.LogInformation("🚀 TaskManager {TaskManagerId}: Starting Kafka-to-Redis operator for topic '{Topic}'", _taskManagerId, _topic);

                // Initialize Redis connection
                await InitializeRedisAsync();

                // Initialize Kafka consumer
                InitializeKafkaConsumer();

                // Start consuming and processing messages
                while (_isRunning && !cancellationToken.IsCancellationRequested)
                {
                    try
                    {
                        var consumeResult = _consumer?.Consume(TimeSpan.FromMilliseconds(1000));
                        if (consumeResult != null && !consumeResult.IsPartitionEOF)
                        {
                            // Process message: increment Redis counters
                            await ProcessMessageAsync();
                            
                            // Update checkpoint state
                            lock (_checkpointLock)
                            {
                                _checkpointState[consumeResult.TopicPartition] = consumeResult.Offset.Value;
                            }

                            // Emit to source context for potential downstream operators
                            await ctx.CollectAsync(consumeResult.Message.Value);
                        }
                    }
                    catch (ConsumeException ex)
                    {
                        _logger?.LogError(ex, "TaskManager {TaskManagerId}: Error consuming from Kafka", _taskManagerId);
                    }
                    catch (Exception ex)
                    {
                        _logger?.LogError(ex, "TaskManager {TaskManagerId}: Error processing message", _taskManagerId);
                    }
                }

                _logger?.LogInformation("TaskManager {TaskManagerId}: Kafka-to-Redis operator completed", _taskManagerId);
            }
            catch (Exception ex)
            {
                _logger?.LogError(ex, "TaskManager {TaskManagerId}: Kafka-to-Redis operator failed", _taskManagerId);
                
                // Signal job failure
                if (_redisDatabase != null)
                {
                    try
                    {
                        await _redisDatabase.StringSetAsync("flinkdotnet:job_execution_error", ex.Message);
                        await _redisDatabase.StringSetAsync("flinkdotnet:job_completion_status", "FAILED");
                    }
                    catch { /* Ignore errors when reporting errors */ }
                }
                throw new InvalidOperationException($"Kafka-to-Redis operator failed on TaskManager {_taskManagerId}", ex);
            }
            finally
            {
                _consumer?.Close();
                _consumer?.Dispose();
                _redisConnection?.Dispose();
            }
        }

        private async Task InitializeRedisAsync()
        {
            var connectionString = GetRedisConnectionString();
            _redisConnection = await ConnectionMultiplexer.ConnectAsync(connectionString);
            _redisDatabase = _redisConnection.GetDatabase();

            // Initialize counters if they don't exist
            await _redisDatabase.StringSetAsync(_redisSinkCounterKey, 0, when: When.NotExists);
            await _redisDatabase.StringSetAsync(_globalSequenceKey, 0, when: When.NotExists);

            _logger?.LogInformation("✅ TaskManager {TaskManagerId}: Redis connection initialized", _taskManagerId);
        }

        private void InitializeKafkaConsumer()
        {
            var config = new ConsumerConfig
            {
                BootstrapServers = GetKafkaBootstrapServers(),
                GroupId = _consumerGroupId,
                AutoOffsetReset = AutoOffsetReset.Earliest,
                EnableAutoCommit = false, // We'll manage offsets through checkpointing
                SessionTimeoutMs = 10000,
                HeartbeatIntervalMs = 3000,
                MaxPollIntervalMs = 300000,
                FetchMinBytes = 1,
                FetchWaitMaxMs = 500
            };

            _consumer = new ConsumerBuilder<Ignore, string>(config)
                .SetValueDeserializer(Deserializers.Utf8)
                .Build();

            _consumer.Subscribe(_topic);
            
            _logger?.LogInformation("✅ TaskManager {TaskManagerId}: Kafka consumer initialized for topic '{Topic}'", _taskManagerId, _topic);
        }

        private async Task ProcessMessageAsync()
        {
            if (_redisDatabase == null) return;

            // Increment both counters atomically
            var batch = _redisDatabase.CreateBatch();
            var counterTask = batch.StringIncrementAsync(_redisSinkCounterKey);
            var sequenceTask = batch.StringIncrementAsync(_globalSequenceKey);
            
            batch.Execute();
            
            var newCount = await counterTask;
            await sequenceTask;

            _messagesProcessed++;

            // Log progress periodically
            if (_messagesProcessed % 10000 == 0)
            {
                _logger?.LogInformation("TaskManager {TaskManagerId}: Processed {MessagesProcessed} messages, Redis counter: {RedisCount}",
                    _taskManagerId, _messagesProcessed, newCount);
            }

            // Check completion
            if (newCount >= _expectedMessages)
            {
                _logger?.LogInformation("🎉 TaskManager {TaskManagerId}: Reached expected message count {ExpectedCount}, signaling completion",
                    _taskManagerId, _expectedMessages);
                
                // Signal job completion
                await _redisDatabase.StringSetAsync("flinkdotnet:job_completion_status", "SUCCESS");
            }
        }

        public void Cancel()
        {
            _isRunning = false;
        }

        public void SnapshotState(long checkpointId, long checkpointTimestamp)
        {
            lock (_checkpointLock)
            {
                _logger?.LogDebug("TaskManager {TaskManagerId}: Snapshotted Kafka offsets for checkpoint {CheckpointId}", 
                    _taskManagerId, checkpointId);
            }
        }

        public void RestoreState(object state)
        {
            try
            {
                if (state is Dictionary<TopicPartition, long> restoredState)
                {
                    lock (_checkpointLock)
                    {
                        _checkpointState.Clear();
                        foreach (var kvp in restoredState)
                        {
                            _checkpointState[kvp.Key] = kvp.Value;
                        }
                    }
                    _logger?.LogInformation("TaskManager {TaskManagerId}: Restored Kafka offsets from checkpoint", _taskManagerId);
                }
            }
            catch (Exception ex)
            {
                _logger?.LogError(ex, "TaskManager {TaskManagerId}: Failed to restore Kafka offset state", _taskManagerId);
            }
        }

        public void NotifyCheckpointComplete(long checkpointId)
        {
            _logger?.LogDebug("TaskManager {TaskManagerId}: Checkpoint {CheckpointId} completed", _taskManagerId, checkpointId);
        }

        private string GetKafkaBootstrapServers()
        {
            var configuration = new ConfigurationBuilder()
                .AddEnvironmentVariables()
                .Build();

            // Try various sources for Kafka bootstrap servers
            var bootstrapServers = configuration.GetConnectionString("kafka") ??
                                 Environment.GetEnvironmentVariable("DOTNET_KAFKA_BOOTSTRAP_SERVERS") ??
                                 Environment.GetEnvironmentVariable("ConnectionStrings__kafka") ??
                                 "localhost:9092";

            // Extract just the bootstrap servers if it's a full connection string
            if (bootstrapServers.Contains("bootstrap.servers="))
            {
                var parts = bootstrapServers.Split(';');
                var serversPart = parts.FirstOrDefault(p => p.StartsWith("bootstrap.servers="));
                if (serversPart != null)
                {
                    bootstrapServers = serversPart.Substring("bootstrap.servers=".Length);
                }
            }

            _logger?.LogInformation("TaskManager {TaskManagerId}: Using Kafka bootstrap servers: {BootstrapServers}", 
                _taskManagerId, bootstrapServers);
            return bootstrapServers;
        }

        private string GetRedisConnectionString()
        {
            var configuration = new ConfigurationBuilder()
                .AddEnvironmentVariables()
                .Build();

            // Try various sources for Redis connection string
            var connectionString = configuration.GetConnectionString("redis") ??
                                 Environment.GetEnvironmentVariable("DOTNET_REDIS_URL") ??
                                 Environment.GetEnvironmentVariable("ConnectionStrings__redis") ??
                                 "localhost:6379";

            // Add password if available
            var password = Environment.GetEnvironmentVariable("SIMULATOR_REDIS_PASSWORD");
            if (!string.IsNullOrEmpty(password) && !connectionString.Contains("password="))
            {
                connectionString += $",password={password}";
            }

            _logger?.LogInformation("TaskManager {TaskManagerId}: Using Redis connection string: {ConnectionString}",
                _taskManagerId, connectionString.Replace(password ?? "", "***"));

            return connectionString;
        }
    }
}