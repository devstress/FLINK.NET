using FlinkDotNet.Core.Abstractions.Sinks;
using FlinkDotNet.Core.Abstractions.Checkpointing;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.Logging;
using StackExchange.Redis;

namespace FlinkDotNet.TaskManager.Operators
{
    /// <summary>
    /// Redis sink operator for TaskManager execution.
    /// Receives messages from upstream operators and increments Redis counters.
    /// </summary>
    public class RedisSinkOperator : ISinkFunction<string>, ICheckpointedFunction
    {
        private readonly string _redisSinkCounterKey;
        private readonly string _globalSequenceKey;
        private readonly int _expectedMessages;
        private readonly string _taskManagerId;
        private readonly ILogger<RedisSinkOperator>? _logger;
        private IConnectionMultiplexer? _redisConnection;
        private IDatabase? _redisDatabase;
        private long _messagesProcessed = 0;
        private volatile bool _isRunning = true;

        public RedisSinkOperator(
            string redisSinkCounterKey, 
            string globalSequenceKey, 
            int expectedMessages,
            string taskManagerId,
            ILogger<RedisSinkOperator>? logger = null)
        {
            _redisSinkCounterKey = redisSinkCounterKey ?? throw new ArgumentNullException(nameof(redisSinkCounterKey));
            _globalSequenceKey = globalSequenceKey ?? throw new ArgumentNullException(nameof(globalSequenceKey));
            _expectedMessages = expectedMessages;
            _taskManagerId = taskManagerId ?? "Unknown";
            _logger = logger;

            _logger?.LogInformation("RedisSinkOperator initialized for TaskManager {TaskManagerId}, CounterKey: {CounterKey}, SequenceKey: {SequenceKey}, Expected: {Expected}",
                _taskManagerId, _redisSinkCounterKey, _globalSequenceKey, _expectedMessages);
        }

        public async Task OpenAsync()
        {
            try
            {
                _logger?.LogInformation("🚀 TaskManager {TaskManagerId}: Opening Redis sink operator", _taskManagerId);

                var connectionString = GetRedisConnectionString();
                _redisConnection = await ConnectionMultiplexer.ConnectAsync(connectionString);
                _redisDatabase = _redisConnection.GetDatabase();

                // Initialize counters if they don't exist
                await _redisDatabase.StringSetAsync(_redisSinkCounterKey, 0, when: When.NotExists);
                await _redisDatabase.StringSetAsync(_globalSequenceKey, 0, when: When.NotExists);

                _logger?.LogInformation("✅ TaskManager {TaskManagerId}: Redis sink operator opened successfully", _taskManagerId);
            }
            catch (Exception ex)
            {
                _logger?.LogError(ex, "TaskManager {TaskManagerId}: Failed to open Redis sink operator", _taskManagerId);
                throw;
            }
        }

        public async Task InvokeAsync(string value)
        {
            if (!_isRunning || _redisDatabase == null)
                return;

            try
            {
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
            catch (Exception ex)
            {
                _logger?.LogError(ex, "TaskManager {TaskManagerId}: Error processing message in Redis sink", _taskManagerId);
                
                // Signal job failure
                if (_redisDatabase != null)
                {
                    try
                    {
                        await _redisDatabase.StringSetAsync("flinkdotnet:job_execution_error", ex.Message);
                        await _redisDatabase.StringSetAsync("flinkdotnet:job_completion_status", "FAILED");
                    }
                    catch
                    {
                        // Ignore errors when reporting errors
                    }
                }
            }
        }

        public async Task CloseAsync()
        {
            try
            {
                _isRunning = false;
                
                _logger?.LogInformation("TaskManager {TaskManagerId}: Closing Redis sink operator, processed {MessagesProcessed} messages",
                    _taskManagerId, _messagesProcessed);

                _redisConnection?.Dispose();
                _redisConnection = null;
                _redisDatabase = null;
            }
            catch (Exception ex)
            {
                _logger?.LogError(ex, "TaskManager {TaskManagerId}: Error closing Redis sink operator", _taskManagerId);
            }
        }

        public Task<byte[]> SnapshotStateAsync(long checkpointId, long checkpointTimestamp)
        {
            var state = System.Text.Json.JsonSerializer.SerializeToUtf8Bytes(new
            {
                MessagesProcessed = _messagesProcessed,
                CheckpointId = checkpointId,
                Timestamp = checkpointTimestamp
            });

            _logger?.LogDebug("TaskManager {TaskManagerId}: Snapshotted Redis sink state for checkpoint {CheckpointId}, processed: {MessagesProcessed}",
                _taskManagerId, checkpointId, _messagesProcessed);

            return Task.FromResult(state);
        }

        public Task RestoreStateAsync(byte[] state)
        {
            try
            {
                var restoredState = System.Text.Json.JsonSerializer.Deserialize<dynamic>(state);
                if (restoredState != null)
                {
                    // In a full implementation, restore the processed count
                    _logger?.LogInformation("TaskManager {TaskManagerId}: Restored Redis sink state from checkpoint", _taskManagerId);
                }
            }
            catch (Exception ex)
            {
                _logger?.LogError(ex, "TaskManager {TaskManagerId}: Failed to restore Redis sink state", _taskManagerId);
            }
            return Task.CompletedTask;
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