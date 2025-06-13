using FlinkDotNet.Core.Abstractions.Context;
using FlinkDotNet.Core.Abstractions.Sinks;
using FlinkDotNet.Core.Abstractions.Operators; // For IOperatorLifecycle
using StackExchange.Redis;

namespace FlinkJobSimulator
{
    public class RedisIncrementSinkFunction<T> : ISinkFunction<T>, IOperatorLifecycle
    {
        private IDatabase? _redisDb;
        private readonly string _redisKey;
        private string _taskName = nameof(RedisIncrementSinkFunction<T>);
        private long _processedCount = 0;
        private const long LogFrequency = 10000;

        // Static configuration for LocalStreamExecutor compatibility
        public static IDatabase? GlobalRedisDatabase { get; set; }
        public static string? GlobalRedisKey { get; set; }

        // Constructor with dependencies (for manual instantiation)
        public RedisIncrementSinkFunction(IDatabase redisDatabase, string? redisKey = null)
        {
            _redisDb = redisDatabase ?? throw new ArgumentNullException(nameof(redisDatabase));
            _redisKey = redisKey ?? "flinkdotnet:sample:counter";
            Console.WriteLine($"RedisIncrementSinkFunction will use Redis key: '{_redisKey}'");
        }

        // Parameterless constructor (for LocalStreamExecutor reflection)
        public RedisIncrementSinkFunction()
        {
            _redisKey = GlobalRedisKey ?? "flinkdotnet:sample:counter";
            Console.WriteLine($"RedisIncrementSinkFunction parameterless constructor: key '{_redisKey}'");
        }

        public void Open(IRuntimeContext context)
        {
            _taskName = context.TaskName;
            Console.WriteLine($"[{_taskName}] Opening RedisIncrementSinkFunction for key '{_redisKey}'.");

            // If using parameterless constructor, get Redis database from static configuration
            if (_redisDb == null)
            {
                _redisDb = GlobalRedisDatabase;
                if (_redisDb == null)
                {
                    throw new InvalidOperationException("Redis database not available. Ensure GlobalRedisDatabase is set before job execution.");
                }
                Console.WriteLine($"[{_taskName}] Using global Redis database from static configuration.");
            }

            try
            {
                // Reset the counter key for a fresh run 
                _redisDb.StringSet(_redisKey, "0");
                Console.WriteLine($"[{_taskName}] Redis sink counter key '{_redisKey}' initialized to 0.");
            }
            catch (RedisConnectionException ex)
            {
                Console.WriteLine($"[{_taskName}] ERROR: Could not initialize Redis key. Error: {ex.Message}");
                // Further error handling or rethrow if connection is critical
                throw;
            }
        }

        public void Invoke(T record, ISinkContext context)
        {
            if (record is string recordString && recordString.StartsWith("BARRIER_"))
            {
                Console.WriteLine($"[{_taskName}] Received Barrier Marker in Redis Sink: {recordString}");
                // In a real scenario, sink would perform checkpointing actions here.
                // For this PoC, we just log and don't process it as data.
                return;
            }

            const int maxRetries = 3;
            for (int attempt = 1; attempt <= maxRetries; attempt++)
            {
                try
                {
                    if (_redisDb == null) throw new InvalidOperationException("Redis database not initialized");
                    _redisDb.StringIncrement(_redisKey);
                    long currentCount = Interlocked.Increment(ref _processedCount);

                    if (currentCount % LogFrequency == 0)
                    {
                        Console.WriteLine($"[{_taskName}] Incremented key '{_redisKey}'. Processed {currentCount} records.");
                    }
                    return; // Success, exit retry loop
                }
                catch (Exception ex) when (attempt < maxRetries)
                {
                    Console.WriteLine($"[{_taskName}] WARNING: Retry {attempt}/{maxRetries} failed for Redis key '{_redisKey}': {ex.GetType().Name} - {ex.Message}");
                    Thread.Sleep(50 * attempt); // Progressive backoff: 50ms, 100ms, 150ms
                }
                catch (Exception ex)
                {
                    Console.WriteLine($"[{_taskName}] ERROR: All {maxRetries} attempts failed for Redis key '{_redisKey}': {ex.GetType().Name} - {ex.Message}");
                    // Don't throw - just log the failure and continue with next record
                }
            }
        }

        public void Close()
        {
            Console.WriteLine($"[{_taskName}] Closing RedisIncrementSinkFunction. Processed {_processedCount} records for key '{_redisKey}'.");
            try
            {
                if (_redisDb != null)
                {
                    long finalValue = (long)_redisDb.StringGet(_redisKey);
                    Console.WriteLine($"[{_taskName}] Final value of Redis key '{_redisKey}': {finalValue}");
                }
                else
                {
                    Console.WriteLine($"[{_taskName}] Redis database was null, could not get final value.");
                }
            }
            catch(Exception ex)
            {
                Console.WriteLine($"[{_taskName}] ERROR: Could not get final value of Redis key '{_redisKey}'. Error: {ex.Message}");
            }
            Console.WriteLine($"[{_taskName}] Redis database reference released.");
        }
    }
}
