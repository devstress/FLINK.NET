using FlinkDotNet.Core.Abstractions.Context;
using FlinkDotNet.Core.Abstractions.Sinks;
using FlinkDotNet.Core.Abstractions.Operators; // For IOperatorLifecycle
using StackExchange.Redis;

namespace FlinkJobSimulator
{
    public class RedisIncrementSinkFunction<T> : ISinkFunction<T>, IOperatorLifecycle
    {
        private readonly IDatabase _redisDb;
        private readonly string _redisKey = "flinkdotnet:sample:counter"; // Default key, make configurable if needed
        private string _taskName = nameof(RedisIncrementSinkFunction<T>);
        private long _processedCount = 0;
        private const long LogFrequency = 10000;

        public RedisIncrementSinkFunction(IDatabase redisDatabase, string? redisKey = null)
        {
            _redisDb = redisDatabase ?? throw new ArgumentNullException(nameof(redisDatabase));
            if (!string.IsNullOrEmpty(redisKey))
            {
                _redisKey = redisKey;
            }
            Console.WriteLine($"RedisIncrementSinkFunction will use Redis key: '{_redisKey}'");
        }

        public void Open(IRuntimeContext context)
        {
            _taskName = context.TaskName;
            Console.WriteLine($"[{_taskName}] Opening RedisIncrementSinkFunction for key '{_redisKey}'.");

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

            try
            {
                _redisDb.StringIncrement(_redisKey);
                long currentCount = Interlocked.Increment(ref _processedCount);

                if (currentCount % LogFrequency == 0)
                {
                    Console.WriteLine($"[{_taskName}] Incremented key '{_redisKey}'. Processed {currentCount} records.");
                }
            }
            catch (Exception ex)
            {
                Console.WriteLine($"[{_taskName}] ERROR: Failed to increment Redis key '{_redisKey}'. Error: {ex.Message}");
            }
        }

        public void Close()
        {
            Console.WriteLine($"[{_taskName}] Closing RedisIncrementSinkFunction. Processed {_processedCount} records for key '{_redisKey}'.");
            try
            {
                long finalValue = (long)_redisDb.StringGet(_redisKey);
                Console.WriteLine($"[{_taskName}] Final value of Redis key '{_redisKey}': {finalValue}");
            }
            catch(Exception ex)
            {
                Console.WriteLine($"[{_taskName}] ERROR: Could not get final value of Redis key '{_redisKey}'. Error: {ex.Message}");
            }
            Console.WriteLine($"[{_taskName}] Redis database reference released.");
        }
    }
}
