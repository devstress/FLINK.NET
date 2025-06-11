using FlinkDotNet.Core.Abstractions.Context;
using FlinkDotNet.Core.Abstractions.Sinks;
using FlinkDotNet.Core.Abstractions.Operators; // For IOperatorLifecycle
using StackExchange.Redis;
using Microsoft.Extensions.Configuration; // Required for reading connection string
using FlinkDotNet.Common.Constants;

namespace FlinkJobSimulator
{
    public class RedisIncrementSinkFunction<T> : ISinkFunction<T>, IOperatorLifecycle
    {
        private ConnectionMultiplexer? _redisConnection;
        private IDatabase? _redisDb;
        private readonly string _redisKey = "flinkdotnet:sample:counter"; // Default key, make configurable if needed
        private string _taskName = nameof(RedisIncrementSinkFunction<T>);
        private long _processedCount = 0;
        private const long LogFrequency = 10000;

        // Configuration to hold connection string
        private static readonly IConfiguration Configuration = new ConfigurationBuilder()
            .AddEnvironmentVariables()
            .Build();

        public RedisIncrementSinkFunction(string? redisKey = null)
        {
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

        // Retrieve connection string (Aspire sets "ConnectionStrings__redis")
        string? redisConnectionString = Configuration?["ConnectionStrings__redis"];
        
        // If Aspire connection string not found, try the alternative format used by IntegrationTestVerifier
        if (string.IsNullOrEmpty(redisConnectionString))
        {
            redisConnectionString = Configuration?["DOTNET_REDIS_URL"];
        }

            if (string.IsNullOrEmpty(redisConnectionString))
            {
                Console.WriteLine($"[{_taskName}] ERROR: Redis connection string not found in 'ConnectionStrings__redis' or 'DOTNET_REDIS_URL' environment variables.");
                // Attempt a local default if not found (useful for non-Aspire testing, though Aspire should provide it)
                redisConnectionString = ServiceUris.RedisConnectionString;
                Console.WriteLine($"[{_taskName}] Using default Redis connection string: {redisConnectionString}");
            }
            else
            {
                 Console.WriteLine($"[{_taskName}] Found Redis connection string from environment.");
            }

            try
            {
                _redisConnection = ConnectionMultiplexer.Connect(redisConnectionString);
                _redisDb = _redisConnection.GetDatabase();
                Console.WriteLine($"[{_taskName}] Successfully connected to Redis at {redisConnectionString.Split(',')[0]}.");

                // Reset the counter key for a fresh run 
                _redisDb.StringSet(_redisKey, "0");
                Console.WriteLine($"[{_taskName}] Redis sink counter key '{_redisKey}' initialized to 0.");

            }
            catch (RedisConnectionException ex)
            {
                Console.WriteLine($"[{_taskName}] ERROR: Could not connect to Redis. ConnectionString: '{redisConnectionString}'. Error: {ex.Message}");
                // Further error handling or rethrow if connection is critical
                throw;
            }
        }

        public void Invoke(T record, ISinkContext context)
        {
            if (_redisDb == null)
            {
                return;
            }

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
                long finalValue = 0;
                if (_redisDb != null)
                {
                    finalValue = (long)_redisDb.StringGet(_redisKey);
                }
                Console.WriteLine($"[{_taskName}] Final value of Redis key '{_redisKey}': {finalValue}");
            }
            catch(Exception ex)
            {
                Console.WriteLine($"[{_taskName}] ERROR: Could not get final value of Redis key '{_redisKey}'. Error: {ex.Message}");
            }
            _redisConnection?.Close();
            _redisConnection?.Dispose();
            Console.WriteLine($"[{_taskName}] Redis connection closed.");
        }
    }
}
