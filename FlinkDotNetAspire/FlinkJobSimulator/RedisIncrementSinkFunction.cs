using FlinkDotNet.Core.Abstractions.Context;
using FlinkDotNet.Core.Abstractions.Sinks;
using FlinkDotNet.Core.Abstractions.Operators; // For IOperatorLifecycle
using FlinkDotNet.Core.Abstractions.Runtime; // For IRuntimeContext
using StackExchange.Redis;
using System;
using System.Threading;
using Microsoft.Extensions.Configuration; // Required for reading connection string

namespace FlinkJobSimulator
{
    public class RedisIncrementSinkFunction<T> : ISinkFunction<T>, IOperatorLifecycle
    {
        private ConnectionMultiplexer? _redisConnection;
        private IDatabase? _redisDb;
        private string _redisKey = "flinkdotnet:sample:counter"; // Default key, make configurable if needed
        private string _taskName = nameof(RedisIncrementSinkFunction<T>);
        private long _processedCount = 0;
        private const long LogFrequency = 10000;

        // Configuration to hold connection string
        private static IConfiguration? _configuration;

        // Static constructor to build configuration once
        static RedisIncrementSinkFunction()
        {
            _configuration = new ConfigurationBuilder()
                .AddEnvironmentVariables()
                .Build();
        }

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
            string? redisConnectionString = _configuration?["ConnectionStrings__redis"];
                                         // Or Environment.GetEnvironmentVariable("ConnectionStrings__redis");

            if (string.IsNullOrEmpty(redisConnectionString))
            {
                Console.WriteLine($"[{_taskName}] ERROR: Redis connection string 'ConnectionStrings__redis' not found in environment variables.");
                // Attempt a local default if not found (useful for non-Aspire testing, though Aspire should provide it)
                redisConnectionString = "localhost:6379";
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

                // Optionally, reset the key for a fresh run of the sample
                // _redisDb.KeyDelete(_redisKey);
                // Console.WriteLine($"[{_taskName}] Deleted key '{_redisKey}' for a fresh start.");

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

            if (record is string recordString && recordString.StartsWith("PROTO_BARRIER_"))
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
                long finalValue = (long)_redisDb?.StringGet(_redisKey);
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
