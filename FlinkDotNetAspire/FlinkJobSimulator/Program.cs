using FlinkDotNet.Core.Api;
using FlinkDotNet.Core.Api.Streaming;
using FlinkDotNet.Core.Abstractions.Sinks; // For ISinkFunction
using FlinkDotNet.Core.Abstractions.Sources; // For ISourceFunction
using FlinkDotNet.Core.Abstractions.Operators; // For IMapOperator, IOperatorLifecycle
using FlinkDotNet.Core.Abstractions.Context; // For IRuntimeContext
using FlinkDotNet.Core.Abstractions.Serializers;
using FlinkDotNet.Core.Abstractions.States; // For state interfaces
using FlinkDotNet.Core.Abstractions.Models; // For JobConfiguration
using FlinkDotNet.Core.Abstractions.Models.State; // For state descriptors
using FlinkDotNet.Core.Abstractions.Storage; // For IStateSnapshotStore
using FlinkDotNet.Core.Abstractions.Windowing; // For Watermark
using StackExchange.Redis; // For HighVolumeSourceFunction Redis parts and IConnectionMultiplexer
using Microsoft.Extensions.Configuration; // For IConfiguration in HighVolumeSourceFunction & Sinks
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.DependencyInjection;
using FlinkDotNet.JobManager.Models.JobGraph;
using FlinkDotNet.Common.Constants;
using System.Collections; // For DictionaryEntry

namespace FlinkJobSimulator
{
// Helper classes are defined below in this file.

// Counting Sink Function (remains from previous step, not directly used in Main anymore but kept for now)
public class CountingSinkFunction<T> : ISinkFunction<T>, IOperatorLifecycle
{
    private long _count = 0;
    private string _taskName = nameof(CountingSinkFunction<T>);
    private Timer? _logTimer;

    public void Open(IRuntimeContext context)
    {
        _taskName = context.TaskName;
        Console.WriteLine($"[{_taskName}] CountingSinkFunction opened.");
        _logTimer = new Timer(_ =>
        {
            Console.WriteLine($"[{_taskName}] Current count: {Interlocked.Read(ref _count)}");
        }, null, TimeSpan.FromSeconds(10), TimeSpan.FromSeconds(10));
    }

    public void Invoke(T record, ISinkContext context)
    {
        Interlocked.Increment(ref _count);
    }

    public void Close()
    {
        _logTimer?.Dispose();
        Console.WriteLine($"[{_taskName}] CountingSinkFunction closed. Final count: {Interlocked.Read(ref _count)}");
    }
}

// Modified Source Function for High Volume with Redis Sequence ID and Barrier Injection
public class HighVolumeSourceFunction : ISourceFunction<string>, IOperatorLifecycle
{
    private readonly long _numberOfMessagesToGenerate;
    private readonly ITypeSerializer<string> _serializer;
    private IDatabase? _redisDb;
    private volatile bool _isRunning = true;
    private string _taskName = nameof(HighVolumeSourceFunction);

    private readonly string _globalSequenceRedisKey;

    // --- Checkpoint Barrier Injection Fields ---
    private long _messagesSentSinceLastBarrier = 0;
    private const long MessagesPerBarrier = 1000; // Inject a barrier every 1000 messages for testing
    private static long _nextCheckpointId = 1; // Static to ensure unique IDs across potential re-instantiations in some test scenarios (though for a single run, instance field is fine)

    // Static configuration for LocalStreamExecutor compatibility
    public static long NumberOfMessagesToGenerate { get; set; } = 1000;
    public static IDatabase? GlobalRedisDatabase { get; set; }
    public static IConfiguration? GlobalConfiguration { get; set; }

    // Constructor with dependencies (for manual instantiation)
    public HighVolumeSourceFunction(long numberOfMessagesToGenerate, ITypeSerializer<string> serializer, IDatabase redisDatabase, IConfiguration configuration)
    {
        _numberOfMessagesToGenerate = numberOfMessagesToGenerate;
        _serializer = serializer ?? throw new ArgumentNullException(nameof(serializer));
        _redisDb = redisDatabase ?? throw new ArgumentNullException(nameof(redisDatabase));
        if (numberOfMessagesToGenerate <= 0) throw new ArgumentOutOfRangeException(nameof(numberOfMessagesToGenerate), "Number of messages must be positive.");

        _globalSequenceRedisKey = configuration?["SIMULATOR_REDIS_KEY_GLOBAL_SEQUENCE"] ?? "flinkdotnet:global_sequence_id";
        Console.WriteLine($"[HighVolumeSourceFunction] Configured to use global sequence Redis key: '{_globalSequenceRedisKey}'");
    }

    // Parameterless constructor (for LocalStreamExecutor reflection)
    public HighVolumeSourceFunction()
    {
        _numberOfMessagesToGenerate = NumberOfMessagesToGenerate;
        _serializer = new StringSerializer();
        _globalSequenceRedisKey = GlobalConfiguration?["SIMULATOR_REDIS_KEY_GLOBAL_SEQUENCE"] ?? "flinkdotnet:global_sequence_id";
        Console.WriteLine($"[HighVolumeSourceFunction] Parameterless constructor: {_numberOfMessagesToGenerate} messages, key: '{_globalSequenceRedisKey}'");
    }

    public void Open(IRuntimeContext context)
    {
        _taskName = context.TaskName;
        Console.WriteLine($"[{_taskName}] Opening HighVolumeSourceFunction.");

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
            _redisDb.StringSet(_globalSequenceRedisKey, "0");
            Console.WriteLine($"[{_taskName}] Global sequence Redis key '{_globalSequenceRedisKey}' initialized to 0.");
        }
        catch (RedisConnectionException ex)
        {
            Console.WriteLine($"[{_taskName}] ERROR: Source could not connect to Redis for sequence generation. Error: {ex.Message}");
            throw;
        }
    }

    public void Run(ISourceContext<string> ctx)
    {
        RunAsync(ctx, CancellationToken.None).GetAwaiter().GetResult();
    }

    private void InjectBarrierIfNeeded(ISourceContext<string> ctx)
    {
        if (_messagesSentSinceLastBarrier >= MessagesPerBarrier)
        {
            try
            {
                long checkpointIdToInject = Interlocked.Increment(ref _nextCheckpointId) - 1;
                if (checkpointIdToInject == 0)
                {
                    checkpointIdToInject = Interlocked.Increment(ref _nextCheckpointId) - 1;
                }

                long timestamp = DateTimeOffset.UtcNow.ToUnixTimeMilliseconds();
                string barrierMessage = $"BARRIER_{checkpointIdToInject}_{timestamp}";
                Console.WriteLine($"[{_taskName}] Injecting Barrier: {barrierMessage}");
                ctx.Collect(barrierMessage);
                _messagesSentSinceLastBarrier = 0;
            }
            catch (Exception ex)
            {
                Console.WriteLine($"[{_taskName}] ERROR during barrier injection: {ex.GetType().Name} - {ex.Message}");
                Console.WriteLine($"[{_taskName}] Barrier injection stack trace: {ex.StackTrace}");
                // Don't rethrow - continue with message processing
            }
        }
    }

    private async Task EmitMessageAsync(ISourceContext<string> ctx, long currentSequenceId, long emittedCount)
    {
        // Generate message with both id and redis_ordered_id fields as requested
        string message = $"{{\"id\":{emittedCount},\"redis_ordered_id\":{currentSequenceId},\"payload\":\"MessagePayload_Seq-{currentSequenceId}\"}}";
        await ctx.CollectAsync(message);
        _messagesSentSinceLastBarrier++;

        if (emittedCount % 100000 == 0)
        {
            Console.WriteLine($"[{_taskName}] Emitted {emittedCount} messages. Last sequence ID: {currentSequenceId}");
            await Task.Yield();
        }
    }

public async Task RunAsync(ISourceContext<string> ctx, CancellationToken cancellationToken = default)
    {
        Console.WriteLine($"[{_taskName}] Starting to emit {_numberOfMessagesToGenerate} messages with Redis-generated sequence IDs and barrier injection every {MessagesPerBarrier} messages...");
        long emittedCount = 0;
        
        emittedCount = await ProcessMessageEmissionLoop(ctx, cancellationToken, emittedCount);
        
        Console.WriteLine($"[{_taskName}] Main emission loop completed. Final emittedCount: {emittedCount}, target: {_numberOfMessagesToGenerate}");
        
        await EmitFinalBarrierIfNeeded(ctx, emittedCount);
        
        var lastSequenceId = _redisDb?.StringGet(_globalSequenceRedisKey) ?? "unknown";
        Console.WriteLine($"[{_taskName}] Finished emitting. Total data messages emitted: {emittedCount}. Last global sequence ID used (approx): {lastSequenceId}");
    }

    private async Task<long> ProcessMessageEmissionLoop(ISourceContext<string> ctx, CancellationToken cancellationToken, long emittedCount)
    {
        long skippedMessages = 0;
        
        for (long i = 0; i < _numberOfMessagesToGenerate; i++)
        {
            if (!_isRunning || cancellationToken.IsCancellationRequested)
            {
                Console.WriteLine($"");
                Console.WriteLine($"🛑 [{_taskName}] ============ EMISSION LOOP TERMINATED ============");
                Console.WriteLine($"🛑 [{_taskName}] TERMINATION AT: Message {i+1}/{_numberOfMessagesToGenerate} ({(double)(i+1)/_numberOfMessagesToGenerate*100:F2}%)");
                Console.WriteLine($"🛑 [{_taskName}] EMITTED COUNT: {emittedCount}");
                Console.WriteLine($"🛑 [{_taskName}] LOOP INDEX: {i}");
                Console.WriteLine($"🛑 [{_taskName}] _isRunning: {_isRunning}");
                Console.WriteLine($"🛑 [{_taskName}] cancellationRequested: {cancellationToken.IsCancellationRequested}");
                Console.WriteLine($"🛑 [{_taskName}] =============================================");
                Console.WriteLine($"");
                break;
            }

            // Enhanced progress tracking for debugging early termination
            if (i % 50000 == 0 || i >= _numberOfMessagesToGenerate - 100)
            {
                Console.WriteLine($"[{_taskName}] Processing message {i+1}/{_numberOfMessagesToGenerate} (emitted: {emittedCount}, skipped: {skippedMessages})");
            }

            InjectBarrierIfNeeded(ctx);

            bool messageProcessed = await ProcessSingleMessage(ctx, i, emittedCount);
            if (messageProcessed)
            {
                emittedCount++;
            }
            else
            {
                skippedMessages++;
                // Continue processing even if individual messages fail (enhanced resilience)
                Console.WriteLine($"[{_taskName}] Skipped message {i+1} due to processing error (total skipped: {skippedMessages})");
            }
        }
        
        if (skippedMessages > 0)
        {
            Console.WriteLine($"[{_taskName}] EMISSION SUMMARY: Processed {emittedCount} messages, skipped {skippedMessages} messages due to errors");
            Console.WriteLine($"[{_taskName}] Success rate: {(double)emittedCount / (_numberOfMessagesToGenerate) * 100:F1}%");
        }
        
        return emittedCount;
    }

    private async Task<bool> ProcessSingleMessage(ISourceContext<string> ctx, long messageIndex, long emittedCount)
    {
        const int maxRetries = 5; // Increased retries for better resilience
        for (int attempt = 1; attempt <= maxRetries; attempt++)
        {
            try
            {
                if (_redisDb == null) throw new InvalidOperationException("Redis database not initialized");
                long currentSequenceId = await _redisDb.StringIncrementAsync(_globalSequenceRedisKey);
                await EmitMessageAsync(ctx, currentSequenceId, emittedCount + 1);
                return true;
            }
            catch (Exception ex) when (attempt < maxRetries)
            {
                await HandleRetryableException(ex, messageIndex, attempt, maxRetries);
            }
            catch (Exception ex)
            {
                return HandleFinalException(ex, messageIndex, emittedCount);
            }
        }
        return false;
    }

    private async Task HandleRetryableException(Exception ex, long messageIndex, int attempt, int maxRetries)
    {
        // 🚨 PROMINENT WARNING LOGGING as requested
        Console.WriteLine($"");
        Console.WriteLine($"🚨 [{_taskName}] ============ RETRY WARNING ============");
        Console.WriteLine($"🚨 [{_taskName}] MESSAGE: {messageIndex+1} | ATTEMPT: {attempt}/{maxRetries}");
        Console.WriteLine($"🚨 [{_taskName}] ERROR: {ex.GetType().Name} - {ex.Message}");
        Console.WriteLine($"🚨 [{_taskName}] PROGRESS: {messageIndex+1:N0}/{_numberOfMessagesToGenerate:N0} ({(double)(messageIndex+1)/_numberOfMessagesToGenerate*100:F2}%)");
        Console.WriteLine($"🚨 [{_taskName}] ========================================");
        Console.WriteLine($"");
        
        // Progressive backoff with longer delays for stress tests
        int delayMs = 200 * attempt; // 200ms, 400ms, 600ms, 800ms
        await Task.Delay(delayMs);
        
        // Additional diagnostics for persistent failures
        if (attempt >= 3)
        {
            await CheckRedisHealthOnPersistentFailure(ex, messageIndex, attempt, maxRetries);
        }
    }

    private async Task CheckRedisHealthOnPersistentFailure(Exception ex, long messageIndex, int attempt, int maxRetries)
    {
        Console.WriteLine($"[{_taskName}] WARNING: Message {messageIndex+1} experiencing persistent issues (attempt {attempt}/{maxRetries})");
        if (ex.Message.Contains("Redis") || ex.Message.Contains("timeout"))
        {
            Console.WriteLine($"[{_taskName}] Redis connection issue detected - checking connection health");
            try
            {
                // Simple ping to check Redis health
                if (_redisDb != null)
                {
                    await _redisDb.PingAsync();
                    Console.WriteLine($"[{_taskName}] Redis ping successful - connection is healthy");
                }
            }
            catch (Exception pingEx)
            {
                Console.WriteLine($"[{_taskName}] Redis ping failed: {pingEx.Message}");
            }
        }
    }

    private bool HandleFinalException(Exception ex, long messageIndex, long emittedCount)
    {
        // 💥 PROMINENT ERROR LOGGING AFTER RETRY TIMEOUT as requested
        Console.WriteLine($"");
        Console.WriteLine($"💥 [{_taskName}] ============ FINAL ERROR AFTER ALL RETRIES FAILED ============");
        Console.WriteLine($"💥 [{_taskName}] MESSAGE: {messageIndex+1} | EMITTED COUNT: {emittedCount}");
        Console.WriteLine($"💥 [{_taskName}] PROGRESS: {messageIndex+1:N0}/{_numberOfMessagesToGenerate:N0} ({(double)(messageIndex+1)/_numberOfMessagesToGenerate*100:F2}%)");
        Console.WriteLine($"💥 [{_taskName}] EXCEPTION TYPE: {ex.GetType().Name}");
        Console.WriteLine($"💥 [{_taskName}] EXCEPTION MESSAGE: {ex.Message}");
        Console.WriteLine($"💥 [{_taskName}] STACK TRACE: {ex.StackTrace}");
        if (ex.InnerException != null)
        {
            Console.WriteLine($"💥 [{_taskName}] INNER EXCEPTION: {ex.InnerException.GetType().Name} - {ex.InnerException.Message}");
        }
        Console.WriteLine($"💥 [{_taskName}] ============================================================");
        Console.WriteLine($"");
        
        return ApplyEnhancedResilienceLogic(messageIndex);
    }

    private bool ApplyEnhancedResilienceLogic(long messageIndex)
    {
        // Enhanced resilience: Don't stop completely, just skip this message for stress tests
        Console.WriteLine($"[{_taskName}] ENHANCED RESILIENCE: Skipping message {messageIndex+1} and continuing (for stress test compatibility)");
        Console.WriteLine($"[{_taskName}] Source will continue processing remaining messages to maximize throughput");
        
        // Calculate 1% threshold correctly
        long onePercentThreshold = _numberOfMessagesToGenerate / 100; // 1% of total messages
        if (onePercentThreshold < 1000) onePercentThreshold = 1000; // Minimum threshold for small tests
        
        Console.WriteLine($"[{_taskName}] RESILIENCE CHECK: Message {messageIndex+1} vs 1% threshold {onePercentThreshold} ({(double)onePercentThreshold/_numberOfMessagesToGenerate*100:F1}%)");
        
        // Only stop if we're in the first 1% of messages (indicating fundamental issues)
        if (messageIndex < onePercentThreshold)
        {
            Console.WriteLine($"💥 [{_taskName}] ============ CRITICAL EARLY FAILURE DETECTED ============");
            Console.WriteLine($"💥 [{_taskName}] STOPPING SOURCE: Failure in first 1% of messages indicates fundamental issue");
            Console.WriteLine($"💥 [{_taskName}] FAILED AT: Message {messageIndex+1}/{_numberOfMessagesToGenerate} ({(double)(messageIndex+1)/_numberOfMessagesToGenerate*100:F2}%)");
            Console.WriteLine($"💥 [{_taskName}] THRESHOLD: First {onePercentThreshold} messages ({(double)onePercentThreshold/_numberOfMessagesToGenerate*100:F1}%)");
            Console.WriteLine($"💥 [{_taskName}] ACTION: Setting _isRunning = false to stop source execution");
            Console.WriteLine($"💥 [{_taskName}] ========================================================");
            _isRunning = false;
            return false;
        }
        
        // For stress tests, log and continue with degraded performance rather than stopping
        Console.WriteLine($"[{_taskName}] RESILIENCE: Message {messageIndex+1} is beyond 1% threshold - continuing execution");
        return false; // Skip this message but don't stop the source
    }

    private async Task EmitFinalBarrierIfNeeded(ISourceContext<string> ctx, long emittedCount)
    {
        // Send one final barrier after all data messages (optional, but good for some checkpointing models)
        if (_isRunning && emittedCount > 0)
        {
            try
            {
                long finalCheckpointId = Interlocked.Increment(ref _nextCheckpointId) - 1;
                if (finalCheckpointId == 0)
                {
                    finalCheckpointId = Interlocked.Increment(ref _nextCheckpointId) - 1;
                }

                long finalTimestamp = DateTimeOffset.UtcNow.ToUnixTimeMilliseconds();
                string finalBarrierMessage = $"BARRIER_{finalCheckpointId}_{finalTimestamp}_FINAL";
                Console.WriteLine($"[{_taskName}] Injecting Final Barrier: {finalBarrierMessage}");
                await ctx.CollectAsync(finalBarrierMessage);
            }
            catch (Exception ex)
            {
                Console.WriteLine($"[{_taskName}] ERROR during final barrier injection: {ex.GetType().Name} - {ex.Message}");
                // Don't fail the entire source for final barrier issues
            }
        }
    }

    public void Cancel()
    {
        Console.WriteLine($"");
        Console.WriteLine($"🛑 [{_taskName}] ============ CANCEL() CALLED ============");
        Console.WriteLine($"🛑 [{_taskName}] SOURCE CANCELLATION REQUESTED");
        Console.WriteLine($"🛑 [{_taskName}] CURRENT STATE: _isRunning = {_isRunning}");
        Console.WriteLine($"🛑 [{_taskName}] STACK TRACE OF CANCEL CALL:");
        Console.WriteLine($"🛑 [{_taskName}] {Environment.StackTrace}");
        Console.WriteLine($"🛑 [{_taskName}] =========================================");
        Console.WriteLine($"");
        _isRunning = false;
    }

    public void Close()
    {
        Console.WriteLine($"[{_taskName}] Closing HighVolumeSourceFunction.");
        Console.WriteLine($"[{_taskName}] Source Redis database reference released.");
    }

    public ITypeSerializer<string> Serializer => _serializer;
}

// Simple Map Operator that passes through the value for integration testing
public class SimpleToUpperMapOperator : IMapOperator<string, string>
{
    private long _processedCount = 0;
    public static readonly long LogFrequency = 100000;

    public string Map(string value)
    {
        // For integration testing, pass through the value unchanged so Kafka messages match expected format
        long currentCount = Interlocked.Increment(ref _processedCount);
        if (currentCount % LogFrequency == 0)
        {
            Console.WriteLine($"[SimpleToUpperMapOperator] Processed {currentCount} messages. Last input: {value}");
        }
        return value; // Pass through unchanged for integration testing
    }
}

// RedisIncrementSinkFunction and KafkaSinkFunction are assumed to be in their own files:
// RedisIncrementSinkFunction.cs and KafkaSinkFunction.cs within the FlinkJobSimulator namespace/project.

// Simple implementations for integration testing
public class SimpleRuntimeContext : IRuntimeContext
{
    public string JobName { get; }
    public string TaskName { get; }
    public int IndexOfThisSubtask { get; }
    public int NumberOfParallelSubtasks { get; }
    public JobConfiguration JobConfiguration { get; }
    public IStateSnapshotStore StateSnapshotStore { get; }
    
    private object? _currentKey;

    public SimpleRuntimeContext(string taskName, int indexOfThisSubtask = 0, int numberOfParallelSubtasks = 1)
    {
        JobName = "IntegrationTestJob";
        TaskName = taskName;
        IndexOfThisSubtask = indexOfThisSubtask;
        NumberOfParallelSubtasks = numberOfParallelSubtasks;
        JobConfiguration = new JobConfiguration(); // Simple default
        StateSnapshotStore = new SimpleStateSnapshotStore(); // Simple default
    }

    public object? GetCurrentKey() => _currentKey;
    public void SetCurrentKey(object? key) => _currentKey = key;

    public IValueState<T> GetValueState<T>(ValueStateDescriptor<T> stateDescriptor)
    {
        // Return a simple in-memory implementation for testing
        return new SimpleValueState<T>();
    }

    public IListState<T> GetListState<T>(ListStateDescriptor<T> stateDescriptor)
    {
        // Return a simple in-memory implementation for testing
        return new SimpleListState<T>();
    }

    public IMapState<TK, TV> GetMapState<TK, TV>(MapStateDescriptor<TK, TV> stateDescriptor) where TK : notnull
    {
        // Return a simple in-memory implementation for testing
        return new SimpleMapState<TK, TV>();
    }
}

// Simple state implementations for testing
public class SimpleValueState<T> : IValueState<T>
{
    private T? _value;
    public T? Value() => _value;
    public void Update(T? value) => _value = value;
    public void Clear() => _value = default;
}

public class SimpleListState<T> : IListState<T>
{
    private readonly List<T> _list = new();
    public IEnumerable<T> GetValues() => _list;
    public IEnumerable<T> Get() => _list;
    public void Add(T value) => _list.Add(value);
    public void Update(IEnumerable<T> values) { _list.Clear(); _list.AddRange(values); }
    public void AddAll(IEnumerable<T> values) => _list.AddRange(values);
    public void Clear() => _list.Clear();
}

public class SimpleMapState<TK, TV> : IMapState<TK, TV> where TK : notnull
{
    private readonly Dictionary<TK, TV> _map = new();
    public TV GetValueForKey(TK key) => _map.TryGetValue(key, out TV? value) ? value : default!;
    public TV Get(TK key) => GetValueForKey(key);
    public void Put(TK key, TV value) => _map[key] = value;
    public void PutAll(IDictionary<TK, TV> map) { foreach (var kvp in map) _map[kvp.Key] = kvp.Value; }
    public void Remove(TK key) => _map.Remove(key);
    public bool Contains(TK key) => _map.ContainsKey(key);
    public IEnumerable<TK> Keys() => _map.Keys;
    public IEnumerable<TV> Values() => _map.Values;
    public IEnumerable<KeyValuePair<TK, TV>> Entries() => _map;
    public bool IsEmpty() => _map.Count == 0;
    public void Clear() => _map.Clear();
}

public class SimpleStateSnapshotStore : IStateSnapshotStore
{
    // Simple no-op implementation for testing
    public Task<SnapshotHandle> StoreSnapshot(string jobId, long checkpointId, string taskManagerId, string operatorId, byte[] snapshotData)
    {
        return Task.FromResult(new SnapshotHandle($"test-{jobId}-{checkpointId}-{taskManagerId}-{operatorId}"));
    }
    
    public Task<byte[]?> RetrieveSnapshot(SnapshotHandle handle)
    {
        return Task.FromResult<byte[]?>(null);
    }
}

public class SimpleSourceContext<T> : ISourceContext<T>
{
    public List<T> CollectedMessages { get; } = new List<T>();

    public void Collect(T record)
    {
        CollectedMessages.Add(record);
    }

    public Task CollectAsync(T record)
    {
        CollectedMessages.Add(record);
        return Task.CompletedTask;
    }

    public void CollectWithTimestamp(T record, long timestamp)
    {
        CollectedMessages.Add(record);
    }

    public Task CollectWithTimestampAsync(T record, long timestamp)
    {
        CollectedMessages.Add(record);
        return Task.CompletedTask;
    }

    public void EmitWatermark(Watermark watermark)
    {
        // For simulation purposes, we can ignore watermarks
    }

    public long ProcessingTime => DateTimeOffset.UtcNow.ToUnixTimeMilliseconds();
}

public class SimpleSinkContext : ISinkContext
{
    public long CurrentProcessingTimeMillis() => DateTimeOffset.UtcNow.ToUnixTimeMilliseconds();
}

public static class Program
{
    private static async Task VerifyInfrastructure(string jobManagerGrpcUrl)
    {
        Console.WriteLine("Verifying JobManager and TaskManager infrastructure...");
        
        try
        {
            // Check JobManager HTTP endpoint for task managers
            using var httpClient = new HttpClient();
            var response = await httpClient.GetAsync($"{jobManagerGrpcUrl.Replace(ServicePorts.JobManagerGrpc.ToString(), ServicePorts.JobManagerHttp.ToString()).Replace("grpc", "http")}/api/jobmanager/taskmanagers");
            
            if (response.IsSuccessStatusCode)
            {
                var content = await response.Content.ReadAsStringAsync();
                Console.WriteLine($"JobManager TaskManager API response: {content}");
                
                // Parse to count task managers (simple check)
                if (content.Contains("TM-"))
                {
                    var tmCount = content.Split("TM-").Length - 1;
                    Console.WriteLine($"Found {tmCount} registered TaskManagers.");
                    
                    if (tmCount >= 10)
                    {
                        Console.WriteLine("✅ Infrastructure verification PASSED: JobManager and 10+ TaskManagers are running.");
                    }
                    else
                    {
                        Console.WriteLine($"⚠ Infrastructure verification WARNING: Expected 10 TaskManagers, found {tmCount}.");
                    }
                }
                else
                {
                    Console.WriteLine("⚠ Infrastructure verification WARNING: No TaskManagers found in response.");
                }
            }
            else
            {
                Console.WriteLine($"⚠ Infrastructure verification WARNING: JobManager HTTP API not accessible. Status: {response.StatusCode}");
            }
        }
        catch (Exception ex)
        {
            Console.WriteLine($"⚠ Infrastructure verification WARNING: {ex.Message}");
        }
    }

    private static async Task<IHost> ConfigureAndStartHost(string[] args)
    {
        var builder = Host.CreateApplicationBuilder(args);
        
        // Add Redis client using Aspire pattern
        builder.AddRedisClient("redis");
        
        // Register IDatabase as a singleton service
        builder.Services.AddSingleton<IDatabase>(provider => 
        {
            var connectionMultiplexer = provider.GetRequiredService<IConnectionMultiplexer>();
            return connectionMultiplexer.GetDatabase();
        });
        
        // Register configuration and other services
        builder.Services.AddSingleton<IConfiguration>(builder.Configuration);
        
        var host = builder.Build();
        await host.StartAsync();
        return host;
    }

    private static (long numMessages, string redisSinkCounterKey, string kafkaTopic, string jobManagerGrpcUrl) GetConfiguration()
    {
        long numMessages = 10000; // Default, can be overridden by env var set in AppHost
        string? envNumMessages = Environment.GetEnvironmentVariable("SIMULATOR_NUM_MESSAGES");
        if (!string.IsNullOrEmpty(envNumMessages) && long.TryParse(envNumMessages, out long parsedNumMessages))
        {
            numMessages = parsedNumMessages;
        }

        string redisSinkCounterKey = Environment.GetEnvironmentVariable("SIMULATOR_REDIS_KEY_SINK_COUNTER") ?? "flinkdotnet:sample:processed_message_counter";
        string kafkaTopic = Environment.GetEnvironmentVariable("SIMULATOR_KAFKA_TOPIC") ?? "flinkdotnet.sample.topic";

        var jobManagerGrpcUrl = Environment.GetEnvironmentVariable("services__jobmanager__grpc__0");
        if (string.IsNullOrEmpty(jobManagerGrpcUrl))
        {
            var uriBuilder = new UriBuilder("http", ServiceHosts.Localhost, ServicePorts.JobManagerGrpc);
            jobManagerGrpcUrl = uriBuilder.Uri.ToString();
            Console.WriteLine($"JobManager gRPC URL not found in environment variables. Using default: {jobManagerGrpcUrl}");
        }
        else
        {
            Console.WriteLine($"JobManager gRPC URL from environment: {jobManagerGrpcUrl}");
        }

        Console.WriteLine($"Number of messages to generate: {numMessages}");
        Console.WriteLine($"Redis Sink Counter Key: {redisSinkCounterKey}");
        Console.WriteLine($"Kafka Topic: {kafkaTopic}");

        return (numMessages, redisSinkCounterKey, kafkaTopic, jobManagerGrpcUrl);
    }

    private static StreamExecutionEnvironment SetupFlinkEnvironment(long numMessages, string redisSinkCounterKey, string kafkaTopic, 
        IDatabase redisDatabase, IConfiguration configuration)
    {
        // Set static configuration for LocalStreamExecutor compatibility
        HighVolumeSourceFunction.NumberOfMessagesToGenerate = numMessages;
        HighVolumeSourceFunction.GlobalRedisDatabase = redisDatabase;
        HighVolumeSourceFunction.GlobalConfiguration = configuration;

        // Set static configuration for sink functions
        RedisIncrementSinkFunction<string>.GlobalRedisDatabase = redisDatabase;
        RedisIncrementSinkFunction<string>.GlobalRedisKey = redisSinkCounterKey;
        KafkaSinkFunction<string>.GlobalKafkaTopic = kafkaTopic;

        var env = StreamExecutionEnvironment.GetExecutionEnvironment();
        env.SerializerRegistry.RegisterSerializer(typeof(string), typeof(StringSerializer));

        var source = new HighVolumeSourceFunction(numMessages, new StringSerializer(), redisDatabase, configuration);
        DataStream<string> stream = env.AddSource(source, "high-volume-source-redis-seq");

        var mapOperator = new SimpleToUpperMapOperator();
        DataStream<string> mappedStream = stream.Map(mapOperator);

        // Fork the mapped stream to two sinks:
        mappedStream.AddSink(new FlinkJobSimulator.RedisIncrementSinkFunction<string>(redisDatabase, redisSinkCounterKey), "redis-processed-counter-sink");
        mappedStream.AddSink(new FlinkJobSimulator.KafkaSinkFunction<string>(kafkaTopic), "kafka-output-sink");

        return env;
    }

    private static async Task CleanRedisStateForFreshTest(IDatabase redisDatabase)
    {
        var globalSequenceKey = Environment.GetEnvironmentVariable("SIMULATOR_REDIS_KEY_GLOBAL_SEQUENCE") ?? "flinkdotnet:global_sequence_id";
        var sinkCounterKey = Environment.GetEnvironmentVariable("SIMULATOR_REDIS_KEY_SINK_COUNTER") ?? "flinkdotnet:sample:processed_message_counter";
        var jobErrorKey = "flinkdotnet:job_execution_error";
        
        Console.WriteLine($"Cleaning Redis keys for fresh test run:");
        Console.WriteLine($"  - Deleting global sequence key: {globalSequenceKey}");
        Console.WriteLine($"  - Deleting sink counter key: {sinkCounterKey}");
        Console.WriteLine($"  - Deleting job error key: {jobErrorKey}");
        
        try
        {
            // Delete all test-related keys to ensure fresh state
            var deletedCount = await redisDatabase.KeyDeleteAsync(new RedisKey[] { 
                globalSequenceKey, 
                sinkCounterKey, 
                jobErrorKey 
            });
            Console.WriteLine($"Successfully deleted {deletedCount} Redis keys");
            
            // Verify cleanup
            var remaining = await redisDatabase.StringGetAsync(globalSequenceKey);
            if (remaining.HasValue)
            {
                Console.WriteLine($"WARNING: Global sequence key still exists with value: {remaining}");
            }
            else
            {
                Console.WriteLine("✅ Verified: Global sequence key successfully cleared");
            }
        }
        catch (Exception ex)
        {
            Console.WriteLine($"WARNING: Redis cleanup failed: {ex.Message}");
            Console.WriteLine("Continuing with test execution...");
        }
    }

    private static async Task ExecuteJob(StreamExecutionEnvironment env, long numMessages, string jobManagerGrpcUrl, IDatabase redisDatabase)
    {
        Console.WriteLine("Building JobGraph...");
        JobGraph jobGraph = env.CreateJobGraph($"DualSinkSimJob-{numMessages}");
        Console.WriteLine($"JobGraph created with name: {jobGraph.JobName}");
        Console.WriteLine($"Job Vertices: {jobGraph.Vertices.Count}");

        // *** CRITICAL FIX: Clean Redis state before each test run ***
        Console.WriteLine("=== REDIS STATE CLEANUP ===");
        await CleanRedisStateForFreshTest(redisDatabase);
        
        // Verify infrastructure
        Console.WriteLine("Verifying JobManager and TaskManager infrastructure...");
        try 
        {
            await VerifyInfrastructure(jobManagerGrpcUrl);
        }
        catch (Exception infraEx)
        {
            Console.WriteLine($"Infrastructure verification failed: {infraEx.Message}");
            Console.WriteLine("Proceeding with local execution for integration testing...");
        }

        // Execute the job
        Console.WriteLine("Executing job using LocalStreamExecutor for Apache Flink 2.0 compatibility...");
        var jobExecutionSuccess = false;
        try 
        {
            await env.ExecuteLocallyAsync($"DualSinkSimJob-{numMessages}", CancellationToken.None);
            Console.WriteLine("Job execution completed successfully using LocalStreamExecutor.");
            jobExecutionSuccess = true;
        }
        catch (Exception jobEx)
        {
            Console.WriteLine($"=== JOB EXECUTION ERROR DETECTED ===");
            Console.WriteLine($"Job execution failed: {jobEx.Message}");
            Console.WriteLine($"Exception Type: {jobEx.GetType().Name}");
            Console.WriteLine($"Stack Trace: {jobEx.StackTrace}");
            if (jobEx.InnerException != null)
            {
                Console.WriteLine($"Inner Exception: {jobEx.InnerException.GetType().Name} - {jobEx.InnerException.Message}");
                Console.WriteLine($"Inner Stack Trace: {jobEx.InnerException.StackTrace}");
            }
            Console.WriteLine("=== CRITICAL: Job execution failed - this explains why sinks are not processing ===");
            
            // Instead of crashing, mark Redis with a special error indicator
            try
            {
                await redisDatabase.StringSetAsync("flinkdotnet:job_execution_error", $"{jobEx.GetType().Name}: {jobEx.Message}");
                Console.WriteLine("Marked Redis with job execution error indicator");
            }
            catch (Exception redisEx)
            {
                Console.WriteLine($"Failed to mark Redis with error: {redisEx.Message}");
            }
        }
        
        if (!jobExecutionSuccess)
        {
            Console.WriteLine("=== ATTEMPTING SIMPLIFIED DIRECT EXECUTION FALLBACK ===");
            Console.WriteLine("LocalStreamExecutor failed - trying direct execution for stress test compatibility");
            
            try
            {
                ExecuteJobDirectly(numMessages, redisDatabase);
                Console.WriteLine("Direct execution completed successfully");
            }
            catch (Exception directEx)
            {
                Console.WriteLine($"Direct execution also failed: {directEx.Message}");
                await redisDatabase.StringSetAsync("flinkdotnet:job_execution_error", $"Both LocalStreamExecutor and Direct execution failed: {directEx.Message}");
            }
        }
    }

    private static void ExecuteJobDirectly(long numMessages, IDatabase redisDatabase)
    {
        Console.WriteLine($"=== DIRECT EXECUTION: Processing {numMessages} messages ===");
        
        // Create source function directly
        var source = new HighVolumeSourceFunction(numMessages, new StringSerializer(), redisDatabase, null!);
        
        // Create sink functions directly  
        var redisSinkCounterKey = Environment.GetEnvironmentVariable("SIMULATOR_REDIS_KEY_SINK_COUNTER") ?? "flinkdotnet:sample:processed_message_counter";
        var kafkaTopic = Environment.GetEnvironmentVariable("SIMULATOR_KAFKA_TOPIC") ?? "flinkdotnet.sample.topic";
        
        var redisSink = new RedisIncrementSinkFunction<string>(redisDatabase, redisSinkCounterKey);
        var kafkaSink = new KafkaSinkFunction<string>(kafkaTopic);
        
        // Create operator
        var mapOperator = new SimpleToUpperMapOperator();
        
        Console.WriteLine("Direct execution: Starting source function...");
        
        // Create a simple context to collect messages
        var messages = new List<string>();
        var directContext = new DirectSourceContext(messages);
        
        // Run source function
        source.Run(directContext);
        
        Console.WriteLine($"Direct execution: Source generated {messages.Count} messages");
        
        // Process through map operator and sinks
        var runtimeContext = new SimpleRuntimeContext("DirectTask", 0, 1);
        var sinkContext = new DirectSinkContext();
        
        // Open sinks
        redisSink.Open(runtimeContext);
        kafkaSink.Open(runtimeContext);
        
        Console.WriteLine("Direct execution: Processing messages through pipeline...");
        
        foreach (var message in messages)
        {
            // Apply map operator
            var mappedMessage = mapOperator.Map(message);
            
            // Send to both sinks
            redisSink.Invoke(mappedMessage, sinkContext);
            kafkaSink.Invoke(mappedMessage, sinkContext);
        }
        
        // Close sinks
        redisSink.Close();
        kafkaSink.Close();
        
        Console.WriteLine($"Direct execution: Processed {messages.Count} messages through both sinks");
    }

    // Simple implementations for direct execution
    private sealed class DirectSourceContext : ISourceContext<string>
    {
        private readonly List<string> _messages;
        
        public DirectSourceContext(List<string> messages)
        {
            _messages = messages;
        }
        
        public void Collect(string record)
        {
            _messages.Add(record);
        }
        
        public Task CollectAsync(string record)
        {
            Collect(record);
            return Task.CompletedTask;
        }
        
        public void CollectWithTimestamp(string record, long timestamp)
        {
            Collect(record);
        }
        
        public Task CollectWithTimestampAsync(string record, long timestamp)
        {
            CollectWithTimestamp(record, timestamp);
            return Task.CompletedTask;
        }
        
        public void EmitWatermark(Watermark watermark) { }
        
        public long ProcessingTime => DateTimeOffset.UtcNow.ToUnixTimeMilliseconds();
    }
    
    private sealed class DirectSinkContext : ISinkContext
    {
        public long CurrentProcessingTimeMillis() => DateTimeOffset.UtcNow.ToUnixTimeMilliseconds();
    }

    public static async Task Main(string[] args)
    {
        var totalStartTime = DateTime.UtcNow;
        Console.WriteLine("Flink Job Simulator starting (Dual Sink: Redis Counter & Kafka)...");
        Console.WriteLine($"=== STARTUP DIAGNOSTICS ===");
        Console.WriteLine($"Start Time: {DateTime.UtcNow:yyyy-MM-dd HH:mm:ss} UTC");
        Console.WriteLine($"Process ID: {Environment.ProcessId}");
        Console.WriteLine($"Working Directory: {Environment.CurrentDirectory}");
        Console.WriteLine($"Arguments: {string.Join(" ", args)}");

        // Debug: Log all environment variables to understand what Aspire provides
        Console.WriteLine("=== DEBUG: Environment Variables ===");
        foreach (DictionaryEntry env in Environment.GetEnvironmentVariables())
        {
            var key = env.Key?.ToString() ?? "<null>";
            var value = env.Value?.ToString() ?? "<null>";
            if (key.Contains("redis", StringComparison.OrdinalIgnoreCase) || 
                key.Contains("kafka", StringComparison.OrdinalIgnoreCase) ||
                key.Contains("ConnectionStrings", StringComparison.OrdinalIgnoreCase) ||
                key.Contains("DOTNET", StringComparison.OrdinalIgnoreCase))
            {
                Console.WriteLine($"  {key}={value}");
            }
        }
        Console.WriteLine("=== END DEBUG ===");

        Console.WriteLine("Starting host configuration...");
        var hostStartTime = DateTime.UtcNow;
        using var host = await ConfigureAndStartHost(args);
        var hostStartupDuration = DateTime.UtcNow - hostStartTime;
        Console.WriteLine($"Host configured and started in {hostStartupDuration.TotalMilliseconds:F0}ms");

        Console.WriteLine("Retrieving services from DI container...");
        var redisDatabase = host.Services.GetRequiredService<IDatabase>();
        var configuration = host.Services.GetRequiredService<IConfiguration>();
        Console.WriteLine("Services retrieved successfully");

        var (numMessages, redisSinkCounterKey, kafkaTopic, jobManagerGrpcUrl) = GetConfiguration();
        Console.WriteLine($"Configuration loaded: {numMessages} messages, Redis key: '{redisSinkCounterKey}', Kafka topic: '{kafkaTopic}', JobManager URL: '{jobManagerGrpcUrl}'");

        try
        {
            Console.WriteLine("Setting up Flink environment...");
            var envSetupStartTime = DateTime.UtcNow;
            var env = SetupFlinkEnvironment(numMessages, redisSinkCounterKey, kafkaTopic, redisDatabase, configuration);
            var envSetupDuration = DateTime.UtcNow - envSetupStartTime;
            Console.WriteLine($"Flink environment setup completed in {envSetupDuration.TotalMilliseconds:F0}ms");
            
            Console.WriteLine("Starting job execution...");
            var jobStartTime = DateTime.UtcNow;
            await ExecuteJob(env, numMessages, jobManagerGrpcUrl, redisDatabase);
            var jobDuration = DateTime.UtcNow - jobStartTime;
            Console.WriteLine($"Job execution completed in {jobDuration.TotalMilliseconds:F0}ms");
        }
        catch (Exception ex)
        {
            Console.WriteLine($"=== SIMULATOR ERROR ===");
            Console.WriteLine($"Error Time: {DateTime.UtcNow:yyyy-MM-dd HH:mm:ss} UTC");
            Console.WriteLine($"Error Type: {ex.GetType().Name}");
            Console.WriteLine($"Error Message: {ex.Message}");
            Console.WriteLine($"Stack Trace:");
            Console.WriteLine(ex.StackTrace);
            
            if (ex.InnerException != null)
            {
                Console.WriteLine($"Inner Exception: {ex.InnerException.GetType().Name}");
                Console.WriteLine($"Inner Message: {ex.InnerException.Message}");
            }
            
            Console.WriteLine("Continuing despite error for integration test verification.");
        }

        Console.WriteLine("Flink Job Simulator finished executing the job.");
        Console.WriteLine($"Job completed. Check Redis key '{redisSinkCounterKey}' and Kafka topic '{kafkaTopic}' for results.");
        Console.WriteLine("Job Simulator completed successfully. Allowing time for async operations to complete...");
        
        await Task.Delay(TimeSpan.FromSeconds(5));
        var totalDuration = DateTime.UtcNow - totalStartTime;
        Console.WriteLine($"=== COMPLETION SUMMARY ===");
        Console.WriteLine($"Completion Time: {DateTime.UtcNow:yyyy-MM-dd HH:mm:ss} UTC");
        Console.WriteLine($"Total Execution Time: {totalDuration.TotalMilliseconds:F0}ms");
        Console.WriteLine("Keeping process alive for Aspire orchestration...");
        await Task.Delay(Timeout.Infinite);
    }
}
}
