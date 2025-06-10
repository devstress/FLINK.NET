using FlinkDotNet.Core.Api;
using FlinkDotNet.Core.Api.Streaming;
using FlinkDotNet.Core.Abstractions.Sinks; // For ISinkFunction
using FlinkDotNet.Core.Abstractions.Sources; // For ISourceFunction
using FlinkDotNet.Core.Abstractions.Operators; // For IMapOperator, IOperatorLifecycle
using FlinkDotNet.Core.Abstractions.Context; // For IRuntimeContext
using FlinkDotNet.Core.Abstractions.Serializers;
using StackExchange.Redis; // For HighVolumeSourceFunction Redis parts
using Microsoft.Extensions.Configuration; // For IConfiguration in HighVolumeSourceFunction & Sinks
using FlinkDotNet.JobManager.Models.JobGraph;
using System.Text.Json;
using System; // For Environment, ArgumentOutOfRangeException etc.
using System.Collections.Generic; // For IEnumerable in source
using System.Threading; // For CancellationToken, Interlocked, Timer
using System.Threading.Tasks; // For Task
using System.Diagnostics; // For launching Aspire Dashboard

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
    private volatile bool _isRunning = true;
    private string _taskName = nameof(HighVolumeSourceFunction);

    private ConnectionMultiplexer? _redisConnection;
    private IDatabase? _redisDb;
    private readonly string _globalSequenceRedisKey;

    private static readonly IConfiguration Configuration = new ConfigurationBuilder()
        .AddEnvironmentVariables()
        .Build();

    // --- Checkpoint Barrier Injection Fields ---
    private long _messagesSentSinceLastBarrier = 0;
    private const long MessagesPerBarrier = 1000; // Inject a barrier every 1000 messages for testing
    private static long _nextCheckpointId = 1; // Static to ensure unique IDs across potential re-instantiations in some test scenarios (though for a single run, instance field is fine)


    public HighVolumeSourceFunction(long numberOfMessagesToGenerate, ITypeSerializer<string> serializer)
    {
        _numberOfMessagesToGenerate = numberOfMessagesToGenerate;
        _serializer = serializer ?? throw new ArgumentNullException(nameof(serializer));
        if (numberOfMessagesToGenerate <= 0) throw new ArgumentOutOfRangeException(nameof(numberOfMessagesToGenerate), "Number of messages must be positive.");

        _globalSequenceRedisKey = Configuration?["SIMULATOR_REDIS_KEY_GLOBAL_SEQUENCE"] ?? "flinkdotnet:global_sequence_id";
        Console.WriteLine($"[HighVolumeSourceFunction] Configured to use global sequence Redis key: '{_globalSequenceRedisKey}'");
    }

    public void Open(IRuntimeContext context)
    {
        _taskName = context.TaskName;
        Console.WriteLine($"[{_taskName}] Opening HighVolumeSourceFunction.");

        string? redisConnectionString = Configuration?["ConnectionStrings__redis"];
        if (string.IsNullOrEmpty(redisConnectionString))
        {
            Console.WriteLine($"[{_taskName}] ERROR: Redis connection string 'ConnectionStrings__redis' not found in environment variables for source.");
            redisConnectionString = "localhost:6379";
            Console.WriteLine($"[{_taskName}] Using default Redis connection string for source: {redisConnectionString}");
        }
        else
        {
            Console.WriteLine($"[{_taskName}] Source found Redis connection string from environment.");
        }

        try
        {
            _redisConnection = ConnectionMultiplexer.Connect(redisConnectionString);
            _redisDb = _redisConnection.GetDatabase();
            Console.WriteLine($"[{_taskName}] Source successfully connected to Redis for sequence generation.");

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
    }

    private async Task EmitMessageAsync(ISourceContext<string> ctx, long currentSequenceId, long emittedCount)
    {
        string message = $"MessagePayload_Seq-{currentSequenceId}";
        ctx.Collect(message);
        _messagesSentSinceLastBarrier++;

        if (emittedCount % 100000 == 0)
        {
            Console.WriteLine($"[{_taskName}] Emitted {emittedCount} messages. Last sequence ID: {currentSequenceId}");
            await Task.Yield();
        }
    }

public async Task RunAsync(ISourceContext<string> ctx, CancellationToken cancellationToken = default)
    {
        if (_redisDb == null)
        {
            Console.WriteLine($"[{_taskName}] ERROR: Redis database not available in Run(). Source cannot generate sequences.");
            return;
        }

        Console.WriteLine($"[{_taskName}] Starting to emit {_numberOfMessagesToGenerate} messages with Redis-generated sequence IDs and barrier injection every {MessagesPerBarrier} messages...");
        long emittedCount = 0;
        for (long i = 0; i < _numberOfMessagesToGenerate; i++)
        {
            if (!_isRunning || cancellationToken.IsCancellationRequested)
            {
                Console.WriteLine($"[{_taskName}] Emission cancelled after {emittedCount} messages.");
                break;
            }

            InjectBarrierIfNeeded(ctx);

            try
            {
                long currentSequenceId = await _redisDb.StringIncrementAsync(_globalSequenceRedisKey);
                emittedCount++;
                await EmitMessageAsync(ctx, currentSequenceId, emittedCount);
            }
            catch (Exception ex)
            {
                Console.WriteLine($"[{_taskName}] ERROR during message emission or Redis INCR: {ex.Message}. Stopping source.");
                _isRunning = false;
                break;
            }
        }
        // Send one final barrier after all data messages (optional, but good for some checkpointing models)
        if (_isRunning && emittedCount > 0)
        {
            long finalCheckpointId = Interlocked.Increment(ref _nextCheckpointId) - 1;
            if (finalCheckpointId == 0)
            {
                finalCheckpointId = Interlocked.Increment(ref _nextCheckpointId) - 1;
            }

            long finalTimestamp = DateTimeOffset.UtcNow.ToUnixTimeMilliseconds();
            string finalBarrierMessage = $"BARRIER_{finalCheckpointId}_{finalTimestamp}_FINAL";
            Console.WriteLine($"[{_taskName}] Injecting Final Barrier: {finalBarrierMessage}");
            ctx.Collect(finalBarrierMessage);
        }

        Console.WriteLine($"[{_taskName}] Finished emitting. Total data messages emitted: {emittedCount}. Last global sequence ID used (approx): {_redisDb.StringGet(_globalSequenceRedisKey)}");
    }

    public void Cancel()
    {
        Console.WriteLine($"[{_taskName}] Cancel called.");
        _isRunning = false;
    }

    public void Close()
    {
        Console.WriteLine($"[{_taskName}] Closing HighVolumeSourceFunction.");
        _redisConnection?.Close();
        _redisConnection?.Dispose();
        Console.WriteLine($"[{_taskName}] Source Redis connection closed.");
    }

    public ITypeSerializer<string> Serializer => _serializer;
}

// Simple Map Operator
public class SimpleToUpperMapOperator : IMapOperator<string, string>
{
    private long _processedCount = 0;
    public static readonly long LogFrequency = 100000;

    public string Map(string value)
    {
        string upperValue = value.ToUpper();
        long currentCount = Interlocked.Increment(ref _processedCount);
        if (currentCount % LogFrequency == 0)
        {
            Console.WriteLine($"[SimpleToUpperMapOperator] Processed {currentCount} messages. Last input: {value}");
        }
        return upperValue;
    }
}

// RedisIncrementSinkFunction and KafkaSinkFunction are assumed to be in their own files:
// RedisIncrementSinkFunction.cs and KafkaSinkFunction.cs within the FlinkJobSimulator namespace/project.

public static class Program
{
    public static void Main(string[] args)
    {
        Console.WriteLine("Flink Job Simulator starting (Dual Sink: Redis Counter & Kafka)...");

        // --- Configuration ---
        long numMessages = 10000; // Default, can be overridden by env var set in AppHost
        string? envNumMessages = Environment.GetEnvironmentVariable("SIMULATOR_NUM_MESSAGES");
        if (!string.IsNullOrEmpty(envNumMessages) && long.TryParse(envNumMessages, out long parsedNumMessages))
        {
            numMessages = parsedNumMessages;
        }
        Console.WriteLine($"Number of messages to generate: {numMessages}");

        string redisSinkCounterKey = Environment.GetEnvironmentVariable("SIMULATOR_REDIS_KEY_SINK_COUNTER") ?? "flinkdotnet:sample:processed_message_counter";
        string kafkaTopic = Environment.GetEnvironmentVariable("SIMULATOR_KAFKA_TOPIC") ?? "flinkdotnet.sample.topic";
        Console.WriteLine($"Redis Sink Counter Key: {redisSinkCounterKey}");
        Console.WriteLine($"Kafka Topic: {kafkaTopic}");
        // Global sequence key for source is handled internally by HighVolumeSourceFunction

        // --- Service Discovery for JobManager gRPC endpoint ---
        var jobManagerGrpcUrl = Environment.GetEnvironmentVariable("services__jobmanager__grpc__0");
        if (string.IsNullOrEmpty(jobManagerGrpcUrl))
        {
            var builder = new UriBuilder("http", "localhost", 50051);
            jobManagerGrpcUrl = builder.Uri.ToString();
            Console.WriteLine($"JobManager gRPC URL not found in environment variables. Using default: {jobManagerGrpcUrl}");
        }
        else
        {
            Console.WriteLine($"JobManager gRPC URL from environment: {jobManagerGrpcUrl}");
        }

        try
        {
            // --- 1. Set up StreamExecutionEnvironment ---
            var env = StreamExecutionEnvironment.GetExecutionEnvironment();
            env.SerializerRegistry.RegisterSerializer(typeof(string), typeof(StringSerializer));

            // --- 2. Define the job ---
            var source = new HighVolumeSourceFunction(numMessages, new StringSerializer());
            DataStream<string> stream = env.AddSource(source, "high-volume-source-redis-seq");

            var mapOperator = new SimpleToUpperMapOperator();
            DataStream<string> mappedStream = stream.Map(mapOperator);

            // Fork the mapped stream to two sinks:
            mappedStream.AddSink(new FlinkJobSimulator.RedisIncrementSinkFunction<string>(redisSinkCounterKey), "redis-processed-counter-sink");
            mappedStream.AddSink(new FlinkJobSimulator.KafkaSinkFunction<string>(kafkaTopic), "kafka-output-sink");

            // --- 3. Build the JobGraph ---
            Console.WriteLine("Building JobGraph...");
            JobGraph jobGraph = env.CreateJobGraph($"DualSinkSimJob-{numMessages}");
            Console.WriteLine($"JobGraph created with name: {jobGraph.JobName}");
            Console.WriteLine($"Job Vertices: {jobGraph.Vertices.Count}");

            // Job submission skipped in this simplified build
            Console.WriteLine("Job submission skipped.");
        }
        catch (Exception ex)
        {
            Console.WriteLine($"An unexpected error occurred in the simulator: {ex.Message}");
            Console.WriteLine(ex.StackTrace);
        }

        Console.WriteLine("Flink Job Simulator finished building the job. Execution is asynchronous on the cluster.");
        Console.WriteLine($"Observe JobManager & TaskManager logs, Aspire dashboard, Redis key '{redisSinkCounterKey}', and Kafka topic '{kafkaTopic}'.");

        // When running locally (not inside a container), try to launch the Aspire Dashboard
        // so developers immediately see service status and logs.
        if (string.IsNullOrEmpty(Environment.GetEnvironmentVariable("DOTNET_RUNNING_IN_CONTAINER")))
        {
            try
            {
                var dashboardUrl = "http://localhost:18888";
                System.Diagnostics.Process.Start(new System.Diagnostics.ProcessStartInfo
                {
                    FileName = dashboardUrl,
                    UseShellExecute = true
                });
                Console.WriteLine($"Opened Aspire Dashboard at {dashboardUrl}");
            }
            catch (Exception ex)
            {
                Console.WriteLine($"Failed to launch Aspire Dashboard automatically: {ex.Message}");
            }
        }

        Console.WriteLine("Press Ctrl+C to exit when you are done observing the simulator.");
        // Keep the process alive so users can inspect the Aspire dashboard and Web UI
        Thread.Sleep(Timeout.Infinite);
    }
}
}
