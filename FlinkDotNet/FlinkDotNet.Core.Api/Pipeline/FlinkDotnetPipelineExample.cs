using FlinkDotNet.Core.Api.BackPressure;
using FlinkDotNet.Core.Api.Pipeline;
using FlinkDotNet.Core.Abstractions.Sources;
using FlinkDotNet.Core.Abstractions.Context;
using FlinkDotNet.Core.Abstractions.Sinks;
using FlinkDotNet.Core.Abstractions.Serializers;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;

namespace FlinkDotNet.Examples.Pipeline;

/// <summary>
/// Complete example of FlinkDotnet 2.0 style pipeline with comprehensive back pressure handling.
/// Demonstrates: Gateway -> KeyGen -> IngressProcessing -> AsyncEgressProcessing -> FinalSink
/// </summary>
public class FlinkDotnetPipelineExample
{
    public static async Task RunExample()
    {
        // Configure logging and DI
        using var host = CreateHost();
        var logger = host.Services.GetRequiredService<ILogger<FlinkDotnetPipelineExample>>();
        var loggerFactory = host.Services.GetRequiredService<ILoggerFactory>();

        logger.LogInformation("Starting FlinkDotnet 2.0 Pipeline Example with comprehensive back pressure");

        // Initialize back pressure controller
        var backPressureController = new PipelineBackPressureController(
            loggerFactory.CreateLogger<PipelineBackPressureController>(),
            new PipelineBackPressureConfiguration
            {
                GatewayBufferSize = 1000,
                KeyGenBufferSize = 2000,
                IngressProcessingBufferSize = 1500,
                AsyncEgressBufferSize = 3000,
                FinalSinkBufferSize = 2500
            });

        try
        {
            // Create source
            var source = new ExampleSource(1000); // Generate 1000 messages
            
            // Create pipeline stages
            var gateway = CreateGatewayStage(loggerFactory, backPressureController);
            var keyGen = CreateKeyGenStage(loggerFactory, backPressureController);
            var ingressProcessing = CreateIngressProcessingStage(loggerFactory, backPressureController);
            var asyncEgress = CreateAsyncEgressStage(loggerFactory, backPressureController);
            var finalSink = CreateFinalSinkStage(loggerFactory, backPressureController);

            // Execute pipeline
            await ExecutePipeline(source, gateway, keyGen, ingressProcessing, asyncEgress, finalSink, logger);

            // Display final metrics
            DisplayPipelineMetrics(backPressureController, logger);
        }
        finally
        {
            backPressureController.Dispose();
        }
    }

    private static IHost CreateHost()
    {
        return Host.CreateDefaultBuilder()
            .ConfigureServices(services =>
            {
                services.AddLogging(builder =>
                {
                    builder.AddConsole();
                    builder.SetMinimumLevel(LogLevel.Information);
                });
            })
            .Build();
    }

    private static GatewayStage<string> CreateGatewayStage(
        ILoggerFactory loggerFactory, 
        PipelineBackPressureController backPressureController)
    {
        return new GatewayStage<string>(
            loggerFactory.CreateLogger<GatewayStage<string>>(),
            backPressureController,
            new GatewayConfiguration
            {
                MaxRequestsPerSecond = 500,
                MaxConcurrentRequests = 50,
                MaxQueueSize = 1000
            });
    }

    private static KeyGenStage<string> CreateKeyGenStage(
        ILoggerFactory loggerFactory,
        PipelineBackPressureController backPressureController)
    {
        return new KeyGenStage<string>(
            loggerFactory.CreateLogger<KeyGenStage<string>>(),
            backPressureController,
            value => $"key-{value.GetHashCode() % 10}", // Simple key extraction
            new KeyGenConfiguration
            {
                NumberOfPartitions = 10,
                EnableLoadAwareness = true,
                LoadImbalanceThreshold = 100
            });
    }

    private static IngressProcessingStage<string> CreateIngressProcessingStage(
        ILoggerFactory loggerFactory,
        PipelineBackPressureController backPressureController)
    {
        return new IngressProcessingStage<string>(
            loggerFactory.CreateLogger<IngressProcessingStage<string>>(),
            backPressureController,
            new IngressProcessingConfiguration
            {
                MaxBufferSize = 1500,
                BufferTimeoutMs = 1000,
                EnableValidation = true,
                EnablePreprocessing = true
            },
            new ExampleValidator(),
            new ExampleProcessor());
    }

    private static AsyncEgressProcessingStage<string> CreateAsyncEgressStage(
        ILoggerFactory loggerFactory,
        PipelineBackPressureController backPressureController)
    {
        return new AsyncEgressProcessingStage<string>(
            loggerFactory.CreateLogger<AsyncEgressProcessingStage<string>>(),
            backPressureController,
            new AsyncEgressConfiguration
            {
                MaxRetries = 3,
                OperationTimeoutMs = 2000,
                BaseRetryDelayMs = 100,
                MaxRetryDelayMs = 1000,
                MaxConcurrentOperations = 20,
                EnableDeadLetterQueue = true,
                MaxDeadLetterQueueSize = 1000
            },
            new ExampleExternalService());
    }

    private static FinalSinkStage<string> CreateFinalSinkStage(
        ILoggerFactory loggerFactory,
        PipelineBackPressureController backPressureController)
    {
        return new FinalSinkStage<string>(
            loggerFactory.CreateLogger<FinalSinkStage<string>>(),
            backPressureController,
            new ExampleKafkaDestination(),
            new FinalSinkConfiguration
            {
                DestinationType = DestinationType.Kafka,
                RequireAcknowledgment = true,
                MaxPendingAcknowledgments = 500,
                AcknowledgmentTimeoutMs = 5000
            });
    }

    private static async Task ExecutePipeline(
        ExampleSource source,
        GatewayStage<string> gateway,
        KeyGenStage<string> keyGen,
        IngressProcessingStage<string> ingressProcessing,
        AsyncEgressProcessingStage<string> asyncEgress,
        FinalSinkStage<string> finalSink,
        ILogger logger)
    {
        logger.LogInformation("Executing FlinkDotnet 2.0 pipeline with back pressure monitoring");

        var processedCount = 0;
        var context = new SimpleRuntimeContext("PipelineExample");
        var sinkContext = new SimpleSinkContext();

        // Open all stages
        gateway.Open(context);
        keyGen.Open(context);
        ingressProcessing.Open(context);
        asyncEgress.Open(context);
        finalSink.Open(context);

        try
        {
            // Process records through the pipeline
            await foreach (var record in source.GenerateRecordsAsync())
            {
                try
                {
                    // Stage 1: Gateway (Ingress Rate Control)
                    var gatewayResult = gateway.Map(record);

                    // Stage 2: KeyGen (Deterministic Partitioning + Load Awareness)
                    var keyedRecord = keyGen.Map(gatewayResult);

                    // Stage 3: IngressProcessing (Validation + Preprocessing with Bounded Buffers)
                    var processedRecord = ingressProcessing.Map(keyedRecord);

                    // Stage 4: AsyncEgressProcessing (External I/O with Timeout, Retry, DLQ)
                    var egressResult = asyncEgress.Map(processedRecord);

                    // Stage 5: Final Sink (e.g., Kafka, DB, Callback) with Acknowledgment
                    finalSink.Invoke(egressResult, sinkContext);

                    processedCount++;

                    // Log progress
                    if (processedCount % 100 == 0)
                    {
                        logger.LogInformation("Processed {Count} records through pipeline", processedCount);
                    }
                }
                catch (Exception ex)
                {
                    logger.LogWarning(ex, "Pipeline processing failed for record: {Record}", record);
                    // Continue processing other records
                }
            }

            logger.LogInformation("Pipeline execution completed. Total processed: {Count} records", processedCount);
        }
        finally
        {
            // Close all stages
            gateway.Close();
            keyGen.Close();
            ingressProcessing.Close();
            asyncEgress.Close();
            finalSink.Close();

            // Dispose stages
            gateway.Dispose();
            keyGen.Dispose();
            ingressProcessing.Dispose();
            asyncEgress.Dispose();
            finalSink.Dispose();
        }
    }

    private static void DisplayPipelineMetrics(PipelineBackPressureController backPressureController, ILogger logger)
    {
        logger.LogInformation("=== FlinkDotnet 2.0 Pipeline Back Pressure Metrics ===");

        var pipelineStatus = backPressureController.GetPipelineStatus();
        logger.LogInformation("Overall Pipeline Pressure Level: {Pressure:F2}", pipelineStatus.OverallPressureLevel);

        foreach (var (stageName, stageStatus) in pipelineStatus.StageStatuses)
        {
            logger.LogInformation("Stage {Stage}: Pressure {Pressure:F2}, Queue Utilization {QueueUtil:F2}, Latency {Latency:F2}ms, Error Rate {ErrorRate:F2}",
                stageName,
                stageStatus.BackPressureLevel,
                stageStatus.QueueUtilization,
                stageStatus.ProcessingLatencyMs,
                stageStatus.ErrorRate);
        }

        var healthStatus = pipelineStatus.OverallPressureLevel switch
        {
            < 0.3 => "✅ HEALTHY",
            < 0.6 => "⚠️ MODERATE PRESSURE",
            < 0.8 => "🔶 HIGH PRESSURE",
            _ => "🔴 CRITICAL PRESSURE"
        };

        logger.LogInformation("Pipeline Health Status: {Status}", healthStatus);
    }
}

// Example implementations for demonstration

public class ExampleSource : ISourceFunction<string>
{
    private readonly int _recordCount;
    public ITypeSerializer<string> Serializer { get; } = new StringSerializer();

    public ExampleSource(int recordCount)
    {
        _recordCount = recordCount;
    }

    public void Run(ISourceContext<string> ctx)
    {
        for (int i = 0; i < _recordCount; i++)
        {
            var record = $"{{\"id\":{i},\"data\":\"example-data-{i}\",\"timestamp\":{DateTimeOffset.UtcNow.ToUnixTimeMilliseconds()}}}";
            ctx.Collect(record);

            // Simulate realistic data rate
            if (i % 10 == 0)
            {
                Thread.Sleep(1); // Small delay to simulate realistic throughput
            }
        }
    }

    public async IAsyncEnumerable<string> GenerateRecordsAsync()
    {
        for (int i = 0; i < _recordCount; i++)
        {
            var record = $"{{\"id\":{i},\"data\":\"example-data-{i}\",\"timestamp\":{DateTimeOffset.UtcNow.ToUnixTimeMilliseconds()}}}";
            yield return record;

            // Simulate realistic data rate
            if (i % 10 == 0)
            {
                await Task.Delay(1);
            }
        }
    }

    public void Cancel() { }
}

public class ExampleValidator : IRecordValidator<string>
{
    public ValidationResult Validate(string record)
    {
        if (string.IsNullOrEmpty(record))
        {
            return new ValidationResult { IsValid = false, ErrorMessage = "Record is null or empty" };
        }

        if (record.Length < 10)
        {
            return new ValidationResult { IsValid = false, ErrorMessage = "Record too short" };
        }

        return new ValidationResult { IsValid = true };
    }
}

public class ExampleProcessor : IRecordProcessor<string>
{
    public string Process(string record)
    {
        // Simple preprocessing - add processing timestamp
        var processedRecord = record.TrimEnd('}') + $",\"processed_at\":\"{DateTime.UtcNow:yyyy-MM-ddTHH:mm:ss.fffZ}\"}}";
        return processedRecord;
    }
}

public class ExampleExternalService : IExternalService<string>
{
    public async Task<string> ProcessAsync(string record, CancellationToken cancellationToken)
    {
        // Simulate external service call
        await Task.Delay(Random.Shared.Next(10, 100), cancellationToken);

        // Simulate occasional failures (5% failure rate)
        if (Random.Shared.NextDouble() < 0.05)
        {
            throw new InvalidOperationException("Simulated external service failure");
        }

        return record + "_external_processed";
    }
}

public class ExampleKafkaDestination : IKafkaDestination<string>
{
    private readonly Dictionary<string, Action<string, bool, string?>> _callbacks = new();

    public void Initialize(Dictionary<string, object> configuration)
    {
        // Initialize Kafka producer configuration
    }

    public async Task<string?> SendAsync(string key, string value, bool requireAcknowledgment)
    {
        // Simulate Kafka send
        await Task.Delay(Random.Shared.Next(5, 50));

        if (requireAcknowledgment)
        {
            var ackId = Guid.NewGuid().ToString();
            
            // Simulate asynchronous acknowledgment
            _ = Task.Delay(Random.Shared.Next(100, 500)).ContinueWith(_ =>
            {
                // Simulate 95% success rate
                var success = Random.Shared.NextDouble() < 0.95;
                if (_callbacks.TryGetValue(ackId, out var callback))
                {
                    callback(ackId, success, success ? null : "Simulated Kafka error");
                    _callbacks.Remove(ackId);
                }
            });

            return ackId;
        }

        return null;
    }

    public void RegisterAcknowledgmentCallback(string acknowledgmentId, Action<string, bool, string?> callback)
    {
        _callbacks[acknowledgmentId] = callback;
    }

    public void Close() { }
    public void Dispose() { }
}

public class SimpleRuntimeContext : IRuntimeContext
{
    public string JobName { get; }
    public string TaskName { get; }
    public int IndexOfThisSubtask => 0;
    public int NumberOfParallelSubtasks => 1;
    public FlinkDotNet.Core.Abstractions.Models.JobConfiguration JobConfiguration { get; }
    public FlinkDotNet.Core.Abstractions.Storage.IStateSnapshotStore StateSnapshotStore { get; }

    private object? _currentKey;

    public SimpleRuntimeContext(string taskName)
    {
        JobName = "FlinkDotnet 2.0 Pipeline Example";
        TaskName = taskName;
        JobConfiguration = new FlinkDotNet.Core.Abstractions.Models.JobConfiguration();
        StateSnapshotStore = new SimpleStateSnapshotStore();
    }

    public object? GetCurrentKey() => _currentKey;
    public void SetCurrentKey(object? key) => _currentKey = key;

    public FlinkDotNet.Core.Abstractions.States.IValueState<T> GetValueState<T>(FlinkDotNet.Core.Abstractions.Models.State.ValueStateDescriptor<T> stateDescriptor)
    {
        return new SimpleValueState<T>();
    }

    public FlinkDotNet.Core.Abstractions.States.IListState<T> GetListState<T>(FlinkDotNet.Core.Abstractions.Models.State.ListStateDescriptor<T> stateDescriptor)
    {
        return new SimpleListState<T>();
    }

    public FlinkDotNet.Core.Abstractions.States.IMapState<TK, TV> GetMapState<TK, TV>(FlinkDotNet.Core.Abstractions.Models.State.MapStateDescriptor<TK, TV> stateDescriptor) where TK : notnull
    {
        return new SimpleMapState<TK, TV>();
    }
}

public class SimpleSinkContext : ISinkContext
{
    public long CurrentProcessingTimeMillis() => DateTimeOffset.UtcNow.ToUnixTimeMilliseconds();
}

// Simple state implementations
public class SimpleValueState<T> : FlinkDotNet.Core.Abstractions.States.IValueState<T>
{
    private T? _value;
    public T? Value() => _value;
    public void Update(T? value) => _value = value;
    public void Clear() => _value = default;
}

public class SimpleListState<T> : FlinkDotNet.Core.Abstractions.States.IListState<T>
{
    private readonly List<T> _list = new();
    public IEnumerable<T> GetValues() => _list;
    public IEnumerable<T> Get() => _list;
    public void Add(T value) => _list.Add(value);
    public void Update(IEnumerable<T> values) { _list.Clear(); _list.AddRange(values); }
    public void AddAll(IEnumerable<T> values) => _list.AddRange(values);
    public void Clear() => _list.Clear();
}

public class SimpleMapState<TK, TV> : FlinkDotNet.Core.Abstractions.States.IMapState<TK, TV> where TK : notnull
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

public class SimpleStateSnapshotStore : FlinkDotNet.Core.Abstractions.Storage.IStateSnapshotStore
{
    public Task<FlinkDotNet.Core.Abstractions.Storage.SnapshotHandle> StoreSnapshot(string jobId, long checkpointId, string taskManagerId, string operatorId, byte[] snapshotData)
    {
        return Task.FromResult(new FlinkDotNet.Core.Abstractions.Storage.SnapshotHandle($"test-{jobId}-{checkpointId}"));
    }

    public Task<byte[]?> RetrieveSnapshot(FlinkDotNet.Core.Abstractions.Storage.SnapshotHandle handle)
    {
        return Task.FromResult<byte[]?>(null);
    }
}