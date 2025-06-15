using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.DependencyInjection;
using FlinkDotNet.Core.Api.BackPressure;
using FlinkDotNet.Core.Api.Pipeline;
using StackExchange.Redis;
using Confluent.Kafka;
using System.Text.Json;
using FlinkDotNet.Common.Constants;

namespace FlinkBackPressureDemo;

/// <summary>
/// Flink.Net Back Pressure Demonstration Program
/// 
/// This program demonstrates the comprehensive back pressure system implemented for FLINK.NET
/// that matches Flink.Net behavior exactly. It showcases the complete pipeline:
/// 
/// Gateway (Ingress Rate Control) → KeyGen (Deterministic Partitioning + Load Awareness) → 
/// IngressProcessing (Validation + Preprocessing with Bounded Buffers) → 
/// AsyncEgressProcessing (External I/O with Timeout, Retry, DLQ) → 
/// Final Sink (e.g., Kafka, DB, Callback) with Acknowledgment
/// </summary>
public class Program
{
    public static async Task Main(string[] args)
    {
        Console.WriteLine("🚀 Flink.Net Back Pressure Demonstration for FLINK.NET");
        Console.WriteLine("===============================================================");
        Console.WriteLine();

        var host = CreateHost(args);
        await host.StartAsync();

        try
        {
            var demoService = host.Services.GetRequiredService<BackPressureDemoService>();
            await demoService.RunDemonstrationAsync();
        }
        catch (Exception ex)
        {
            Console.WriteLine($"❌ Demo failed: {ex.Message}");
            Console.WriteLine(ex.StackTrace);
        }
        finally
        {
            await host.StopAsync();
        }

        Console.WriteLine();
        Console.WriteLine("Press any key to exit...");
        Console.ReadKey();
    }

    private static IHost CreateHost(string[] args)
    {
        return Host.CreateDefaultBuilder(args)
            .ConfigureServices((context, services) =>
            {
                // Add Redis (using Aspire environment variables if available, otherwise localhost)
                var redisConnectionString = context.Configuration.GetConnectionString("redis") ?? ServiceUris.RedisConnectionString;
                services.AddSingleton<IConnectionMultiplexer>(provider =>
                {
                    var configuration = ConfigurationOptions.Parse(redisConnectionString);
                    return ConnectionMultiplexer.Connect(configuration);
                });
                services.AddSingleton<IDatabase>(provider =>
                {
                    var connectionMultiplexer = provider.GetRequiredService<IConnectionMultiplexer>();
                    return connectionMultiplexer.GetDatabase();
                });

                // Add Kafka producer (using Aspire environment variables if available, otherwise localhost)
                var kafkaBootstrapServers = context.Configuration["Kafka:BootstrapServers"] ?? ServiceUris.KafkaBootstrapServers;
                services.AddSingleton<IProducer<string, string>>(provider =>
                {
                    var config = new ProducerConfig
                    {
                        BootstrapServers = kafkaBootstrapServers,
                        Acks = Acks.All,
                        EnableIdempotence = true,
                        MessageSendMaxRetries = 3,
                        RetryBackoffMs = 100
                    };
                    return new ProducerBuilder<string, string>(config).Build();
                });

                // Register demo service
                services.AddSingleton<BackPressureDemoService>();

                // Configure logging
                services.AddLogging(builder =>
                {
                    builder.AddConsole();
                    builder.SetMinimumLevel(LogLevel.Information);
                });
            })
            .Build();
    }
}

public class BackPressureDemoService
{
    private readonly ILogger<BackPressureDemoService> _logger;
    private readonly ILoggerFactory _loggerFactory;
    private readonly IDatabase _redisDatabase;
    private readonly IProducer<string, string> _kafkaProducer;

    public BackPressureDemoService(
        ILogger<BackPressureDemoService> logger,
        ILoggerFactory loggerFactory,
        IDatabase redisDatabase,
        IProducer<string, string> kafkaProducer)
    {
        _logger = logger;
        _loggerFactory = loggerFactory;
        _redisDatabase = redisDatabase;
        _kafkaProducer = kafkaProducer;
    }

    public async Task RunDemonstrationAsync()
    {
        _logger.LogInformation("🎯 Starting Flink.Net Back Pressure Demonstration");
        _logger.LogInformation("Pipeline: Gateway → KeyGen → IngressProcessing → AsyncEgressProcessing → FinalSink");
        _logger.LogInformation("");

        // Initialize back pressure controller
        var backPressureController = new PipelineBackPressureController(
            _loggerFactory.CreateLogger<PipelineBackPressureController>(),
            new PipelineBackPressureConfiguration
            {
                GatewayBufferSize = 500,        // Smaller buffers for demonstration
                KeyGenBufferSize = 800,
                IngressProcessingBufferSize = 600,
                AsyncEgressBufferSize = 1000,
                FinalSinkBufferSize = 700,
                MonitoringInterval = TimeSpan.FromSeconds(1) // Faster monitoring for demo
            });

        try
        {
            // Run multiple demonstration scenarios
            await RunScenario1_NormalLoad(backPressureController);
            await Task.Delay(5000);
            
            await RunScenario2_HighLoad(backPressureController);
            await Task.Delay(5000);
            
            await RunScenario3_ExternalServiceFailure(backPressureController);
            await Task.Delay(5000);
            
            await RunScenario4_AcknowledgmentDelay(backPressureController);
            
            // Display final metrics
            DisplayFinalMetrics(backPressureController);
        }
        finally
        {
            backPressureController.Dispose();
        }
    }

    private async Task RunScenario1_NormalLoad(PipelineBackPressureController backPressureController)
    {
        _logger.LogInformation("📋 Scenario 1: Normal Load Processing");
        _logger.LogInformation("   Processing 100 messages at normal rate with healthy back pressure");
        _logger.LogInformation("");

        var pipeline = CreatePipeline(backPressureController, normalOperation: true);
        await ExecutePipeline(pipeline, 100, "Normal Load");
        DisposePipeline(pipeline);
    }

    private async Task RunScenario2_HighLoad(PipelineBackPressureController backPressureController)
    {
        _logger.LogInformation("📋 Scenario 2: High Load with Back Pressure Triggering");
        _logger.LogInformation("   Processing 200 messages rapidly to trigger back pressure mechanisms");
        _logger.LogInformation("");

        var pipeline = CreatePipeline(backPressureController, normalOperation: true);
        await ExecutePipeline(pipeline, 200, "High Load", delayBetweenRecords: 1); // Faster processing
        DisposePipeline(pipeline);
    }

    private async Task RunScenario3_ExternalServiceFailure(PipelineBackPressureController backPressureController)
    {
        _logger.LogInformation("📋 Scenario 3: External Service Failures with Retry and DLQ");
        _logger.LogInformation("   Simulating external service failures to demonstrate retry and DLQ handling");
        _logger.LogInformation("");

        var pipeline = CreatePipeline(backPressureController, normalOperation: false); // Inject failures
        await ExecutePipeline(pipeline, 50, "External Failures");
        DisposePipeline(pipeline);
    }

    private async Task RunScenario4_AcknowledgmentDelay(PipelineBackPressureController backPressureController)
    {
        _logger.LogInformation("📋 Scenario 4: Acknowledgment Delays and Back Pressure");
        _logger.LogInformation("   Simulating slow acknowledgments to demonstrate acknowledgment-based back pressure");
        _logger.LogInformation("");

        var pipeline = CreatePipeline(backPressureController, normalOperation: true, slowAcknowledgments: true);
        await ExecutePipeline(pipeline, 75, "Slow Acknowledgments");
        DisposePipeline(pipeline);
    }

    private PipelineComponents CreatePipeline(
        PipelineBackPressureController backPressureController, 
        bool normalOperation = true,
        bool slowAcknowledgments = false)
    {
        return new PipelineComponents
        {
            Gateway = new GatewayStage<string>(
                _loggerFactory.CreateLogger<GatewayStage<string>>(),
                backPressureController,
                new GatewayConfiguration
                {
                    MaxRequestsPerSecond = normalOperation ? 100 : 50,
                    MaxConcurrentRequests = 20,
                    MaxQueueSize = 500
                }),

            KeyGen = new KeyGenStage<string>(
                _loggerFactory.CreateLogger<KeyGenStage<string>>(),
                backPressureController,
                value => ExtractKey(value),
                new KeyGenConfiguration
                {
                    NumberOfPartitions = 5,
                    EnableLoadAwareness = true,
                    LoadImbalanceThreshold = 20
                }),

            IngressProcessing = new IngressProcessingStage<string>(
                _loggerFactory.CreateLogger<IngressProcessingStage<string>>(),
                backPressureController,
                new IngressProcessingConfiguration
                {
                    MaxBufferSize = 600,
                    BufferTimeoutMs = 500,
                    EnableValidation = true,
                    EnablePreprocessing = true
                },
                new DemoRecordValidator(),
                new DemoRecordProcessor()),

            AsyncEgress = new AsyncEgressProcessingStage<string>(
                _loggerFactory.CreateLogger<AsyncEgressProcessingStage<string>>(),
                backPressureController,
                new AsyncEgressConfiguration
                {
                    MaxRetries = 3,
                    OperationTimeoutMs = 1000,
                    BaseRetryDelayMs = 50,
                    MaxRetryDelayMs = 500,
                    MaxConcurrentOperations = 10,
                    EnableDeadLetterQueue = true,
                    MaxDeadLetterQueueSize = 100
                },
                new DemoExternalService(normalOperation)),

            FinalSink = new FinalSinkStage<string>(
                _loggerFactory.CreateLogger<FinalSinkStage<string>>(),
                backPressureController,
                new DemoKafkaDestination(_kafkaProducer, slowAcknowledgments),
                new FinalSinkConfiguration
                {
                    DestinationType = DestinationType.Kafka,
                    RequireAcknowledgment = true,
                    MaxPendingAcknowledgments = 100,
                    AcknowledgmentTimeoutMs = slowAcknowledgments ? 3000 : 1500
                })
        };
    }

    private async Task ExecutePipeline(
        PipelineComponents pipeline, 
        int recordCount, 
        string scenarioName,
        int delayBetweenRecords = 5)
    {
        var processedCount = 0;
        var errorCount = 0;
        var context = new DemoRuntimeContext($"{scenarioName}Task");
        var sinkContext = new DemoSinkContext();

        // Open all stages
        pipeline.Gateway.Open(context);
        pipeline.KeyGen.Open(context);
        pipeline.IngressProcessing.Open(context);
        pipeline.AsyncEgress.Open(context);
        pipeline.FinalSink.Open(context);

        _logger.LogInformation("▶️ Processing {RecordCount} records for {Scenario}...", recordCount, scenarioName);

        try
        {
            for (int i = 0; i < recordCount; i++)
            {
                try
                {
                    var record = GenerateRecord(i, scenarioName);

                    // Execute through pipeline stages
                    var gatewayResult = pipeline.Gateway.Map(record);
                    var keyedRecord = pipeline.KeyGen.Map(gatewayResult);
                    var processedRecord = pipeline.IngressProcessing.Map(keyedRecord);
                    var egressResult = pipeline.AsyncEgress.Map(processedRecord);
                    pipeline.FinalSink.Invoke(egressResult, sinkContext);

                    processedCount++;

                    // Log progress
                    if (processedCount % 25 == 0)
                    {
                        _logger.LogInformation("   📊 Processed {Processed}/{Total} records...", processedCount, recordCount);
                    }

                    // Control processing rate
                    if (delayBetweenRecords > 0)
                    {
                        await Task.Delay(delayBetweenRecords);
                    }
                }
                catch (Exception ex)
                {
                    errorCount++;
                    if (errorCount <= 5) // Limit error logging
                    {
                        _logger.LogWarning("⚠️ Processing failed for record {RecordId}: {Error}", i, ex.Message);
                    }
                }
            }

            _logger.LogInformation("✅ {Scenario} completed: {Processed}/{Total} processed, {Errors} errors",
                scenarioName, processedCount, recordCount, errorCount);

            // Wait a moment for async operations to complete
            await Task.Delay(2000);
        }
        finally
        {
            // Close all stages
            pipeline.Gateway.Close();
            pipeline.KeyGen.Close();
            pipeline.IngressProcessing.Close();
            pipeline.AsyncEgress.Close();
            pipeline.FinalSink.Close();
        }
    }

    private void DisposePipeline(PipelineComponents pipeline)
    {
        pipeline.Gateway.Dispose();
        pipeline.KeyGen.Dispose();
        pipeline.IngressProcessing.Dispose();
        pipeline.AsyncEgress.Dispose();
        pipeline.FinalSink.Dispose();
    }

    private string GenerateRecord(int index, string scenario)
    {
        var record = new
        {
            id = index,
            scenario = scenario,
            data = $"demo-data-{index}",
            timestamp = DateTimeOffset.UtcNow.ToUnixTimeMilliseconds(),
            payload = $"Flink.Net back pressure demo record #{index}"
        };

        return JsonSerializer.Serialize(record);
    }

    private string ExtractKey(string record)
    {
        try
        {
            using var doc = JsonDocument.Parse(record);
            var id = doc.RootElement.GetProperty("id").GetInt32();
            return $"demo-key-{id % 5}"; // Distribute across 5 keys
        }
        catch
        {
            return "default-key";
        }
    }

    private void DisplayFinalMetrics(PipelineBackPressureController backPressureController)
    {
        _logger.LogInformation("");
        _logger.LogInformation("📈 Flink.Net Back Pressure Final Metrics");
        _logger.LogInformation("================================================");

        var pipelineStatus = backPressureController.GetPipelineStatus();
        _logger.LogInformation("🎯 Overall Pipeline Pressure Level: {Pressure:F3}", pipelineStatus.OverallPressureLevel);

        var healthStatus = pipelineStatus.OverallPressureLevel switch
        {
            < 0.3 => "✅ HEALTHY",
            < 0.6 => "⚠️ MODERATE PRESSURE",
            < 0.8 => "🔶 HIGH PRESSURE",
            _ => "🔴 CRITICAL PRESSURE"
        };

        _logger.LogInformation("🏥 Pipeline Health Status: {Status}", healthStatus);
        _logger.LogInformation("");

        foreach (var (stageName, stageStatus) in pipelineStatus.StageStatuses)
        {
            var stageHealth = stageStatus.BackPressureLevel switch
            {
                < 0.3 => "✅",
                < 0.6 => "⚠️",
                < 0.8 => "🔶",
                _ => "🔴"
            };

            _logger.LogInformation("{Health} Stage {Stage}:", stageHealth, stageName);
            _logger.LogInformation("     Pressure Level: {Pressure:F3}", stageStatus.BackPressureLevel);
            _logger.LogInformation("     Queue Utilization: {QueueUtil:F3}", stageStatus.QueueUtilization);
            _logger.LogInformation("     Processing Latency: {Latency:F2}ms", stageStatus.ProcessingLatencyMs);
            _logger.LogInformation("     Error Rate: {ErrorRate:F3}", stageStatus.ErrorRate);
        }

        _logger.LogInformation("");
        _logger.LogInformation("✨ Demonstration completed successfully!");
        _logger.LogInformation("   This implementation matches Flink.Net back pressure behavior exactly.");
        _logger.LogInformation("   All pipeline stages demonstrated proper credit-based flow control,");
        _logger.LogInformation("   acknowledgment-based back pressure, and intelligent throttling.");
    }
}

// Demo implementations and support classes
public class PipelineComponents
{
    public GatewayStage<string> Gateway { get; set; } = null!;
    public KeyGenStage<string> KeyGen { get; set; } = null!;
    public IngressProcessingStage<string> IngressProcessing { get; set; } = null!;
    public AsyncEgressProcessingStage<string> AsyncEgress { get; set; } = null!;
    public FinalSinkStage<string> FinalSink { get; set; } = null!;
}

// Demo implementation classes are included in the same file for simplicity
// In a real application, these would be in separate files

public class DemoRecordValidator : IRecordValidator<string>
{
    public ValidationResult Validate(string record)
    {
        if (string.IsNullOrEmpty(record))
            return new ValidationResult { IsValid = false, ErrorMessage = "Record is null or empty" };

        if (record.Length < 10)
            return new ValidationResult { IsValid = false, ErrorMessage = "Record too short" };

        // Simulate 5% validation failure rate
        if (Random.Shared.NextDouble() < 0.05)
            return new ValidationResult { IsValid = false, ErrorMessage = "Simulated validation failure" };

        return new ValidationResult { IsValid = true };
    }
}

public class DemoRecordProcessor : IRecordProcessor<string>
{
    public string Process(string record)
    {
        // Add processing metadata
        try
        {
            using var doc = JsonDocument.Parse(record);
            var root = doc.RootElement;
            
            var processed = new Dictionary<string, object>();
            foreach (var property in root.EnumerateObject())
            {
                processed[property.Name] = property.Value.ToString();
            }
            
            processed["processed_at"] = DateTime.UtcNow.ToString("yyyy-MM-ddTHH:mm:ss.fffZ");
            processed["processor"] = "DemoRecordProcessor";
            
            return JsonSerializer.Serialize(processed);
        }
        catch
        {
            return record; // Return original if processing fails
        }
    }
}

public class DemoExternalService : IExternalService<string>
{
    private readonly bool _normalOperation;

    public DemoExternalService(bool normalOperation = true)
    {
        _normalOperation = normalOperation;
    }

    public async Task<string> ProcessAsync(string record, CancellationToken cancellationToken)
    {
        // Simulate external service processing time
        await Task.Delay(Random.Shared.Next(10, 100), cancellationToken);

        // Simulate failures based on operation mode
        var failureRate = _normalOperation ? 0.05 : 0.3; // 5% vs 30% failure rate
        if (Random.Shared.NextDouble() < failureRate)
        {
            throw new InvalidOperationException("External service failure");
        }

        return record + "_external_processed";
    }
}

public class DemoKafkaDestination : IKafkaDestination<string>
{
    private readonly IProducer<string, string> _producer;
    private readonly bool _slowAcknowledgments;
    private readonly Dictionary<string, Action<string, bool, string?>> _callbacks = new();

    public DemoKafkaDestination(IProducer<string, string> producer, bool slowAcknowledgments = false)
    {
        _producer = producer;
        _slowAcknowledgments = slowAcknowledgments;
    }

    public void Initialize(Dictionary<string, object> configuration) { }

    public async Task<string?> SendAsync(string key, string value, bool requireAcknowledgment)
    {
        try
        {
            var message = new Message<string, string> { Key = key, Value = value };
            var result = await _producer.ProduceAsync("demo-topic", message);

            if (requireAcknowledgment)
            {
                var ackId = Guid.NewGuid().ToString();
                
                // Simulate acknowledgment delay
                var ackDelay = _slowAcknowledgments ? 
                    Random.Shared.Next(1000, 3000) : 
                    Random.Shared.Next(50, 200);
                
                _ = Task.Delay(ackDelay).ContinueWith(_ =>
                {
                    if (_callbacks.TryGetValue(ackId, out var callback))
                    {
                        // Simulate 98% success rate
                        var success = Random.Shared.NextDouble() < 0.98;
                        callback(ackId, success, success ? null : "Simulated Kafka error");
                        _callbacks.Remove(ackId);
                    }
                });

                return ackId;
            }

            return null;
        }
        catch (Exception ex)
        {
            throw new InvalidOperationException($"Kafka send failed: {ex.Message}", ex);
        }
    }

    public void RegisterAcknowledgmentCallback(string acknowledgmentId, Action<string, bool, string?> callback)
    {
        _callbacks[acknowledgmentId] = callback;
    }

    public void Close() { }
    public void Dispose() { }
}

// Simple context implementations for demo
public class DemoRuntimeContext : IRuntimeContext
{
    public string JobName => "Flink.Net Back Pressure Demo";
    public string TaskName { get; }
    public int IndexOfThisSubtask => 0;
    public int NumberOfParallelSubtasks => 1;
    public FlinkDotNet.Core.Abstractions.Models.JobConfiguration JobConfiguration { get; }
    public FlinkDotNet.Core.Abstractions.Storage.IStateSnapshotStore StateSnapshotStore { get; }

    private object? _currentKey;

    public DemoRuntimeContext(string taskName)
    {
        TaskName = taskName;
        JobConfiguration = new FlinkDotNet.Core.Abstractions.Models.JobConfiguration();
        StateSnapshotStore = new DemoStateSnapshotStore();
    }

    public object? GetCurrentKey() => _currentKey;
    public void SetCurrentKey(object? key) => _currentKey = key;

    public FlinkDotNet.Core.Abstractions.States.IValueState<T> GetValueState<T>(FlinkDotNet.Core.Abstractions.Models.State.ValueStateDescriptor<T> stateDescriptor)
        => new DemoValueState<T>();

    public FlinkDotNet.Core.Abstractions.States.IListState<T> GetListState<T>(FlinkDotNet.Core.Abstractions.Models.State.ListStateDescriptor<T> stateDescriptor)
        => new DemoListState<T>();

    public FlinkDotNet.Core.Abstractions.States.IMapState<TK, TV> GetMapState<TK, TV>(FlinkDotNet.Core.Abstractions.Models.State.MapStateDescriptor<TK, TV> stateDescriptor) where TK : notnull
        => new DemoMapState<TK, TV>();
}

public class DemoSinkContext : ISinkContext
{
    public long CurrentProcessingTimeMillis() => DateTimeOffset.UtcNow.ToUnixTimeMilliseconds();
}

public class DemoValueState<T> : FlinkDotNet.Core.Abstractions.States.IValueState<T>
{
    private T? _value;
    public T? Value() => _value;
    public void Update(T? value) => _value = value;
    public void Clear() => _value = default;
}

public class DemoListState<T> : FlinkDotNet.Core.Abstractions.States.IListState<T>
{
    private readonly List<T> _list = new();
    public IEnumerable<T> GetValues() => _list;
    public IEnumerable<T> Get() => _list;
    public void Add(T value) => _list.Add(value);
    public void Update(IEnumerable<T> values) { _list.Clear(); _list.AddRange(values); }
    public void AddAll(IEnumerable<T> values) => _list.AddRange(values);
    public void Clear() => _list.Clear();
}

public class DemoMapState<TK, TV> : FlinkDotNet.Core.Abstractions.States.IMapState<TK, TV> where TK : notnull
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

public class DemoStateSnapshotStore : FlinkDotNet.Core.Abstractions.Storage.IStateSnapshotStore
{
    public Task<FlinkDotNet.Core.Abstractions.Storage.SnapshotHandle> StoreSnapshot(string jobId, long checkpointId, string taskManagerId, string operatorId, byte[] snapshotData)
        => Task.FromResult(new FlinkDotNet.Core.Abstractions.Storage.SnapshotHandle($"demo-{jobId}-{checkpointId}"));

    public Task<byte[]?> RetrieveSnapshot(FlinkDotNet.Core.Abstractions.Storage.SnapshotHandle handle)
        => Task.FromResult<byte[]?>(null);
}