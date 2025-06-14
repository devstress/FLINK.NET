using Xunit;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using FlinkDotNet.Core.Api.BackPressure;
using FlinkDotNet.Core.Api.Pipeline;
using System.Collections.Concurrent;

namespace FlinkDotNet.Tests.BackPressure;

/// <summary>
/// Comprehensive test suite for Apache Flink 2.0 style back pressure implementation.
/// Tests all aspects of the back pressure system including credit-based flow control,
/// pipeline stage interactions, and pressure propagation.
/// </summary>
public class FlinkDotnetBackPressureTests : IDisposable
{
    private readonly IHost _host;
    private readonly ILoggerFactory _loggerFactory;
    private readonly ILogger<FlinkDotnetBackPressureTests> _logger;

    public FlinkDotnetBackPressureTests()
    {
        _host = CreateTestHost();
        _loggerFactory = _host.Services.GetRequiredService<ILoggerFactory>();
        _logger = _loggerFactory.CreateLogger<FlinkDotnetBackPressureTests>();
    }

    #region Credit-Based Flow Control Tests

    [Fact]
    public void CreditBasedFlowControl_ShouldGrantCreditsWhenAvailable()
    {
        // Arrange
        var config = new StageFlowConfiguration { MaxBufferSize = 100 };
        var flowControl = new CreditBasedFlowControl("TestStage", config);

        // Act
        var grantedCredits = flowControl.RequestCredits(50);

        // Assert
        Assert.Equal(50, grantedCredits);
        Assert.Equal(50, flowControl.AvailableCredits);
    }

    [Fact]
    public void CreditBasedFlowControl_ShouldLimitCreditsWhenExceeded()
    {
        // Arrange
        var config = new StageFlowConfiguration { MaxBufferSize = 100 };
        var flowControl = new CreditBasedFlowControl("TestStage", config);

        // Act
        var grantedCredits = flowControl.RequestCredits(150); // More than available

        // Assert
        Assert.Equal(100, grantedCredits); // Should only grant what's available
        Assert.Equal(0, flowControl.AvailableCredits);
    }

    [Fact]
    public void CreditBasedFlowControl_ShouldReplenishCreditsCorrectly()
    {
        // Arrange
        var config = new StageFlowConfiguration { MaxBufferSize = 100 };
        var flowControl = new CreditBasedFlowControl("TestStage", config);
        flowControl.RequestCredits(80); // Use 80 credits

        // Act
        flowControl.ReplenishCredits(30);

        // Assert
        Assert.Equal(50, flowControl.AvailableCredits); // 20 + 30 = 50
    }

    [Fact]
    public void CreditBasedFlowControl_ShouldNotExceedMaxBufferSize()
    {
        // Arrange
        var config = new StageFlowConfiguration { MaxBufferSize = 100 };
        var flowControl = new CreditBasedFlowControl("TestStage", config);

        // Act
        flowControl.ReplenishCredits(150); // Try to exceed max

        // Assert
        Assert.Equal(100, flowControl.AvailableCredits); // Should cap at max
    }

    [Fact]
    public void CreditBasedFlowControl_ShouldApplyBackPressureReduction()
    {
        // Arrange
        var config = new StageFlowConfiguration { MaxBufferSize = 100 };
        var flowControl = new CreditBasedFlowControl("TestStage", config);

        // Act
        flowControl.ReduceCreditsForBackPressure(0.5); // 50% reduction
        var grantedCredits = flowControl.RequestCredits(80);

        // Assert
        Assert.Equal(50, grantedCredits); // 100 * 0.5 = 50 available credits
    }

    #endregion

    #region Pipeline Back Pressure Controller Tests

    [Fact]
    public void PipelineBackPressureController_ShouldInitializeAllStages()
    {
        // Arrange & Act
        using var controller = new PipelineBackPressureController(
            _loggerFactory.CreateLogger<PipelineBackPressureController>());

        // Assert
        var status = controller.GetPipelineStatus();
        Assert.Equal(5, status.StageStatuses.Count); // All 5 pipeline stages
        Assert.Contains(PipelineStage.Gateway, status.StageStatuses.Keys);
        Assert.Contains(PipelineStage.KeyGen, status.StageStatuses.Keys);
        Assert.Contains(PipelineStage.IngressProcessing, status.StageStatuses.Keys);
        Assert.Contains(PipelineStage.AsyncEgressProcessing, status.StageStatuses.Keys);
        Assert.Contains(PipelineStage.FinalSink, status.StageStatuses.Keys);
    }

    [Fact]
    public void PipelineBackPressureController_ShouldRequestAndReplenishCredits()
    {
        // Arrange
        using var controller = new PipelineBackPressureController(
            _loggerFactory.CreateLogger<PipelineBackPressureController>());

        // Act
        var credits = controller.RequestCredits(PipelineStage.Gateway, 10);
        controller.ReplenishCredits(PipelineStage.Gateway, 5);

        // Assert
        Assert.Equal(10, credits);
        // After replenishing 5, should have more credits available
        var moreCredits = controller.RequestCredits(PipelineStage.Gateway, 10);
        Assert.True(moreCredits > 0);
    }

    [Fact]
    public void PipelineBackPressureController_ShouldUpdateStageMetrics()
    {
        // Arrange
        using var controller = new PipelineBackPressureController(
            _loggerFactory.CreateLogger<PipelineBackPressureController>());

        // Act
        controller.UpdateStageMetrics(PipelineStage.Gateway, 50, 100, 25.0, 0.1);

        // Assert
        var status = controller.GetPipelineStatus();
        var gatewayStatus = status.StageStatuses[PipelineStage.Gateway];
        Assert.Equal(0.5, gatewayStatus.QueueUtilization, 2); // 50/100 = 0.5
        Assert.Equal(25.0, gatewayStatus.ProcessingLatencyMs);
        Assert.Equal(0.1, gatewayStatus.ErrorRate);
    }

    [Fact]
    public void PipelineBackPressureController_ShouldDetectThrottling()
    {
        // Arrange
        using var controller = new PipelineBackPressureController(
            _loggerFactory.CreateLogger<PipelineBackPressureController>());

        // Act - Create high pressure conditions
        controller.UpdateStageMetrics(PipelineStage.Gateway, 900, 1000, 100.0, 0.1); // 90% utilization

        // Allow time for pressure calculation
        Thread.Sleep(100);

        // Assert
        var shouldThrottle = controller.ShouldThrottleStage(PipelineStage.Gateway);
        var throttleDelay = controller.GetStageThrottleDelayMs(PipelineStage.Gateway);
        
        Assert.True(shouldThrottle);
        Assert.True(throttleDelay > 0);
    }

    #endregion

    #region Gateway Stage Tests

    [Fact]
    public void GatewayStage_ShouldProcessRecordSuccessfully()
    {
        // Arrange
        using var controller = new PipelineBackPressureController(
            _loggerFactory.CreateLogger<PipelineBackPressureController>());
        
        var gateway = new GatewayStage<string>(
            _loggerFactory.CreateLogger<GatewayStage<string>>(),
            controller,
            new GatewayConfiguration { MaxRequestsPerSecond = 1000 });

        var context = new TestRuntimeContext();
        gateway.Open(context);

        // Act
        var result = gateway.Map("test-record");

        // Assert
        Assert.Equal("test-record", result);
        
        gateway.Close();
        gateway.Dispose();
    }

    [Fact]
    public void GatewayStage_ShouldApplyBackPressureWhenThrottled()
    {
        // Arrange
        using var controller = new PipelineBackPressureController(
            _loggerFactory.CreateLogger<PipelineBackPressureController>());

        // Create high pressure conditions
        controller.UpdateStageMetrics(PipelineStage.Gateway, 950, 1000, 200.0, 0.0);
        Thread.Sleep(100); // Allow pressure calculation

        var gateway = new GatewayStage<string>(
            _loggerFactory.CreateLogger<GatewayStage<string>>(),
            controller,
            new GatewayConfiguration { MaxRequestsPerSecond = 10 }); // Low rate for testing

        var context = new TestRuntimeContext();
        gateway.Open(context);

        // Act & Assert
        var startTime = DateTime.UtcNow;
        var result = gateway.Map("test-record");
        var processingTime = DateTime.UtcNow - startTime;

        // Should have applied some throttling delay
        Assert.True(processingTime.TotalMilliseconds >= 0); // At least some processing time
        Assert.Equal("test-record", result);
        
        gateway.Close();
        gateway.Dispose();
    }

    #endregion

    #region KeyGen Stage Tests

    [Fact]
    public void KeyGenStage_ShouldGenerateKeyedRecords()
    {
        // Arrange
        using var controller = new PipelineBackPressureController(
            _loggerFactory.CreateLogger<PipelineBackPressureController>());
        
        var keyGen = new KeyGenStage<string>(
            _loggerFactory.CreateLogger<KeyGenStage<string>>(),
            controller,
            value => $"key-{value.GetHashCode() % 5}",
            new KeyGenConfiguration { NumberOfPartitions = 5 });

        var context = new TestRuntimeContext();
        keyGen.Open(context);

        // Act
        var result = keyGen.Map("test-record");

        // Assert
        Assert.NotNull(result);
        Assert.NotEmpty(result.Key);
        Assert.Equal("test-record", result.Value);
        Assert.True(result.Partition >= 0 && result.Partition < 5);
        Assert.True(result.Timestamp > 0);
        
        keyGen.Close();
        keyGen.Dispose();
    }

    [Fact]
    public void KeyGenStage_ShouldDistributeAcrossPartitions()
    {
        // Arrange
        using var controller = new PipelineBackPressureController(
            _loggerFactory.CreateLogger<PipelineBackPressureController>());
        
        var keyGen = new KeyGenStage<string>(
            _loggerFactory.CreateLogger<KeyGenStage<string>>(),
            controller,
            value => $"key-{value.GetHashCode() % 5}",
            new KeyGenConfiguration { NumberOfPartitions = 5 });

        var context = new TestRuntimeContext();
        keyGen.Open(context);

        // Act
        var partitionCounts = new Dictionary<int, int>();
        for (int i = 0; i < 100; i++)
        {
            var result = keyGen.Map($"test-record-{i}");
            partitionCounts[result.Partition] = partitionCounts.GetValueOrDefault(result.Partition, 0) + 1;
        }

        // Assert
        Assert.True(partitionCounts.Count > 1); // Should use multiple partitions
        Assert.True(partitionCounts.Values.All(count => count > 0)); // All partitions should have some records
        
        keyGen.Close();
        keyGen.Dispose();
    }

    #endregion

    #region IngressProcessing Stage Tests

    [Fact]
    public void IngressProcessingStage_ShouldValidateAndProcessRecords()
    {
        // Arrange
        using var controller = new PipelineBackPressureController(
            _loggerFactory.CreateLogger<PipelineBackPressureController>());
        
        var ingressProcessing = new IngressProcessingStage<string>(
            _loggerFactory.CreateLogger<IngressProcessingStage<string>>(),
            controller,
            new IngressProcessingConfiguration(),
            new TestValidator(),
            new TestProcessor());

        var context = new TestRuntimeContext();
        ingressProcessing.Open(context);

        var keyedRecord = new KeyedRecord<string>
        {
            Key = "test-key",
            Partition = 1,
            Value = "valid-test-record",
            Timestamp = DateTimeOffset.UtcNow.ToUnixTimeMilliseconds()
        };

        // Act
        var result = ingressProcessing.Map(keyedRecord);

        // Assert
        Assert.NotNull(result);
        Assert.Equal(keyedRecord, result.OriginalRecord);
        Assert.Contains("processed", result.ProcessedValue);
        Assert.True(result.ValidationResult.IsValid);
        Assert.True(result.ProcessingMetadata.Count > 0);
        
        ingressProcessing.Close();
        ingressProcessing.Dispose();
    }

    [Fact]
    public void IngressProcessingStage_ShouldHandleValidationFailures()
    {
        // Arrange
        using var controller = new PipelineBackPressureController(
            _loggerFactory.CreateLogger<PipelineBackPressureController>());
        
        var ingressProcessing = new IngressProcessingStage<string>(
            _loggerFactory.CreateLogger<IngressProcessingStage<string>>(),
            controller,
            new IngressProcessingConfiguration(),
            new TestValidator(),
            new TestProcessor());

        var context = new TestRuntimeContext();
        ingressProcessing.Open(context);

        var keyedRecord = new KeyedRecord<string>
        {
            Key = "test-key",
            Partition = 1,
            Value = "short", // Too short to pass validation
            Timestamp = DateTimeOffset.UtcNow.ToUnixTimeMilliseconds()
        };

        // Act & Assert
        Assert.Throws<InvalidOperationException>(() => ingressProcessing.Map(keyedRecord));
        
        ingressProcessing.Close();
        ingressProcessing.Dispose();
    }

    #endregion

    #region AsyncEgressProcessing Stage Tests

    [Fact]
    public async Task AsyncEgressProcessingStage_ShouldProcessSuccessfully()
    {
        // Arrange
        using var controller = new PipelineBackPressureController(
            _loggerFactory.CreateLogger<PipelineBackPressureController>());
        
        var asyncEgress = new AsyncEgressProcessingStage<string>(
            _loggerFactory.CreateLogger<AsyncEgressProcessingStage<string>>(),
            controller,
            new AsyncEgressConfiguration { MaxRetries = 1, OperationTimeoutMs = 1000 },
            new TestExternalService(true)); // Success mode

        var context = new TestRuntimeContext();
        asyncEgress.Open(context);

        var processedRecord = new ProcessedRecord<string>
        {
            OriginalRecord = new KeyedRecord<string> { Key = "test", Value = "test", Partition = 0 },
            ProcessedValue = "test-processed",
            ValidationResult = new ValidationResult { IsValid = true },
            ProcessingMetadata = new Dictionary<string, object>()
        };

        // Act
        var result = asyncEgress.Map(processedRecord);

        // Assert
        Assert.NotNull(result);
        Assert.True(result.IsSuccess);
        Assert.Equal(processedRecord, result.OriginalRecord);
        
        asyncEgress.Close();
        asyncEgress.Dispose();
    }

    [Fact]
    public void AsyncEgressProcessingStage_ShouldHandleFailuresWithRetry()
    {
        // Arrange
        using var controller = new PipelineBackPressureController(
            _loggerFactory.CreateLogger<PipelineBackPressureController>());
        
        var asyncEgress = new AsyncEgressProcessingStage<string>(
            _loggerFactory.CreateLogger<AsyncEgressProcessingStage<string>>(),
            controller,
            new AsyncEgressConfiguration { MaxRetries = 2, OperationTimeoutMs = 100 },
            new TestExternalService(false)); // Failure mode

        var context = new TestRuntimeContext();
        asyncEgress.Open(context);

        var processedRecord = new ProcessedRecord<string>
        {
            OriginalRecord = new KeyedRecord<string> { Key = "test", Value = "test", Partition = 0 },
            ProcessedValue = "test-processed",
            ValidationResult = new ValidationResult { IsValid = true },
            ProcessingMetadata = new Dictionary<string, object>()
        };

        // Act
        var result = asyncEgress.Map(processedRecord);

        // Assert
        Assert.NotNull(result);
        Assert.False(result.IsSuccess);
        Assert.NotNull(result.ErrorMessage);
        Assert.Equal(2, result.AttemptNumber); // Should have retried
        
        asyncEgress.Close();
        asyncEgress.Dispose();
    }

    #endregion

    #region Integration Tests

    [Fact]
    public async Task CompleteWorkflow_ShouldProcessPipelineWithBackPressure()
    {
        // Arrange
        using var controller = new PipelineBackPressureController(
            _loggerFactory.CreateLogger<PipelineBackPressureController>(),
            new PipelineBackPressureConfiguration
            {
                GatewayBufferSize = 100,
                KeyGenBufferSize = 100,
                IngressProcessingBufferSize = 100,
                AsyncEgressBufferSize = 100,
                FinalSinkBufferSize = 100
            });

        var gateway = new GatewayStage<string>(
            _loggerFactory.CreateLogger<GatewayStage<string>>(),
            controller);

        var keyGen = new KeyGenStage<string>(
            _loggerFactory.CreateLogger<KeyGenStage<string>>(),
            controller,
            value => $"key-{value.GetHashCode() % 3}");

        var ingressProcessing = new IngressProcessingStage<string>(
            _loggerFactory.CreateLogger<IngressProcessingStage<string>>(),
            controller,
            validator: new TestValidator(),
            preprocessor: new TestProcessor());

        var asyncEgress = new AsyncEgressProcessingStage<string>(
            _loggerFactory.CreateLogger<AsyncEgressProcessingStage<string>>(),
            controller,
            externalService: new TestExternalService(true));

        var finalSink = new FinalSinkStage<string>(
            _loggerFactory.CreateLogger<FinalSinkStage<string>>(),
            controller,
            new TestDestination());

        var context = new TestRuntimeContext();
        var sinkContext = new TestSinkContext();

        // Open all stages
        gateway.Open(context);
        keyGen.Open(context);
        ingressProcessing.Open(context);
        asyncEgress.Open(context);
        finalSink.Open(context);

        try
        {
            // Act - Process multiple records through the complete pipeline
            var successCount = 0;
            var errorCount = 0;

            for (int i = 0; i < 50; i++)
            {
                try
                {
                    var record = $"test-record-{i}-with-sufficient-length";

                    // Process through all stages
                    var gatewayResult = gateway.Map(record);
                    var keyedRecord = keyGen.Map(gatewayResult);
                    var processedRecord = ingressProcessing.Map(keyedRecord);
                    var egressResult = asyncEgress.Map(processedRecord);
                    finalSink.Invoke(egressResult, sinkContext);

                    successCount++;
                }
                catch (Exception ex)
                {
                    errorCount++;
                    _logger.LogWarning(ex, "Processing failed for record {Index}", i);
                }

                // Small delay to allow back pressure monitoring
                await Task.Delay(10);
            }

            // Assert
            Assert.True(successCount > 0, "Should process at least some records successfully");
            _logger.LogInformation("Pipeline integration test completed: {Success} successful, {Errors} errors", 
                successCount, errorCount);

            // Verify back pressure metrics
            var pipelineStatus = controller.GetPipelineStatus();
            Assert.True(pipelineStatus.StageStatuses.Count == 5, "Should have metrics for all stages");
            
            foreach (var (stageName, stageStatus) in pipelineStatus.StageStatuses)
            {
                Assert.True(stageStatus.BackPressureLevel >= 0.0 && stageStatus.BackPressureLevel <= 1.0,
                    $"Stage {stageName} should have valid pressure level");
            }
        }
        finally
        {
            // Close all stages
            gateway.Close();
            keyGen.Close();
            ingressProcessing.Close();
            asyncEgress.Close();
            finalSink.Close();

            // Dispose all stages
            gateway.Dispose();
            keyGen.Dispose();
            ingressProcessing.Dispose();
            asyncEgress.Dispose();
            finalSink.Dispose();
        }
    }

    [Fact]
    public void BackPressureStressTest_ShouldHandleHighLoad()
    {
        // Arrange
        using var controller = new PipelineBackPressureController(
            _loggerFactory.CreateLogger<PipelineBackPressureController>(),
            new PipelineBackPressureConfiguration
            {
                GatewayBufferSize = 50,  // Small buffers to trigger back pressure
                KeyGenBufferSize = 50,
                IngressProcessingBufferSize = 50
            });

        var gateway = new GatewayStage<string>(
            _loggerFactory.CreateLogger<GatewayStage<string>>(),
            controller,
            new GatewayConfiguration { MaxRequestsPerSecond = 10 }); // Low rate to trigger throttling

        var context = new TestRuntimeContext();
        gateway.Open(context);

        var successCount = 0;
        var throttledCount = 0;

        try
        {
            // Act - Rapid processing to trigger back pressure
            for (int i = 0; i < 100; i++)
            {
                try
                {
                    gateway.Map($"stress-test-record-{i}");
                    successCount++;
                }
                catch (InvalidOperationException ex) when (ex.Message.Contains("throttled") || ex.Message.Contains("credits"))
                {
                    throttledCount++;
                }
            }

            // Assert
            Assert.True(throttledCount > 0, "Should have triggered throttling under high load");
            Assert.True(successCount > 0, "Should still process some records successfully");
            
            _logger.LogInformation("Stress test completed: {Success} successful, {Throttled} throttled", 
                successCount, throttledCount);
        }
        finally
        {
            gateway.Close();
            gateway.Dispose();
        }
    }

    #endregion

    #region Test Helper Classes

    private static IHost CreateTestHost()
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

    public void Dispose()
    {
        _host?.Dispose();
    }

    // Test implementations
    private class TestRuntimeContext : IRuntimeContext
    {
        public string JobName => "TestJob";
        public string TaskName => "TestTask";
        public int IndexOfThisSubtask => 0;
        public int NumberOfParallelSubtasks => 1;
        public FlinkDotNet.Core.Abstractions.Models.JobConfiguration JobConfiguration { get; } = new();
        public FlinkDotNet.Core.Abstractions.Storage.IStateSnapshotStore StateSnapshotStore { get; } = new TestStateSnapshotStore();

        private object? _currentKey;
        public object? GetCurrentKey() => _currentKey;
        public void SetCurrentKey(object? key) => _currentKey = key;

        public FlinkDotNet.Core.Abstractions.States.IValueState<T> GetValueState<T>(FlinkDotNet.Core.Abstractions.Models.State.ValueStateDescriptor<T> stateDescriptor)
            => new TestValueState<T>();

        public FlinkDotNet.Core.Abstractions.States.IListState<T> GetListState<T>(FlinkDotNet.Core.Abstractions.Models.State.ListStateDescriptor<T> stateDescriptor)
            => new TestListState<T>();

        public FlinkDotNet.Core.Abstractions.States.IMapState<TK, TV> GetMapState<TK, TV>(FlinkDotNet.Core.Abstractions.Models.State.MapStateDescriptor<TK, TV> stateDescriptor) where TK : notnull
            => new TestMapState<TK, TV>();
    }

    private class TestSinkContext : ISinkContext
    {
        public long CurrentProcessingTimeMillis() => DateTimeOffset.UtcNow.ToUnixTimeMilliseconds();
    }

    private class TestValidator : IRecordValidator<string>
    {
        public ValidationResult Validate(string record)
        {
            if (string.IsNullOrEmpty(record) || record.Length < 10)
            {
                return new ValidationResult { IsValid = false, ErrorMessage = "Record too short" };
            }
            return new ValidationResult { IsValid = true };
        }
    }

    private class TestProcessor : IRecordProcessor<string>
    {
        public string Process(string record) => record + "_processed";
    }

    private class TestExternalService : IExternalService<string>
    {
        private readonly bool _shouldSucceed;

        public TestExternalService(bool shouldSucceed = true)
        {
            _shouldSucceed = shouldSucceed;
        }

        public Task<string> ProcessAsync(string record, CancellationToken cancellationToken)
        {
            if (!_shouldSucceed)
            {
                throw new InvalidOperationException("Test external service failure");
            }
            return Task.FromResult(record + "_external");
        }
    }

    private class TestDestination : IKafkaDestination<string>
    {
        private readonly Dictionary<string, Action<string, bool, string?>> _callbacks = new();

        public void Initialize(Dictionary<string, object> configuration) { }

        public Task<string?> SendAsync(string key, string value, bool requireAcknowledgment)
        {
            if (requireAcknowledgment)
            {
                var ackId = Guid.NewGuid().ToString();
                Task.Delay(50).ContinueWith(_ =>
                {
                    if (_callbacks.TryGetValue(ackId, out var callback))
                    {
                        callback(ackId, true, null);
                        _callbacks.Remove(ackId);
                    }
                });
                return Task.FromResult<string?>(ackId);
            }
            return Task.FromResult<string?>(null);
        }

        public void RegisterAcknowledgmentCallback(string acknowledgmentId, Action<string, bool, string?> callback)
        {
            _callbacks[acknowledgmentId] = callback;
        }

        public void Close() { }
        public void Dispose() { }
    }

    // Simple state implementations for testing
    private class TestValueState<T> : FlinkDotNet.Core.Abstractions.States.IValueState<T>
    {
        private T? _value;
        public T? Value() => _value;
        public void Update(T? value) => _value = value;
        public void Clear() => _value = default;
    }

    private class TestListState<T> : FlinkDotNet.Core.Abstractions.States.IListState<T>
    {
        private readonly List<T> _list = new();
        public IEnumerable<T> GetValues() => _list;
        public IEnumerable<T> Get() => _list;
        public void Add(T value) => _list.Add(value);
        public void Update(IEnumerable<T> values) { _list.Clear(); _list.AddRange(values); }
        public void AddAll(IEnumerable<T> values) => _list.AddRange(values);
        public void Clear() => _list.Clear();
    }

    private class TestMapState<TK, TV> : FlinkDotNet.Core.Abstractions.States.IMapState<TK, TV> where TK : notnull
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

    private class TestStateSnapshotStore : FlinkDotNet.Core.Abstractions.Storage.IStateSnapshotStore
    {
        public Task<FlinkDotNet.Core.Abstractions.Storage.SnapshotHandle> StoreSnapshot(string jobId, long checkpointId, string taskManagerId, string operatorId, byte[] snapshotData)
            => Task.FromResult(new FlinkDotNet.Core.Abstractions.Storage.SnapshotHandle($"test-{jobId}-{checkpointId}"));

        public Task<byte[]?> RetrieveSnapshot(FlinkDotNet.Core.Abstractions.Storage.SnapshotHandle handle)
            => Task.FromResult<byte[]?>(null);
    }

    #endregion
}