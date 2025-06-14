using System.Collections.Concurrent;
using System.Diagnostics.CodeAnalysis;
using FlinkDotNet.Core.Abstractions.Operators;
using FlinkDotNet.Core.Abstractions.Context;
using FlinkDotNet.Core.Api.BackPressure;
using Microsoft.Extensions.Logging;

namespace FlinkDotNet.Core.Api.Pipeline;

/// <summary>
/// Flink.Net style IngressProcessing stage that provides validation 
/// and preprocessing with bounded buffers and back pressure handling.
/// </summary>
[SuppressMessage("Design", "S3881:Fix this implementation of 'IDisposable' to conform to the dispose pattern", Justification = "Simple disposal pattern is sufficient for this ingress stage")]
public class IngressProcessingStage<T> : IMapOperator<KeyedRecord<T>, ProcessedRecord<T>>, IOperatorLifecycle, IDisposable
{
    private readonly ILogger<IngressProcessingStage<T>> _logger;
    private readonly PipelineBackPressureController _backPressureController;
    private readonly IRecordValidator<T>? _validator;
    private readonly IRecordProcessor<T>? _preprocessor;
    private readonly IngressProcessingConfiguration _config;
    private readonly ConcurrentQueue<KeyedRecord<T>> _boundedBuffer;
    private readonly SemaphoreSlim _bufferSemaphore;
    private readonly Timer _metricsTimer;
    
    private long _processedCount;
    private long _validationFailures;
    private long _processingFailures;
    private volatile bool _disposed;

    public IngressProcessingStage(
        ILogger<IngressProcessingStage<T>> logger,
        PipelineBackPressureController backPressureController,
        IngressProcessingConfiguration? config = null,
        IRecordValidator<T>? validator = null,
        IRecordProcessor<T>? preprocessor = null)
    {
        _logger = logger ?? throw new ArgumentNullException(nameof(logger));
        _backPressureController = backPressureController ?? throw new ArgumentNullException(nameof(backPressureController));
        _config = config ?? new IngressProcessingConfiguration();
        _validator = validator;
        _preprocessor = preprocessor;
        
        _boundedBuffer = new ConcurrentQueue<KeyedRecord<T>>();
        _bufferSemaphore = new SemaphoreSlim(_config.MaxBufferSize, _config.MaxBufferSize);
        
        // Update metrics every 2 seconds
        _metricsTimer = new Timer(UpdateMetrics, null, TimeSpan.FromSeconds(2), TimeSpan.FromSeconds(2));
        
        _logger.LogInformation("IngressProcessingStage initialized with buffer size {BufferSize}", _config.MaxBufferSize);
    }

    public void Open(IRuntimeContext context)
    {
        _logger.LogInformation("IngressProcessingStage opened for task {TaskName}", context.TaskName);
    }

    public ProcessedRecord<T> Map(KeyedRecord<T> value)
    {
        var startTime = DateTime.UtcNow;
        
        try
        {
            // Check back pressure
            if (_backPressureController.ShouldThrottleStage(PipelineStage.IngressProcessing))
            {
                var throttleDelay = _backPressureController.GetStageThrottleDelayMs(PipelineStage.IngressProcessing);
                if (throttleDelay > 0)
                {
                    Thread.Sleep(throttleDelay);
                }
            }

            // Request credits for processing
            var credits = _backPressureController.RequestCredits(PipelineStage.IngressProcessing, 1);
            if (credits == 0)
            {
                throw new InvalidOperationException("No processing credits available for IngressProcessing");
            }

            // Apply bounded buffer back pressure
            if (!_bufferSemaphore.Wait(TimeSpan.FromMilliseconds(_config.BufferTimeoutMs)))
            {
                throw new InvalidOperationException("Bounded buffer full - back pressure applied");
            }

            try
            {
                // Add to bounded buffer
                _boundedBuffer.Enqueue(value);

                // Validate the record
                var validationResult = ValidateRecord(value);
                if (!validationResult.IsValid)
                {
                    Interlocked.Increment(ref _validationFailures);
                    throw new InvalidOperationException($"Validation failed: {validationResult.ErrorMessage}");
                }

                // Preprocess the record
                var processedValue = PreprocessRecord(value);
                
                var processedRecord = new ProcessedRecord<T>
                {
                    OriginalRecord = value,
                    ProcessedValue = processedValue,
                    ProcessingTimestamp = DateTimeOffset.UtcNow.ToUnixTimeMilliseconds(),
                    ValidationResult = validationResult,
                    ProcessingMetadata = CreateProcessingMetadata(value)
                };

                // Remove from bounded buffer
                _boundedBuffer.TryDequeue(out _);
                
                // Replenish credits
                _backPressureController.ReplenishCredits(PipelineStage.IngressProcessing, 1);
                
                Interlocked.Increment(ref _processedCount);
                
                var processingTime = (DateTime.UtcNow - startTime).TotalMilliseconds;
                UpdateStageMetrics(processingTime);
                
                return processedRecord;
            }
            finally
            {
                _bufferSemaphore.Release();
            }
        }
        catch (Exception ex)
        {
            Interlocked.Increment(ref _processingFailures);
            _logger.LogError(ex, "IngressProcessing failed for record with key {Key}", value.Key);
            throw new InvalidOperationException($"IngressProcessing failed for record with key {value.Key}", ex);
        }
    }

    /// <summary>
    /// Validate record according to configured rules
    /// </summary>
    private ValidationResult ValidateRecord(KeyedRecord<T> record)
    {
        try
        {
            if (_validator != null)
            {
                return _validator.Validate(record.Value);
            }
            
            // Default validation - check for null values
            if (EqualityComparer<T>.Default.Equals(record.Value, default(T)))
            {
                return new ValidationResult { IsValid = false, ErrorMessage = "Record value is null" };
            }
            
            if (string.IsNullOrEmpty(record.Key))
            {
                return new ValidationResult { IsValid = false, ErrorMessage = "Record key is null or empty" };
            }
            
            return new ValidationResult { IsValid = true };
        }
        catch (Exception ex)
        {
            return new ValidationResult 
            { 
                IsValid = false, 
                ErrorMessage = $"Validation exception: {ex.Message}" 
            };
        }
    }

    /// <summary>
    /// Preprocess record with configured processor
    /// </summary>
    private T PreprocessRecord(KeyedRecord<T> record)
    {
        if (_preprocessor != null)
        {
            return _preprocessor.Process(record.Value);
        }
        
        // Default preprocessing - return original value
        return record.Value;
    }

    /// <summary>
    /// Create processing metadata for the record
    /// </summary>
    private static Dictionary<string, object> CreateProcessingMetadata(KeyedRecord<T> record)
    {
        return new Dictionary<string, object>
        {
            ["processed_at"] = DateTime.UtcNow,
            ["partition"] = record.Partition,
            ["original_timestamp"] = record.Timestamp,
            ["stage"] = PipelineStage.IngressProcessing
        };
    }

    /// <summary>
    /// Update stage metrics for back pressure monitoring
    /// </summary>
    private void UpdateStageMetrics(double processingTimeMs)
    {
        var errorRate = _processedCount + _validationFailures + _processingFailures > 0 ?
            (double)(_validationFailures + _processingFailures) / (_processedCount + _validationFailures + _processingFailures) : 0;
        
        _backPressureController.UpdateStageMetrics(
            PipelineStage.IngressProcessing,
            _boundedBuffer.Count,
            _config.MaxBufferSize,
            processingTimeMs,
            errorRate);
    }

    /// <summary>
    /// Update metrics for monitoring
    /// </summary>
    private void UpdateMetrics(object? state)
    {
        if (_disposed) return;

        try
        {
            var bufferUtilization = (double)_boundedBuffer.Count / _config.MaxBufferSize;
            
            if (bufferUtilization > 0.8)
            {
                _logger.LogWarning("High buffer utilization in IngressProcessing: {Utilization:F2}", bufferUtilization);
            }
            
            // Log performance metrics periodically
            if (_processedCount % 10000 == 0 && _processedCount > 0)
            {
                _logger.LogInformation("IngressProcessing metrics: Processed {Processed}, Validation failures {ValidationFailures}, Processing failures {ProcessingFailures}",
                    _processedCount, _validationFailures, _processingFailures);
            }
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "Error updating IngressProcessing metrics");
        }
    }

    public void Close()
    {
        _logger.LogInformation("IngressProcessingStage closing. Processed {Processed} records, Validation failures {ValidationFailures}, Processing failures {ProcessingFailures}",
            _processedCount, _validationFailures, _processingFailures);
    }

    public void Dispose()
    {
        if (_disposed) return;
        
        _disposed = true;
        _metricsTimer?.Dispose();
        _bufferSemaphore?.Dispose();
        
        _logger.LogInformation("IngressProcessingStage disposed");
    }
}

/// <summary>
/// Flink.Net style AsyncEgressProcessing stage that handles external I/O
/// with timeout, retry, and Dead Letter Queue (DLQ) support.
/// </summary>
[SuppressMessage("Design", "S3881:Fix this implementation of 'IDisposable' to conform to the dispose pattern", Justification = "Simple disposal pattern is sufficient for this async egress stage")]
public class AsyncEgressProcessingStage<T> : IMapOperator<ProcessedRecord<T>, EgressResult<T>>, IOperatorLifecycle, IDisposable
{
    private readonly ILogger<AsyncEgressProcessingStage<T>> _logger;
    private readonly PipelineBackPressureController _backPressureController;
    private readonly IExternalService<T>? _externalService;
    private readonly AsyncEgressConfiguration _config;
    private readonly ConcurrentQueue<ProcessedRecord<T>> _deadLetterQueue;
    private readonly SemaphoreSlim _concurrencyLimiter;
    private readonly Timer _metricsTimer;
    
    private long _processedCount;
    private long _successCount;
    private long _failureCount;
    private long _dlqCount;
    private volatile bool _disposed;

    public AsyncEgressProcessingStage(
        ILogger<AsyncEgressProcessingStage<T>> logger,
        PipelineBackPressureController backPressureController,
        AsyncEgressConfiguration? config = null,
        IExternalService<T>? externalService = null)
    {
        _logger = logger ?? throw new ArgumentNullException(nameof(logger));
        _backPressureController = backPressureController ?? throw new ArgumentNullException(nameof(backPressureController));
        _config = config ?? new AsyncEgressConfiguration();
        _externalService = externalService;
        
        _deadLetterQueue = new ConcurrentQueue<ProcessedRecord<T>>();
        _concurrencyLimiter = new SemaphoreSlim(_config.MaxConcurrentOperations, _config.MaxConcurrentOperations);
        
        // Update metrics every 3 seconds
        _metricsTimer = new Timer(UpdateMetrics, null, TimeSpan.FromSeconds(3), TimeSpan.FromSeconds(3));
        
        _logger.LogInformation("AsyncEgressProcessingStage initialized with timeout {Timeout}ms, max retries {MaxRetries}",
            _config.OperationTimeoutMs, _config.MaxRetries);
    }

    public void Open(IRuntimeContext context)
    {
        _logger.LogInformation("AsyncEgressProcessingStage opened for task {TaskName}", context.TaskName);
    }

    public EgressResult<T> Map(ProcessedRecord<T> value)
    {
        var startTime = DateTime.UtcNow;
        
        try
        {
            // Check back pressure
            if (_backPressureController.ShouldThrottleStage(PipelineStage.AsyncEgressProcessing))
            {
                var throttleDelay = _backPressureController.GetStageThrottleDelayMs(PipelineStage.AsyncEgressProcessing);
                if (throttleDelay > 0)
                {
                    Thread.Sleep(throttleDelay);
                }
            }

            // Request credits for processing
            var credits = _backPressureController.RequestCredits(PipelineStage.AsyncEgressProcessing, 1);
            if (credits == 0)
            {
                throw new InvalidOperationException("No processing credits available for AsyncEgressProcessing");
            }

            // Limit concurrency for external operations
            if (!_concurrencyLimiter.Wait(TimeSpan.FromMilliseconds(_config.ConcurrencyTimeoutMs)))
            {
                throw new InvalidOperationException("Concurrency limit reached for external operations");
            }

            try
            {
                // Process with retry logic
                var result = ProcessWithRetry(value).GetAwaiter().GetResult();
                
                if (result.IsSuccess)
                {
                    Interlocked.Increment(ref _successCount);
                }
                else
                {
                    Interlocked.Increment(ref _failureCount);
                    
                    // Send to DLQ if configured
                    if (_config.EnableDeadLetterQueue)
                    {
                        SendToDeadLetterQueue(value);
                    }
                }

                // Replenish credits
                _backPressureController.ReplenishCredits(PipelineStage.AsyncEgressProcessing, 1);
                
                Interlocked.Increment(ref _processedCount);
                
                var processingTime = (DateTime.UtcNow - startTime).TotalMilliseconds;
                UpdateStageMetrics(processingTime);
                
                return result;
            }
            finally
            {
                _concurrencyLimiter.Release();
            }
        }
        catch (Exception ex)
        {
            Interlocked.Increment(ref _failureCount);
            _logger.LogError(ex, "AsyncEgressProcessing failed for record");
            throw new InvalidOperationException("AsyncEgressProcessing failed", ex);
        }
    }

    /// <summary>
    /// Process record with retry logic and timeout handling
    /// </summary>
    private async Task<EgressResult<T>> ProcessWithRetry(ProcessedRecord<T> record)
    {
        var lastException = new Exception("Unknown error");
        
        for (int attempt = 1; attempt <= _config.MaxRetries; attempt++)
        {
            try
            {
                using var cts = new CancellationTokenSource(TimeSpan.FromMilliseconds(_config.OperationTimeoutMs));
                
                if (_externalService != null)
                {
                    var externalResult = await _externalService.ProcessAsync(record.ProcessedValue, cts.Token);
                    
                    return new EgressResult<T>
                    {
                        OriginalRecord = record,
                        IsSuccess = true,
                        Result = externalResult,
                        ProcessingTime = DateTime.UtcNow,
                        AttemptNumber = attempt
                    };
                }
                else
                {
                    // Simulate external service call
                    await Task.Delay(Random.Shared.Next(10, 100), cts.Token);
                    
                    return new EgressResult<T>
                    {
                        OriginalRecord = record,
                        IsSuccess = true,
                        Result = record.ProcessedValue,
                        ProcessingTime = DateTime.UtcNow,
                        AttemptNumber = attempt
                    };
                }
            }
            catch (OperationCanceledException) when (attempt < _config.MaxRetries)
            {
                lastException = new TimeoutException($"Operation timeout on attempt {attempt}");
                await ApplyExponentialBackoff(attempt);
            }
            catch (Exception ex) when (attempt < _config.MaxRetries)
            {
                lastException = ex;
                await ApplyExponentialBackoff(attempt);
                _logger.LogWarning(ex, "External operation failed on attempt {Attempt}", attempt);
            }
        }
        
        return new EgressResult<T>
        {
            OriginalRecord = record,
            IsSuccess = false,
            ErrorMessage = $"Failed after {_config.MaxRetries} attempts: {lastException.Message}",
            ProcessingTime = DateTime.UtcNow,
            AttemptNumber = _config.MaxRetries
        };
    }

    /// <summary>
    /// Apply exponential backoff between retry attempts
    /// </summary>
    private async Task ApplyExponentialBackoff(int attemptNumber)
    {
        var delayMs = Math.Min(_config.BaseRetryDelayMs * (int)Math.Pow(2, attemptNumber - 1), _config.MaxRetryDelayMs);
        await Task.Delay(delayMs);
    }

    /// <summary>
    /// Send failed record to Dead Letter Queue
    /// </summary>
    private void SendToDeadLetterQueue(ProcessedRecord<T> record)
    {
        _deadLetterQueue.Enqueue(record);
        Interlocked.Increment(ref _dlqCount);
        
        _logger.LogWarning("Sent record to Dead Letter Queue. DLQ size: {DlqSize}", _deadLetterQueue.Count);
        
        // Prevent DLQ from growing too large
        if (_deadLetterQueue.Count > _config.MaxDeadLetterQueueSize && _deadLetterQueue.TryDequeue(out _))
        {
            _logger.LogWarning("DLQ full, dropped oldest record");
        }
    }

    /// <summary>
    /// Update stage metrics for back pressure monitoring
    /// </summary>
    private void UpdateStageMetrics(double processingTimeMs)
    {
        var errorRate = _processedCount > 0 ? (double)_failureCount / _processedCount : 0;
        
        _backPressureController.UpdateStageMetrics(
            PipelineStage.AsyncEgressProcessing,
            _deadLetterQueue.Count,
            _config.MaxDeadLetterQueueSize,
            processingTimeMs,
            errorRate);
    }

    /// <summary>
    /// Update metrics for monitoring
    /// </summary>
    private void UpdateMetrics(object? state)
    {
        if (_disposed) return;

        try
        {
            var successRate = _processedCount > 0 ? (double)_successCount / _processedCount : 0;
            
            if (successRate < 0.95 && _processedCount > 100) // Alert if success rate below 95%
            {
                _logger.LogWarning("Low success rate in AsyncEgressProcessing: {SuccessRate:F2}", successRate);
            }
            
            // Log performance metrics periodically
            if (_processedCount % 5000 == 0 && _processedCount > 0)
            {
                _logger.LogInformation("AsyncEgressProcessing metrics: Processed {Processed}, Success {Success}, Failures {Failures}, DLQ {Dlq}",
                    _processedCount, _successCount, _failureCount, _dlqCount);
            }
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "Error updating AsyncEgressProcessing metrics");
        }
    }

    /// <summary>
    /// Get Dead Letter Queue contents for manual processing
    /// </summary>
    public IEnumerable<ProcessedRecord<T>> GetDeadLetterQueueContents()
    {
        var contents = new List<ProcessedRecord<T>>();
        while (_deadLetterQueue.TryDequeue(out var record))
        {
            contents.Add(record);
        }
        return contents;
    }

    public void Close()
    {
        _logger.LogInformation("AsyncEgressProcessingStage closing. Processed {Processed} records, Success rate {SuccessRate:F2}, DLQ size {DlqSize}",
            _processedCount, _processedCount > 0 ? (double)_successCount / _processedCount : 0, _dlqCount);
    }

    public void Dispose()
    {
        if (_disposed) return;
        
        _disposed = true;
        _metricsTimer?.Dispose();
        _concurrencyLimiter?.Dispose();
        
        _logger.LogInformation("AsyncEgressProcessingStage disposed");
    }
}

/// <summary>
/// Configuration for IngressProcessing stage
/// </summary>
public class IngressProcessingConfiguration
{
    public int MaxBufferSize { get; set; } = 1500;
    public int BufferTimeoutMs { get; set; } = 1000;
    public bool EnableValidation { get; set; } = true;
    public bool EnablePreprocessing { get; set; } = true;
}

/// <summary>
/// Configuration for AsyncEgressProcessing stage
/// </summary>
public class AsyncEgressConfiguration
{
    public int MaxRetries { get; set; } = 3;
    public int OperationTimeoutMs { get; set; } = 5000;
    public int BaseRetryDelayMs { get; set; } = 100;
    public int MaxRetryDelayMs { get; set; } = 2000;
    public int MaxConcurrentOperations { get; set; } = 50;
    public int ConcurrencyTimeoutMs { get; set; } = 2000;
    public bool EnableDeadLetterQueue { get; set; } = true;
    public int MaxDeadLetterQueueSize { get; set; } = 10000;
}

/// <summary>
/// Represents a processed record with validation and processing metadata
/// </summary>
public class ProcessedRecord<T>
{
    public KeyedRecord<T> OriginalRecord { get; set; } = null!;
    public T ProcessedValue { get; set; } = default!;
    public long ProcessingTimestamp { get; set; }
    public ValidationResult ValidationResult { get; set; } = null!;
    public Dictionary<string, object> ProcessingMetadata { get; set; } = new();
}

/// <summary>
/// Result of external processing operation
/// </summary>
public class EgressResult<T>
{
    public ProcessedRecord<T> OriginalRecord { get; set; } = null!;
    public bool IsSuccess { get; set; }
    public T? Result { get; set; }
    public string? ErrorMessage { get; set; }
    public DateTime ProcessingTime { get; set; }
    public int AttemptNumber { get; set; }
}

/// <summary>
/// Result of record validation
/// </summary>
public class ValidationResult
{
    public bool IsValid { get; set; }
    public string? ErrorMessage { get; set; }
    public Dictionary<string, object> ValidationMetadata { get; set; } = new();
}

/// <summary>
/// Interface for record validation
/// </summary>
[SuppressMessage("Design", "S3246:Add the 'in' keyword to parameter 'T' to make it 'contravariant'", Justification = "T parameter needs to be invariant for validation operations")]
public interface IRecordValidator<T>
{
    ValidationResult Validate(T record);
}

/// <summary>
/// Interface for record preprocessing
/// </summary>
public interface IRecordProcessor<T>
{
    T Process(T record);
}

/// <summary>
/// Interface for external service operations
/// </summary>
public interface IExternalService<T>
{
    Task<T> ProcessAsync(T record, CancellationToken cancellationToken);
}