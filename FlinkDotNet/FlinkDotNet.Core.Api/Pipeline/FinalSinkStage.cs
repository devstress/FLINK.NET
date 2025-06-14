using System.Collections.Concurrent;
using System.Diagnostics.CodeAnalysis;
using FlinkDotNet.Core.Abstractions.Sinks;
using FlinkDotNet.Core.Abstractions.Context;
using FlinkDotNet.Core.Abstractions.Operators;
using FlinkDotNet.Core.Api.BackPressure;
using Microsoft.Extensions.Logging;

namespace FlinkDotNet.Core.Api.Pipeline;

/// <summary>
/// Flink.Net style FinalSink stage that provides acknowledgment-based
/// back pressure for Kafka, Database, or Callback destinations.
/// </summary>
[SuppressMessage("Design", "S3881:Fix this implementation of 'IDisposable' to conform to the dispose pattern", Justification = "Simple disposal pattern is sufficient for this sink stage")]
public class FinalSinkStage<T> : ISinkFunction<EgressResult<T>>, IOperatorLifecycle, IDisposable
{
    private readonly ILogger<FinalSinkStage<T>> _logger;
    private readonly PipelineBackPressureController _backPressureController;
    private readonly IFinalDestination<T> _destination;
    private readonly FinalSinkConfiguration _config;
    private readonly ConcurrentDictionary<string, PendingAcknowledgment<T>> _pendingAcks;
    private readonly SemaphoreSlim _acknowledgmentSemaphore;
    private readonly Timer _metricsTimer;
    private readonly Timer _acknowledgmentTimer;
    
    private long _processedCount;
    private long _acknowledgedCount;
    private long _failedCount;
    private volatile bool _disposed;

    public FinalSinkStage(
        ILogger<FinalSinkStage<T>> logger,
        PipelineBackPressureController backPressureController,
        IFinalDestination<T> destination,
        FinalSinkConfiguration? config = null)
    {
        _logger = logger ?? throw new ArgumentNullException(nameof(logger));
        _backPressureController = backPressureController ?? throw new ArgumentNullException(nameof(backPressureController));
        _destination = destination ?? throw new ArgumentNullException(nameof(destination));
        _config = config ?? new FinalSinkConfiguration();
        
        _pendingAcks = new ConcurrentDictionary<string, PendingAcknowledgment<T>>();
        _acknowledgmentSemaphore = new SemaphoreSlim(_config.MaxPendingAcknowledgments, _config.MaxPendingAcknowledgments);
        
        // Update metrics every 2 seconds
        _metricsTimer = new Timer(UpdateMetrics, null, TimeSpan.FromSeconds(2), TimeSpan.FromSeconds(2));
        
        // Check for acknowledgment timeouts every 5 seconds
        _acknowledgmentTimer = new Timer(CheckAcknowledgmentTimeouts, null, 
            TimeSpan.FromSeconds(5), TimeSpan.FromSeconds(5));
        
        _logger.LogInformation("FinalSinkStage initialized for destination {DestinationType} with max pending acks {MaxPendingAcks}",
            destination.GetType().Name, _config.MaxPendingAcknowledgments);
    }

    public void Open(IRuntimeContext context)
    {
        _logger.LogInformation("FinalSinkStage opened for task {TaskName}", context.TaskName);
        
        // Initialize destination
        _destination.Initialize(_config.DestinationConfiguration);
    }

    public void Invoke(EgressResult<T> value, ISinkContext context)
    {
        var startTime = DateTime.UtcNow;
        
        try
        {
            // Check back pressure
            if (_backPressureController.ShouldThrottleStage(PipelineStage.FinalSink))
            {
                var throttleDelay = _backPressureController.GetStageThrottleDelayMs(PipelineStage.FinalSink);
                if (throttleDelay > 0)
                {
                    Thread.Sleep(throttleDelay);
                }
            }

            // Request credits for processing
            var credits = _backPressureController.RequestCredits(PipelineStage.FinalSink, 1);
            if (credits == 0)
            {
                throw new InvalidOperationException("No processing credits available for FinalSink");
            }

            // Apply acknowledgment-based back pressure
            if (!_acknowledgmentSemaphore.Wait(TimeSpan.FromMilliseconds(_config.AcknowledgmentTimeoutMs)))
            {
                throw new InvalidOperationException("Too many pending acknowledgments - back pressure applied");
            }

            try
            {
                // Send to final destination and handle acknowledgment
                var acknowledgmentId = SendToDestination(value);
                
                if (_config.RequireAcknowledgment && acknowledgmentId != null)
                {
                    // Track pending acknowledgment
                    TrackPendingAcknowledgment(acknowledgmentId, value, startTime);
                }
                else
                {
                    // No acknowledgment required, complete immediately
                    CompleteProcessing(value, startTime);
                }
            }
            catch
            {
                _acknowledgmentSemaphore.Release();
                throw;
            }
        }
        catch (Exception ex)
        {
            Interlocked.Increment(ref _failedCount);
            _logger.LogError(ex, "FinalSink processing failed for record");
            throw new InvalidOperationException("FinalSink processing failed", ex);
        }
    }

    /// <summary>
    /// Send record to final destination (Kafka, Database, Callback)
    /// </summary>
    private string? SendToDestination(EgressResult<T> result)
    {
        try
        {
            if (!result.IsSuccess)
            {
                _logger.LogWarning("Sending failed egress result to destination for potential recovery");
            }

            switch (_config.DestinationType)
            {
                case DestinationType.Kafka:
                    return SendToKafka(result);
                
                case DestinationType.Database:
                    return SendToDatabase(result);
                
                case DestinationType.Callback:
                    return SendToCallback(result);
                
                default:
                    throw new ArgumentException($"Unsupported destination type: {_config.DestinationType}");
            }
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "Failed to send record to destination {DestinationType}", _config.DestinationType);
            throw new InvalidOperationException($"Failed to send record to destination {_config.DestinationType}", ex);
        }
    }

    /// <summary>
    /// Send record to Kafka with acknowledgment support
    /// </summary>
    private string? SendToKafka(EgressResult<T> result)
    {
        if (_destination is IKafkaDestination<T> kafkaDestination)
        {
            var messageKey = result.OriginalRecord.OriginalRecord.Key;
            var messageValue = result.Result ?? result.OriginalRecord.ProcessedValue;
            
            return kafkaDestination.SendAsync(messageKey, messageValue, _config.RequireAcknowledgment)
                .GetAwaiter().GetResult();
        }
        
        throw new InvalidOperationException("Destination is not a Kafka destination");
    }

    /// <summary>
    /// Send record to Database with transaction support
    /// </summary>
    private string? SendToDatabase(EgressResult<T> result)
    {
        if (_destination is IDatabaseDestination<T> databaseDestination)
        {
            var record = result.Result ?? result.OriginalRecord.ProcessedValue;
            
            return databaseDestination.InsertAsync(record, _config.RequireAcknowledgment)
                .GetAwaiter().GetResult();
        }
        
        throw new InvalidOperationException("Destination is not a Database destination");
    }

    /// <summary>
    /// Send record to Callback with response handling
    /// </summary>
    private string? SendToCallback(EgressResult<T> result)
    {
        if (_destination is ICallbackDestination<T> callbackDestination)
        {
            var record = result.Result ?? result.OriginalRecord.ProcessedValue;
            
            return callbackDestination.InvokeAsync(record, _config.RequireAcknowledgment)
                .GetAwaiter().GetResult();
        }
        
        throw new InvalidOperationException("Destination is not a Callback destination");
    }

    /// <summary>
    /// Track pending acknowledgment for back pressure management
    /// </summary>
    private void TrackPendingAcknowledgment(string acknowledgmentId, EgressResult<T> result, DateTime startTime)
    {
        var pendingAck = new PendingAcknowledgment<T>
        {
            AcknowledgmentId = acknowledgmentId,
            Record = result,
            StartTime = startTime,
            TimeoutAt = DateTime.UtcNow.AddMilliseconds(_config.AcknowledgmentTimeoutMs)
        };

        _pendingAcks[acknowledgmentId] = pendingAck;
        
        // Register for acknowledgment callback
        _destination.RegisterAcknowledgmentCallback(acknowledgmentId, (ackId, success, error) =>
        {
            HandleAcknowledgment(ackId, success, error);
        });
    }

    /// <summary>
    /// Handle acknowledgment from destination
    /// </summary>
    private void HandleAcknowledgment(string acknowledgmentId, bool success, string? error)
    {
        if (_pendingAcks.TryRemove(acknowledgmentId, out var pendingAck))
        {
            if (success)
            {
                Interlocked.Increment(ref _acknowledgedCount);
                CompleteProcessing(pendingAck.Record, pendingAck.StartTime);
            }
            else
            {
                Interlocked.Increment(ref _failedCount);
                _logger.LogWarning("Acknowledgment failed for record {AckId}: {Error}", acknowledgmentId, error);
                
                // Handle failed acknowledgment (could retry, send to DLQ, etc.)
                HandleFailedAcknowledgment(pendingAck, error);
            }
            
            _acknowledgmentSemaphore.Release();
        }
    }

    /// <summary>
    /// Complete processing and update metrics
    /// </summary>
    private void CompleteProcessing(EgressResult<T> result, DateTime startTime)
    {
        // Replenish credits
        _backPressureController.ReplenishCredits(PipelineStage.FinalSink, 1);
        
        Interlocked.Increment(ref _processedCount);
        
        // Log success if result indicates success
        if (result.IsSuccess)
        {
            var successProcessingTime = (DateTime.UtcNow - startTime).TotalMilliseconds;
            _logger.LogDebug("FinalSink processed record successfully in {ProcessingTime}ms", successProcessingTime);
        }
        
        var processingTime = (DateTime.UtcNow - startTime).TotalMilliseconds;
        UpdateStageMetrics(processingTime);
        
        if (!_config.RequireAcknowledgment)
        {
            _acknowledgmentSemaphore.Release();
        }
    }

    /// <summary>
    /// Handle failed acknowledgment
    /// </summary>
    private void HandleFailedAcknowledgment(PendingAcknowledgment<T> pendingAck, string? error)
    {
        // Could implement retry logic, DLQ, etc.
        _logger.LogError("Acknowledgment failed for record, implementing recovery strategy: {Error}", error);
        
        // For now, just log and continue
        CompleteProcessing(pendingAck.Record, pendingAck.StartTime);
    }

    /// <summary>
    /// Check for acknowledgment timeouts
    /// </summary>
    private void CheckAcknowledgmentTimeouts(object? state)
    {
        if (_disposed) return;

        try
        {
            var now = DateTime.UtcNow;
            var timedOutAcks = _pendingAcks.Values
                .Where(ack => ack.TimeoutAt <= now)
                .ToList();

            foreach (var timedOutAck in timedOutAcks)
            {
                if (_pendingAcks.TryRemove(timedOutAck.AcknowledgmentId, out _))
                {
                    Interlocked.Increment(ref _failedCount);
                    _logger.LogWarning("Acknowledgment timeout for record {AckId}", timedOutAck.AcknowledgmentId);
                    
                    HandleFailedAcknowledgment(timedOutAck, "Acknowledgment timeout");
                }
            }
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "Error checking acknowledgment timeouts");
        }
    }

    /// <summary>
    /// Update stage metrics for back pressure monitoring
    /// </summary>
    private void UpdateStageMetrics(double processingTimeMs)
    {
        var errorRate = _processedCount > 0 ? (double)_failedCount / _processedCount : 0;
        
        _backPressureController.UpdateStageMetrics(
            PipelineStage.FinalSink,
            _pendingAcks.Count,
            _config.MaxPendingAcknowledgments,
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
            var acknowledgmentRate = _processedCount > 0 ? (double)_acknowledgedCount / _processedCount : 0;
            var pendingCount = _pendingAcks.Count;
            
            if (acknowledgmentRate < 0.95 && _processedCount > 100)
            {
                _logger.LogWarning("Low acknowledgment rate in FinalSink: {AckRate:F2}", acknowledgmentRate);
            }
            
            if (pendingCount > _config.MaxPendingAcknowledgments * 0.8)
            {
                _logger.LogWarning("High pending acknowledgments in FinalSink: {PendingCount}/{MaxPending}",
                    pendingCount, _config.MaxPendingAcknowledgments);
            }
            
            // Log performance metrics periodically
            if (_processedCount % 5000 == 0 && _processedCount > 0)
            {
                _logger.LogInformation("FinalSink metrics: Processed {Processed}, Acknowledged {Acknowledged}, Failed {Failed}, Pending {Pending}",
                    _processedCount, _acknowledgedCount, _failedCount, pendingCount);
            }
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "Error updating FinalSink metrics");
        }
    }

    public void Close()
    {
        _logger.LogInformation("FinalSinkStage closing. Processed {Processed} records, Acknowledgment rate {AckRate:F2}, Pending {Pending}",
            _processedCount, _processedCount > 0 ? (double)_acknowledgedCount / _processedCount : 0, _pendingAcks.Count);
        
        _destination.Close();
    }

    public void Dispose()
    {
        if (_disposed) return;
        
        _disposed = true;
        _metricsTimer?.Dispose();
        _acknowledgmentTimer?.Dispose();
        _acknowledgmentSemaphore?.Dispose();
        _destination?.Dispose();
        
        _logger.LogInformation("FinalSinkStage disposed");
    }
}

/// <summary>
/// Configuration for FinalSink stage
/// </summary>
public class FinalSinkConfiguration
{
    public DestinationType DestinationType { get; set; } = DestinationType.Kafka;
    public bool RequireAcknowledgment { get; set; } = true;
    public int MaxPendingAcknowledgments { get; set; } = 1000;
    public int AcknowledgmentTimeoutMs { get; set; } = 10000;
    public Dictionary<string, object> DestinationConfiguration { get; set; } = new();
}

/// <summary>
/// Types of final destinations
/// </summary>
public enum DestinationType
{
    Kafka,
    Database,
    Callback
}

/// <summary>
/// Represents a pending acknowledgment
/// </summary>
public class PendingAcknowledgment<T>
{
    public string AcknowledgmentId { get; set; } = string.Empty;
    public EgressResult<T> Record { get; set; } = null!;
    public DateTime StartTime { get; set; }
    public DateTime TimeoutAt { get; set; }
}

/// <summary>
/// Base interface for final destinations
/// </summary>
[SuppressMessage("Design", "S2326:'T' is not used in the interface", Justification = "Generic type parameter T is used by implementing classes for type safety")]
public interface IFinalDestination<T> : IDisposable
{
    void Initialize(Dictionary<string, object> configuration);
    void RegisterAcknowledgmentCallback(string acknowledgmentId, Action<string, bool, string?> callback);
    void Close();
}

/// <summary>
/// Kafka destination interface
/// </summary>
public interface IKafkaDestination<T> : IFinalDestination<T>
{
    Task<string?> SendAsync(string key, T value, bool requireAcknowledgment);
}

/// <summary>
/// Database destination interface
/// </summary>
public interface IDatabaseDestination<T> : IFinalDestination<T>
{
    Task<string?> InsertAsync(T record, bool requireAcknowledgment);
}

/// <summary>
/// Callback destination interface
/// </summary>
public interface ICallbackDestination<T> : IFinalDestination<T>
{
    Task<string?> InvokeAsync(T record, bool requireAcknowledgment);
}