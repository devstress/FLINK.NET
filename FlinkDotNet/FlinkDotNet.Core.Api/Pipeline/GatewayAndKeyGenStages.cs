using System.Collections.Concurrent;
using FlinkDotNet.Core.Abstractions.Operators;
using FlinkDotNet.Core.Abstractions.Context;
using FlinkDotNet.Core.Api.BackPressure;
using Microsoft.Extensions.Logging;

namespace FlinkDotNet.Core.Api.Pipeline;

/// <summary>
/// FlinkDotnet 2.0 style Gateway stage that provides ingress rate control
/// and back pressure management for the pipeline entry point.
/// </summary>
public class GatewayStage<T> : IMapOperator<T, T>, IOperatorLifecycle, IDisposable
{
    private readonly ILogger<GatewayStage<T>> _logger;
    private readonly PipelineBackPressureController _backPressureController;
    private readonly GatewayConfiguration _config;
    private readonly ConcurrentQueue<T> _ingressQueue;
    private readonly SemaphoreSlim _rateLimitSemaphore;
    private readonly Timer _metricsTimer;
    
    private long _processedCount;
    private long _throttledCount;
    private DateTime _lastMetricsUpdate;
    private volatile bool _disposed;

    public GatewayStage(
        ILogger<GatewayStage<T>> logger,
        PipelineBackPressureController backPressureController,
        GatewayConfiguration? config = null)
    {
        _logger = logger ?? throw new ArgumentNullException(nameof(logger));
        _backPressureController = backPressureController ?? throw new ArgumentNullException(nameof(backPressureController));
        _config = config ?? new GatewayConfiguration();
        
        _ingressQueue = new ConcurrentQueue<T>();
        _rateLimitSemaphore = new SemaphoreSlim(_config.MaxConcurrentRequests, _config.MaxConcurrentRequests);
        
        // Update metrics every second
        _metricsTimer = new Timer(UpdateMetrics, null, TimeSpan.FromSeconds(1), TimeSpan.FromSeconds(1));
        
        _lastMetricsUpdate = DateTime.UtcNow;
        
        _logger.LogInformation("GatewayStage initialized with rate limit {RateLimit} requests/sec", 
            _config.MaxRequestsPerSecond);
    }

    public void Open(IRuntimeContext context)
    {
        _logger.LogInformation("GatewayStage opened for task {TaskName}", context.TaskName);
    }

    public T Map(T value)
    {
        var startTime = DateTime.UtcNow;
        
        try
        {
            // Apply rate limiting at gateway
            if (!ApplyRateLimiting())
            {
                Interlocked.Increment(ref _throttledCount);
                _logger.LogDebug("Request throttled at gateway due to rate limiting");
                throw new InvalidOperationException("Request throttled due to rate limiting");
            }

            // Check back pressure from downstream stages
            if (_backPressureController.ShouldThrottleStage(PipelineStage.Gateway))
            {
                var throttleDelay = _backPressureController.GetStageThrottleDelayMs(PipelineStage.Gateway);
                if (throttleDelay > 0)
                {
                    Thread.Sleep(throttleDelay);
                    _logger.LogDebug("Applied back pressure throttling: {DelayMs}ms", throttleDelay);
                }
            }

            // Request credits for processing
            var credits = _backPressureController.RequestCredits(PipelineStage.Gateway, 1);
            if (credits == 0)
            {
                Interlocked.Increment(ref _throttledCount);
                _logger.LogDebug("No credits available for gateway processing");
                throw new InvalidOperationException("No processing credits available");
            }

            // Process the record
            var result = ProcessRecord(value);
            
            // Replenish credits after processing
            _backPressureController.ReplenishCredits(PipelineStage.Gateway, 1);
            
            Interlocked.Increment(ref _processedCount);
            
            var processingTime = (DateTime.UtcNow - startTime).TotalMilliseconds;
            LogPerformanceMetrics(processingTime);
            
            return result;
        }
        catch (Exception ex)
        {
            _logger.LogWarning(ex, "Gateway processing failed for record");
            throw;
        }
    }

    /// <summary>
    /// Apply rate limiting at the gateway level
    /// </summary>
    private bool ApplyRateLimiting()
    {
        // Use semaphore for concurrent request limiting
        if (!_rateLimitSemaphore.Wait(TimeSpan.FromMilliseconds(100)))
        {
            return false; // Rate limit exceeded
        }

        // Release semaphore after a delay to implement rate limiting
        _ = Task.Delay(TimeSpan.FromSeconds(1.0 / _config.MaxRequestsPerSecond))
              .ContinueWith(_ => _rateLimitSemaphore.Release());

        return true;
    }

    /// <summary>
    /// Process individual record with gateway-specific logic
    /// </summary>
    private T ProcessRecord(T value)
    {
        // Gateway can perform initial validation, enrichment, etc.
        // For now, pass through the value
        return value;
    }

    /// <summary>
    /// Update metrics for back pressure monitoring
    /// </summary>
    private void UpdateMetrics(object? state)
    {
        if (_disposed) return;

        try
        {
            var now = DateTime.UtcNow;
            var timeDelta = (now - _lastMetricsUpdate).TotalMilliseconds;
            
            // Calculate processing latency (simplified)
            var avgProcessingLatency = timeDelta > 0 ? 
                (_processedCount > 0 ? timeDelta / _processedCount : 0) : 0;
            
            // Calculate error rate
            var totalRequests = _processedCount + _throttledCount;
            var errorRate = totalRequests > 0 ? (double)_throttledCount / totalRequests : 0;
            
            // Update back pressure controller with current metrics
            _backPressureController.UpdateStageMetrics(
                PipelineStage.Gateway,
                _ingressQueue.Count,
                _config.MaxQueueSize,
                avgProcessingLatency,
                errorRate);
            
            _lastMetricsUpdate = now;
            
            // Log performance metrics periodically
            if (_processedCount % 10000 == 0 && _processedCount > 0)
            {
                _logger.LogInformation("Gateway metrics: Processed {Processed}, Throttled {Throttled}, Queue size {QueueSize}",
                    _processedCount, _throttledCount, _ingressQueue.Count);
            }
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "Error updating gateway metrics");
        }
    }

    /// <summary>
    /// Log performance metrics for monitoring
    /// </summary>
    private void LogPerformanceMetrics(double processingTimeMs)
    {
        if (processingTimeMs > _config.SlowProcessingThresholdMs)
        {
            _logger.LogWarning("Slow processing detected at Gateway: {ProcessingTime:F2}ms", processingTimeMs);
        }
    }

    public void Close()
    {
        _logger.LogInformation("GatewayStage closing. Processed {Processed} records, Throttled {Throttled} requests",
            _processedCount, _throttledCount);
    }

    public void Dispose()
    {
        if (_disposed) return;
        
        _disposed = true;
        _metricsTimer?.Dispose();
        _rateLimitSemaphore?.Dispose();
        
        _logger.LogInformation("GatewayStage disposed");
    }
}

/// <summary>
/// FlinkDotnet 2.0 style KeyGen stage that provides deterministic partitioning
/// with load awareness and back pressure handling.
/// </summary>
public class KeyGenStage<T> : IMapOperator<T, KeyedRecord<T>>, IOperatorLifecycle, IDisposable
{
    private readonly ILogger<KeyGenStage<T>> _logger;
    private readonly PipelineBackPressureController _backPressureController;
    private readonly Func<T, string> _keyExtractor;
    private readonly KeyGenConfiguration _config;
    private readonly ConcurrentDictionary<string, PartitionMetrics> _partitionMetrics;
    private readonly Timer _loadBalancingTimer;
    
    private long _processedCount;
    private volatile bool _disposed;

    public KeyGenStage(
        ILogger<KeyGenStage<T>> logger,
        PipelineBackPressureController backPressureController,
        Func<T, string> keyExtractor,
        KeyGenConfiguration? config = null)
    {
        _logger = logger ?? throw new ArgumentNullException(nameof(logger));
        _backPressureController = backPressureController ?? throw new ArgumentNullException(nameof(backPressureController));
        _keyExtractor = keyExtractor ?? throw new ArgumentNullException(nameof(keyExtractor));
        _config = config ?? new KeyGenConfiguration();
        
        _partitionMetrics = new ConcurrentDictionary<string, PartitionMetrics>();
        
        // Monitor load balancing every 5 seconds
        _loadBalancingTimer = new Timer(MonitorLoadBalancing, null, 
            TimeSpan.FromSeconds(5), TimeSpan.FromSeconds(5));
        
        _logger.LogInformation("KeyGenStage initialized with {Partitions} partitions", _config.NumberOfPartitions);
    }

    public void Open(IRuntimeContext context)
    {
        _logger.LogInformation("KeyGenStage opened for task {TaskName}", context.TaskName);
    }

    public KeyedRecord<T> Map(T value)
    {
        var startTime = DateTime.UtcNow;
        
        try
        {
            // Check back pressure
            if (_backPressureController.ShouldThrottleStage(PipelineStage.KeyGen))
            {
                var throttleDelay = _backPressureController.GetStageThrottleDelayMs(PipelineStage.KeyGen);
                if (throttleDelay > 0)
                {
                    Thread.Sleep(throttleDelay);
                }
            }

            // Request credits for processing
            var credits = _backPressureController.RequestCredits(PipelineStage.KeyGen, 1);
            if (credits == 0)
            {
                throw new InvalidOperationException("No processing credits available for KeyGen");
            }

            // Extract key and determine partition with load awareness
            var key = _keyExtractor(value);
            var partition = DetermineOptimalPartition(key);
            
            var keyedRecord = new KeyedRecord<T>
            {
                Key = key,
                Partition = partition,
                Value = value,
                Timestamp = DateTimeOffset.UtcNow.ToUnixTimeMilliseconds()
            };

            // Update partition metrics
            UpdatePartitionMetrics(partition);
            
            // Replenish credits
            _backPressureController.ReplenishCredits(PipelineStage.KeyGen, 1);
            
            Interlocked.Increment(ref _processedCount);
            
            var processingTime = (DateTime.UtcNow - startTime).TotalMilliseconds;
            UpdateStageMetrics(processingTime);
            
            return keyedRecord;
        }
        catch (Exception ex)
        {
            _logger.LogWarning(ex, "KeyGen processing failed for record");
            throw;
        }
    }

    /// <summary>
    /// Determine optimal partition based on load balancing
    /// </summary>
    private int DetermineOptimalPartition(string key)
    {
        // Default hash-based partitioning
        var hashPartition = Math.Abs(key.GetHashCode()) % _config.NumberOfPartitions;
        
        // Check if load balancing is needed
        if (_config.EnableLoadAwareness && ShouldRebalance())
        {
            return FindLeastLoadedPartition();
        }
        
        return hashPartition;
    }

    /// <summary>
    /// Check if rebalancing is needed based on partition load
    /// </summary>
    private bool ShouldRebalance()
    {
        if (_partitionMetrics.Count < 2) return false;
        
        var loads = _partitionMetrics.Values.Select(m => m.RecordCount).ToList();
        var maxLoad = loads.Max();
        var minLoad = loads.Min();
        
        // Rebalance if load difference exceeds threshold
        return maxLoad - minLoad > _config.LoadImbalanceThreshold;
    }

    /// <summary>
    /// Find the least loaded partition for rebalancing
    /// </summary>
    private int FindLeastLoadedPartition()
    {
        var leastLoadedPartition = 0;
        var minLoad = long.MaxValue;
        
        for (int i = 0; i < _config.NumberOfPartitions; i++)
        {
            var load = _partitionMetrics.TryGetValue(i.ToString(), out var metrics) ? 
                metrics.RecordCount : 0;
            
            if (load < minLoad)
            {
                minLoad = load;
                leastLoadedPartition = i;
            }
        }
        
        return leastLoadedPartition;
    }

    /// <summary>
    /// Update partition-specific metrics
    /// </summary>
    private void UpdatePartitionMetrics(int partition)
    {
        var partitionKey = partition.ToString();
        _partitionMetrics.AddOrUpdate(partitionKey,
            new PartitionMetrics { PartitionId = partition, RecordCount = 1, LastUpdated = DateTime.UtcNow },
            (key, existing) =>
            {
                existing.RecordCount++;
                existing.LastUpdated = DateTime.UtcNow;
                return existing;
            });
    }

    /// <summary>
    /// Update stage metrics for back pressure monitoring
    /// </summary>
    private void UpdateStageMetrics(double processingTimeMs)
    {
        // Simplified queue size (could be actual queue in real implementation)
        var queueSize = _partitionMetrics.Values.Sum(m => m.RecordCount) % 1000;
        
        _backPressureController.UpdateStageMetrics(
            PipelineStage.KeyGen,
            (int)queueSize,
            _config.MaxQueueSize,
            processingTimeMs);
    }

    /// <summary>
    /// Monitor load balancing across partitions
    /// </summary>
    private void MonitorLoadBalancing(object? state)
    {
        if (_disposed) return;

        try
        {
            var totalRecords = _partitionMetrics.Values.Sum(m => m.RecordCount);
            if (totalRecords == 0) return;

            var partitionLoads = _partitionMetrics.Values
                .Select(m => new { m.PartitionId, Load = (double)m.RecordCount / totalRecords })
                .OrderByDescending(p => p.Load)
                .ToList();

            if (partitionLoads.Count > 1)
            {
                var maxLoad = partitionLoads.First().Load;
                var minLoad = partitionLoads.Last().Load;
                var imbalance = maxLoad - minLoad;

                if (imbalance > 0.2) // 20% imbalance threshold
                {
                    _logger.LogWarning("Partition load imbalance detected: Max {MaxLoad:F2}, Min {MinLoad:F2}, Difference {Imbalance:F2}",
                        maxLoad, minLoad, imbalance);
                }
            }
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "Error monitoring load balancing");
        }
    }

    public void Close()
    {
        _logger.LogInformation("KeyGenStage closing. Processed {Processed} records across {Partitions} partitions",
            _processedCount, _partitionMetrics.Count);
    }

    public void Dispose()
    {
        if (_disposed) return;
        
        _disposed = true;
        _loadBalancingTimer?.Dispose();
        
        _logger.LogInformation("KeyGenStage disposed");
    }
}

/// <summary>
/// Configuration for Gateway stage
/// </summary>
public class GatewayConfiguration
{
    public int MaxRequestsPerSecond { get; set; } = 1000;
    public int MaxConcurrentRequests { get; set; } = 100;
    public int MaxQueueSize { get; set; } = 1000;
    public double SlowProcessingThresholdMs { get; set; } = 100;
}

/// <summary>
/// Configuration for KeyGen stage
/// </summary>
public class KeyGenConfiguration
{
    public int NumberOfPartitions { get; set; } = 10;
    public bool EnableLoadAwareness { get; set; } = true;
    public long LoadImbalanceThreshold { get; set; } = 1000;
    public int MaxQueueSize { get; set; } = 2000;
}

/// <summary>
/// Represents a keyed record with partition information
/// </summary>
public class KeyedRecord<T>
{
    public string Key { get; set; } = string.Empty;
    public int Partition { get; set; }
    public T Value { get; set; } = default!;
    public long Timestamp { get; set; }
}

/// <summary>
/// Metrics for partition load monitoring
/// </summary>
public class PartitionMetrics
{
    public int PartitionId { get; set; }
    public long RecordCount { get; set; }
    public DateTime LastUpdated { get; set; }
}