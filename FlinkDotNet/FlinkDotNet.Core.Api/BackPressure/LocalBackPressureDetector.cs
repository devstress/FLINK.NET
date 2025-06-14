using System.Collections.Concurrent;
using Microsoft.Extensions.Logging;

namespace FlinkDotNet.Core.Api.BackPressure;

/// <summary>
/// Local back pressure detection and throttling for LocalStreamExecutor.
/// Provides Flink.Net style back pressure handling within a single process.
/// </summary>
public class LocalBackPressureDetector : IDisposable
{
    private readonly ILogger<LocalBackPressureDetector>? _logger;
    private readonly ConcurrentDictionary<string, QueueMetrics> _queueMetrics;
    private readonly Timer _monitoringTimer;
    private readonly LocalBackPressureConfiguration _config;
    private bool _disposed;

    public LocalBackPressureDetector(LocalBackPressureConfiguration? config = null, ILogger<LocalBackPressureDetector>? logger = null)
    {
        _logger = logger;
        _config = config ?? new LocalBackPressureConfiguration();
        _queueMetrics = new ConcurrentDictionary<string, QueueMetrics>();
        
        // Monitor back pressure every 2 seconds for local execution
        _monitoringTimer = new Timer(MonitorBackPressure, null, TimeSpan.FromSeconds(2), TimeSpan.FromSeconds(2));
        
        _logger?.LogInformation("LocalBackPressureDetector initialized with threshold: {Threshold}", _config.BackPressureThreshold);
    }

    /// <summary>
    /// Records queue size for a specific operator to monitor back pressure.
    /// </summary>
    public void RecordQueueSize(string operatorId, int queueSize, int maxCapacity)
    {
        _queueMetrics.AddOrUpdate(operatorId, 
            new QueueMetrics { OperatorId = operatorId, CurrentSize = queueSize, MaxCapacity = maxCapacity, LastUpdated = DateTime.UtcNow },
            (key, existing) => 
            {
                existing.CurrentSize = queueSize;
                existing.MaxCapacity = maxCapacity;
                existing.LastUpdated = DateTime.UtcNow;
                return existing;
            });
    }

    /// <summary>
    /// Determines if the system should throttle data processing due to back pressure.
    /// </summary>
    public bool ShouldThrottle(string? operatorId = null)
    {
        if (operatorId != null && _queueMetrics.TryGetValue(operatorId, out var specificMetrics))
        {
            return CalculatePressureLevel(specificMetrics) > _config.BackPressureThreshold;
        }

        // Check overall system pressure
        return GetOverallPressureLevel() > _config.BackPressureThreshold;
    }

    /// <summary>
    /// Gets the recommended throttle delay in milliseconds.
    /// </summary>
    public int GetThrottleDelayMs()
    {
        var pressureLevel = GetOverallPressureLevel();
        if (pressureLevel < _config.BackPressureThreshold)
            return 0;

        // Exponential backoff based on pressure level
        var normalizedPressure = Math.Min(1.0, pressureLevel);
        return (int)Math.Round(_config.BaseThrottleDelayMs * Math.Pow(2, normalizedPressure * 4));
    }

    /// <summary>
    /// Gets overall system back pressure level (0.0 to 1.0).
    /// </summary>
    public double GetOverallPressureLevel()
    {
        if (_queueMetrics.IsEmpty) return 0.0;

        var validMetrics = _queueMetrics.Values
            .Where(m => DateTime.UtcNow - m.LastUpdated < TimeSpan.FromSeconds(10))
            .ToList();

        if (!validMetrics.Any()) return 0.0;

        return validMetrics.Average(CalculatePressureLevel);
    }

    private static double CalculatePressureLevel(QueueMetrics metrics)
    {
        if (metrics.MaxCapacity <= 0) return 0.0;
        return Math.Min(1.0, (double)metrics.CurrentSize / metrics.MaxCapacity);
    }

    private void MonitorBackPressure(object? state)
    {
        try
        {
            var overallPressure = GetOverallPressureLevel();
            
            if (overallPressure > _config.BackPressureThreshold)
            {
                _logger?.LogWarning("Back pressure detected: {PressureLevel:F2} (threshold: {Threshold:F2})", 
                    overallPressure, _config.BackPressureThreshold);
                
                // Log individual queue pressures for debugging
                foreach (var metrics in _queueMetrics.Values)
                {
                    var pressure = CalculatePressureLevel(metrics);
                    if (pressure > _config.BackPressureThreshold)
                    {
                        _logger?.LogWarning("High pressure in operator {OperatorId}: {Pressure:F2} ({CurrentSize}/{MaxCapacity})",
                            metrics.OperatorId, pressure, metrics.CurrentSize, metrics.MaxCapacity);
                    }
                }
            }
            else if (overallPressure > 0.1) // Log moderate pressure levels
            {
                _logger?.LogDebug("System pressure level: {PressureLevel:F2}", overallPressure);
            }
        }
        catch (Exception ex)
        {
            _logger?.LogError(ex, "Error monitoring back pressure");
        }
    }

    public void Dispose()
    {
        Dispose(true);
        GC.SuppressFinalize(this);
    }

    protected virtual void Dispose(bool disposing)
    {
        if (!_disposed && disposing)
        {
            _monitoringTimer?.Dispose();
            _queueMetrics.Clear();
            _disposed = true;
            _logger?.LogInformation("LocalBackPressureDetector disposed");
        }
    }
}

/// <summary>
/// Configuration for local back pressure detection.
/// </summary>
public class LocalBackPressureConfiguration
{
    /// <summary>
    /// Back pressure threshold (0.0 to 1.0). When queue utilization exceeds this, throttling is applied.
    /// </summary>
    public double BackPressureThreshold { get; set; } = 0.8;

    /// <summary>
    /// Base throttle delay in milliseconds when back pressure is detected.
    /// </summary>
    public int BaseThrottleDelayMs { get; set; } = 10;

    /// <summary>
    /// Maximum queue size for each operator before considering it under pressure.
    /// </summary>
    public int DefaultMaxQueueSize { get; set; } = 1000;
}

/// <summary>
/// Metrics for monitoring queue back pressure.
/// </summary>
public class QueueMetrics
{
    public string OperatorId { get; set; } = string.Empty;
    public int CurrentSize { get; set; }
    public int MaxCapacity { get; set; }
    public DateTime LastUpdated { get; set; }
}