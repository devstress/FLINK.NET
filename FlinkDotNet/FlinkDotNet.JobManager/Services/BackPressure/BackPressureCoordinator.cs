using System.Collections.Concurrent;
using FlinkDotNet.JobManager.Services.StateManagement;

namespace FlinkDotNet.JobManager.Services.BackPressure;

/// <summary>
/// Apache Flink 2.0 style back pressure coordinator that monitors system pressure
/// and coordinates dynamic scaling of TaskManager instances to handle load fluctuations.
/// </summary>
public class BackPressureCoordinator : IDisposable
{
    private readonly ILogger<BackPressureCoordinator> _logger;
    private readonly StateCoordinator _stateCoordinator;
    private readonly TaskManagerOrchestrator _taskManagerOrchestrator;
    private readonly ConcurrentDictionary<string, BackPressureMetrics> _pressureMetrics;
    private readonly Timer _monitoringTimer;
    private readonly BackPressureConfiguration _config;
    private bool _disposed;

    public BackPressureCoordinator(
        ILogger<BackPressureCoordinator> logger,
        StateCoordinator stateCoordinator,
        TaskManagerOrchestrator taskManagerOrchestrator,
        BackPressureConfiguration? config = null)
    {
        _logger = logger ?? throw new ArgumentNullException(nameof(logger));
        _stateCoordinator = stateCoordinator ?? throw new ArgumentNullException(nameof(stateCoordinator));
        _taskManagerOrchestrator = taskManagerOrchestrator ?? throw new ArgumentNullException(nameof(taskManagerOrchestrator));
        _config = config ?? new BackPressureConfiguration();
        _pressureMetrics = new ConcurrentDictionary<string, BackPressureMetrics>();

        // Apache Flink 2.0 style monitoring every 5 seconds
        _monitoringTimer = new Timer(MonitorBackPressure, null, TimeSpan.FromSeconds(5), TimeSpan.FromSeconds(5));
        
        _logger.LogInformation("BackPressureCoordinator initialized with monitoring interval: {Interval}s", 5);
    }

    /// <summary>
    /// Monitors system pressure and triggers scaling decisions
    /// </summary>
    private void MonitorBackPressure(object? state)
    {
        try
        {
            var allStateMetrics = _stateCoordinator.GetAllStateMetrics();
            var taskManagerMetrics = _taskManagerOrchestrator.GetAllTaskManagerMetrics();

            foreach (var (stateBackendId, stateMetrics) in allStateMetrics)
            {
                var pressure = CalculateBackPressure(stateMetrics, taskManagerMetrics);
                _pressureMetrics.AddOrUpdate(stateBackendId, pressure, (key, old) => pressure);

                // Apply scaling decisions based on pressure
                ApplyScalingDecision(stateBackendId, pressure);
            }

            // Log overall system health
            LogSystemHealth();
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "Error monitoring back pressure");
        }
    }

    /// <summary>
    /// Calculates back pressure level using Apache Flink 2.0 heuristics
    /// </summary>
    private BackPressureMetrics CalculateBackPressure(StateMetrics stateMetrics, Dictionary<string, TaskManagerMetrics> taskManagerMetrics)
    {
        var metrics = new BackPressureMetrics
        {
            StateBackendPressure = CalculateStatePressure(stateMetrics),
            NetworkPressure = CalculateNetworkPressure(taskManagerMetrics),
            CpuPressure = CalculateCpuPressure(taskManagerMetrics),
            MemoryPressure = CalculateMemoryPressure(stateMetrics, taskManagerMetrics),
            Timestamp = DateTime.UtcNow
        };

        // Calculate overall pressure level (0.0 to 1.0)
        metrics.OverallPressure = Math.Max(Math.Max(metrics.StateBackendPressure, metrics.NetworkPressure),
                                          Math.Max(metrics.CpuPressure, metrics.MemoryPressure));

        return metrics;
    }

    private static double CalculateStatePressure(StateMetrics stateMetrics)
    {
        // High write latency or memory usage indicates state pressure
        var latencyPressure = Math.Min(stateMetrics.WriteLatencyMs / 1000.0, 1.0); // Normalize to 1 second
        var memoryPressure = Math.Min(stateMetrics.MemoryUsageBytes / (double)(1024 * 1024 * 1024), 1.0); // Normalize to 1GB
        
        return Math.Max(latencyPressure, memoryPressure);
    }

    private static double CalculateNetworkPressure(Dictionary<string, TaskManagerMetrics> taskManagerMetrics)
    {
        if (!taskManagerMetrics.Any()) return 0.0;

        var avgNetworkUtilization = taskManagerMetrics.Values.Average(tm => tm.NetworkUtilizationPercent);
        return Math.Min(avgNetworkUtilization / 100.0, 1.0);
    }

    private static double CalculateCpuPressure(Dictionary<string, TaskManagerMetrics> taskManagerMetrics)
    {
        if (!taskManagerMetrics.Any()) return 0.0;

        var avgCpuUtilization = taskManagerMetrics.Values.Average(tm => tm.CpuUtilizationPercent);
        return Math.Min(avgCpuUtilization / 100.0, 1.0);
    }

    private static double CalculateMemoryPressure(StateMetrics stateMetrics, Dictionary<string, TaskManagerMetrics> taskManagerMetrics)
    {
        var statePressure = Math.Min(stateMetrics.MemoryUsageBytes / (double)(512 * 1024 * 1024), 1.0); // 512MB threshold
        
        if (!taskManagerMetrics.Any()) return statePressure;

        var avgMemoryUtilization = taskManagerMetrics.Values.Average(tm => tm.MemoryUtilizationPercent);
        var taskManagerPressure = Math.Min(avgMemoryUtilization / 100.0, 1.0);

        return Math.Max(statePressure, taskManagerPressure);
    }

    /// <summary>
    /// Applies scaling decisions based on pressure levels
    /// </summary>
    private void ApplyScalingDecision(string stateBackendId, BackPressureMetrics pressure)
    {
        try
        {
            var currentTaskManagers = _taskManagerOrchestrator.GetTaskManagerCount();

            if (pressure.OverallPressure > _config.ScaleUpThreshold)
            {
                if (currentTaskManagers < _config.MaxTaskManagers)
                {
                    _logger.LogInformation("High pressure detected ({Pressure:F2}) for {StateBackendId}. Scaling up TaskManagers.", 
                        pressure.OverallPressure, stateBackendId);
                    
                    _ = Task.Run(async () => await _taskManagerOrchestrator.ScaleUpAsync());
                }
                else
                {
                    _logger.LogWarning("Maximum TaskManager count ({MaxCount}) reached. Cannot scale up further.", _config.MaxTaskManagers);
                }
            }
            else if (pressure.OverallPressure < _config.ScaleDownThreshold && currentTaskManagers > _config.MinTaskManagers)
            {
                _logger.LogInformation("Low pressure detected ({Pressure:F2}) for {StateBackendId}. Scaling down TaskManagers.", 
                    pressure.OverallPressure, stateBackendId);
                
                _ = Task.Run(async () => await _taskManagerOrchestrator.ScaleDownAsync());
            }
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "Error applying scaling decision for {StateBackendId}", stateBackendId);
        }
    }

    private void LogSystemHealth()
    {
        if (_pressureMetrics.IsEmpty) return;

        var avgPressure = _pressureMetrics.Values.Average(p => p.OverallPressure);
        var maxPressure = _pressureMetrics.Values.Max(p => p.OverallPressure);
        var taskManagerCount = _taskManagerOrchestrator.GetTaskManagerCount();

        var healthStatus = avgPressure switch
        {
            < 0.3 => "HEALTHY",
            < 0.7 => "MODERATE",
            < 0.9 => "HIGH_PRESSURE",
            _ => "CRITICAL"
        };

        _logger.LogInformation("System Health: {Status}, Avg Pressure: {AvgPressure:F2}, Max Pressure: {MaxPressure:F2}, TaskManagers: {Count}", 
            healthStatus, avgPressure, maxPressure, taskManagerCount);
    }

    /// <summary>
    /// Gets current back pressure metrics for external monitoring
    /// </summary>
    public IReadOnlyDictionary<string, BackPressureMetrics> GetBackPressureMetrics()
    {
        return _pressureMetrics.ToDictionary(kvp => kvp.Key, kvp => kvp.Value);
    }

    /// <summary>
    /// Gets overall system pressure level (0.0 to 1.0)
    /// </summary>
    public double GetOverallSystemPressure()
    {
        return _pressureMetrics.IsEmpty ? 0.0 : _pressureMetrics.Values.Average(p => p.OverallPressure);
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
            _pressureMetrics.Clear();
            _disposed = true;
            
            _logger.LogInformation("BackPressureCoordinator disposed");
        }
    }
}

/// <summary>
/// Configuration for back pressure monitoring and scaling
/// </summary>
public class BackPressureConfiguration
{
    public double ScaleUpThreshold { get; set; } = 0.8; // 80% pressure triggers scale up
    public double ScaleDownThreshold { get; set; } = 0.3; // 30% pressure triggers scale down
    public int MinTaskManagers { get; set; } = 1;
    public int MaxTaskManagers { get; set; } = 10;
    public TimeSpan CooldownPeriod { get; set; } = TimeSpan.FromMinutes(2); // Prevent rapid scaling
}

/// <summary>
/// Back pressure metrics for system monitoring
/// </summary>
public class BackPressureMetrics
{
    public double StateBackendPressure { get; set; }
    public double NetworkPressure { get; set; }
    public double CpuPressure { get; set; }
    public double MemoryPressure { get; set; }
    public double OverallPressure { get; set; }
    public DateTime Timestamp { get; set; }

    public string GetPressureLevel()
    {
        return OverallPressure switch
        {
            < 0.3 => "LOW",
            < 0.7 => "MODERATE", 
            < 0.9 => "HIGH",
            _ => "CRITICAL"
        };
    }
}