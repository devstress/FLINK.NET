using FlinkDotNet.Storage.RocksDB;
using FlinkDotNet.JobManager.Checkpointing;
using System.Collections.Concurrent;

namespace FlinkDotNet.JobManager.Services.StateManagement;

/// <summary>
/// FlinkDotnet 2.0 style state coordinator that manages RocksDB state backends
/// across TaskManager instances, providing centralized state lifecycle management
/// and distributed checkpointing coordination.
/// </summary>
public class StateCoordinator : IDisposable
{
    private readonly ILogger<StateCoordinator> _logger;
    private readonly ConcurrentDictionary<string, RocksDBStateBackend> _stateBackends;
    private readonly ConcurrentDictionary<string, StateMetrics> _stateMetrics;
    private readonly Timer _metricsTimer;
    private readonly CheckpointCoordinator _checkpointCoordinator;
    private bool _disposed;

    public StateCoordinator(ILogger<StateCoordinator> logger, CheckpointCoordinator checkpointCoordinator)
    {
        _logger = logger ?? throw new ArgumentNullException(nameof(logger));
        _checkpointCoordinator = checkpointCoordinator ?? throw new ArgumentNullException(nameof(checkpointCoordinator));
        _stateBackends = new ConcurrentDictionary<string, RocksDBStateBackend>();
        _stateMetrics = new ConcurrentDictionary<string, StateMetrics>();
        
        // FlinkDotnet 2.0 style metrics collection every 10 seconds
        _metricsTimer = new Timer(CollectMetrics, null, TimeSpan.FromSeconds(10), TimeSpan.FromSeconds(10));
        
        _logger.LogInformation("StateCoordinator initialized with RocksDB backend management");
    }

    /// <summary>
    /// Creates and manages a RocksDB state backend for a specific TaskManager instance.
    /// This follows FlinkDotnet 2.0 pattern of centralized state lifecycle management.
    /// </summary>
    public async Task<string> CreateStateBackendAsync(string taskManagerId, string jobId, StateBackendConfig config)
    {
        var stateBackendId = $"{taskManagerId}-{jobId}-{Guid.NewGuid():N}";
        
        try
        {
            var rocksDbConfig = new RocksDBConfiguration
            {
                DbPath = Path.Combine(config.StateDir, stateBackendId),
                ColumnFamilies = config.ColumnFamilies ?? new[] { "default", "user_state", "operator_state" },
                WriteBufferSize = config.WriteBufferSize,
                MaxBackgroundJobs = config.MaxBackgroundJobs,
                BlockBasedTableOptions = new BlockBasedTableOptions
                {
                    BlockSize = 64 * 1024, // 64KB blocks for optimal SSD performance
                    CacheSize = 256 * 1024 * 1024, // 256MB cache
                    BloomFilterBitsPerKey = 10
                }
            };

            var loggerFactory = LoggerFactory.Create(builder => builder.AddConsole());
            var rocksDbLogger = loggerFactory.CreateLogger<RocksDBStateBackend>();
            var stateBackend = new RocksDBStateBackend(rocksDbConfig, rocksDbLogger);
            await stateBackend.InitializeAsync();

            _stateBackends.TryAdd(stateBackendId, stateBackend);
            _stateMetrics.TryAdd(stateBackendId, new StateMetrics());

            _logger.LogInformation("Created RocksDB state backend {StateBackendId} for TaskManager {TaskManagerId}, Job {JobId}", 
                stateBackendId, taskManagerId, jobId);

            // Register with checkpoint coordinator for distributed checkpointing
            await _checkpointCoordinator.RegisterStateBackendAsync(stateBackendId, stateBackend);

            return stateBackendId;
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "Failed to create state backend for TaskManager {TaskManagerId}, Job {JobId}", taskManagerId, jobId);
            throw new InvalidOperationException($"State backend creation failed for TaskManager {taskManagerId} in Job {jobId}", ex);
        }
    }

    /// <summary>
    /// Removes and properly disposes a state backend when TaskManager shuts down or job finishes.
    /// Ensures proper cleanup of RocksDB resources.
    /// </summary>
    public async Task RemoveStateBackendAsync(string stateBackendId)
    {
        try
        {
            if (_stateBackends.TryRemove(stateBackendId, out var stateBackend))
            {
                await _checkpointCoordinator.UnregisterStateBackendAsync(stateBackendId);
                await stateBackend.DisposeAsync();
                _stateMetrics.TryRemove(stateBackendId, out _);
                
                _logger.LogInformation("Removed and disposed state backend {StateBackendId}", stateBackendId);
            }
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "Error removing state backend {StateBackendId}", stateBackendId);
        }
    }

    /// <summary>
    /// Gets state backend metrics for monitoring and back pressure detection.
    /// Used by BackPressureCoordinator to make scaling decisions.
    /// </summary>
    public StateMetrics? GetStateMetrics(string stateBackendId)
    {
        return _stateMetrics.TryGetValue(stateBackendId, out var metrics) ? metrics : null;
    }

    /// <summary>
    /// Gets all state backends for comprehensive monitoring.
    /// </summary>
    public IReadOnlyDictionary<string, StateMetrics> GetAllStateMetrics()
    {
        return _stateMetrics.ToDictionary(kvp => kvp.Key, kvp => kvp.Value);
    }

    /// <summary>
    /// Triggers a coordinated checkpoint across all managed state backends.
    /// This is called by the CheckpointCoordinator for consistent distributed snapshots.
    /// </summary>
    public async Task<bool> TriggerCheckpointAsync(long checkpointId)
    {
        _logger.LogInformation("Triggering checkpoint {CheckpointId} across {Count} state backends", 
            checkpointId, _stateBackends.Count);

        var checkpointTasks = _stateBackends.Select(async kvp =>
        {
            var (stateBackendId, stateBackend) = kvp;
            try
            {
                await stateBackend.CreateCheckpointAsync(checkpointId);
                return true;
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "Checkpoint {CheckpointId} failed for state backend {StateBackendId}", 
                    checkpointId, stateBackendId);
                return false;
            }
        });

        var results = await Task.WhenAll(checkpointTasks);
        var successCount = results.Count(r => r);
        
        _logger.LogInformation("Checkpoint {CheckpointId} completed: {SuccessCount}/{TotalCount} state backends succeeded", 
            checkpointId, successCount, _stateBackends.Count);

        return successCount == _stateBackends.Count;
    }

    private void CollectMetrics(object? state)
    {
        try
        {
            foreach (var kvp in _stateBackends)
            {
                var stateBackendId = kvp.Key;
                var stateBackend = kvp.Value;

                if (_stateMetrics.TryGetValue(stateBackendId, out var metrics))
                {
                    // Collect RocksDB statistics
                    metrics.UpdateFromRocksDB(stateBackend);
                    
                    // Log critical metrics for monitoring
                    if (metrics.MemoryUsageBytes > 1024 * 1024 * 1024) // > 1GB
                    {
                        _logger.LogWarning("High memory usage in state backend {StateBackendId}: {MemoryUsage} MB", 
                            stateBackendId, metrics.MemoryUsageBytes / 1024 / 1024);
                    }
                }
            }
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "Error collecting state metrics");
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
            _metricsTimer?.Dispose();
            
            // Dispose all state backends
            foreach (var stateBackend in _stateBackends.Values)
            {
                try
                {
                    stateBackend.DisposeAsync().AsTask().Wait(TimeSpan.FromSeconds(10));
                }
                catch (Exception ex)
                {
                    _logger.LogError(ex, "Error disposing state backend during shutdown");
                }
            }
            
            _stateBackends.Clear();
            _stateMetrics.Clear();
            _disposed = true;
        }
    }
}

/// <summary>
/// Configuration for RocksDB state backend creation
/// </summary>
public class StateBackendConfig
{
    public string StateDir { get; set; } = Path.Combine(Path.GetTempPath(), "flink-state-" + Environment.ProcessId);
    public string[]? ColumnFamilies { get; set; }
    public ulong WriteBufferSize { get; set; } = 64 * 1024 * 1024; // 64MB
    public int MaxBackgroundJobs { get; set; } = 4;
}

/// <summary>
/// Real-time metrics for state backend monitoring and back pressure detection
/// </summary>
public class StateMetrics
{
    public long MemoryUsageBytes { get; private set; }
    public long DiskUsageBytes { get; private set; }
    public double CpuUsagePercent { get; private set; }
    public long RecordsPerSecond { get; private set; }
    public double WriteLatencyMs { get; private set; }
    public double ReadLatencyMs { get; private set; }
    public DateTime LastUpdated { get; private set; }

    public void UpdateFromRocksDB(RocksDBStateBackend stateBackend)
    {
        try
        {
            // Get RocksDB statistics
            var stats = stateBackend.GetStatistics();
            
            MemoryUsageBytes = stats.MemoryUsage;
            DiskUsageBytes = stats.DiskUsage;
            WriteLatencyMs = stats.AverageWriteLatencyMs;
            ReadLatencyMs = stats.AverageReadLatencyMs;
            CpuUsagePercent = stats.CpuUsagePercent; // Add CPU usage tracking
            LastUpdated = DateTime.UtcNow;
            
            // Calculate records per second from previous measurement
            // This would be enhanced with proper time-window calculations
            RecordsPerSecond = stats.WritesPerSecond;
        }
        catch (Exception)
        {
            // Silently handle metrics collection errors
            LastUpdated = DateTime.UtcNow;
        }
    }

    /// <summary>
    /// Determines if this state backend is under pressure and needs scaling.
    /// Uses FlinkDotnet 2.0 style heuristics.
    /// </summary>
    public bool IsUnderPressure()
    {
        return MemoryUsageBytes > 800 * 1024 * 1024 || // > 800MB memory usage
               WriteLatencyMs > 100 || // > 100ms write latency
               CpuUsagePercent > 80; // > 80% CPU usage
    }
}