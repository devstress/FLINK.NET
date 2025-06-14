using System.Collections.Concurrent;
using Microsoft.Extensions.Logging;

namespace FlinkDotNet.Core.Api.BackPressure;

/// <summary>
/// Apache Flink 2.0 style pipeline back pressure controller that manages back pressure
/// across a complex multi-stage pipeline with proper credit-based flow control.
/// 
/// Supports the pipeline: Gateway -> KeyGen -> IngressProcessing -> AsyncEgressProcessing -> Final Sink
/// </summary>
public class PipelineBackPressureController : IDisposable
{
    private readonly ILogger<PipelineBackPressureController> _logger;
    private readonly PipelineBackPressureConfiguration _config;
    private readonly ConcurrentDictionary<string, PipelineStageMetrics> _stageMetrics;
    private readonly ConcurrentDictionary<string, CreditBasedFlowControl> _stageFlowControls;
    private readonly Timer _monitoringTimer;
    private volatile bool _disposed;

    public PipelineBackPressureController(
        ILogger<PipelineBackPressureController> logger,
        PipelineBackPressureConfiguration? config = null)
    {
        _logger = logger ?? throw new ArgumentNullException(nameof(logger));
        _config = config ?? new PipelineBackPressureConfiguration();
        _stageMetrics = new ConcurrentDictionary<string, PipelineStageMetrics>();
        _stageFlowControls = new ConcurrentDictionary<string, CreditBasedFlowControl>();
        
        // Initialize flow controls for each pipeline stage
        InitializeStageFlowControls();
        
        // Start monitoring back pressure every 2 seconds (Apache Flink 2.0 style)
        _monitoringTimer = new Timer(MonitorPipelineBackPressure, null, 
            TimeSpan.FromSeconds(2), TimeSpan.FromSeconds(2));
        
        _logger.LogInformation("PipelineBackPressureController initialized for Apache Flink 2.0 style pipeline back pressure");
    }

    /// <summary>
    /// Initialize credit-based flow controls for each pipeline stage
    /// </summary>
    private void InitializeStageFlowControls()
    {
        var stages = new[]
        {
            PipelineStage.Gateway,
            PipelineStage.KeyGen,
            PipelineStage.IngressProcessing,
            PipelineStage.AsyncEgressProcessing,
            PipelineStage.FinalSink
        };

        foreach (var stage in stages)
        {
            var stageConfig = GetStageConfiguration(stage);
            _stageFlowControls[stage] = new CreditBasedFlowControl(stage, stageConfig);
            _stageMetrics[stage] = new PipelineStageMetrics { StageName = stage };
            
            _logger.LogDebug("Initialized flow control for stage {Stage} with buffer size {BufferSize}", 
                stage, stageConfig.MaxBufferSize);
        }
    }

    /// <summary>
    /// Get stage-specific configuration based on Apache Flink 2.0 best practices
    /// </summary>
    private StageFlowConfiguration GetStageConfiguration(string stage)
    {
        return stage switch
        {
            PipelineStage.Gateway => new StageFlowConfiguration
            {
                MaxBufferSize = _config.GatewayBufferSize,
                BackPressureThreshold = 0.7, // Gateway should throttle early
                CreditReplenishRate = _config.GatewayCreditReplenishRate
            },
            PipelineStage.KeyGen => new StageFlowConfiguration
            {
                MaxBufferSize = _config.KeyGenBufferSize,
                BackPressureThreshold = 0.8, // KeyGen can handle more load
                CreditReplenishRate = _config.KeyGenCreditReplenishRate
            },
            PipelineStage.IngressProcessing => new StageFlowConfiguration
            {
                MaxBufferSize = _config.IngressProcessingBufferSize,
                BackPressureThreshold = 0.75, // Validation stage needs stability
                CreditReplenishRate = _config.IngressProcessingCreditReplenishRate
            },
            PipelineStage.AsyncEgressProcessing => new StageFlowConfiguration
            {
                MaxBufferSize = _config.AsyncEgressBufferSize,
                BackPressureThreshold = 0.9, // External I/O can queue more
                CreditReplenishRate = _config.AsyncEgressCreditReplenishRate
            },
            PipelineStage.FinalSink => new StageFlowConfiguration
            {
                MaxBufferSize = _config.FinalSinkBufferSize,
                BackPressureThreshold = 0.85, // Sink acknowledgments
                CreditReplenishRate = _config.FinalSinkCreditReplenishRate
            },
            _ => new StageFlowConfiguration() // Default configuration
        };
    }

    /// <summary>
    /// Monitor pipeline back pressure and apply Apache Flink 2.0 style throttling
    /// </summary>
    private void MonitorPipelineBackPressure(object? state)
    {
        if (_disposed) return;

        try
        {
            // Calculate overall pipeline pressure
            var overallPressure = CalculateOverallPipelinePressure();
            
            // Apply back pressure decisions for each stage
            ApplyPipelineBackPressureDecisions(overallPressure);
            
            // Log system health
            LogPipelineHealth(overallPressure);
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "Error monitoring pipeline back pressure");
        }
    }

    /// <summary>
    /// Calculate overall pipeline pressure using Apache Flink 2.0 heuristics
    /// </summary>
    private double CalculateOverallPipelinePressure()
    {
        if (_stageMetrics.IsEmpty) return 0.0;

        var totalPressure = 0.0;
        var stageCount = 0;

        foreach (var (stageName, metrics) in _stageMetrics)
        {
            if (_stageFlowControls.TryGetValue(stageName, out var flowControl))
            {
                var stagePressure = CalculateStagePressure(stageName, metrics, flowControl);
                totalPressure += stagePressure;
                stageCount++;
                
                metrics.BackPressureLevel = stagePressure;
                metrics.LastUpdated = DateTime.UtcNow;
            }
        }

        return stageCount > 0 ? totalPressure / stageCount : 0.0;
    }

    /// <summary>
    /// Calculate back pressure for a specific pipeline stage
    /// </summary>
    private double CalculateStagePressure(string stageName, PipelineStageMetrics metrics, CreditBasedFlowControl flowControl)
    {
        // Queue utilization pressure
        var queuePressure = (double)metrics.QueueSize / Math.Max(1, metrics.MaxQueueSize);
        
        // Credit availability pressure
        var creditPressure = flowControl.GetBackPressureLevel();
        
        // Processing latency pressure
        var latencyPressure = Math.Min(metrics.ProcessingLatencyMs / 1000.0, 1.0); // Normalize to 1 second
        
        // Error rate pressure
        var errorPressure = Math.Min(metrics.ErrorRate, 1.0);
        
        // Combine pressures with stage-specific weighting
        var stagePressure = stageName switch
        {
            PipelineStage.Gateway => Math.Max(queuePressure, creditPressure) * 0.8 + latencyPressure * 0.2,
            PipelineStage.KeyGen => queuePressure * 0.6 + creditPressure * 0.3 + latencyPressure * 0.1,
            PipelineStage.IngressProcessing => queuePressure * 0.5 + creditPressure * 0.3 + errorPressure * 0.2,
            PipelineStage.AsyncEgressProcessing => creditPressure * 0.4 + latencyPressure * 0.4 + errorPressure * 0.2,
            PipelineStage.FinalSink => queuePressure * 0.7 + creditPressure * 0.3,
            _ => Math.Max(queuePressure, creditPressure)
        };

        return Math.Min(stagePressure, 1.0);
    }

    /// <summary>
    /// Apply back pressure decisions across the pipeline
    /// </summary>
    private void ApplyPipelineBackPressureDecisions(double overallPressure)
    {
        foreach (var (stageName, flowControl) in _stageFlowControls)
        {
            if (_stageMetrics.TryGetValue(stageName, out var metrics))
            {
                var stagePressure = metrics.BackPressureLevel;
                
                // Apply Apache Flink 2.0 style throttling decisions
                if (stagePressure > 0.8)
                {
                    ApplyHighPressureThrottling(stageName, stagePressure);
                }
                else if (stagePressure > 0.6)
                {
                    ApplyModeratePressureThrottling(stageName, stagePressure);
                }
                else if (stagePressure < 0.3)
                {
                    RelievePressureThrottling(stageName);
                }
            }
        }
    }

    /// <summary>
    /// Apply high pressure throttling to a pipeline stage
    /// </summary>
    private void ApplyHighPressureThrottling(string stageName, double pressure)
    {
        if (_stageFlowControls.TryGetValue(stageName, out var flowControl))
        {
            // Reduce credit allocation significantly
            flowControl.ReduceCreditsForBackPressure(0.5); // 50% reduction
            
            _logger.LogWarning("High back pressure detected in stage {Stage} (pressure: {Pressure:F2}), applying significant throttling",
                stageName, pressure);
        }
    }

    /// <summary>
    /// Apply moderate pressure throttling to a pipeline stage
    /// </summary>
    private void ApplyModeratePressureThrottling(string stageName, double pressure)
    {
        if (_stageFlowControls.TryGetValue(stageName, out var flowControl))
        {
            // Reduce credit allocation moderately
            flowControl.ReduceCreditsForBackPressure(0.8); // 20% reduction
            
            _logger.LogInformation("Moderate back pressure detected in stage {Stage} (pressure: {Pressure:F2}), applying throttling",
                stageName, pressure);
        }
    }

    /// <summary>
    /// Relieve pressure throttling for a pipeline stage
    /// </summary>
    private void RelievePressureThrottling(string stageName)
    {
        if (_stageFlowControls.TryGetValue(stageName, out var flowControl))
        {
            // Restore normal credit allocation
            flowControl.RestoreNormalCredits();
        }
    }

    /// <summary>
    /// Log overall pipeline health status
    /// </summary>
    private void LogPipelineHealth(double overallPressure)
    {
        var healthStatus = overallPressure switch
        {
            < 0.3 => "HEALTHY",
            < 0.6 => "MODERATE_PRESSURE",
            < 0.8 => "HIGH_PRESSURE",
            _ => "CRITICAL_PRESSURE"
        };

        _logger.LogInformation("Pipeline Health: {Status}, Overall Pressure: {Pressure:F2}", 
            healthStatus, overallPressure);

        // Log individual stage status
        foreach (var (stageName, metrics) in _stageMetrics)
        {
            _logger.LogDebug("Stage {Stage}: Pressure {Pressure:F2}, Queue {Queue}/{MaxQueue}, Latency {Latency}ms",
                stageName, metrics.BackPressureLevel, metrics.QueueSize, metrics.MaxQueueSize, metrics.ProcessingLatencyMs);
        }
    }

    /// <summary>
    /// Request credits for processing data in a specific pipeline stage
    /// </summary>
    public int RequestCredits(string stageName, int requestedCredits)
    {
        if (_stageFlowControls.TryGetValue(stageName, out var flowControl))
        {
            return flowControl.RequestCredits(requestedCredits);
        }
        
        _logger.LogWarning("Unknown pipeline stage: {Stage}", stageName);
        return 0;
    }

    /// <summary>
    /// Replenish credits when processing completes in a pipeline stage
    /// </summary>
    public void ReplenishCredits(string stageName, int processedRecords)
    {
        if (_stageFlowControls.TryGetValue(stageName, out var flowControl))
        {
            flowControl.ReplenishCredits(processedRecords);
        }
    }

    /// <summary>
    /// Update stage metrics for back pressure calculation
    /// </summary>
    public void UpdateStageMetrics(string stageName, int queueSize, int maxQueueSize, double processingLatencyMs, double errorRate = 0.0)
    {
        if (_stageMetrics.TryGetValue(stageName, out var metrics))
        {
            metrics.QueueSize = queueSize;
            metrics.MaxQueueSize = maxQueueSize;
            metrics.ProcessingLatencyMs = processingLatencyMs;
            metrics.ErrorRate = errorRate;
            metrics.LastUpdated = DateTime.UtcNow;
        }
    }

    /// <summary>
    /// Check if a specific pipeline stage should throttle intake
    /// </summary>
    public bool ShouldThrottleStage(string stageName)
    {
        if (_stageMetrics.TryGetValue(stageName, out var metrics))
        {
            return metrics.BackPressureLevel > 0.8;
        }
        return false;
    }

    /// <summary>
    /// Get throttle delay for a specific pipeline stage
    /// </summary>
    public int GetStageThrottleDelayMs(string stageName)
    {
        if (_stageMetrics.TryGetValue(stageName, out var metrics))
        {
            return metrics.BackPressureLevel switch
            {
                > 0.9 => 100, // High delay for critical pressure
                > 0.8 => 50,  // Moderate delay for high pressure
                > 0.6 => 20,  // Small delay for moderate pressure
                _ => 0        // No delay for low pressure
            };
        }
        return 0;
    }

    /// <summary>
    /// Get pipeline back pressure status for monitoring
    /// </summary>
    public PipelineBackPressureStatus GetPipelineStatus()
    {
        var overallPressure = CalculateOverallPipelinePressure();
        var stageStatuses = _stageMetrics.ToDictionary(
            kvp => kvp.Key,
            kvp => new StageBackPressureStatus
            {
                StageName = kvp.Key,
                BackPressureLevel = kvp.Value.BackPressureLevel,
                QueueUtilization = (double)kvp.Value.QueueSize / Math.Max(1, kvp.Value.MaxQueueSize),
                ProcessingLatencyMs = kvp.Value.ProcessingLatencyMs,
                ErrorRate = kvp.Value.ErrorRate,
                LastUpdated = kvp.Value.LastUpdated
            });

        return new PipelineBackPressureStatus
        {
            OverallPressureLevel = overallPressure,
            StageStatuses = stageStatuses,
            Timestamp = DateTime.UtcNow
        };
    }

    public void Dispose()
    {
        if (_disposed) return;
        
        _disposed = true;
        _monitoringTimer?.Dispose();
        
        foreach (var flowControl in _stageFlowControls.Values)
        {
            flowControl.Dispose();
        }
        
        _stageFlowControls.Clear();
        _stageMetrics.Clear();
        
        _logger.LogInformation("PipelineBackPressureController disposed");
    }
}

/// <summary>
/// Pipeline stage names for Apache Flink 2.0 style processing
/// </summary>
public static class PipelineStage
{
    public const string Gateway = "Gateway";
    public const string KeyGen = "KeyGen";
    public const string IngressProcessing = "IngressProcessing";
    public const string AsyncEgressProcessing = "AsyncEgressProcessing";
    public const string FinalSink = "FinalSink";
}

/// <summary>
/// Configuration for pipeline back pressure system
/// </summary>
public class PipelineBackPressureConfiguration
{
    // Gateway stage configuration
    public int GatewayBufferSize { get; set; } = 1000;
    public int GatewayCreditReplenishRate { get; set; } = 100;
    
    // KeyGen stage configuration
    public int KeyGenBufferSize { get; set; } = 2000;
    public int KeyGenCreditReplenishRate { get; set; } = 200;
    
    // IngressProcessing stage configuration
    public int IngressProcessingBufferSize { get; set; } = 1500;
    public int IngressProcessingCreditReplenishRate { get; set; } = 150;
    
    // AsyncEgressProcessing stage configuration
    public int AsyncEgressBufferSize { get; set; } = 3000;
    public int AsyncEgressCreditReplenishRate { get; set; } = 300;
    
    // FinalSink stage configuration
    public int FinalSinkBufferSize { get; set; } = 2500;
    public int FinalSinkCreditReplenishRate { get; set; } = 250;
    
    // Global settings
    public TimeSpan MonitoringInterval { get; set; } = TimeSpan.FromSeconds(2);
    public double CriticalPressureThreshold { get; set; } = 0.9;
    public double HighPressureThreshold { get; set; } = 0.8;
    public double ModeratePressureThreshold { get; set; } = 0.6;
}

/// <summary>
/// Metrics for a specific pipeline stage
/// </summary>
public class PipelineStageMetrics
{
    public string StageName { get; set; } = string.Empty;
    public int QueueSize { get; set; }
    public int MaxQueueSize { get; set; }
    public double ProcessingLatencyMs { get; set; }
    public double ErrorRate { get; set; }
    public double BackPressureLevel { get; set; }
    public DateTime LastUpdated { get; set; }
}

/// <summary>
/// Overall pipeline back pressure status
/// </summary>
public class PipelineBackPressureStatus
{
    public double OverallPressureLevel { get; set; }
    public Dictionary<string, StageBackPressureStatus> StageStatuses { get; set; } = new();
    public DateTime Timestamp { get; set; }
}

/// <summary>
/// Back pressure status for a specific stage
/// </summary>
public class StageBackPressureStatus
{
    public string StageName { get; set; } = string.Empty;
    public double BackPressureLevel { get; set; }
    public double QueueUtilization { get; set; }
    public double ProcessingLatencyMs { get; set; }
    public double ErrorRate { get; set; }
    public DateTime LastUpdated { get; set; }
}