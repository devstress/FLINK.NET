using System.Diagnostics.CodeAnalysis;

namespace FlinkDotNet.Core.Api.BackPressure;

/// <summary>
/// Flink.Net style credit-based flow control for individual pipeline stages.
/// Manages credits and back pressure for a specific stage in the processing pipeline.
/// </summary>
[SuppressMessage("Design", "S3881:Fix this implementation of 'IDisposable' to conform to the dispose pattern", Justification = "Simple disposal pattern is sufficient for this flow control class")]
public class CreditBasedFlowControl : IDisposable
{
    private readonly string _stageName;
    [SuppressMessage("Performance", "S4487:Remove this unread private field '_config' or refactor the code to use its value", Justification = "Configuration may be used for future enhancements")]
    private readonly StageFlowConfiguration _config;
    private readonly object _lock = new object();
    
    private int _availableCredits;
    private readonly int _maxCredits;
    private long _totalCreditsRequested;
    private long _totalCreditsGranted;
    private double _creditReductionFactor = 1.0;
    private DateTime _lastActivity;
    private volatile bool _disposed;

    public CreditBasedFlowControl(string stageName, StageFlowConfiguration config)
    {
        _stageName = stageName ?? throw new ArgumentNullException(nameof(stageName));
        _config = config ?? throw new ArgumentNullException(nameof(config));
        
        _maxCredits = config.MaxBufferSize;
        _availableCredits = _maxCredits;
        _lastActivity = DateTime.UtcNow;
    }

    /// <summary>
    /// Request credits for processing data in this stage
    /// </summary>
    public int RequestCredits(int requestedCredits)
    {
        if (_disposed || requestedCredits <= 0) return 0;

        lock (_lock)
        {
            _totalCreditsRequested += requestedCredits;
            
            // Apply credit reduction factor for back pressure
            var adjustedAvailableCredits = (int)(_availableCredits * _creditReductionFactor);
            var grantedCredits = Math.Min(requestedCredits, adjustedAvailableCredits);
            
            _availableCredits -= grantedCredits;
            _totalCreditsGranted += grantedCredits;
            _lastActivity = DateTime.UtcNow;
            
            return grantedCredits;
        }
    }

    /// <summary>
    /// Replenish credits when processing completes
    /// </summary>
    public void ReplenishCredits(int processedRecords)
    {
        if (_disposed || processedRecords <= 0) return;

        lock (_lock)
        {
            _availableCredits = Math.Min(_maxCredits, _availableCredits + processedRecords);
            _lastActivity = DateTime.UtcNow;
        }
    }

    /// <summary>
    /// Reduce credits for back pressure management
    /// </summary>
    public void ReduceCreditsForBackPressure(double reductionFactor)
    {
        if (_disposed) return;

        lock (_lock)
        {
            _creditReductionFactor = Math.Max(0.1, Math.Min(1.0, reductionFactor));
        }
    }

    /// <summary>
    /// Restore normal credit allocation
    /// </summary>
    public void RestoreNormalCredits()
    {
        if (_disposed) return;

        lock (_lock)
        {
            _creditReductionFactor = 1.0;
        }
    }

    /// <summary>
    /// Get current back pressure level (0.0 to 1.0)
    /// </summary>
    public double GetBackPressureLevel()
    {
        if (_disposed) return 0.0;

        lock (_lock)
        {
            // Back pressure is based on credit utilization and reduction factor
            var creditUtilization = 1.0 - ((double)_availableCredits / _maxCredits);
            var reductionPressure = 1.0 - _creditReductionFactor;
            
            return Math.Max(creditUtilization, reductionPressure);
        }
    }

    /// <summary>
    /// Get current available credits
    /// </summary>
    public int AvailableCredits
    {
        get
        {
            lock (_lock)
            {
                return (int)(_availableCredits * _creditReductionFactor);
            }
        }
    }

    /// <summary>
    /// Get maximum credits for this stage
    /// </summary>
    public int MaxCredits => _maxCredits;

    /// <summary>
    /// Get stage name
    /// </summary>
    public string StageName => _stageName;

    /// <summary>
    /// Get credit statistics for monitoring
    /// </summary>
    public CreditStatistics GetStatistics()
    {
        lock (_lock)
        {
            return new CreditStatistics
            {
                StageName = _stageName,
                AvailableCredits = _availableCredits,
                MaxCredits = _maxCredits,
                CreditReductionFactor = _creditReductionFactor,
                TotalCreditsRequested = _totalCreditsRequested,
                TotalCreditsGranted = _totalCreditsGranted,
                BackPressureLevel = GetBackPressureLevel(),
                LastActivity = _lastActivity
            };
        }
    }

    public void Dispose()
    {
        _disposed = true;
    }
}

/// <summary>
/// Configuration for stage-specific flow control
/// </summary>
public class StageFlowConfiguration
{
    /// <summary>
    /// Maximum buffer size (credits) for this stage
    /// </summary>
    public int MaxBufferSize { get; set; } = 1000;

    /// <summary>
    /// Back pressure threshold (0.0 to 1.0)
    /// </summary>
    public double BackPressureThreshold { get; set; } = 0.8;

    /// <summary>
    /// Rate at which credits are replenished per second
    /// </summary>
    public int CreditReplenishRate { get; set; } = 100;

    /// <summary>
    /// Timeout for credit allocation requests
    /// </summary>
    public TimeSpan CreditRequestTimeout { get; set; } = TimeSpan.FromMilliseconds(100);
}

/// <summary>
/// Statistics for credit-based flow control monitoring
/// </summary>
public class CreditStatistics
{
    public string StageName { get; set; } = string.Empty;
    public int AvailableCredits { get; set; }
    public int MaxCredits { get; set; }
    public double CreditReductionFactor { get; set; }
    public long TotalCreditsRequested { get; set; }
    public long TotalCreditsGranted { get; set; }
    public double BackPressureLevel { get; set; }
    public DateTime LastActivity { get; set; }
    
    /// <summary>
    /// Calculate credit efficiency (granted/requested ratio)
    /// </summary>
    public double CreditEfficiency => 
        TotalCreditsRequested > 0 ? (double)TotalCreditsGranted / TotalCreditsRequested : 1.0;
}