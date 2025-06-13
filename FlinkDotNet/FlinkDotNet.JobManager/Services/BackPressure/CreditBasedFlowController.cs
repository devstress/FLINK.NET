using System.Collections.Concurrent;

namespace FlinkDotNet.JobManager.Services.BackPressure;

/// <summary>
/// Apache Flink 2.0 style credit-based flow control system that prevents
/// system overload by regulating data flow between operators and task managers.
/// </summary>
public class CreditBasedFlowController : IDisposable
{
    private readonly ILogger<CreditBasedFlowController> _logger;
    private readonly ConcurrentDictionary<string, OperatorCredit> _operatorCredits;
    private readonly CreditFlowConfiguration _config;
    private bool _disposed;

    public CreditBasedFlowController(
        ILogger<CreditBasedFlowController> logger,
        CreditFlowConfiguration? config = null)
    {
        _logger = logger ?? throw new ArgumentNullException(nameof(logger));
        _config = config ?? new CreditFlowConfiguration();
        _operatorCredits = new ConcurrentDictionary<string, OperatorCredit>();
        
        _logger.LogInformation("CreditBasedFlowController initialized with buffer size: {BufferSize}, threshold: {Threshold}", 
            _config.MaxBufferSize, _config.BackPressureThreshold);
    }

    /// <summary>
    /// Requests credits for an operator to send data downstream.
    /// Returns the number of credits granted (may be less than requested).
    /// </summary>
    public int RequestCredits(string operatorId, int requestedCredits)
    {
        var credit = _operatorCredits.GetOrAdd(operatorId, id => new OperatorCredit(id, _config));
        
        var grantedCredits = credit.RequestCredits(requestedCredits);
        
        if (grantedCredits < requestedCredits)
        {
            _logger.LogDebug("Back pressure detected for operator {OperatorId}: requested {Requested}, granted {Granted}", 
                operatorId, requestedCredits, grantedCredits);
        }
        
        return grantedCredits;
    }

    /// <summary>
    /// Replenishes credits for an operator when downstream processing completes.
    /// </summary>
    public void ReplenishCredits(string operatorId, int processedRecords)
    {
        if (_operatorCredits.TryGetValue(operatorId, out var credit))
        {
            credit.ReplenishCredits(processedRecords);
        }
    }

    /// <summary>
    /// Gets the current back pressure level for an operator (0.0 to 1.0).
    /// </summary>
    public double GetBackPressureLevel(string operatorId)
    {
        if (_operatorCredits.TryGetValue(operatorId, out var credit))
        {
            return credit.GetBackPressureLevel();
        }
        return 0.0;
    }

    /// <summary>
    /// Gets overall system back pressure level across all operators.
    /// </summary>
    public double GetSystemBackPressureLevel()
    {
        if (_operatorCredits.IsEmpty) return 0.0;
        
        return _operatorCredits.Values.Average(c => c.GetBackPressureLevel());
    }

    /// <summary>
    /// Determines if the system should throttle new data intake.
    /// </summary>
    public bool ShouldThrottle()
    {
        return GetSystemBackPressureLevel() > _config.BackPressureThreshold;
    }

    /// <summary>
    /// Gets all operator credit information for monitoring.
    /// </summary>
    public IReadOnlyDictionary<string, OperatorCreditInfo> GetAllOperatorCredits()
    {
        return _operatorCredits.ToDictionary(
            kvp => kvp.Key, 
            kvp => new OperatorCreditInfo
            {
                OperatorId = kvp.Value.OperatorId,
                AvailableCredits = kvp.Value.AvailableCredits,
                TotalBufferSize = kvp.Value.TotalBufferSize,
                BackPressureLevel = kvp.Value.GetBackPressureLevel()
            });
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
            _operatorCredits.Clear();
            _disposed = true;
            _logger.LogInformation("CreditBasedFlowController disposed");
        }
    }
}

/// <summary>
/// Credit management for a single operator in the flow control system.
/// </summary>
public class OperatorCredit
{
    private readonly object _lock = new object();
    private readonly CreditFlowConfiguration _config;
    private int _availableCredits;
    private long _totalRequestedCredits;
    private long _totalGrantedCredits;
    private DateTime _lastActivity;

    public string OperatorId { get; }
    public int AvailableCredits => _availableCredits;
    public int TotalBufferSize => _config.MaxBufferSize;

    public OperatorCredit(string operatorId, CreditFlowConfiguration config)
    {
        OperatorId = operatorId;
        _config = config;
        _availableCredits = config.MaxBufferSize;
        _lastActivity = DateTime.UtcNow;
    }

    public int RequestCredits(int requestedCredits)
    {
        lock (_lock)
        {
            _totalRequestedCredits += requestedCredits;
            var grantedCredits = Math.Min(requestedCredits, _availableCredits);
            _availableCredits -= grantedCredits;
            _totalGrantedCredits += grantedCredits;
            _lastActivity = DateTime.UtcNow;
            return grantedCredits;
        }
    }

    public void ReplenishCredits(int processedRecords)
    {
        lock (_lock)
        {
            _availableCredits = Math.Min(_config.MaxBufferSize, _availableCredits + processedRecords);
            _lastActivity = DateTime.UtcNow;
        }
    }

    public double GetBackPressureLevel()
    {
        lock (_lock)
        {
            // Back pressure level is the percentage of buffer that's consumed
            return 1.0 - ((double)_availableCredits / _config.MaxBufferSize);
        }
    }
}

/// <summary>
/// Configuration for credit-based flow control system.
/// </summary>
public class CreditFlowConfiguration
{
    /// <summary>
    /// Maximum buffer size per operator (number of records).
    /// </summary>
    public int MaxBufferSize { get; set; } = 1000;

    /// <summary>
    /// Back pressure threshold (0.0 to 1.0). When exceeded, system should throttle.
    /// </summary>
    public double BackPressureThreshold { get; set; } = 0.8;

    /// <summary>
    /// How often to replenish credits for idle operators (to prevent starvation).
    /// </summary>
    public TimeSpan IdleReplenishInterval { get; set; } = TimeSpan.FromSeconds(5);
}

/// <summary>
/// Information about operator credit status for monitoring.
/// </summary>
public class OperatorCreditInfo
{
    public string OperatorId { get; set; } = string.Empty;
    public int AvailableCredits { get; set; }
    public int TotalBufferSize { get; set; }
    public double BackPressureLevel { get; set; }
}