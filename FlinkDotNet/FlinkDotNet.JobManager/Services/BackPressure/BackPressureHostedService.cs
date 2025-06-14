namespace FlinkDotNet.JobManager.Services.BackPressure;

/// <summary>
/// Hosted service that ensures the BackPressureCoordinator is initialized and started
/// when the JobManager application starts, following FlinkDotnet 2.0 patterns.
/// </summary>
public class BackPressureHostedService : IHostedService
{
    private readonly BackPressureCoordinator _backPressureCoordinator;
    private readonly ILogger<BackPressureHostedService> _logger;

    public BackPressureHostedService(
        BackPressureCoordinator backPressureCoordinator,
        ILogger<BackPressureHostedService> logger)
    {
        _backPressureCoordinator = backPressureCoordinator ?? throw new ArgumentNullException(nameof(backPressureCoordinator));
        _logger = logger ?? throw new ArgumentNullException(nameof(logger));
    }

    public Task StartAsync(CancellationToken cancellationToken)
    {
        _logger.LogInformation("BackPressureHostedService starting - initializing FlinkDotnet 2.0 style back pressure coordination");
        
        // The BackPressureCoordinator starts monitoring automatically in its constructor
        // This service just ensures it's instantiated and available for the application lifetime
        
        _logger.LogInformation("BackPressureHostedService started - back pressure monitoring is now active");
        return Task.CompletedTask;
    }

    public Task StopAsync(CancellationToken cancellationToken)
    {
        _logger.LogInformation("BackPressureHostedService stopping - cleaning up back pressure coordination");
        
        // Dispose the coordinator to clean up resources
        _backPressureCoordinator?.Dispose();
        
        _logger.LogInformation("BackPressureHostedService stopped");
        return Task.CompletedTask;
    }
}