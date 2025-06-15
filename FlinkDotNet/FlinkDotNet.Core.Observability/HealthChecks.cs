using FlinkDotNet.Core.Abstractions.Observability;
using Microsoft.Extensions.Diagnostics.HealthChecks;
using Microsoft.Extensions.Logging;

namespace FlinkDotNet.Core.Observability
{
    /// <summary>
    /// Health check for Flink components following Apache Flink patterns.
    /// Integrates with ASP.NET Core health check framework.
    /// </summary>
    public class FlinkComponentHealthCheck : IHealthCheck
    {
        private readonly IFlinkHealthMonitor _healthMonitor;
        private readonly ILogger<FlinkComponentHealthCheck> _logger;

        public FlinkComponentHealthCheck(IFlinkHealthMonitor healthMonitor, ILogger<FlinkComponentHealthCheck> logger)
        {
            _healthMonitor = healthMonitor;
            _logger = logger;
        }

        public async Task<HealthCheckResult> CheckHealthAsync(HealthCheckContext context, 
            CancellationToken cancellationToken = default)
        {
            try
            {
                var healthResults = await _healthMonitor.CheckOverallHealthAsync(cancellationToken);
                
                var overallStatus = HealthStatus.Healthy;
                var data = new Dictionary<string, object>();
                var failedComponents = new List<string>();

                foreach (var result in healthResults)
                {
                    var componentStatus = result.Value.Status switch
                    {
                        FlinkHealthStatus.Healthy => HealthStatus.Healthy,
                        FlinkHealthStatus.Degraded => HealthStatus.Degraded,
                        FlinkHealthStatus.Unhealthy => HealthStatus.Unhealthy,
                        FlinkHealthStatus.Failed => HealthStatus.Unhealthy,
                        _ => HealthStatus.Unhealthy
                    };

                    data[result.Key] = new
                    {
                        Status = result.Value.Status.ToString(),
                        Description = result.Value.Description,
                        Duration = result.Value.CheckDuration.TotalMilliseconds,
                        Data = result.Value.Data
                    };

                    if (componentStatus == HealthStatus.Unhealthy)
                    {
                        overallStatus = HealthStatus.Unhealthy;
                        failedComponents.Add(result.Key);
                    }
                    else if (componentStatus == HealthStatus.Degraded && overallStatus == HealthStatus.Healthy)
                    {
                        overallStatus = HealthStatus.Degraded;
                    }
                }

                string description;
                if (overallStatus == HealthStatus.Healthy)
                {
                    description = "All Flink components are healthy";
                }
                else if (overallStatus == HealthStatus.Degraded)
                {
                    description = "Some Flink components are degraded";
                }
                else
                {
                    description = $"Flink components failed: {string.Join(", ", failedComponents)}";
                }

                _logger.LogInformation("Flink component health check completed: {Status} - {Description}", 
                    overallStatus, description);

                return new HealthCheckResult(overallStatus, description, data: data);
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "Flink component health check failed");
                return new HealthCheckResult(HealthStatus.Unhealthy, 
                    $"Health check failed: {ex.Message}", ex);
            }
        }
    }

    /// <summary>
    /// Health check for Flink jobs following Apache Flink patterns.
    /// Monitors job execution status and performance metrics.
    /// </summary>
    public class FlinkJobHealthCheck : IHealthCheck
    {
        private readonly IFlinkHealthMonitor _healthMonitor;
        private readonly ILogger<FlinkJobHealthCheck> _logger;

        public FlinkJobHealthCheck(IFlinkHealthMonitor healthMonitor, ILogger<FlinkJobHealthCheck> logger)
        {
            _healthMonitor = healthMonitor;
            _logger = logger;
        }

        public async Task<HealthCheckResult> CheckHealthAsync(HealthCheckContext context, 
            CancellationToken cancellationToken = default)
        {
            try
            {
                // Check health of default job (could be made configurable)
                var jobId = "default_job";
                var jobHealth = await _healthMonitor.CheckJobHealthAsync(jobId, cancellationToken);

                var status = jobHealth.Status switch
                {
                    FlinkHealthStatus.Healthy => HealthStatus.Healthy,
                    FlinkHealthStatus.Degraded => HealthStatus.Degraded,
                    FlinkHealthStatus.Unhealthy => HealthStatus.Unhealthy,
                    FlinkHealthStatus.Failed => HealthStatus.Unhealthy,
                    _ => HealthStatus.Unhealthy
                };

                var data = new Dictionary<string, object>
                {
                    ["job_id"] = jobId,
                    ["status"] = jobHealth.Status.ToString(),
                    ["description"] = jobHealth.Description,
                    ["check_duration_ms"] = jobHealth.CheckDuration.TotalMilliseconds,
                    ["job_data"] = jobHealth.Data
                };

                _logger.LogInformation("Flink job health check completed for job {JobId}: {Status}", 
                    jobId, status);

                return new HealthCheckResult(status, jobHealth.Description, data: data);
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "Flink job health check failed");
                return new HealthCheckResult(HealthStatus.Unhealthy, 
                    $"Job health check failed: {ex.Message}", ex);
            }
        }
    }
}