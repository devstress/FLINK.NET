namespace FlinkDotNet.Core.Abstractions.Observability
{
    /// <summary>
    /// Defines health check states following Apache Flink patterns.
    /// Maps to Flink's component health model.
    /// </summary>
    public enum FlinkHealthStatus
    {
        /// <summary>Component is healthy and operating normally.</summary>
        Healthy,
        /// <summary>Component is degraded but still operational.</summary>
        Degraded,
        /// <summary>Component is unhealthy and may fail soon.</summary>
        Unhealthy,
        /// <summary>Component has failed and is not operational.</summary>
        Failed
    }

    /// <summary>
    /// Represents a health check result with detailed information.
    /// </summary>
    public class FlinkHealthCheckResult
    {
        public FlinkHealthStatus Status { get; init; }
        public string ComponentName { get; init; } = string.Empty;
        public string Description { get; init; } = string.Empty;
        public Dictionary<string, object> Data { get; init; } = new();
        public TimeSpan CheckDuration { get; init; }
        public DateTime Timestamp { get; init; } = DateTime.UtcNow;
        public Exception? Exception { get; init; }
    }

    /// <summary>
    /// Provides comprehensive health monitoring following Apache Flink 2.0 standards.
    /// Implements health checks for all critical Flink components.
    /// </summary>
    public interface IFlinkHealthMonitor
    {
        /// <summary>
        /// Checks the health of a specific operator.
        /// Validates operator state, performance, and resource usage.
        /// </summary>
        Task<FlinkHealthCheckResult> CheckOperatorHealthAsync(string operatorName, string taskId, 
            CancellationToken cancellationToken = default);

        /// <summary>
        /// Checks the health of job execution.
        /// Validates job progress, resource consumption, and error rates.
        /// </summary>
        Task<FlinkHealthCheckResult> CheckJobHealthAsync(string jobId, 
            CancellationToken cancellationToken = default);

        /// <summary>
        /// Checks the health of state backend.
        /// Validates state access, checkpointing, and storage performance.
        /// </summary>
        Task<FlinkHealthCheckResult> CheckStateBackendHealthAsync(
            CancellationToken cancellationToken = default);

        /// <summary>
        /// Checks the health of network communication.
        /// Validates inter-operator communication and data flow.
        /// </summary>
        Task<FlinkHealthCheckResult> CheckNetworkHealthAsync(
            CancellationToken cancellationToken = default);

        /// <summary>
        /// Checks the health of resource allocation.
        /// Validates memory usage, CPU utilization, and resource availability.
        /// </summary>
        Task<FlinkHealthCheckResult> CheckResourceHealthAsync(
            CancellationToken cancellationToken = default);

        /// <summary>
        /// Performs a comprehensive health check of all components.
        /// Returns aggregated health status and detailed component information.
        /// </summary>
        Task<Dictionary<string, FlinkHealthCheckResult>> CheckOverallHealthAsync(
            CancellationToken cancellationToken = default);

        /// <summary>
        /// Registers a custom health check for application-specific components.
        /// Enables extensible health monitoring for custom operators.
        /// </summary>
        void RegisterCustomHealthCheck(string name, Func<CancellationToken, Task<FlinkHealthCheckResult>> healthCheck);

        /// <summary>
        /// Gets the current health status of all monitored components.
        /// Provides real-time health overview.
        /// </summary>
        Task<Dictionary<string, FlinkHealthStatus>> GetHealthStatusSummaryAsync(
            CancellationToken cancellationToken = default);
    }
}