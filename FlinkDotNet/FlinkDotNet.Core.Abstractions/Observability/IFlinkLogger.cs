using Microsoft.Extensions.Logging;

namespace FlinkDotNet.Core.Abstractions.Observability
{
    /// <summary>
    /// Provides structured logging capabilities following Apache Flink 2.0 standards.
    /// Implements consistent logging patterns across all Flink components.
    /// </summary>
    public interface IFlinkLogger
    {
        /// <summary>
        /// Logs operator lifecycle events (start, stop, restart).
        /// Provides visibility into operator state transitions.
        /// </summary>
        void LogOperatorLifecycle(LogLevel level, string operatorName, string taskId, string lifecycle, 
            Dictionary<string, object>? context = null);

        /// <summary>
        /// Logs record processing events with timing and context.
        /// Enables tracing of individual record flow through operators.
        /// </summary>
        void LogRecordProcessing(LogLevel level, string operatorName, string taskId, string action,
            long recordCount = 0, TimeSpan? processingTime = null, Dictionary<string, object>? context = null);

        /// <summary>
        /// Logs checkpoint events with detailed state information.
        /// Tracks checkpoint coordination and completion status.
        /// </summary>
        void LogCheckpoint(LogLevel level, string jobId, long checkpointId, string phase,
            TimeSpan? duration = null, long? sizeBytes = null, Dictionary<string, object>? context = null);

        /// <summary>
        /// Logs backpressure events for flow control monitoring.
        /// Identifies bottlenecks in the processing pipeline.
        /// </summary>
        void LogBackpressure(LogLevel level, string operatorName, string taskId, string reason,
            TimeSpan duration, Dictionary<string, object>? context = null);

        /// <summary>
        /// Logs error events with full context for debugging.
        /// Provides structured error information for analysis.
        /// </summary>
        void LogError(string operatorName, string taskId, Exception exception, string action,
            Dictionary<string, object>? context = null);

        /// <summary>
        /// Logs performance metrics and statistics.
        /// Records throughput, latency, and resource usage information.
        /// </summary>
        void LogPerformance(LogLevel level, string operatorName, string taskId, string metricName,
            double value, string unit, Dictionary<string, object>? context = null);

        /// <summary>
        /// Logs state operations (create, restore, snapshot).
        /// Tracks state backend interactions and performance.
        /// </summary>
        void LogStateOperation(LogLevel level, string operatorName, string taskId, string operation,
            string stateType, TimeSpan? duration = null, long? sizeBytes = null, Dictionary<string, object>? context = null);

        /// <summary>
        /// Logs network communication events between operators.
        /// Tracks data flow and network performance.
        /// </summary>
        void LogNetworkOperation(LogLevel level, string sourceTask, string targetTask, string operation,
            long bytesTransferred = 0, TimeSpan? latency = null, Dictionary<string, object>? context = null);

        /// <summary>
        /// Logs job-level events (submission, execution, completion).
        /// Provides high-level job execution visibility.
        /// </summary>
        void LogJobEvent(LogLevel level, string jobId, string jobName, string eventName,
            Dictionary<string, object>? context = null);

        /// <summary>
        /// Creates a scoped logger with additional context.
        /// Maintains contextual information across related operations.
        /// </summary>
        IFlinkLogger WithContext(Dictionary<string, object> additionalContext);
    }
}