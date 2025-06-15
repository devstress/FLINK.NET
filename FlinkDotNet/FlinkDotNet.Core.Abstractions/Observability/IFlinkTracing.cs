using System.Diagnostics;

namespace FlinkDotNet.Core.Abstractions.Observability
{
    /// <summary>
    /// Provides distributed tracing capabilities following Apache Flink 2.0 patterns.
    /// Implements trace correlation across job components for end-to-end visibility.
    /// </summary>
    public interface IFlinkTracing : IDisposable
    {
        /// <summary>
        /// Creates a new trace span for operator execution.
        /// Maps to Flink's span creation for operator chains.
        /// </summary>
        Activity? StartOperatorSpan(string operatorName, string taskId, string? parentSpanId = null);

        /// <summary>
        /// Creates a span for record processing within an operator.
        /// Enables fine-grained tracing of record flow.
        /// </summary>
        Activity? StartRecordProcessingSpan(string operatorName, string taskId, string recordId);

        /// <summary>
        /// Creates a span for checkpoint operations.
        /// Traces checkpoint coordination and state persistence.
        /// </summary>
        Activity? StartCheckpointSpan(string jobId, long checkpointId);

        /// <summary>
        /// Creates a span for state operations (read/write/recovery).
        /// Traces state backend interactions for performance analysis.
        /// </summary>
        Activity? StartStateOperationSpan(string operatorName, string taskId, string operation);

        /// <summary>
        /// Creates a span for network communication between operators.
        /// Traces data flow across task boundaries.
        /// </summary>
        Activity? StartNetworkSpan(string sourceTask, string targetTask, string direction);

        /// <summary>
        /// Adds contextual information to the current span.
        /// Enriches traces with Flink-specific metadata.
        /// </summary>
        void AddSpanAttribute(string key, object value);

        /// <summary>
        /// Adds span event for significant occurrences.
        /// Records important events like backpressure, restarts, etc.
        /// </summary>
        void AddSpanEvent(string eventName, Dictionary<string, object>? attributes = null);

        /// <summary>
        /// Records an error in the current span.
        /// Correlates errors with trace context for debugging.
        /// </summary>
        void RecordSpanError(Exception exception, string? description = null);

        /// <summary>
        /// Gets the current trace context for propagation.
        /// Enables trace correlation across async boundaries.
        /// </summary>
        string? GetCurrentTraceContext();

        /// <summary>
        /// Sets the trace context from upstream components.
        /// Continues traces across component boundaries.
        /// </summary>
        void SetTraceContext(string traceContext);
    }
}