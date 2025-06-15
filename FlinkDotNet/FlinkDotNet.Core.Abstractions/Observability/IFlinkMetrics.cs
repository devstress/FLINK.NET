using System.Diagnostics.Metrics;

namespace FlinkDotNet.Core.Abstractions.Observability
{
    /// <summary>
    /// Provides comprehensive metrics collection following Apache Flink 2.0 standards.
    /// Implements the same metric patterns as Apache Flink for consistency and compatibility.
    /// </summary>
    public interface IFlinkMetrics : IDisposable
    {
        /// <summary>
        /// Records processed incoming records for throughput calculation.
        /// Maps to Flink's numRecordsIn metric.
        /// </summary>
        void RecordIncomingRecord(string operatorName, string taskId);

        /// <summary>
        /// Records processed outgoing records for throughput calculation.
        /// Maps to Flink's numRecordsOut metric.
        /// </summary>
        void RecordOutgoingRecord(string operatorName, string taskId);

        /// <summary>
        /// Records bytes received from upstream operators.
        /// Maps to Flink's numBytesIn metric.
        /// </summary>
        void RecordBytesIn(string operatorName, string taskId, long bytes);

        /// <summary>
        /// Records bytes sent to downstream operators.
        /// Maps to Flink's numBytesOut metric.
        /// </summary>
        void RecordBytesOut(string operatorName, string taskId, long bytes);

        /// <summary>
        /// Records processing latency for performance monitoring.
        /// Maps to Flink's latency histogram metrics.
        /// </summary>
        void RecordLatency(string operatorName, string taskId, TimeSpan latency);

        /// <summary>
        /// Records backpressure indication for flow control monitoring.
        /// Maps to Flink's backPressuredTimeMsPerSecond metric.
        /// </summary>
        void RecordBackpressure(string operatorName, string taskId, TimeSpan backpressureTime);

        /// <summary>
        /// Records checkpoint duration for reliability monitoring.
        /// Maps to Flink's checkpointDuration metric.
        /// </summary>
        void RecordCheckpointDuration(string jobId, TimeSpan duration);

        /// <summary>
        /// Records checkpoint size for state monitoring.
        /// Maps to Flink's checkpointSize metric.
        /// </summary>
        void RecordCheckpointSize(string jobId, long sizeBytes);

        /// <summary>
        /// Records operator restart count for failure tracking.
        /// Maps to Flink's numRestarts metric.
        /// </summary>
        void RecordRestart(string operatorName, string taskId);

        /// <summary>
        /// Records error occurrences for reliability monitoring.
        /// Maps to Flink's error rate metrics.
        /// </summary>
        void RecordError(string operatorName, string taskId, string errorType);

        /// <summary>
        /// Records current queue size for buffer monitoring.
        /// Maps to Flink's inPoolUsage and outPoolUsage metrics.
        /// </summary>
        void RecordQueueSize(string operatorName, string taskId, int queueSize);

        /// <summary>
        /// Records watermark lag for event time processing monitoring.
        /// Maps to Flink's watermarkLag metric.
        /// </summary>
        void RecordWatermarkLag(string operatorName, string taskId, TimeSpan lag);

        /// <summary>
        /// Records state size for memory usage monitoring.
        /// Maps to Flink's managedMemoryUsed metric.
        /// </summary>
        void RecordStateSize(string operatorName, string taskId, long sizeBytes);
    }
}