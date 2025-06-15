using System.Diagnostics.Metrics;
using FlinkDotNet.Core.Abstractions.Observability;
using Microsoft.Extensions.Logging;

namespace FlinkDotNet.Core.Observability
{
    /// <summary>
    /// Implementation of comprehensive metrics collection following Apache Flink 2.0 standards.
    /// Uses OpenTelemetry for metrics export and follows Flink metric naming conventions.
    /// </summary>
    public class FlinkMetricsCollector : IFlinkMetrics
    {
        private readonly Meter _meter;
        private readonly ILogger<FlinkMetricsCollector> _logger;
        private readonly Counter<long> _recordsInCounter;
        private readonly Counter<long> _recordsOutCounter;
        private readonly Counter<long> _bytesInCounter;
        private readonly Counter<long> _bytesOutCounter;
        private readonly Histogram<double> _latencyHistogram;
        private readonly Counter<long> _backpressureCounter;
        private readonly Histogram<double> _checkpointDurationHistogram;
        private readonly Counter<long> _checkpointSizeCounter;
        private readonly Counter<long> _restartsCounter;
        private readonly Counter<long> _errorsCounter;
        private readonly Gauge<int> _queueSizeGauge;
        private readonly Histogram<double> _watermarkLagHistogram;
        private readonly Gauge<long> _stateSizeGauge;

        public FlinkMetricsCollector(ILogger<FlinkMetricsCollector> logger)
        {
            _logger = logger;
            _meter = new Meter("FlinkDotNet.Core", "1.0.0");

            // Initialize counters following Flink naming conventions
            _recordsInCounter = _meter.CreateCounter<long>(
                "flink_taskmanager_job_task_operator_numRecordsIn",
                "record",
                "Number of records this operator/task has received");

            _recordsOutCounter = _meter.CreateCounter<long>(
                "flink_taskmanager_job_task_operator_numRecordsOut", 
                "record",
                "Number of records this operator/task has emitted");

            _bytesInCounter = _meter.CreateCounter<long>(
                "flink_taskmanager_job_task_operator_numBytesIn",
                "byte",
                "Number of bytes this operator/task has received");

            _bytesOutCounter = _meter.CreateCounter<long>(
                "flink_taskmanager_job_task_operator_numBytesOut",
                "byte", 
                "Number of bytes this operator/task has emitted");

            _latencyHistogram = _meter.CreateHistogram<double>(
                "flink_taskmanager_job_latency",
                "ms",
                "Latency histogram for records processed by this operator");

            _backpressureCounter = _meter.CreateCounter<long>(
                "flink_taskmanager_job_task_backPressuredTimeMsPerSecond",
                "ms",
                "Time spent being backpressured per second");

            _checkpointDurationHistogram = _meter.CreateHistogram<double>(
                "flink_jobmanager_job_lastCheckpointDuration",
                "ms",
                "Duration of the last checkpoint");

            _checkpointSizeCounter = _meter.CreateCounter<long>(
                "flink_jobmanager_job_lastCheckpointSize",
                "byte",
                "Size of the last checkpoint");

            _restartsCounter = _meter.CreateCounter<long>(
                "flink_jobmanager_job_numRestarts",
                "restart",
                "Number of times this job has restarted");

            _errorsCounter = _meter.CreateCounter<long>(
                "flink_taskmanager_job_task_operator_numErrors",
                "error",
                "Number of errors encountered by this operator");

            _queueSizeGauge = _meter.CreateGauge<int>(
                "flink_taskmanager_job_task_buffers_inPoolUsage",
                "buffer",
                "Current input buffer pool usage");

            _watermarkLagHistogram = _meter.CreateHistogram<double>(
                "flink_taskmanager_job_task_operator_watermarkLag",
                "ms",
                "Current watermark lag for this operator");

            _stateSizeGauge = _meter.CreateGauge<long>(
                "flink_taskmanager_job_task_operator_managedMemoryUsed",
                "byte",
                "Managed memory used by this operator's state");
        }

        public void RecordIncomingRecord(string operatorName, string taskId)
        {
            var tags = new KeyValuePair<string, object?>[]
            {
                new("operator_name", operatorName),
                new("task_id", taskId),
                new("job_id", GetJobIdFromTask(taskId))
            };

            _recordsInCounter.Add(1, tags);
            _logger.LogTrace("Recorded incoming record for operator {OperatorName}, task {TaskId}", 
                operatorName, taskId);
        }

        public void RecordOutgoingRecord(string operatorName, string taskId)
        {
            var tags = new KeyValuePair<string, object?>[]
            {
                new("operator_name", operatorName),
                new("task_id", taskId),
                new("job_id", GetJobIdFromTask(taskId))
            };

            _recordsOutCounter.Add(1, tags);
            _logger.LogTrace("Recorded outgoing record for operator {OperatorName}, task {TaskId}", 
                operatorName, taskId);
        }

        public void RecordBytesIn(string operatorName, string taskId, long bytes)
        {
            var tags = new KeyValuePair<string, object?>[]
            {
                new("operator_name", operatorName),
                new("task_id", taskId),
                new("job_id", GetJobIdFromTask(taskId))
            };

            _bytesInCounter.Add(bytes, tags);
            _logger.LogTrace("Recorded {Bytes} bytes in for operator {OperatorName}, task {TaskId}", 
                bytes, operatorName, taskId);
        }

        public void RecordBytesOut(string operatorName, string taskId, long bytes)
        {
            var tags = new KeyValuePair<string, object?>[]
            {
                new("operator_name", operatorName),
                new("task_id", taskId),
                new("job_id", GetJobIdFromTask(taskId))
            };

            _bytesOutCounter.Add(bytes, tags);
            _logger.LogTrace("Recorded {Bytes} bytes out for operator {OperatorName}, task {TaskId}", 
                bytes, operatorName, taskId);
        }

        public void RecordLatency(string operatorName, string taskId, TimeSpan latency)
        {
            var tags = new KeyValuePair<string, object?>[]
            {
                new("operator_name", operatorName),
                new("task_id", taskId),
                new("job_id", GetJobIdFromTask(taskId))
            };

            _latencyHistogram.Record(latency.TotalMilliseconds, tags);
            _logger.LogTrace("Recorded latency {Latency}ms for operator {OperatorName}, task {TaskId}", 
                latency.TotalMilliseconds, operatorName, taskId);
        }

        public void RecordBackpressure(string operatorName, string taskId, TimeSpan backpressureTime)
        {
            var tags = new KeyValuePair<string, object?>[]
            {
                new("operator_name", operatorName),
                new("task_id", taskId),
                new("job_id", GetJobIdFromTask(taskId))
            };

            _backpressureCounter.Add((long)backpressureTime.TotalMilliseconds, tags);
            _logger.LogWarning("Recorded backpressure {BackpressureTime}ms for operator {OperatorName}, task {TaskId}", 
                backpressureTime.TotalMilliseconds, operatorName, taskId);
        }

        public void RecordCheckpointDuration(string jobId, TimeSpan duration)
        {
            var tags = new KeyValuePair<string, object?>[]
            {
                new("job_id", jobId)
            };

            _checkpointDurationHistogram.Record(duration.TotalMilliseconds, tags);
            _logger.LogInformation("Recorded checkpoint duration {Duration}ms for job {JobId}", 
                duration.TotalMilliseconds, jobId);
        }

        public void RecordCheckpointSize(string jobId, long sizeBytes)
        {
            var tags = new KeyValuePair<string, object?>[]
            {
                new("job_id", jobId)
            };

            _checkpointSizeCounter.Add(sizeBytes, tags);
            _logger.LogInformation("Recorded checkpoint size {Size} bytes for job {JobId}", 
                sizeBytes, jobId);
        }

        public void RecordRestart(string operatorName, string taskId)
        {
            var tags = new KeyValuePair<string, object?>[]
            {
                new("operator_name", operatorName),
                new("task_id", taskId),
                new("job_id", GetJobIdFromTask(taskId))
            };

            _restartsCounter.Add(1, tags);
            _logger.LogWarning("Recorded restart for operator {OperatorName}, task {TaskId}", 
                operatorName, taskId);
        }

        public void RecordError(string operatorName, string taskId, string errorType)
        {
            var tags = new KeyValuePair<string, object?>[]
            {
                new("operator_name", operatorName),
                new("task_id", taskId),
                new("job_id", GetJobIdFromTask(taskId)),
                new("error_type", errorType)
            };

            _errorsCounter.Add(1, tags);
            _logger.LogError("Recorded error of type {ErrorType} for operator {OperatorName}, task {TaskId}", 
                errorType, operatorName, taskId);
        }

        public void RecordQueueSize(string operatorName, string taskId, int queueSize)
        {
            var tags = new KeyValuePair<string, object?>[]
            {
                new("operator_name", operatorName),
                new("task_id", taskId),
                new("job_id", GetJobIdFromTask(taskId))
            };

            _queueSizeGauge.Record(queueSize, tags);
            _logger.LogTrace("Recorded queue size {QueueSize} for operator {OperatorName}, task {TaskId}", 
                queueSize, operatorName, taskId);
        }

        public void RecordWatermarkLag(string operatorName, string taskId, TimeSpan lag)
        {
            var tags = new KeyValuePair<string, object?>[]
            {
                new("operator_name", operatorName),
                new("task_id", taskId),
                new("job_id", GetJobIdFromTask(taskId))
            };

            _watermarkLagHistogram.Record(lag.TotalMilliseconds, tags);
            _logger.LogTrace("Recorded watermark lag {Lag}ms for operator {OperatorName}, task {TaskId}", 
                lag.TotalMilliseconds, operatorName, taskId);
        }

        public void RecordStateSize(string operatorName, string taskId, long sizeBytes)
        {
            var tags = new KeyValuePair<string, object?>[]
            {
                new("operator_name", operatorName),
                new("task_id", taskId),
                new("job_id", GetJobIdFromTask(taskId))
            };

            _stateSizeGauge.Record(sizeBytes, tags);
            _logger.LogTrace("Recorded state size {Size} bytes for operator {OperatorName}, task {TaskId}", 
                sizeBytes, operatorName, taskId);
        }

        private static string GetJobIdFromTask(string taskId)
        {
            // Extract job ID from task ID (assuming format: jobId_operatorId_subtaskIndex)
            var parts = taskId.Split('_');
            return parts.Length > 0 ? parts[0] : "unknown";
        }

        public void Dispose()
        {
            _meter?.Dispose();
        }
    }
}