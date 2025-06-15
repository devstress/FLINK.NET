using FlinkDotNet.Core.Abstractions.Observability;
using Microsoft.Extensions.Logging;
using System.Text.Json;

namespace FlinkDotNet.Core.Observability
{
    /// <summary>
    /// Implementation of structured logging following Apache Flink 2.0 standards.
    /// Provides consistent, contextual logging across all Flink components.
    /// </summary>
    public class FlinkStructuredLogger : IFlinkLogger
    {
        private readonly ILogger _logger;
        private readonly Dictionary<string, object> _baseContext;

        public FlinkStructuredLogger(ILogger logger, Dictionary<string, object>? baseContext = null)
        {
            _logger = logger;
            _baseContext = baseContext ?? new Dictionary<string, object>();
        }

        public void LogOperatorLifecycle(LogLevel level, string operatorName, string taskId, string lifecycle, 
            Dictionary<string, object>? context = null)
        {
            var logContext = CreateLogContext(new Dictionary<string, object>
            {
                ["flink.operator.name"] = operatorName,
                ["flink.task.id"] = taskId,
                ["flink.job.id"] = GetJobIdFromTask(taskId),
                ["flink.lifecycle"] = lifecycle,
                ["flink.event.type"] = "operator_lifecycle"
            }, context);

            _logger.Log(level, "Operator {OperatorName} lifecycle event: {Lifecycle} for task {TaskId}. Context: {Context}",
                operatorName, lifecycle, taskId, JsonSerializer.Serialize(logContext));
        }

        public void LogRecordProcessing(LogLevel level, string operatorName, string taskId, string action,
            long recordCount = 0, TimeSpan? processingTime = null, Dictionary<string, object>? context = null)
        {
            var logContext = CreateLogContext(new Dictionary<string, object>
            {
                ["flink.operator.name"] = operatorName,
                ["flink.task.id"] = taskId,
                ["flink.job.id"] = GetJobIdFromTask(taskId),
                ["flink.action"] = action,
                ["flink.record.count"] = recordCount,
                ["flink.event.type"] = "record_processing"
            }, context);

            if (processingTime.HasValue)
            {
                logContext["flink.processing.time.ms"] = processingTime.Value.TotalMilliseconds;
            }

            _logger.Log(level, "Record processing {Action} in operator {OperatorName}, task {TaskId}: {RecordCount} records in {ProcessingTime}ms. Context: {Context}",
                action, operatorName, taskId, recordCount, processingTime?.TotalMilliseconds ?? 0, JsonSerializer.Serialize(logContext));
        }

        public void LogCheckpoint(LogLevel level, string jobId, long checkpointId, string phase,
            TimeSpan? duration = null, long? sizeBytes = null, Dictionary<string, object>? context = null)
        {
            var logContext = CreateLogContext(new Dictionary<string, object>
            {
                ["flink.job.id"] = jobId,
                ["flink.checkpoint.id"] = checkpointId,
                ["flink.checkpoint.phase"] = phase,
                ["flink.event.type"] = "checkpoint"
            }, context);

            if (duration.HasValue)
            {
                logContext["flink.checkpoint.duration.ms"] = duration.Value.TotalMilliseconds;
            }

            if (sizeBytes.HasValue)
            {
                logContext["flink.checkpoint.size.bytes"] = sizeBytes.Value;
            }

            _logger.Log(level, "Checkpoint {CheckpointId} {Phase} for job {JobId}: duration={Duration}ms, size={Size}bytes. Context: {Context}",
                checkpointId, phase, jobId, duration?.TotalMilliseconds ?? 0, sizeBytes ?? 0, JsonSerializer.Serialize(logContext));
        }

        public void LogBackpressure(LogLevel level, string operatorName, string taskId, string reason,
            TimeSpan duration, Dictionary<string, object>? context = null)
        {
            var logContext = CreateLogContext(new Dictionary<string, object>
            {
                ["flink.operator.name"] = operatorName,
                ["flink.task.id"] = taskId,
                ["flink.job.id"] = GetJobIdFromTask(taskId),
                ["flink.backpressure.reason"] = reason,
                ["flink.backpressure.duration.ms"] = duration.TotalMilliseconds,
                ["flink.event.type"] = "backpressure"
            }, context);

            _logger.Log(level, "Backpressure detected in operator {OperatorName}, task {TaskId}: {Reason} for {Duration}ms. Context: {Context}",
                operatorName, taskId, reason, duration.TotalMilliseconds, JsonSerializer.Serialize(logContext));
        }

        public void LogError(string operatorName, string taskId, Exception exception, string action,
            Dictionary<string, object>? context = null)
        {
            var logContext = CreateLogContext(new Dictionary<string, object>
            {
                ["flink.operator.name"] = operatorName,
                ["flink.task.id"] = taskId,
                ["flink.job.id"] = GetJobIdFromTask(taskId),
                ["flink.action"] = action,
                ["flink.error.type"] = exception.GetType().Name,
                ["flink.error.message"] = exception.Message,
                ["flink.event.type"] = "error"
            }, context);

            if (exception.StackTrace != null)
            {
                logContext["flink.error.stacktrace"] = exception.StackTrace;
            }

            _logger.LogError(exception, "Error in operator {OperatorName}, task {TaskId} during {Action}. Context: {Context}",
                operatorName, taskId, action, JsonSerializer.Serialize(logContext));
        }

        public void LogPerformance(LogLevel level, string operatorName, string taskId, string metricName,
            double value, string unit, Dictionary<string, object>? context = null)
        {
            var logContext = CreateLogContext(new Dictionary<string, object>
            {
                ["flink.operator.name"] = operatorName,
                ["flink.task.id"] = taskId,
                ["flink.job.id"] = GetJobIdFromTask(taskId),
                ["flink.metric.name"] = metricName,
                ["flink.metric.value"] = value,
                ["flink.metric.unit"] = unit,
                ["flink.event.type"] = "performance"
            }, context);

            _logger.Log(level, "Performance metric {MetricName} for operator {OperatorName}, task {TaskId}: {Value} {Unit}. Context: {Context}",
                metricName, operatorName, taskId, value, unit, JsonSerializer.Serialize(logContext));
        }

        public void LogStateOperation(LogLevel level, string operatorName, string taskId, string operation,
            string stateType, TimeSpan? duration = null, long? sizeBytes = null, Dictionary<string, object>? context = null)
        {
            var logContext = CreateLogContext(new Dictionary<string, object>
            {
                ["flink.operator.name"] = operatorName,
                ["flink.task.id"] = taskId,
                ["flink.job.id"] = GetJobIdFromTask(taskId),
                ["flink.state.operation"] = operation,
                ["flink.state.type"] = stateType,
                ["flink.event.type"] = "state_operation"
            }, context);

            if (duration.HasValue)
            {
                logContext["flink.state.duration.ms"] = duration.Value.TotalMilliseconds;
            }

            if (sizeBytes.HasValue)
            {
                logContext["flink.state.size.bytes"] = sizeBytes.Value;
            }

            _logger.Log(level, "State operation {Operation} ({StateType}) in operator {OperatorName}, task {TaskId}: duration={Duration}ms, size={Size}bytes. Context: {Context}",
                operation, stateType, operatorName, taskId, duration?.TotalMilliseconds ?? 0, sizeBytes ?? 0, JsonSerializer.Serialize(logContext));
        }

        public void LogNetworkOperation(LogLevel level, string sourceTask, string targetTask, string operation,
            long bytesTransferred = 0, TimeSpan? latency = null, Dictionary<string, object>? context = null)
        {
            var logContext = CreateLogContext(new Dictionary<string, object>
            {
                ["flink.source.task"] = sourceTask,
                ["flink.target.task"] = targetTask,
                ["flink.network.operation"] = operation,
                ["flink.network.bytes"] = bytesTransferred,
                ["flink.event.type"] = "network_operation"
            }, context);

            if (latency.HasValue)
            {
                logContext["flink.network.latency.ms"] = latency.Value.TotalMilliseconds;
            }

            _logger.Log(level, "Network operation {Operation} from {SourceTask} to {TargetTask}: {Bytes} bytes, latency={Latency}ms. Context: {Context}",
                operation, sourceTask, targetTask, bytesTransferred, latency?.TotalMilliseconds ?? 0, JsonSerializer.Serialize(logContext));
        }

        public void LogJobEvent(LogLevel level, string jobId, string jobName, string eventName,
            Dictionary<string, object>? context = null)
        {
            var logContext = CreateLogContext(new Dictionary<string, object>
            {
                ["flink.job.id"] = jobId,
                ["flink.job.name"] = jobName,
                ["flink.job.event"] = eventName,
                ["flink.event.type"] = "job_event"
            }, context);

            _logger.Log(level, "Job event {Event} for job {JobName} (ID: {JobId}). Context: {Context}",
                eventName, jobName, jobId, JsonSerializer.Serialize(logContext));
        }

        public IFlinkLogger WithContext(Dictionary<string, object> additionalContext)
        {
            var mergedContext = new Dictionary<string, object>(_baseContext);
            foreach (var kvp in additionalContext)
            {
                mergedContext[kvp.Key] = kvp.Value;
            }
            return new FlinkStructuredLogger(_logger, mergedContext);
        }

        private Dictionary<string, object> CreateLogContext(Dictionary<string, object> eventContext, 
            Dictionary<string, object>? additionalContext = null)
        {
            var logContext = new Dictionary<string, object>(_baseContext);
            
            foreach (var kvp in eventContext)
            {
                logContext[kvp.Key] = kvp.Value;
            }

            if (additionalContext != null)
            {
                foreach (var kvp in additionalContext)
                {
                    logContext[kvp.Key] = kvp.Value;
                }
            }

            logContext["flink.timestamp"] = DateTimeOffset.UtcNow.ToString("O");
            
            return logContext;
        }

        private static string GetJobIdFromTask(string taskId)
        {
            // Extract job ID from task ID (assuming format: jobId_operatorId_subtaskIndex)
            var parts = taskId.Split('_');
            return parts.Length > 0 ? parts[0] : "unknown";
        }
    }
}