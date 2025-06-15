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
        private const string FlinkOperatorNameKey = "flink.operator.name";
        private const string FlinkTaskIdKey = "flink.task.id";
        private const string FlinkJobIdKey = "flink.job.id";
        private const string FlinkEventTypeKey = "flink.event.type";
        
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
                [FlinkOperatorNameKey] = operatorName,
                [FlinkTaskIdKey] = taskId,
                [FlinkJobIdKey] = GetJobIdFromTask(taskId),
                ["flink.lifecycle"] = lifecycle,
                [FlinkEventTypeKey] = "operator_lifecycle"
            }, context);

            _logger.Log(level, "Operator {OperatorName} lifecycle event: {Lifecycle} for task {TaskId}. Context: {Context}",
                operatorName, lifecycle, taskId, JsonSerializer.Serialize(logContext));
        }

        public void LogRecordProcessing(LogLevel level, string operatorName, string taskId, string action,
            long recordCount = 0, TimeSpan? processingTime = null, Dictionary<string, object>? context = null)
        {
            var logContext = CreateLogContext(new Dictionary<string, object>
            {
                [FlinkOperatorNameKey] = operatorName,
                [FlinkTaskIdKey] = taskId,
                [FlinkJobIdKey] = GetJobIdFromTask(taskId),
                ["flink.action"] = action,
                ["flink.record.count"] = recordCount,
                [FlinkEventTypeKey] = "record_processing"
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
                [FlinkJobIdKey] = jobId,
                ["flink.checkpoint.id"] = checkpointId,
                ["flink.checkpoint.phase"] = phase,
                [FlinkEventTypeKey] = "checkpoint"
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
                [FlinkOperatorNameKey] = operatorName,
                [FlinkTaskIdKey] = taskId,
                [FlinkJobIdKey] = GetJobIdFromTask(taskId),
                ["flink.backpressure.reason"] = reason,
                ["flink.backpressure.duration.ms"] = duration.TotalMilliseconds,
                [FlinkEventTypeKey] = "backpressure"
            }, context);

            _logger.Log(level, "Backpressure detected in operator {OperatorName}, task {TaskId}: {Reason} for {Duration}ms. Context: {Context}",
                operatorName, taskId, reason, duration.TotalMilliseconds, JsonSerializer.Serialize(logContext));
        }

        public void LogError(string operatorName, string taskId, Exception exception, string action,
            Dictionary<string, object>? context = null)
        {
            var logContext = CreateLogContext(new Dictionary<string, object>
            {
                [FlinkOperatorNameKey] = operatorName,
                [FlinkTaskIdKey] = taskId,
                [FlinkJobIdKey] = GetJobIdFromTask(taskId),
                ["flink.action"] = action,
                ["flink.error.type"] = exception.GetType().Name,
                ["flink.error.message"] = exception.Message,
                [FlinkEventTypeKey] = "error"
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
                [FlinkOperatorNameKey] = operatorName,
                [FlinkTaskIdKey] = taskId,
                [FlinkJobIdKey] = GetJobIdFromTask(taskId),
                ["flink.metric.name"] = metricName,
                ["flink.metric.value"] = value,
                ["flink.metric.unit"] = unit,
                [FlinkEventTypeKey] = "performance"
            }, context);

            _logger.Log(level, "Performance metric {MetricName} for operator {OperatorName}, task {TaskId}: {Value} {Unit}. Context: {Context}",
                metricName, operatorName, taskId, value, unit, JsonSerializer.Serialize(logContext));
        }

        public void LogStateOperation(LogLevel level, StateOperationInfo stateInfo)
        {
            var logContext = CreateLogContext(new Dictionary<string, object>
            {
                [FlinkOperatorNameKey] = stateInfo.OperatorName,
                [FlinkTaskIdKey] = stateInfo.TaskId,
                [FlinkJobIdKey] = GetJobIdFromTask(stateInfo.TaskId),
                ["flink.state.operation"] = stateInfo.Operation,
                ["flink.state.type"] = stateInfo.StateType,
                [FlinkEventTypeKey] = "state_operation"
            }, stateInfo.Context);

            if (stateInfo.Duration.HasValue)
            {
                logContext["flink.state.duration.ms"] = stateInfo.Duration.Value.TotalMilliseconds;
            }

            if (stateInfo.SizeBytes.HasValue)
            {
                logContext["flink.state.size.bytes"] = stateInfo.SizeBytes.Value;
            }

            _logger.Log(level, "State operation {Operation} ({StateType}) in operator {OperatorName}, task {TaskId}: duration={Duration}ms, size={Size}bytes. Context: {Context}",
                stateInfo.Operation, stateInfo.StateType, stateInfo.OperatorName, stateInfo.TaskId, 
                stateInfo.Duration?.TotalMilliseconds ?? 0, stateInfo.SizeBytes ?? 0, JsonSerializer.Serialize(logContext));
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
                [FlinkEventTypeKey] = "network_operation"
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
                [FlinkJobIdKey] = jobId,
                ["flink.job.name"] = jobName,
                ["flink.job.event"] = eventName,
                [FlinkEventTypeKey] = "job_event"
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