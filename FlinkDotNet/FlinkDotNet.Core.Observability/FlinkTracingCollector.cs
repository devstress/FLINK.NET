using System.Diagnostics;
using FlinkDotNet.Core.Abstractions.Observability;
using Microsoft.Extensions.Logging;

namespace FlinkDotNet.Core.Observability
{
    /// <summary>
    /// Implementation of distributed tracing following Apache Flink 2.0 patterns.
    /// Provides end-to-end trace correlation across job components.
    /// </summary>
    public class FlinkTracingCollector : IFlinkTracing
    {
        private readonly ActivitySource _activitySource;
        private readonly ILogger<FlinkTracingCollector> _logger;
        private readonly string _serviceName;

        public FlinkTracingCollector(ILogger<FlinkTracingCollector> logger, string serviceName = "FlinkDotNet")
        {
            _logger = logger;
            _serviceName = serviceName;
            _activitySource = new ActivitySource($"{serviceName}.Tracing", "1.0.0");
        }

        public Activity? StartOperatorSpan(string operatorName, string taskId, string? parentSpanId = null)
        {
            var activity = _activitySource.StartActivity($"operator.{operatorName}");
            if (activity != null)
            {
                activity.SetTag("flink.operator.name", operatorName);
                activity.SetTag("flink.task.id", taskId);
                activity.SetTag("flink.job.id", GetJobIdFromTask(taskId));
                activity.SetTag("flink.component.type", "operator");
                
                if (!string.IsNullOrEmpty(parentSpanId))
                {
                    activity.SetTag("flink.parent.span.id", parentSpanId);
                }

                _logger.LogTrace("Started operator span for {OperatorName}, task {TaskId}, span {SpanId}", 
                    operatorName, taskId, activity.Id);
            }

            return activity;
        }

        public Activity? StartRecordProcessingSpan(string operatorName, string taskId, string recordId)
        {
            var activity = _activitySource.StartActivity($"record.processing.{operatorName}");
            if (activity != null)
            {
                activity.SetTag("flink.operator.name", operatorName);
                activity.SetTag("flink.task.id", taskId);
                activity.SetTag("flink.record.id", recordId);
                activity.SetTag("flink.job.id", GetJobIdFromTask(taskId));
                activity.SetTag("flink.component.type", "record_processing");

                _logger.LogTrace("Started record processing span for {OperatorName}, task {TaskId}, record {RecordId}", 
                    operatorName, taskId, recordId);
            }

            return activity;
        }

        public Activity? StartCheckpointSpan(string jobId, long checkpointId)
        {
            var activity = _activitySource.StartActivity("checkpoint.coordination");
            if (activity != null)
            {
                activity.SetTag("flink.job.id", jobId);
                activity.SetTag("flink.checkpoint.id", checkpointId);
                activity.SetTag("flink.component.type", "checkpoint");

                _logger.LogInformation("Started checkpoint span for job {JobId}, checkpoint {CheckpointId}", 
                    jobId, checkpointId);
            }

            return activity;
        }

        public Activity? StartStateOperationSpan(string operatorName, string taskId, string operation)
        {
            var activity = _activitySource.StartActivity($"state.{operation}");
            if (activity != null)
            {
                activity.SetTag("flink.operator.name", operatorName);
                activity.SetTag("flink.task.id", taskId);
                activity.SetTag("flink.state.operation", operation);
                activity.SetTag("flink.job.id", GetJobIdFromTask(taskId));
                activity.SetTag("flink.component.type", "state");

                _logger.LogTrace("Started state operation span for {OperatorName}, task {TaskId}, operation {Operation}", 
                    operatorName, taskId, operation);
            }

            return activity;
        }

        public Activity? StartNetworkSpan(string sourceTask, string targetTask, string direction)
        {
            var activity = _activitySource.StartActivity($"network.{direction}");
            if (activity != null)
            {
                activity.SetTag("flink.source.task", sourceTask);
                activity.SetTag("flink.target.task", targetTask);
                activity.SetTag("flink.network.direction", direction);
                activity.SetTag("flink.component.type", "network");

                _logger.LogTrace("Started network span from {SourceTask} to {TargetTask}, direction {Direction}", 
                    sourceTask, targetTask, direction);
            }

            return activity;
        }

        public void AddSpanAttribute(string key, object value)
        {
            var activity = Activity.Current;
            if (activity != null)
            {
                activity.SetTag($"flink.{key}", value);
                _logger.LogTrace("Added span attribute {Key}={Value} to span {SpanId}", 
                    key, value, activity.Id);
            }
        }

        public void AddSpanEvent(string eventName, Dictionary<string, object>? attributes = null)
        {
            var activity = Activity.Current;
            if (activity != null)
            {
                var activityEvent = new ActivityEvent(eventName, DateTimeOffset.UtcNow, 
                    new ActivityTagsCollection(attributes ?? new Dictionary<string, object>()));
                activity.AddEvent(activityEvent);

                _logger.LogTrace("Added span event {EventName} to span {SpanId}", 
                    eventName, activity.Id);
            }
        }

        public void RecordSpanError(Exception exception, string? description = null)
        {
            var activity = Activity.Current;
            if (activity != null)
            {
                activity.SetStatus(ActivityStatusCode.Error, description ?? exception.Message);
                activity.SetTag("flink.error.type", exception.GetType().Name);
                activity.SetTag("flink.error.message", exception.Message);
                
                if (exception.StackTrace != null)
                {
                    activity.SetTag("flink.error.stacktrace", exception.StackTrace);
                }

                _logger.LogError(exception, "Recorded error in span {SpanId}: {Description}", 
                    activity.Id, description ?? exception.Message);
            }
        }

        public string? GetCurrentTraceContext()
        {
            var activity = Activity.Current;
            return activity?.Id;
        }

        public void SetTraceContext(string traceContext)
        {
            // In a real implementation, you would restore the trace context
            // This is a simplified version for demonstration
            _logger.LogTrace("Set trace context: {TraceContext}", traceContext);
        }

        private static string GetJobIdFromTask(string taskId)
        {
            // Extract job ID from task ID (assuming format: jobId_operatorId_subtaskIndex)
            var parts = taskId.Split('_');
            return parts.Length > 0 ? parts[0] : "unknown";
        }

        public void Dispose()
        {
            _activitySource?.Dispose();
        }
    }
}