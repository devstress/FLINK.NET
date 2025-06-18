using FlinkDotNet.Core.Abstractions.Execution;
using FlinkDotNet.Core.Abstractions.Storage;
using FlinkDotNet.TaskManager.Services;
using FlinkDotNet.TaskManager.Operators;
using FlinkDotNet.Proto.Internal;
using System.Text.Json;
using Microsoft.Extensions.Logging;
using FlinkDotNet.Core.Abstractions.Sources;
using FlinkDotNet.Core.Abstractions.Windowing;

namespace FlinkDotNet.TaskManager
{
    public class TaskExecutor
    {
        private readonly ActiveTaskRegistry _activeTaskRegistry;
        private readonly ILogger<TaskExecutor>? _logger;
        private readonly string _taskManagerId;

        public ActiveTaskRegistry Registry => _activeTaskRegistry;

        public TaskExecutor(
            ActiveTaskRegistry activeTaskRegistry,
            TaskManagerCheckpointingServiceImpl checkpointingService,
            SerializerRegistry serializerRegistry,
            string taskManagerId,
            IStateSnapshotStore stateStore,
            ILogger<TaskExecutor>? logger = null)
        {
            _activeTaskRegistry = activeTaskRegistry;
            _taskManagerId = taskManagerId;
            _logger = logger;
        }

        public async Task ExecuteFromDescriptor(
            TaskDeploymentDescriptor descriptor,
            Dictionary<string, string> operatorProperties,
            CancellationToken cancellationToken)
        {
            _logger?.LogInformation("[TaskExecutor] Starting task execution for '{TaskName}' with operator '{OperatorName}' on TaskManager {TaskManagerId}", 
                descriptor.TaskName, descriptor.FullyQualifiedOperatorName, _taskManagerId);

            try
            {
                // Parse operator configuration
                var operatorConfig = JsonSerializer.Deserialize<Dictionary<string, object>>(
                    descriptor.OperatorConfiguration.ToStringUtf8()) ?? new Dictionary<string, object>();

                // Since we're using a simplified architecture, we'll use the combined Kafka-to-Redis operator
                await ExecuteKafkaToRedisTask(descriptor, operatorConfig, cancellationToken);
            }
            catch (OperationCanceledException)
            {
                _logger?.LogInformation("[TaskExecutor] Task '{TaskName}' was cancelled", descriptor.TaskName);
            }
            catch (Exception ex)
            {
                _logger?.LogError(ex, "[TaskExecutor] Task '{TaskName}' failed with error", descriptor.TaskName);
                throw;
            }
        }

        private async Task ExecuteKafkaToRedisTask(
            TaskDeploymentDescriptor descriptor, 
            Dictionary<string, object> config,
            CancellationToken cancellationToken)
        {
            _logger?.LogInformation("[TaskExecutor] Executing Kafka-to-Redis task for TaskManager {TaskManagerId}, Subtask {SubtaskIndex}", 
                _taskManagerId, descriptor.SubtaskIndex);

            try
            {
                // Extract configuration
                var topic = config.TryGetValue("topic", out var topicObj) ? topicObj.ToString() : "flinkdotnet.sample.topic";
                var consumerGroupId = config.TryGetValue("consumerGroupId", out var groupObj) ? groupObj.ToString() : "flinkdotnet-consumer-group";
                var redisSinkCounterKey = config.TryGetValue("redisSinkCounterKey", out var counterObj) ? 
                    counterObj.ToString() : "flinkdotnet:sample:processed_message_counter";
                var globalSequenceKey = config.TryGetValue("globalSequenceKey", out var seqObj) ? 
                    seqObj.ToString() : "flinkdotnet:global_sequence_id";
                var expectedMessages = config.TryGetValue("expectedMessages", out var expectedObj) ? 
                    Convert.ToInt32(expectedObj) : 1000000;

                // Create and configure combined Kafka-to-Redis operator
                var kafkaToRedisOperator = new KafkaToRedisOperator(
                    topic, 
                    consumerGroupId, 
                    redisSinkCounterKey,
                    globalSequenceKey,
                    expectedMessages,
                    _taskManagerId, 
                    null); // Don't pass logger due to type mismatch
                
                _logger?.LogInformation("[TaskExecutor] Starting Kafka-to-Redis processing for topic '{Topic}' with group '{ConsumerGroup}' on TaskManager {TaskManagerId}", 
                    topic, consumerGroupId, _taskManagerId);

                // Create a simple source context
                var sourceContext = new SimpleSourceContext(_taskManagerId, _logger);

                // Run the operator - this will consume from Kafka and write to Redis
                await kafkaToRedisOperator.RunAsync(sourceContext, cancellationToken);
            }
            catch (Exception ex)
            {
                _logger?.LogError(ex, "[TaskExecutor] Kafka-to-Redis task failed on TaskManager {TaskManagerId}", _taskManagerId);
                throw;
            }
        }

        public static IOperatorBarrierHandler? GetOperatorBarrierHandler(string jobVertexId, int subtaskIndex) 
        {
            // Implementation placeholder - registry available for future use
            return null;
        }
    }

    /// <summary>
    /// Simple source context implementation for TaskManager execution
    /// </summary>
    public class SimpleSourceContext : ISourceContext<string>
    {
        private readonly string _taskManagerId;
        private readonly ILogger? _logger;

        public SimpleSourceContext(string taskManagerId, ILogger? logger)
        {
            _taskManagerId = taskManagerId;
            _logger = logger;
        }

        public void Collect(string record)
        {
            // In the simplified architecture, the combined operator handles both Kafka and Redis
            // This just logs successful collection
            _logger?.LogDebug("TaskManager {TaskManagerId}: Collected record", _taskManagerId);
        }

        public Task CollectAsync(string record)
        {
            Collect(record);
            return Task.CompletedTask;
        }

        public void CollectWithTimestamp(string record, long timestamp)
        {
            Collect(record);
        }

        public Task CollectWithTimestampAsync(string record, long timestamp)
        {
            CollectWithTimestamp(record, timestamp);
            return Task.CompletedTask;
        }

        public void EmitWatermark(FlinkDotNet.Core.Abstractions.Windowing.Watermark watermark)
        {
            // Watermark emission for event time processing
            _logger?.LogDebug("TaskManager {TaskManagerId}: Emitted watermark {Timestamp}", _taskManagerId, watermark.Timestamp);
        }

        public long ProcessingTime => DateTimeOffset.UtcNow.ToUnixTimeMilliseconds();
    }
}
