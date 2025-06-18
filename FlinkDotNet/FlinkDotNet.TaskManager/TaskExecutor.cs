using FlinkDotNet.Core.Abstractions.Execution;
using FlinkDotNet.Core.Abstractions.Storage;
using FlinkDotNet.TaskManager.Services;
using FlinkDotNet.TaskManager.Operators;
using FlinkDotNet.Proto.Internal;
using System.Text.Json;
using Microsoft.Extensions.Logging;
using FlinkDotNet.Core.Abstractions.Sources;
using FlinkDotNet.Core.Abstractions.Sinks;
using System.Reflection;

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

                // Execute based on operator type
                if (descriptor.FullyQualifiedOperatorName.Contains("KafkaSourceFunction"))
                {
                    await ExecuteKafkaSourceTask(descriptor, operatorConfig, cancellationToken);
                }
                else if (descriptor.FullyQualifiedOperatorName.Contains("RedisIncrementSinkFunction"))
                {
                    await ExecuteRedisSinkTask(descriptor, operatorConfig, cancellationToken);
                }
                else
                {
                    _logger?.LogWarning("[TaskExecutor] Unknown operator type: {OperatorName}", descriptor.FullyQualifiedOperatorName);
                }
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

        private async Task ExecuteKafkaSourceTask(
            TaskDeploymentDescriptor descriptor, 
            Dictionary<string, object> config,
            CancellationToken cancellationToken)
        {
            _logger?.LogInformation("[TaskExecutor] Executing Kafka source task for TaskManager {TaskManagerId}, Subtask {SubtaskIndex}", 
                _taskManagerId, descriptor.SubtaskIndex);

            try
            {
                // Extract configuration
                var topic = config.TryGetValue("topic", out var topicObj) ? topicObj.ToString() : "flinkdotnet.sample.topic";
                var consumerGroupId = config.TryGetValue("consumerGroupId", out var groupObj) ? groupObj.ToString() : "flinkdotnet-consumer-group";

                // Create and configure Kafka source operator
                var kafkaSource = new KafkaSourceOperator(topic, consumerGroupId, _taskManagerId, _logger);
                
                _logger?.LogInformation("[TaskExecutor] Starting Kafka consumption for topic '{Topic}' with group '{ConsumerGroup}' on TaskManager {TaskManagerId}", 
                    topic, consumerGroupId, _taskManagerId);

                // Create a source context that forwards to Redis sink
                var sourceContext = new RedisForwardingSourceContext(config, _taskManagerId, _logger);
                await sourceContext.OpenAsync();

                try
                {
                    // Run the source function - this will consume from Kafka and forward to Redis
                    await kafkaSource.RunAsync(sourceContext, cancellationToken);
                }
                finally
                {
                    await sourceContext.CloseAsync();
                }
            }
            catch (Exception ex)
            {
                _logger?.LogError(ex, "[TaskExecutor] Kafka source task failed on TaskManager {TaskManagerId}", _taskManagerId);
                throw;
            }
        }

        private async Task ExecuteRedisSinkTask(
            TaskDeploymentDescriptor descriptor,
            Dictionary<string, object> config, 
            CancellationToken cancellationToken)
        {
            _logger?.LogInformation("[TaskExecutor] Executing Redis sink task for TaskManager {TaskManagerId}, Subtask {SubtaskIndex}", 
                _taskManagerId, descriptor.SubtaskIndex);

            try
            {
                // Extract configuration
                var redisSinkCounterKey = config.TryGetValue("redisSinkCounterKey", out var counterObj) ? 
                    counterObj.ToString() : "flinkdotnet:sample:processed_message_counter";
                var globalSequenceKey = config.TryGetValue("globalSequenceKey", out var seqObj) ? 
                    seqObj.ToString() : "flinkdotnet:global_sequence_id";
                var expectedMessages = config.TryGetValue("expectedMessages", out var expectedObj) ? 
                    Convert.ToInt32(expectedObj) : 1000000;

                _logger?.LogInformation("[TaskExecutor] Starting Redis sink for counter '{CounterKey}' and sequence '{SequenceKey}' on TaskManager {TaskManagerId}", 
                    redisSinkCounterKey, globalSequenceKey, _taskManagerId);

                // Create and configure Redis sink operator
                var redisSink = new RedisSinkOperator(
                    redisSinkCounterKey, globalSequenceKey, expectedMessages, _taskManagerId, _logger);

                await redisSink.OpenAsync();
                
                try
                {
                    // Keep sink alive to receive data from upstream (in a real implementation)
                    // For now, just keep it running
                    await Task.Delay(Timeout.Infinite, cancellationToken);
                }
                finally
                {
                    await redisSink.CloseAsync();
                }
            }
            catch (Exception ex)
            {
                _logger?.LogError(ex, "[TaskExecutor] Redis sink task failed on TaskManager {TaskManagerId}", _taskManagerId);
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
    /// Source context that forwards data directly to Redis sink for simplified architecture
    /// </summary>
    public class RedisForwardingSourceContext : ISourceContext<string>
    {
        private readonly RedisSinkOperator _redisSink;
        private readonly ILogger? _logger;
        private readonly string _taskManagerId;

        public RedisForwardingSourceContext(Dictionary<string, object> config, string taskManagerId, ILogger? logger)
        {
            _taskManagerId = taskManagerId;
            _logger = logger;

            // Extract Redis configuration
            var redisSinkCounterKey = config.TryGetValue("redisSinkCounterKey", out var counterObj) ? 
                counterObj.ToString() : "flinkdotnet:sample:processed_message_counter";
            var globalSequenceKey = config.TryGetValue("globalSequenceKey", out var seqObj) ? 
                seqObj.ToString() : "flinkdotnet:global_sequence_id";
            var expectedMessages = config.TryGetValue("expectedMessages", out var expectedObj) ? 
                Convert.ToInt32(expectedObj) : 1000000;

            _redisSink = new RedisSinkOperator(redisSinkCounterKey, globalSequenceKey, expectedMessages, taskManagerId, logger);
        }

        public async Task OpenAsync()
        {
            await _redisSink.OpenAsync();
        }

        public async Task CloseAsync()
        {
            await _redisSink.CloseAsync();
        }

        public void Collect(string record)
        {
            // Forward record directly to Redis sink
            _ = Task.Run(async () =>
            {
                try
                {
                    await _redisSink.InvokeAsync(record);
                }
                catch (Exception ex)
                {
                    _logger?.LogError(ex, "Error forwarding record to Redis sink on TaskManager {TaskManagerId}", _taskManagerId);
                }
            });
        }

        public void CollectWithTimestamp(string record, long timestamp)
        {
            Collect(record);
        }

        public void EmitWatermark(long timestamp)
        {
            // Watermark emission for event time processing
        }

        public void MarkAsTemporarilyIdle()
        {
            // Mark source as temporarily idle
        }

        public void Close()
        {
            _ = Task.Run(async () => await CloseAsync());
        }
    }
}
