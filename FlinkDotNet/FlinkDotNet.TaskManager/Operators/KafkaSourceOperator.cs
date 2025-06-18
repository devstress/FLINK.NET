using FlinkDotNet.Core.Abstractions.Sources;
using FlinkDotNet.Core.Abstractions.Checkpointing;
using FlinkDotNet.Connectors.Sources.Kafka;
using Confluent.Kafka;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.Logging;
using StackExchange.Redis;

namespace FlinkDotNet.TaskManager.Operators
{
    /// <summary>
    /// Kafka source operator for TaskManager execution.
    /// Consumes messages from Kafka topics and forwards them to downstream operators.
    /// </summary>
    public class KafkaSourceOperator : ISourceFunction<string>, ICheckpointedFunction
    {
        private readonly string _topic;
        private readonly string _consumerGroupId;
        private readonly string _taskManagerId;
        private readonly ILogger<KafkaSourceOperator>? _logger;
        private FlinkKafkaConsumerGroup? _consumerGroup;
        private volatile bool _isRunning = true;
        private readonly Dictionary<TopicPartition, long> _checkpointState;
        private readonly object _checkpointLock = new object();

        public KafkaSourceOperator(string topic, string consumerGroupId, string taskManagerId, ILogger<KafkaSourceOperator>? logger = null)
        {
            _topic = topic ?? throw new ArgumentNullException(nameof(topic));
            _consumerGroupId = consumerGroupId ?? throw new ArgumentNullException(nameof(consumerGroupId));
            _taskManagerId = taskManagerId ?? "Unknown";
            _logger = logger;
            _checkpointState = new Dictionary<TopicPartition, long>();
            
            _logger?.LogInformation("KafkaSourceOperator initialized for TaskManager {TaskManagerId}, Topic: {Topic}, ConsumerGroup: {ConsumerGroup}", 
                _taskManagerId, _topic, _consumerGroupId);
        }

        public async Task RunAsync(ISourceContext<string> ctx, CancellationToken cancellationToken = default)
        {
            try
            {
                _logger?.LogInformation("🚀 TaskManager {TaskManagerId}: Starting Kafka source operator for topic '{Topic}'", _taskManagerId, _topic);

                // Initialize FlinkKafkaConsumerGroup for proper Apache Flink 2.0 coordination
                _consumerGroup = new FlinkKafkaConsumerGroup(
                    GetKafkaBootstrapServers(),
                    new List<string> { _topic },
                    _consumerGroupId,
                    _taskManagerId);

                // Start consuming messages
                await foreach (var message in _consumerGroup.ConsumeAsync(cancellationToken))
                {
                    if (!_isRunning || cancellationToken.IsCancellationRequested)
                        break;

                    try
                    {
                        // Forward message to downstream operators via source context
                        ctx.Collect(message.Value);
                        
                        // Update checkpoint state
                        lock (_checkpointLock)
                        {
                            _checkpointState[message.TopicPartition] = message.Offset.Value;
                        }
                    }
                    catch (Exception ex)
                    {
                        _logger?.LogError(ex, "TaskManager {TaskManagerId}: Error processing message from Kafka", _taskManagerId);
                    }
                }

                _logger?.LogInformation("TaskManager {TaskManagerId}: Kafka source operator completed", _taskManagerId);
            }
            catch (Exception ex)
            {
                _logger?.LogError(ex, "TaskManager {TaskManagerId}: Kafka source operator failed", _taskManagerId);
                throw;
            }
            finally
            {
                _consumerGroup?.Dispose();
            }
        }

        public void Cancel()
        {
            _isRunning = false;
            _consumerGroup?.Stop();
        }

        public Task<byte[]> SnapshotStateAsync(long checkpointId, long checkpointTimestamp)
        {
            lock (_checkpointLock)
            {
                var state = System.Text.Json.JsonSerializer.SerializeToUtf8Bytes(_checkpointState);
                _logger?.LogDebug("TaskManager {TaskManagerId}: Snapshotted Kafka offsets for checkpoint {CheckpointId}", 
                    _taskManagerId, checkpointId);
                return Task.FromResult(state);
            }
        }

        public Task RestoreStateAsync(byte[] state)
        {
            try
            {
                var restoredState = System.Text.Json.JsonSerializer.Deserialize<Dictionary<TopicPartition, long>>(state);
                if (restoredState != null)
                {
                    lock (_checkpointLock)
                    {
                        _checkpointState.Clear();
                        foreach (var kvp in restoredState)
                        {
                            _checkpointState[kvp.Key] = kvp.Value;
                        }
                    }
                    _logger?.LogInformation("TaskManager {TaskManagerId}: Restored Kafka offsets from checkpoint", _taskManagerId);
                }
            }
            catch (Exception ex)
            {
                _logger?.LogError(ex, "TaskManager {TaskManagerId}: Failed to restore Kafka offset state", _taskManagerId);
            }
            return Task.CompletedTask;
        }

        private string GetKafkaBootstrapServers()
        {
            var configuration = new ConfigurationBuilder()
                .AddEnvironmentVariables()
                .Build();

            // Try various sources for Kafka bootstrap servers
            var bootstrapServers = configuration.GetConnectionString("kafka") ??
                                 Environment.GetEnvironmentVariable("DOTNET_KAFKA_BOOTSTRAP_SERVERS") ??
                                 Environment.GetEnvironmentVariable("ConnectionStrings__kafka") ??
                                 "localhost:9092";

            // Extract just the bootstrap servers if it's a full connection string
            if (bootstrapServers.Contains("bootstrap.servers="))
            {
                var parts = bootstrapServers.Split(';');
                var serversPart = parts.FirstOrDefault(p => p.StartsWith("bootstrap.servers="));
                if (serversPart != null)
                {
                    bootstrapServers = serversPart.Substring("bootstrap.servers=".Length);
                }
            }

            _logger?.LogInformation("TaskManager {TaskManagerId}: Using Kafka bootstrap servers: {BootstrapServers}", 
                _taskManagerId, bootstrapServers);
            return bootstrapServers;
        }
    }
}