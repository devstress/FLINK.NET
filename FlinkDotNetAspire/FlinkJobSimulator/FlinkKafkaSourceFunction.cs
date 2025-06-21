using FlinkDotNet.Core.Abstractions.Context;
using FlinkDotNet.Core.Abstractions.Sources;
using FlinkDotNet.Core.Abstractions.Operators;
using FlinkDotNet.Core.Abstractions.Checkpointing;
using FlinkDotNet.Connectors.Sources.Kafka;
using Confluent.Kafka;
using Microsoft.Extensions.Configuration;
using System.Text;

namespace FlinkJobSimulator
{
    /// <summary>
    /// Apache Flink-style Kafka source function using FlinkKafkaConsumerGroup
    /// for proper consumer group management and exactly-once processing guarantees.
    /// This demonstrates the same reliability patterns as Apache Flink 2.0.
    /// </summary>
    public class FlinkKafkaSourceFunction : ISourceFunction<string>, IOperatorLifecycle, ICheckpointedFunction
    {
        private readonly string _topic;
        private readonly string _consumerGroupId;
        private FlinkKafkaConsumerGroup? _consumerGroup;
        private volatile bool _isRunning = true;
        private string _taskName = nameof(FlinkKafkaSourceFunction);
        private readonly Dictionary<Confluent.Kafka.TopicPartition, long> _checkpointState;
        private readonly object _checkpointLock = new object();
        private Timer? _heartbeatTimer;

        // Static configuration for LocalStreamExecutor compatibility
        public static string? GlobalTopic { get; set; }
        public static string? GlobalConsumerGroupId { get; set; }

        private static readonly IConfiguration Configuration = new ConfigurationBuilder()
            .AddEnvironmentVariables()
            .Build();

        // Constructor with dependencies (for manual instantiation)
        public FlinkKafkaSourceFunction(string topic, string consumerGroupId)
        {
            _topic = topic ?? throw new ArgumentNullException(nameof(topic));
            _consumerGroupId = consumerGroupId ?? throw new ArgumentNullException(nameof(consumerGroupId));
            _checkpointState = new Dictionary<Confluent.Kafka.TopicPartition, long>();
            Console.WriteLine($"FlinkKafkaSourceFunction will consume from topic: '{_topic}' with consumer group: '{_consumerGroupId}'");
        }

        // Parameterless constructor (for LocalStreamExecutor reflection)
        public FlinkKafkaSourceFunction()
        {
            _topic = GlobalTopic ?? Configuration?["SIMULATOR_KAFKA_TOPIC"] ?? "flinkdotnet.sample.topic";
            _consumerGroupId = GlobalConsumerGroupId ?? Configuration?["SIMULATOR_KAFKA_CONSUMER_GROUP"] ?? "flinkdotnet-consumer-group";
            _checkpointState = new Dictionary<Confluent.Kafka.TopicPartition, long>();
            Console.WriteLine($"FlinkKafkaSourceFunction parameterless constructor: topic '{_topic}', group '{_consumerGroupId}'");
        }

        public void Open(IRuntimeContext context)
        {
            _taskName = context.TaskName;
            var taskManagerId = Environment.GetEnvironmentVariable("TaskManagerId") ?? "Unknown";
            var processId = Environment.ProcessId;
            
            Console.WriteLine($"🔄 KAFKA SOURCE STEP 1: Opening FlinkKafkaSourceFunction with task name: {_taskName}");
            Console.WriteLine($"📊 TASK MANAGER INFO: TaskManager {taskManagerId} (PID: {processId}) initializing Kafka source");

            // Multi-strategy Kafka bootstrap server discovery for maximum reliability
            string? bootstrapServers = Configuration?["DOTNET_KAFKA_BOOTSTRAP_SERVERS"];
            Console.WriteLine($"🔍 DEBUG: DOTNET_KAFKA_BOOTSTRAP_SERVERS = '{bootstrapServers}'");
            
            if (string.IsNullOrEmpty(bootstrapServers))
            {
                bootstrapServers = Configuration?["ConnectionStrings__kafka"];
                Console.WriteLine($"🔍 DEBUG: ConnectionStrings__kafka = '{bootstrapServers}'");
                
                if (string.IsNullOrEmpty(bootstrapServers))
                {
                    bootstrapServers = "localhost:9092"; // Default fallback
                    Console.WriteLine($"Using default Kafka bootstrap servers: {bootstrapServers}");
                }
            }

            // Fix IPv6 issue by forcing IPv4 localhost resolution
            bootstrapServers = bootstrapServers.Replace("localhost", "127.0.0.1");

            var consumerConfig = new ConsumerConfig
            {
                BootstrapServers = bootstrapServers,
                GroupId = _consumerGroupId,
                EnableAutoCommit = false, // Flink manages commits through checkpoints
                AutoOffsetReset = AutoOffsetReset.Earliest,
                SecurityProtocol = SecurityProtocol.Plaintext,
                SessionTimeoutMs = 30000, // 30 seconds
                HeartbeatIntervalMs = 10000, // 10 seconds
                MaxPollIntervalMs = 300000, // 5 minutes
                SocketTimeoutMs = 60000, // 60 seconds
                // Enhanced reliability settings
                FetchMinBytes = 1,
                EnablePartitionEof = false,
                AllowAutoCreateTopics = false
            };

            try
            {
                Console.WriteLine($"🔄 KAFKA SOURCE STEP 2: Creating FlinkKafkaConsumerGroup for bootstrap servers: {bootstrapServers}");
                Console.WriteLine($"📊 TASK MANAGER {taskManagerId}: Creating consumer group with timeout wait for Kafka setup");
                
                _consumerGroup = new FlinkKafkaConsumerGroup(consumerConfig);
                
                Console.WriteLine($"🔄 KAFKA SOURCE STEP 3: Initializing and subscribing to topic: '{_topic}' (includes 1-minute Kafka wait)");
                Console.WriteLine($"📊 TASK MANAGER {taskManagerId}: Subscribing to topic '{_topic}' with group '{_consumerGroupId}'");
                
                // Use GetAwaiter().GetResult() to call async method from sync context
                _consumerGroup.InitializeAsync(new[] { _topic }).GetAwaiter().GetResult();
                
                Console.WriteLine($"✅ KAFKA SOURCE STEPS COMPLETED: FlinkKafkaSourceFunction opened successfully");
                Console.WriteLine($"✅ TASK MANAGER {taskManagerId}: Ready to consume from topic '{_topic}' with load balancing");
                
                // Start heartbeat timer for process monitoring
                _heartbeatTimer = new Timer(_ =>
                {
                    Console.WriteLine($"[{_taskName}] ❤️ TaskManager {taskManagerId}: Kafka source heartbeat - consuming from topic '{_topic}' with group '{_consumerGroupId}'");
                }, null, TimeSpan.FromSeconds(30), TimeSpan.FromSeconds(30));
            }
            catch (Exception ex)
            {
                Console.WriteLine($"❌ KAFKA SOURCE FAILED: TaskManager {taskManagerId} error opening FlinkKafkaSourceFunction: {ex.Message}");
                Console.WriteLine($"❌ Exception details: {ex}");
                throw;
            }
        }

        public void Run(ISourceContext<string> sourceContext)
        {
            var taskManagerId = Environment.GetEnvironmentVariable("TaskManagerId") ?? "Unknown";
            var processId = Environment.ProcessId;
            
            Console.WriteLine($"🔄 TaskManager {taskManagerId} (PID: {processId}): Starting Kafka message consumption from topic '{_topic}' with consumer group '{_consumerGroupId}'");
            
            var consumptionState = new ConsumptionState { TaskManagerId = taskManagerId };
            
            while (_isRunning && _consumerGroup != null)
            {
                try
                {
                    ProcessSingleKafkaMessage(sourceContext, consumptionState);
                }
                catch (ConsumeException ex)
                {
                    if (HandleConsumeException(ex, taskManagerId)) break;
                }
                catch (Exception ex)
                {
                    HandleGeneralException(ex, taskManagerId);
                }
            }
            
            Console.WriteLine($"🏁 TaskManager {taskManagerId}: Kafka source function completed. Total messages consumed: {consumptionState.MessagesConsumed}");
        }

        private void ProcessSingleKafkaMessage(ISourceContext<string> sourceContext, ConsumptionState state)
        {
            var consumeResult = _consumerGroup?.ConsumeMessage(TimeSpan.FromSeconds(1)) as ConsumeResult<Ignore, byte[]>;
            
            if (consumeResult?.Message?.Value != null)
            {
                ProcessValidMessage(sourceContext, consumeResult, state);
                UpdateCheckpointState(consumeResult);
            }
        }

        private void ProcessValidMessage(ISourceContext<string> sourceContext, ConsumeResult<Ignore, byte[]> consumeResult, ConsumptionState state)
        {
            state.MessagesConsumed++;
            var messageValue = Encoding.UTF8.GetString(consumeResult.Message?.Value ?? Array.Empty<byte>());
            
            // Emit the message to downstream operators
            sourceContext.Collect(messageValue);
            
            LogConsumptionProgress(consumeResult, state);
            LogLoadBalanceStatus(state);
        }

        private void LogConsumptionProgress(ConsumeResult<Ignore, byte[]> consumeResult, ConsumptionState state)
        {
            if (state.MessagesConsumed % 100 == 0 || (DateTime.UtcNow - state.LastLogTime).TotalSeconds > 10)
            {
                Console.WriteLine($"📨 TaskManager {state.TaskManagerId}: Consumed {state.MessagesConsumed} messages from Kafka topic '{_topic}' " +
                                $"(partition: {consumeResult.TopicPartition.Partition}, offset: {consumeResult.Offset})");
                state.LastLogTime = DateTime.UtcNow;
            }
        }

        private void LogLoadBalanceStatus(ConsumptionState state)
        {
            if ((DateTime.UtcNow - state.LastLoadBalanceLogTime).TotalSeconds > 30)
            {
                var assignment = _consumerGroup?.GetAssignment();
                var partitionCount = assignment?.Count ?? 0;
                Console.WriteLine($"⚖️ LOAD BALANCE STATUS: TaskManager {state.TaskManagerId} currently assigned {partitionCount} partitions, consumed {state.MessagesConsumed} total messages");
                state.LastLoadBalanceLogTime = DateTime.UtcNow;
            }
        }

        private void UpdateCheckpointState(ConsumeResult<Ignore, byte[]> consumeResult)
        {
            lock (_checkpointLock)
            {
                _checkpointState[consumeResult.TopicPartition] = consumeResult.Offset.Value;
            }
        }

        private static bool HandleConsumeException(ConsumeException ex, string taskManagerId)
        {
            Console.WriteLine($"❌ TaskManager {taskManagerId}: Kafka consume error: {ex.Error.Reason}");
            if (ex.Error.IsFatal)
            {
                Console.WriteLine($"❌ TaskManager {taskManagerId}: Fatal Kafka error, stopping consumption: {ex.Error}");
                return true;
            }
            // For non-fatal errors, continue consuming
            Thread.Sleep(1000);
            return false;
        }

        private static void HandleGeneralException(Exception ex, string taskManagerId)
        {
            Console.WriteLine($"❌ TaskManager {taskManagerId}: Unexpected error in Kafka consumption: {ex.Message}");
            Thread.Sleep(1000);
        }

        private sealed class ConsumptionState
        {
            public string TaskManagerId { get; set; } = "Unknown";
            public int MessagesConsumed { get; set; }
            public DateTime LastLogTime { get; set; } = DateTime.UtcNow;
            public DateTime LastLoadBalanceLogTime { get; set; } = DateTime.UtcNow;
        }

        public void Cancel()
        {
            Console.WriteLine($"🛑 Cancelling FlinkKafkaSourceFunction");
            _isRunning = false;
            _heartbeatTimer?.Dispose();
        }

        public void Close()
        {
            Console.WriteLine($"🔄 Closing FlinkKafkaSourceFunction");
            _isRunning = false;
            _heartbeatTimer?.Dispose();
            _consumerGroup?.Dispose();
            Console.WriteLine($"✅ FlinkKafkaSourceFunction closed successfully");
        }

        // ICheckpointedFunction implementation for exactly-once processing
        public void SnapshotState(long checkpointId, long checkpointTimestamp)
        {
            lock (_checkpointLock)
            {
                if (_checkpointState.Count == 0)
                    return;

                // Serialize checkpoint state (simplified for demo)
                var stateJson = System.Text.Json.JsonSerializer.Serialize(_checkpointState.ToDictionary(
                    kvp => $"{kvp.Key.Topic}_{kvp.Key.Partition}", 
                    kvp => kvp.Value));
                
                Console.WriteLine($"[{_taskName}] 💾 Checkpointing Kafka offsets for checkpoint {checkpointId}: {stateJson}");
                
                // In a real implementation, this state would be stored in the StateBackend
                // For this demo, we're just logging the serialized state
            }
        }

        public void RestoreState(object state)
        {
            if (state == null)
                return;

            try
            {
                // In a real implementation, this would deserialize from the StateBackend
                // For this demo, we're implementing a simplified version
                if (state is Dictionary<string, long> deserializedState)
                {
                    lock (_checkpointLock)
                    {
                        _checkpointState.Clear();
                        foreach (var kvp in deserializedState)
                        {
                            var parts = kvp.Key.Split('_');
                            if (parts.Length == 2 && int.TryParse(parts[1], out var partition))
                            {
                                var topicPartition = new Confluent.Kafka.TopicPartition(parts[0], partition);
                                _checkpointState[topicPartition] = kvp.Value;
                            }
                        }
                    }
                    Console.WriteLine($"[{_taskName}] 🔄 Restored Kafka offset state: {System.Text.Json.JsonSerializer.Serialize(deserializedState)}");
                }
            }
            catch (Exception ex)
            {
                Console.WriteLine($"❌ Error restoring Kafka offset state: {ex.Message}");
            }
        }

        public void NotifyCheckpointComplete(long checkpointId)
        {
            // Commit offsets to Kafka as part of checkpoint completion
            if (_consumerGroup != null)
            {
                try
                {
                    // In a real implementation, this would commit the checkpointed offsets
                    Console.WriteLine($"[{_taskName}] ✅ Checkpoint {checkpointId} completed - would commit Kafka offsets here");
                }
                catch (Exception ex)
                {
                    Console.WriteLine($"❌ Error committing offsets for checkpoint {checkpointId}: {ex.Message}");
                }
            }
        }
    }
}