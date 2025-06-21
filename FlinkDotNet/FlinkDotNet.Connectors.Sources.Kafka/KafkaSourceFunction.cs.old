using System;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;
using Confluent.Kafka;
using FlinkDotNet.Core.Abstractions.Sources;
using FlinkDotNet.Core.Abstractions.Checkpointing;
using Microsoft.Extensions.Logging;

namespace FlinkDotNet.Connectors.Sources.Kafka
{
    /// <summary>
    /// Kafka source function that implements the unified source API for both bounded and unbounded reading.
    /// Supports stream processing with event time and watermarks.
    /// Uses Apache Flink-style consumer group management for proper coordination between
    /// Flink's checkpointing mechanism and Kafka's consumer group protocol.
    /// </summary>
    /// <typeparam name="T">The type of records to produce</typeparam>
    public class KafkaSourceFunction<T> : IUnifiedSource<T>, ICheckpointedFunction
    {
        private readonly ConsumerConfig _consumerConfig;
        private readonly List<string> _topics;
        private readonly IDeserializer<T> _valueDeserializer;
        private readonly bool _isBounded;
        private readonly TimeSpan? _readTimeout;
        private readonly ILogger? _logger;
        private FlinkKafkaConsumerGroup? _consumerGroup;
        private CancellationTokenSource? _cancellationTokenSource;
        private readonly Dictionary<TopicPartition, long> _checkpointState;
        private readonly object _checkpointLock = new object();

        public KafkaSourceFunction(
            ConsumerConfig consumerConfig,
            List<string> topics,
            IDeserializer<T> valueDeserializer,
            bool isBounded = false,
            TimeSpan? readTimeout = null,
            ILogger? logger = null)
        {
            _consumerConfig = consumerConfig ?? throw new ArgumentNullException(nameof(consumerConfig));
            _topics = topics ?? throw new ArgumentNullException(nameof(topics));
            _valueDeserializer = valueDeserializer ?? throw new ArgumentNullException(nameof(valueDeserializer));
            _isBounded = isBounded;
            _readTimeout = readTimeout;
            _logger = logger;
            _checkpointState = new Dictionary<TopicPartition, long>();
        }

        public bool IsBounded => _isBounded;

        public void Run(ISourceContext<T> ctx)
        {
            RunAsync(ctx, CancellationToken.None).GetAwaiter().GetResult();
        }

        public async Task RunAsync(ISourceContext<T> ctx, CancellationToken cancellationToken)
        {
            _cancellationTokenSource = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken);
            var combinedToken = _cancellationTokenSource.Token;
            
            await InitializeConsumerGroupAsync();
            
            try
            {
                await ConsumeMessagesAsync(ctx, combinedToken);
            }
            finally
            {
                CleanupResources();
            }
        }

        private async Task InitializeConsumerGroupAsync()
        {
            _consumerGroup = new FlinkKafkaConsumerGroup(_consumerConfig, _logger);
            await _consumerGroup.InitializeAsync(_topics);
            
            // Restore offsets from checkpoint state if available
            Dictionary<TopicPartition, Offset>? offsetsToRestore = null;
            lock (_checkpointLock)
            {
                if (_checkpointState.Count > 0)
                {
                    offsetsToRestore = new Dictionary<TopicPartition, Offset>();
                    foreach (var kvp in _checkpointState)
                    {
                        offsetsToRestore[kvp.Key] = new Offset(kvp.Value);
                    }
                }
            }
            
            if (offsetsToRestore != null)
            {
                await _consumerGroup.RestoreFromCheckpointAsync(offsetsToRestore);
                _logger?.LogInformation("Restored consumer positions from checkpoint state");
            }
        }

        private async Task ConsumeMessagesAsync(ISourceContext<T> ctx, CancellationToken cancellationToken)
        {
            while (!cancellationToken.IsCancellationRequested)
            {
                try
                {
                    var consumeResult = _consumerGroup!.ConsumeMessage(_readTimeout ?? TimeSpan.FromMilliseconds(100));
                    
                    if (await ProcessConsumeResult(ctx, consumeResult))
                    {
                        break; // End of partition reached in bounded mode
                    }
                }
                catch (ConsumeException ex)
                {
                    if (HandleConsumeException(ex))
                    {
                        break; // Fatal error encountered
                    }
                }
                catch (Exception ex)
                {
                    _logger?.LogError(ex, "Unexpected error in Kafka consumer loop");
                    // Continue processing unless it's a fatal error
                    if (ex is InvalidOperationException)
                        break;
                }
            }
        }

        private async Task<bool> ProcessConsumeResult(ISourceContext<T> ctx, ConsumeResult<Ignore, byte[]>? consumeResult)
        {
            if (consumeResult?.Message == null)
                return false;

            // For byte[] messages, we need to handle the conversion to T
            T value;
            if (typeof(T) == typeof(byte[]))
            {
                // Direct byte array assignment for byte[] type
                value = (T)(object)consumeResult.Message.Value;
            }
            else if (typeof(T) == typeof(string))
            {
                // Convert bytes to string for string type  
                value = (T)(object)System.Text.Encoding.UTF8.GetString(consumeResult.Message.Value);
            }
            else
            {
                // Use the deserializer for other types
                try
                {
                    value = _valueDeserializer.Deserialize(consumeResult.Message.Value, false, SerializationContext.Empty);
                }
                catch (Exception ex)
                {
                    _logger?.LogError(ex, "Failed to deserialize message from topic {Topic}, partition {Partition}, offset {Offset}", 
                        consumeResult.Topic, consumeResult.Partition, consumeResult.Offset);
                    return false; // Skip malformed messages
                }
            }

            var timestamp = consumeResult.Message.Timestamp.UnixTimestampMs;
            
            // Emit the record with event time if available
            if (timestamp > 0)
            {
                await ctx.CollectWithTimestampAsync(value, timestamp);
            }
            else
            {
                await ctx.CollectAsync(value);
            }
            
            // For bounded mode, stop after consuming all available messages
            if (_isBounded && consumeResult.IsPartitionEOF)
            {
                _logger?.LogInformation("Reached end of partition for bounded Kafka source");
                return true;
            }

            return false;
        }

        private bool HandleConsumeException(ConsumeException ex)
        {
            _logger?.LogError(ex, "Error consuming from Kafka: {Error}", ex.Error.Reason);
            
            // For critical errors, we should stop processing
            return ex.Error.IsFatal;
        }

        private void CleanupResources()
        {
            _consumerGroup?.Dispose();
            _cancellationTokenSource?.Dispose();
            _logger?.LogInformation("Kafka consumer group closed");
        }

        public void Cancel()
        {
            _cancellationTokenSource?.Cancel();
            _logger?.LogInformation("Kafka source cancellation requested");
        }

        // Implement ICheckpointedFunction for Apache Flink-style state management
        public void SnapshotState(long checkpointId, long checkpointTimestamp)
        {
            lock (_checkpointLock)
            {
                // Save current consumer positions to checkpoint state
                var assignment = _consumerGroup?.GetAssignment() ?? new List<TopicPartition>();
                _checkpointState.Clear();
                
                foreach (var partition in assignment)
                {
                    // Get committed offset + 1 (next offset to consume)
                    // This will be restored on recovery
                    var groupId = _consumerGroup?.GetConsumerGroupId();
                    if (groupId != null)
                    {
                        // Store partition offsets for checkpoint
                        // In a real implementation, this would get the actual position
                        // For now, this is a placeholder for the checkpoint mechanism
                        _checkpointState[partition] = 0; // Placeholder
                    }
                }
                
                _logger?.LogDebug("Snapshotted state for checkpoint {CheckpointId}: {PartitionCount} partitions", 
                    checkpointId, _checkpointState.Count);
            }
        }

        public void RestoreState(object state)
        {
            if (state is Dictionary<TopicPartition, long> restoredState)
            {
                lock (_checkpointLock)
                {
                    _checkpointState.Clear();
                    foreach (var kvp in restoredState)
                    {
                        _checkpointState[kvp.Key] = kvp.Value;
                    }
                    
                    _logger?.LogInformation("Restored state from checkpoint: {PartitionCount} partitions", 
                        _checkpointState.Count);
                }
            }
        }

        public void NotifyCheckpointComplete(long checkpointId)
        {
            // Commit offsets after successful checkpoint
            _consumerGroup?.CommitCheckpointOffsetsAsync(checkpointId).GetAwaiter().GetResult();
            _logger?.LogDebug("Notified of checkpoint {CheckpointId} completion, offsets committed", checkpointId);
        }
    }

    /// <summary>
    /// Builder for creating Kafka source functions with fluent API
    /// </summary>
    public class KafkaSourceBuilder<T>
    {
        private ConsumerConfig? _consumerConfig;
        private readonly List<string> _topics = new();
        private IDeserializer<T>? _valueDeserializer;
        private bool _isBounded = false;
        private TimeSpan? _readTimeout;
        private ILogger? _logger;

        public KafkaSourceBuilder<T> BootstrapServers(string servers)
        {
            _consumerConfig ??= new ConsumerConfig();
            _consumerConfig.BootstrapServers = servers;
            return this;
        }

        public KafkaSourceBuilder<T> GroupId(string groupId)
        {
            _consumerConfig ??= new ConsumerConfig();
            _consumerConfig.GroupId = groupId;
            return this;
        }

        public KafkaSourceBuilder<T> Topic(string topic)
        {
            _topics.Add(topic);
            return this;
        }

        public KafkaSourceBuilder<T> Topics(params string[] topics)
        {
            _topics.AddRange(topics);
            return this;
        }

        public KafkaSourceBuilder<T> ValueDeserializer(IDeserializer<T> deserializer)
        {
            _valueDeserializer = deserializer;
            return this;
        }

        public KafkaSourceBuilder<T> Bounded(bool bounded = true)
        {
            _isBounded = bounded;
            return this;
        }

        public KafkaSourceBuilder<T> ReadTimeout(TimeSpan timeout)
        {
            _readTimeout = timeout;
            return this;
        }

        public KafkaSourceBuilder<T> Logger(ILogger logger)
        {
            _logger = logger;
            return this;
        }

        public KafkaSourceBuilder<T> ConsumerConfig(ConsumerConfig config)
        {
            _consumerConfig = config;
            return this;
        }

        public KafkaSourceFunction<T> Build()
        {
            if (_consumerConfig == null)
                throw new InvalidOperationException("Consumer configuration is required");
            if (_topics.Count == 0)
                throw new InvalidOperationException("At least one topic must be specified");
            if (_valueDeserializer == null)
                throw new InvalidOperationException("Value deserializer is required");

            return new KafkaSourceFunction<T>(
                _consumerConfig, 
                _topics, 
                _valueDeserializer, 
                _isBounded, 
                _readTimeout,
                _logger);
        }
    }
}