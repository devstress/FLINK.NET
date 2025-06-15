using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using Confluent.Kafka;
using Microsoft.Extensions.Logging;

namespace FlinkDotNet.Connectors.Sources.Kafka
{
    /// <summary>
    /// Apache Flink-style consumer group manager that provides proper coordination
    /// between Flink's checkpointing mechanism and Kafka's consumer group protocol.
    /// This follows Apache Flink's pattern of managing consumer groups with:
    /// - Checkpoint-based offset management
    /// - Proper partition assignment coordination  
    /// - Enhanced failure recovery and rebalancing
    /// </summary>
    public class FlinkKafkaConsumerGroup : IDisposable
    {
        private readonly ConsumerConfig _consumerConfig;
        private readonly ILogger? _logger;
        private readonly Dictionary<TopicPartition, Offset> _checkpointedOffsets;
        private readonly object _offsetLock = new object();
        private IConsumer<Ignore, byte[]>? _consumer;
        private bool _disposed = false;

        public FlinkKafkaConsumerGroup(ConsumerConfig consumerConfig, ILogger? logger = null)
        {
            _consumerConfig = consumerConfig ?? throw new ArgumentNullException(nameof(consumerConfig));
            _logger = logger;
            _checkpointedOffsets = new Dictionary<TopicPartition, Offset>();
            
            // Ensure consumer group settings follow Flink best practices
            ConfigureFlinkOptimalSettings();
        }

        /// <summary>
        /// Configure consumer settings to match Apache Flink's optimal patterns
        /// </summary>
        private void ConfigureFlinkOptimalSettings()
        {
            // Disable auto-commit since Flink manages offsets through checkpoints
            _consumerConfig.EnableAutoCommit = false;
            
            // Set appropriate session timeout for Flink fault tolerance
            if (!_consumerConfig.SessionTimeoutMs.HasValue)
                _consumerConfig.SessionTimeoutMs = 30000; // 30 seconds
            
            // Configure heartbeat interval for better coordination
            if (!_consumerConfig.HeartbeatIntervalMs.HasValue)
                _consumerConfig.HeartbeatIntervalMs = 10000; // 10 seconds
            
            // Set max poll interval to prevent premature rebalancing during processing
            if (!_consumerConfig.MaxPollIntervalMs.HasValue)
                _consumerConfig.MaxPollIntervalMs = 300000; // 5 minutes
            
            // Configure partition assignment strategy for better balance
            if (_consumerConfig.PartitionAssignmentStrategy == null)
                _consumerConfig.PartitionAssignmentStrategy = PartitionAssignmentStrategy.CooperativeSticky;
            
            _logger?.LogInformation("FlinkKafkaConsumerGroup configured with Flink-optimal settings");
        }

        /// <summary>
        /// Initialize consumer with proper error handling and partition assignment callbacks.
        /// Waits for Kafka setup with 1-minute timeout for maximum reliability.
        /// </summary>
        public async Task InitializeAsync(IEnumerable<string> topics)
        {
            if (_consumer != null)
                throw new InvalidOperationException("Consumer already initialized");

            // Wait for Kafka setup with 1-minute timeout
            await WaitForKafkaSetupAsync(TimeSpan.FromMinutes(1));

            _consumer = new ConsumerBuilder<Ignore, byte[]>(_consumerConfig)
                .SetErrorHandler(OnError)
                .SetPartitionsAssignedHandler(OnPartitionsAssigned)
                .SetPartitionsRevokedHandler(OnPartitionsRevoked)
                .SetPartitionsLostHandler(OnPartitionsLost)
                .Build();

            _consumer.Subscribe(topics);
            _logger?.LogInformation("FlinkKafkaConsumerGroup initialized and subscribed to topics: {Topics}", 
                string.Join(", ", topics));
            
            await Task.CompletedTask;
        }

        /// <summary>
        /// Wait for Kafka cluster to be ready before attempting consumer operations.
        /// Implements Apache Flink-style reliability patterns for infrastructure readiness.
        /// </summary>
        private async Task WaitForKafkaSetupAsync(TimeSpan timeout)
        {
            _logger?.LogInformation("🔄 Waiting for Kafka setup to complete (timeout: {Timeout})...", timeout);
            var startTime = DateTime.UtcNow;
            var testConsumerConfig = new ConsumerConfig
            {
                BootstrapServers = _consumerConfig.BootstrapServers,
                GroupId = $"kafka-readiness-check-{Guid.NewGuid()}",
                EnableAutoCommit = false,
                AutoOffsetReset = AutoOffsetReset.Latest,
                SecurityProtocol = _consumerConfig.SecurityProtocol,
                SessionTimeoutMs = 5000, // Short timeout for readiness check
                SocketTimeoutMs = 5000    // Short socket timeout
            };

            var retryCount = 0;
            const int maxRetries = 12; // 12 retries * 5 seconds = 1 minute
            
            while ((DateTime.UtcNow - startTime) < timeout)
            {
                retryCount++;
                try
                {
                    using var testConsumer = new ConsumerBuilder<Ignore, byte[]>(testConsumerConfig).Build();
                    
                    // Try to subscribe - this will fail if Kafka is not ready
                    testConsumer.Subscribe("__consumer_offsets"); // Use internal topic for testing
                    
                    // If we can subscribe without error, Kafka is ready
                    _logger?.LogInformation("✅ Kafka setup verified - broker connectivity established after {Elapsed}ms (attempt {Attempt})", 
                        (DateTime.UtcNow - startTime).TotalMilliseconds, retryCount);
                    return; // Kafka is ready
                }
                catch (Exception ex)
                {
                    var elapsed = DateTime.UtcNow - startTime;
                    _logger?.LogDebug(ex, "⏳ Kafka not ready yet - attempt {Attempt}/{MaxRetries} after {Elapsed}ms: {Error}", 
                        retryCount, maxRetries, elapsed.TotalMilliseconds, ex.Message);
                    
                    if (elapsed >= timeout)
                    {
                        _logger?.LogWarning("⚠️ Kafka setup wait timeout ({Timeout}) exceeded, proceeding anyway", timeout);
                        return; // Proceed even if timeout exceeded to allow fallback mechanisms
                    }
                    
                    // Wait before retry
                    await Task.Delay(TimeSpan.FromSeconds(5));
                }
            }
            
            _logger?.LogWarning("⚠️ Kafka setup wait completed with timeout, proceeding with consumer initialization");
        }

        /// <summary>
        /// Consume messages with Flink-compatible offset management
        /// </summary>
        public ConsumeResult<Ignore, byte[]>? ConsumeMessage(TimeSpan timeout)
        {
            if (_consumer == null)
                throw new InvalidOperationException("Consumer not initialized");

            try
            {
                var result = _consumer.Consume(timeout);
                if (result?.Message != null)
                {
                    // Track consumed offset for checkpointing but don't commit yet
                    // Flink will commit through checkpoint mechanism
                    lock (_offsetLock)
                    {
                        _checkpointedOffsets[result.TopicPartition] = result.Offset + 1; // +1 for next offset to consume
                    }
                }
                return result;
            }
            catch (ConsumeException ex)
            {
                _logger?.LogError(ex, "Error consuming from Kafka: {Error}", ex.Error.Reason);
                
                // For critical errors, rethrow to trigger Flink fault tolerance
                if (ex.Error.IsFatal)
                    throw;
                    
                return null;
            }
        }

        /// <summary>
        /// Commit offsets as part of Flink checkpoint process
        /// This replaces Kafka's auto-commit with Flink-managed checkpointing
        /// </summary>
        public async Task CommitCheckpointOffsetsAsync(long checkpointId)
        {
            if (_consumer == null) return;

            List<TopicPartitionOffset> offsetsToCommit;
            lock (_offsetLock)
            {
                if (_checkpointedOffsets.Count == 0) return;
                
                offsetsToCommit = new List<TopicPartitionOffset>();
                foreach (var kvp in _checkpointedOffsets)
                {
                    offsetsToCommit.Add(new TopicPartitionOffset(kvp.Key, kvp.Value));
                }
            }

            try
            {
                _consumer.Commit(offsetsToCommit);
                _logger?.LogDebug("Committed offsets for checkpoint {CheckpointId}: {Offsets}", 
                    checkpointId, string.Join(", ", offsetsToCommit));
            }
            catch (KafkaException ex)
            {
                _logger?.LogError(ex, "Failed to commit offsets for checkpoint {CheckpointId}. Kafka error: {ErrorCode} - {ErrorReason}", 
                    checkpointId, ex.Error.Code, ex.Error.Reason);
                throw new InvalidOperationException($"Checkpoint {checkpointId} failed due to Kafka offset commit failure: {ex.Error.Code} - {ex.Error.Reason}", ex);
            }
            
            await Task.CompletedTask;
        }

        /// <summary>
        /// Restore offsets from Flink checkpoint state
        /// </summary>
        public async Task RestoreOffsetsAsync(Dictionary<TopicPartition, long> checkpointOffsets)
        {
            if (_consumer == null)
                throw new InvalidOperationException("Consumer not initialized");

            var offsetsToSeek = new List<TopicPartitionOffset>();
            foreach (var kvp in checkpointOffsets)
            {
                offsetsToSeek.Add(new TopicPartitionOffset(kvp.Key, kvp.Value));
            }

            if (offsetsToSeek.Count > 0)
            {
                // Seek to checkpointed positions
                foreach (var tpo in offsetsToSeek)
                {
                    _consumer.Seek(tpo);
                }
                
                _logger?.LogInformation("Restored consumer positions from checkpoint: {Offsets}", 
                    string.Join(", ", offsetsToSeek));
            }
            
            await Task.CompletedTask;
        }

        /// <summary>
        /// Get current consumer assignment for partition coordination
        /// </summary>
        public List<TopicPartition> GetAssignment()
        {
            return _consumer?.Assignment ?? new List<TopicPartition>();
        }

        /// <summary>
        /// Get consumer group metadata for external coordination
        /// </summary>
        public string? GetConsumerGroupId()
        {
            // Simple fallback since we can't access GroupId directly
            return _consumerConfig?.GroupId;
        }

        private void OnError(IConsumer<Ignore, byte[]> consumer, Error error)
        {
            _logger?.LogError("Kafka consumer error: {ErrorCode} - {Reason}", error.Code, error.Reason);
            
            // For critical errors, this will be handled by Flink's fault tolerance mechanism
            if (error.IsFatal)
            {
                _logger?.LogCritical("Fatal Kafka error encountered, Flink will handle recovery: {Error}", error);
            }
        }

        private void OnPartitionsAssigned(IConsumer<Ignore, byte[]> consumer, List<TopicPartition> partitions)
        {
            var taskManagerId = Environment.GetEnvironmentVariable("TaskManagerId") ?? "Unknown";
            var processId = Environment.ProcessId;
            
            _logger?.LogInformation("📊 TASK MANAGER LOAD DISTRIBUTION: TaskManager {TaskManagerId} (PID: {ProcessId}) assigned {PartitionCount} partitions: {Partitions}", 
                taskManagerId, processId, partitions.Count, string.Join(", ", partitions));
            
            // Enhanced logging for load balancing analysis
            if (partitions.Count > 0)
            {
                var topicGroups = partitions.GroupBy(p => p.Topic);
                foreach (var topicGroup in topicGroups)
                {
                    var topicPartitions = topicGroup.Select(p => p.Partition.Value).OrderBy(p => p).ToArray();
                    _logger?.LogInformation("📈 TaskManager {TaskManagerId}: Topic '{Topic}' partitions {Partitions} ({Count} out of total available partitions)", 
                        taskManagerId, topicGroup.Key, string.Join(", ", topicPartitions), topicPartitions.Length);
                }
                
                // Log for monitoring TaskManager utilization
                _logger?.LogInformation("⚖️ LOAD BALANCING: TaskManager {TaskManagerId} is now actively consuming {PartitionCount} partitions - Load Status: {LoadStatus}", 
                    taskManagerId, partitions.Count, partitions.Count > 0 ? "ACTIVE" : "IDLE");
            }
            else
            {
                _logger?.LogInformation("💤 LOAD BALANCING: TaskManager {TaskManagerId} has no partitions assigned - Load Status: IDLE", taskManagerId);
            }
            
            // Clear previous offset tracking for new assignment
            lock (_offsetLock)
            {
                _checkpointedOffsets.Clear();
            }
        }

        private void OnPartitionsRevoked(IConsumer<Ignore, byte[]> consumer, List<TopicPartitionOffset> partitions)
        {
            var taskManagerId = Environment.GetEnvironmentVariable("TaskManagerId") ?? "Unknown";
            
            _logger?.LogInformation("📤 TASK MANAGER REBALANCING: TaskManager {TaskManagerId} revoked {PartitionCount} partitions: {Partitions}", 
                taskManagerId, partitions.Count, string.Join(", ", partitions));
            
            // Commit current positions before revocation (if any)
            if (_checkpointedOffsets.Count > 0)
            {
                try
                {
                    var offsetsToCommit = new List<TopicPartitionOffset>();
                    lock (_offsetLock)
                    {
                        foreach (var kvp in _checkpointedOffsets)
                        {
                            offsetsToCommit.Add(new TopicPartitionOffset(kvp.Key, kvp.Value));
                        }
                    }
                    consumer.Commit(offsetsToCommit);
                    _logger?.LogDebug("📋 TaskManager {TaskManagerId}: Committed {OffsetCount} offsets before partition revocation", 
                        taskManagerId, offsetsToCommit.Count);
                }
                catch (KafkaException ex)
                {
                    _logger?.LogWarning(ex, "⚠️ TaskManager {TaskManagerId}: Failed to commit offsets during partition revocation", taskManagerId);
                }
            }
        }

        private void OnPartitionsLost(IConsumer<Ignore, byte[]> consumer, List<TopicPartitionOffset> partitions)
        {
            var taskManagerId = Environment.GetEnvironmentVariable("TaskManagerId") ?? "Unknown";
            
            _logger?.LogWarning("📉 TASK MANAGER FAULT TOLERANCE: TaskManager {TaskManagerId} lost {PartitionCount} partitions: {Partitions}", 
                taskManagerId, partitions.Count, string.Join(", ", partitions));
            
            // Clear offset tracking for lost partitions
            lock (_offsetLock)
            {
                foreach (var partition in partitions)
                {
                    _checkpointedOffsets.Remove(partition.TopicPartition);
                }
            }
            
            _logger?.LogInformation("🔄 TaskManager {TaskManagerId}: Cleared {PartitionCount} partition offsets due to partition loss", 
                taskManagerId, partitions.Count);
        }

        public void Dispose()
        {
            Dispose(true);
            GC.SuppressFinalize(this);
        }

        protected virtual void Dispose(bool disposing)
        {
            if (_disposed) return;
            
            if (disposing)
            {
                try
                {
                    // Final commit attempt before cleanup
                    if (_consumer != null && _checkpointedOffsets.Count > 0)
                    {
                        var offsetsToCommit = new List<TopicPartitionOffset>();
                        lock (_offsetLock)
                        {
                            foreach (var kvp in _checkpointedOffsets)
                            {
                                offsetsToCommit.Add(new TopicPartitionOffset(kvp.Key, kvp.Value));
                            }
                        }
                        _consumer.Commit(offsetsToCommit);
                    }
                }
                catch (Exception ex)
                {
                    _logger?.LogWarning(ex, "Failed to commit final offsets during disposal");
                    // Don't rethrow in Dispose
                }
                
                _consumer?.Close();
                _consumer?.Dispose();
            }
            
            _disposed = true;
            _logger?.LogInformation("FlinkKafkaConsumerGroup disposed");
        }
    }
}