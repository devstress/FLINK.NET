using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using Confluent.Kafka;
using Microsoft.Extensions.Logging;

namespace FlinkDotNet.Connectors.Sources.Kafka
{
    /// <summary>
    /// Apache Flink 2.0-compliant consumer group manager that provides proper coordination
    /// between Flink's checkpointing mechanism and Kafka's consumer group protocol.
    /// This follows Apache Flink 2.0 patterns of managing consumer groups with:
    /// - Checkpoint-based offset management
    /// - Proper partition assignment coordination  
    /// - Enhanced failure recovery and rebalancing
    /// - Automatic resumption capabilities with exponential backoff
    /// - Circuit breaker pattern for robust error handling
    /// </summary>
    public class FlinkKafkaConsumerGroup : IDisposable
    {
        private readonly ConsumerConfig _consumerConfig;
        private readonly ILogger? _logger;
        private readonly Dictionary<TopicPartition, Offset> _checkpointedOffsets;
        private readonly object _offsetLock = new object();
        private IConsumer<Ignore, byte[]>? _consumer;
        private bool _disposed = false;
        
        // Apache Flink 2.0 resumption state
        private IEnumerable<string>? _subscribedTopics;
        private int _consecutiveFailures = 0;
        private bool _isInRecoveryMode = false;
        private readonly object _recoveryLock = new object();

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
        /// Stores subscription topics for resumption capabilities.
        /// </summary>
        public async Task InitializeAsync(IEnumerable<string> topics)
        {
            if (_consumer != null)
                throw new InvalidOperationException("Consumer already initialized");

            _subscribedTopics = topics.ToList(); // Store for resumption

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
            
            // Reset recovery state on successful initialization
            lock (_recoveryLock)
            {
                _consecutiveFailures = 0;
                _isInRecoveryMode = false;
            }
            
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
                SessionTimeoutMs = 10000, // Use 10s minimum session timeout for Kafka compatibility
                SocketTimeoutMs = 5000     // Short socket timeout
            };

            var retryCount = 0;
            const int maxRetries = 12; // 12 retries * 5 seconds = 1 minute
            
            while ((DateTime.UtcNow - startTime) < timeout)
            {
                retryCount++;
                var elapsed = DateTime.UtcNow - startTime;
                
                try
                {
                    using var testConsumer = new ConsumerBuilder<Ignore, byte[]>(testConsumerConfig)
                        .SetErrorHandler((consumer, error) => {
                            _logger?.LogDebug("Kafka readiness test error: {ErrorCode} - {Reason}", error.Code, error.Reason);
                        })
                        .Build();
                    
                    // Try to subscribe to test topic - this will fail if Kafka is not ready
                    testConsumer.Subscribe("__consumer_offsets"); // Use internal topic for testing
                    
                    // Try a simple consume operation with short timeout to verify connectivity
                    _ = testConsumer.Consume(TimeSpan.FromSeconds(2));
                    // We don't care about the result, just that no exception was thrown
                    
                    _logger?.LogInformation("✅ Kafka setup verified - broker connectivity established after {Elapsed}ms (attempt {Attempt})", 
                        (DateTime.UtcNow - startTime).TotalMilliseconds, retryCount);
                    return; // Kafka is ready
                }
                catch (Exception ex)
                {
                    var errorMessage = ex.Message;
                    if (ex.InnerException != null)
                        errorMessage = $"{errorMessage} (Inner: {ex.InnerException.Message})";
                    
                    _logger?.LogDebug(ex, "⏳ Kafka not ready yet - attempt {Attempt}/{MaxRetries} after {Elapsed}ms: {Error}", 
                        retryCount, maxRetries, elapsed.TotalMilliseconds, errorMessage);
                    
                    // Log specific error types for better diagnostics
                    if (ex.Message.Contains("ApiVersion"))
                    {
                        _logger?.LogDebug("🔍 Kafka diagnostics: ApiVersion request issue detected - broker may be starting up");
                    }
                    else if (ex.Message.Contains("security.protocol"))
                    {
                        _logger?.LogWarning("🔍 Kafka diagnostics: Security protocol mismatch detected - check SSL/plaintext configuration");
                    }
                    else if (ex.Message.Contains("broker") && ex.Message.Contains("down"))
                    {
                        _logger?.LogDebug("🔍 Kafka diagnostics: All brokers down - waiting for Kafka startup");
                    }
                }
                
                if ((DateTime.UtcNow - startTime) >= timeout)
                {
                    _logger?.LogWarning("⚠️ Kafka setup wait timeout ({Timeout}) exceeded after {RetryCount} attempts, proceeding anyway", 
                        timeout, retryCount);
                    return; // Proceed even if timeout exceeded to allow fallback mechanisms
                }
                
                // Wait before retry
                await Task.Delay(TimeSpan.FromSeconds(5));
            }
            
            _logger?.LogWarning("⚠️ Kafka setup wait completed with timeout after {RetryCount} attempts, proceeding with consumer initialization", retryCount);
        }

        /// <summary>
        /// Consume messages with Flink-compatible offset management and automatic resumption.
        /// Implements Apache Flink 2.0 recovery patterns with exponential backoff.
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
                    
                    // Reset failure count on successful consumption
                    lock (_recoveryLock)
                    {
                        if (_consecutiveFailures > 0)
                        {
                            _logger?.LogInformation("✅ FlinkKafkaConsumerGroup: Recovery successful after {FailureCount} failures", _consecutiveFailures);
                            _consecutiveFailures = 0;
                            _isInRecoveryMode = false;
                        }
                    }
                }
                return result;
            }
            catch (ConsumeException ex)
            {
                _logger?.LogError(ex, "Error consuming from Kafka: {Error}", ex.Error.Reason);
                
                // For critical errors, rethrow to trigger Flink fault tolerance
                if (ex.Error.IsFatal)
                {
                    _logger?.LogCritical("Fatal Kafka error encountered: {Error}", ex.Error);
                    throw;
                }
                
                // Handle recoverable errors with Apache Flink 2.0 patterns
                return HandleRecoverableError(ex);
            }
        }
        
        /// <summary>
        /// Handle recoverable Kafka errors with Apache Flink 2.0 resumption patterns.
        /// Implements exponential backoff and automatic consumer group resumption.
        /// </summary>
        private ConsumeResult<Ignore, byte[]>? HandleRecoverableError(ConsumeException ex)
        {
            lock (_recoveryLock)
            {
                _consecutiveFailures++;
                
                // Check if error indicates need for consumer resumption
                if (IsConsumerResumptionNeeded(ex))
                {
                    _logger?.LogWarning("🔄 FlinkKafkaConsumerGroup: Resumption needed due to error: {Error} (Failure #{FailureCount})", 
                        ex.Error.Reason, _consecutiveFailures);
                    
                    // Attempt resumption with exponential backoff
                    if (AttemptConsumerResumption())
                    {
                        _logger?.LogInformation("✅ FlinkKafkaConsumerGroup: Resumption successful");
                        return null; // Return null to retry consumption
                    }
                    else
                    {
                        _logger?.LogError("❌ FlinkKafkaConsumerGroup: Resumption failed, marking as in recovery mode");
                        _isInRecoveryMode = true;
                    }
                }
                
                // Apply circuit breaker pattern - if too many consecutive failures, temporarily disable
                if (_consecutiveFailures >= 10)
                {
                    _logger?.LogError("🚨 FlinkKafkaConsumerGroup: Circuit breaker activated after {FailureCount} consecutive failures", _consecutiveFailures);
                    _isInRecoveryMode = true;
                    
                    // Reset after exponential backoff period
                    var backoffTime = CalculateExponentialBackoff(_consecutiveFailures);
                    _logger?.LogInformation("⏳ FlinkKafkaConsumerGroup: Waiting {BackoffTime}ms before reset attempt", backoffTime.TotalMilliseconds);
                    Task.Delay(backoffTime).Wait();
                    
                    // Reset failure count to allow recovery attempts
                    _consecutiveFailures = 5; // Partial reset to prevent immediate circuit breaker re-activation
                }
            }
            
            return null;
        }
        
        /// <summary>
        /// Determine if a Kafka error requires consumer resumption based on Apache Flink patterns.
        /// </summary>
        private static bool IsConsumerResumptionNeeded(ConsumeException ex)
        {
            var errorCode = ex.Error.Code;
            var errorReason = ex.Error.Reason;
            
            // Errors that typically require consumer recreation
            return errorCode == ErrorCode.BrokerNotAvailable ||
                   errorCode == ErrorCode.NetworkException ||
                   errorCode == ErrorCode.RequestTimedOut ||
                   errorCode == ErrorCode.UnknownMemberId ||
                   errorReason.Contains("connection", StringComparison.OrdinalIgnoreCase) ||
                   errorReason.Contains("broker", StringComparison.OrdinalIgnoreCase) ||
                   errorReason.Contains("timeout", StringComparison.OrdinalIgnoreCase);
        }
        
        /// <summary>
        /// Attempt to resume the FlinkKafkaConsumerGroup by recreating the consumer
        /// while preserving checkpoint state. Follows Apache Flink 2.0 recovery patterns.
        /// </summary>
        private bool AttemptConsumerResumption()
        {
            try
            {
                _logger?.LogInformation("🔄 FlinkKafkaConsumerGroup: Starting resumption process...");
                
                // Preserve current checkpoint state
                Dictionary<TopicPartition, Offset> preservedState;
                lock (_offsetLock)
                {
                    preservedState = new Dictionary<TopicPartition, Offset>(_checkpointedOffsets);
                }
                
                // Safely dispose existing consumer
                try
                {
                    _consumer?.Close();
                    _consumer?.Dispose();
                }
                catch (Exception ex)
                {
                    _logger?.LogWarning(ex, "⚠️ FlinkKafkaConsumerGroup: Error disposing consumer during resumption");
                }
                
                _consumer = null;
                
                // Apply exponential backoff before resumption attempt
                var backoffTime = CalculateExponentialBackoff(_consecutiveFailures);
                _logger?.LogDebug("⏳ FlinkKafkaConsumerGroup: Applying {BackoffTime}ms backoff before resumption", backoffTime.TotalMilliseconds);
                Task.Delay(backoffTime).Wait();
                
                // Recreate consumer with same configuration
                _consumer = new ConsumerBuilder<Ignore, byte[]>(_consumerConfig)
                    .SetErrorHandler(OnError)
                    .SetPartitionsAssignedHandler(OnPartitionsAssigned)
                    .SetPartitionsRevokedHandler(OnPartitionsRevoked)
                    .SetPartitionsLostHandler(OnPartitionsLost)
                    .Build();
                
                // Re-subscribe to topics
                if (_subscribedTopics != null)
                {
                    _consumer.Subscribe(_subscribedTopics);
                    _logger?.LogInformation("✅ FlinkKafkaConsumerGroup: Re-subscribed to topics: {Topics}", 
                        string.Join(", ", _subscribedTopics));
                }
                
                // Restore checkpoint state if available
                if (preservedState.Count > 0)
                {
                    _logger?.LogInformation("🔄 FlinkKafkaConsumerGroup: Restoring {PartitionCount} partition offsets after resumption", 
                        preservedState.Count);
                    
                    // Wait briefly for partition assignment before seeking
                    Task.Delay(TimeSpan.FromSeconds(2)).Wait();
                    
                    try
                    {
                        foreach (var kvp in preservedState)
                        {
                            _consumer.Seek(new TopicPartitionOffset(kvp.Key, kvp.Value));
                        }
                        
                        // Restore internal state
                        lock (_offsetLock)
                        {
                            _checkpointedOffsets.Clear();
                            foreach (var kvp in preservedState)
                            {
                                _checkpointedOffsets[kvp.Key] = kvp.Value;
                            }
                        }
                        
                        _logger?.LogInformation("✅ FlinkKafkaConsumerGroup: Successfully restored checkpoint state");
                    }
                    catch (Exception ex)
                    {
                        _logger?.LogWarning(ex, "⚠️ FlinkKafkaConsumerGroup: Failed to restore offsets during resumption, will start from current position");
                    }
                }
                
                _logger?.LogInformation("✅ FlinkKafkaConsumerGroup: Resumption completed successfully");
                return true;
            }
            catch (Exception ex)
            {
                _logger?.LogError(ex, "❌ FlinkKafkaConsumerGroup: Resumption failed: {Error}", ex.Message);
                return false;
            }
        }
        
        /// <summary>
        /// Calculate exponential backoff delay based on failure count.
        /// Implements Apache Flink's exponential backoff pattern with maximum delay cap.
        /// </summary>
        private static TimeSpan CalculateExponentialBackoff(int failureCount)
        {
            // Start with 1 second, double each time, max 30 seconds
            var baseDelayMs = 1000;
            var maxDelayMs = 30000;
            
            var delayMs = Math.Min(baseDelayMs * Math.Pow(2, Math.Min(failureCount - 1, 5)), maxDelayMs);
            return TimeSpan.FromMilliseconds(delayMs);
        }
        
        /// <summary>
        /// Check if the consumer group is currently in recovery mode.
        /// Used by external components to determine health status.
        /// </summary>
        public bool IsInRecoveryMode()
        {
            lock (_recoveryLock)
            {
                return _isInRecoveryMode;
            }
        }
        
        /// <summary>
        /// Get the number of consecutive failures for monitoring purposes.
        /// </summary>
        public int GetConsecutiveFailureCount()
        {
            lock (_recoveryLock)
            {
                return _consecutiveFailures;
            }
        }

        /// <summary>
        /// Commit offsets as part of Flink checkpoint process
        /// This replaces Kafka's auto-commit with Flink-managed checkpointing
        /// Following Apache Flink 2.0 exactly-once semantics patterns
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
                // Commit offsets synchronously to ensure exactly-once semantics
                _consumer.Commit(offsetsToCommit);
                _logger?.LogDebug("Successfully committed checkpoint {CheckpointId} with {PartitionCount} partition offsets", 
                    checkpointId, offsetsToCommit.Count);
                    
                // Clear checkpointed offsets after successful commit (Apache Flink pattern)
                lock (_offsetLock)
                {
                    _checkpointedOffsets.Clear();
                }
            }
            catch (KafkaException ex)
            {
                _logger?.LogError(ex, "Failed to commit checkpoint {CheckpointId} offsets: {Error}", 
                    checkpointId, ex.Error.Reason);
                    
                // For exactly-once semantics, checkpoint failures should be escalated
                throw new InvalidOperationException($"Apache Flink checkpoint {checkpointId} offset commit failed", ex);
            }
            
            await Task.CompletedTask;
        }

        /// <summary>
        /// Restore offsets from Flink checkpoint during recovery
        /// This implements Apache Flink's state recovery pattern
        /// </summary>
        public async Task RestoreFromCheckpointAsync(Dictionary<TopicPartition, Offset> checkpointState)
        {
            if (_consumer == null)
                throw new InvalidOperationException("Consumer not initialized");

            if (checkpointState?.Count > 0)
            {
                try
                {
                    var topicPartitionOffsets = checkpointState
                        .Select(kvp => new TopicPartitionOffset(kvp.Key, kvp.Value))
                        .ToList();

                    // Seek to checkpointed offsets (Apache Flink recovery pattern)
                    foreach (var tpo in topicPartitionOffsets)
                    {
                        _consumer.Seek(tpo);
                    }

                    _logger?.LogInformation("Restored {PartitionCount} partition offsets from Flink checkpoint", 
                        checkpointState.Count);

                    // Update internal state
                    lock (_offsetLock)
                    {
                        _checkpointedOffsets.Clear();
                        foreach (var kvp in checkpointState)
                        {
                            _checkpointedOffsets[kvp.Key] = kvp.Value;
                        }
                    }
                }
                catch (Exception ex)
                {
                    _logger?.LogError(ex, "Failed to restore from checkpoint state");
                    throw new InvalidOperationException("Apache Flink checkpoint state recovery failed", ex);
                }
            }
            
            await Task.CompletedTask;
        }

        /// <summary>
        /// Get current checkpoint state for Flink snapshot
        /// Returns copy of current offset state for exactly-once semantics
        /// </summary>
        public Dictionary<TopicPartition, Offset> GetCheckpointState()
        {
            lock (_offsetLock)
            {
                return new Dictionary<TopicPartition, Offset>(_checkpointedOffsets);
            }
        }
        
        /// <summary>
        /// Get consumer assignment for partition coordination
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
            else
            {
                // For non-fatal errors, check if resumption is needed
                lock (_recoveryLock)
                {
                    _consecutiveFailures++;
                }
                
                _logger?.LogWarning("Non-fatal Kafka error (failure #{FailureCount}): {ErrorCode} - {Reason}", 
                    _consecutiveFailures, error.Code, error.Reason);
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
                    taskManagerId, partitions.Count, "ACTIVE");
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