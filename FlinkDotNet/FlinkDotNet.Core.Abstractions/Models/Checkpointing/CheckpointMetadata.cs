using System;
using System.Collections.Generic;

namespace FlinkDotNet.Core.Abstractions.Models.Checkpointing
{
    /// <summary>
    /// Represents metadata about a specific operators state within a checkpoint.
    /// Corresponds to an item in the "operatorStates" array in the Checkpoints JSON schema.
    /// </summary>
    public class OperatorCheckpointState
    {
        /// <summary>
        /// Identifier for the operator instance, e.g., "MapOperator_Instance_0".
        /// </summary>
        public string? OperatorInstanceId { get; set; }

        /// <summary>
        /// Input stream offsets for each source partition handled by this operator instance.
        /// Key: Source Name/ID (e.g., KafkaTopicA_Partition0), Value: Offset.
        /// </summary>
        public Dictionary<string, long>? InputOffsets { get; set; }

        /// <summary>
        /// A token or pointer representing the committed state for this operator instance
        /// at this checkpoint. This could be a path to a file in durable storage,
        /// a version ID, or a database reference.
        /// </summary>
        public string? StateCommitToken { get; set; } // Or StateLocationUri, StateReference etc.
    }

    /// <summary>
    /// Represents a pointer or reference to a sink transaction associated with a checkpoint.
    /// Corresponds to an item in the "sinkTransactions" array in the Checkpoints JSON schema.
    /// </summary>
    public class SinkTransactionStatePointer
    {
        /// <summary>
        /// Identifier for the sink instance.
        /// </summary>
        public string? SinkInstanceId { get; set; }

        /// <summary>
        /// The unique transaction ID used with the external sink system.
        /// </summary>
        public Guid TransactionId { get; set; }
        // Potentially add status of this specific sink transaction if known at checkpoint time,
        // though the global SinkTransactions collection (Section 2.5 of design doc)
        // would be the primary source for transaction status.
    }

    /// <summary>
    /// Represents the metadata for a checkpoint.
    /// Corresponds to the "Checkpoints" JSON schema (Section 2.3 of the design document).
    /// </summary>
    public class CheckpointMetadata
    {
        /// <summary>
        /// Unique document ID, typically "jobId_checkpointId".
        /// </summary>
        public string? Id { get; set; }

        /// <summary>
        /// Partition key for storage, typically "jobId".
        /// </summary>
        public string? PartitionKey { get; set; }

        public string? JobId { get; set; }
        public long CheckpointId { get; set; } // Flink uses long for checkpoint IDs
        public DateTime Timestamp { get; set; }

        /// <summary>
        /// Status of the checkpoint, e.g., "COMPLETED", "IN_PROGRESS", "FAILED".
        /// </summary>
        public string? Status { get; set; }

        /// <summary>
        /// List of state metadata for each operator instance included in this checkpoint.
        /// </summary>
        public List<OperatorCheckpointState>? OperatorStates { get; set; }

        /// <summary>
        /// List of pointers to sink transactions associated with this checkpoint.
        /// </summary>
        public List<SinkTransactionStatePointer>? SinkTransactions { get; set; }

        // Optional: Total checkpoint duration, size, etc., if not derived from operatorStates.
        // public long? TotalDurationMs { get; set; }
        // public long? TotalSizeBytes { get; set; }
    }
}
