using System;
using System.Collections.Generic;

namespace FlinkDotNet.Core.Abstractions.Models.Checkpointing
{
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
    }
}
