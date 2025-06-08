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
}
