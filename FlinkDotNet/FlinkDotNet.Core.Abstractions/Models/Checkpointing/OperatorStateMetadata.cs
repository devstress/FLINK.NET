namespace FlinkDotNet.Core.Abstractions.Models.Checkpointing
{
    /// <summary>
    /// Represents the metadata for a specific piece of keyed operator state,
    /// typically as stored or referenced as part of a checkpoint.
    /// Corresponds to the "OperatorState" JSON schema (Section 2.2 of the design document).
    /// This describes the persisted snapshot of a keyed state, not the live state object.
    /// </summary>
    public class OperatorStateMetadata
    {
        /// <summary>
        /// Unique document ID, e.g., "jobId_operatorInstanceId_keyHash".
        /// </summary>
        public string? Id { get; set; }

        /// <summary>
        /// Partition key for storage, typically "jobId_operatorInstanceId".
        /// </summary>
        public string? PartitionKey { get; set; }

        public string? JobId { get; set; }

        /// <summary>
        /// Identifier for the specific operator instance this state belongs to.
        /// </summary>
        public string? OperatorId { get; set; } // Or OperatorInstanceId

        /// <summary>
        /// Hash of the data key (e.g., customer ID) this state is for.
        /// </summary>
        public string? KeyHash { get; set; }
        // Consider also storing the actual key if its not too large and useful for debugging/management.
        // public string Key {get; set;}

        /// <summary>
        /// Placeholder for the actual serialized state data or a pointer/reference to it.
        /// This could be a URI to a file in blob storage (MinIO, S3, Azure Blob),
        /// a base64 encoded string for small states, or a JSON serialized version.
        /// The interpretation of this field depends on the chosen state persistence strategy.
        /// </summary>
        public string? StateDataLocation { get; set; } // Changed from "stateData" to be more specific about its role as a locator or serialized form

        /// <summary>
        /// The checkpoint ID with which this state version was last updated/snapshotted.
        /// </summary>
        public long LastUpdatedCheckpointId { get; set; }
    }
}
