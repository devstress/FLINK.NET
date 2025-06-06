#nullable enable
namespace FlinkDotNet.Core.Abstractions.Models.Checkpointing
{
    /// <summary>
    /// Represents a checkpoint barrier that flows through the data streams.
    /// Barriers are used to trigger state snapshots and ensure exactly-once semantics.
    /// </summary>
    public class CheckpointBarrier
    {
        /// <summary>
        /// The unique ID of the checkpoint.
        /// </summary>
        public long CheckpointId { get; }

        /// <summary>
        /// The timestamp (UTC) when the checkpoint was initiated by the JobManager.
        /// </summary>
        public long Timestamp { get; }

        /// <summary>
        /// Initializes a new instance of the <see cref="CheckpointBarrier"/> class.
        /// </summary>
        /// <param name="checkpointId">The ID of the checkpoint.</param>
        /// <param name="timestamp">The timestamp of the checkpoint initiation.</param>
        public CheckpointBarrier(long checkpointId, long timestamp)
        {
            CheckpointId = checkpointId;
            Timestamp = timestamp;
        }

        // Optional: Add methods for serialization if barriers need to be sent over network
        // without relying on the general stream serializer for this specific type.
        // For now, assume it will be handled like any other record in the stream if it needs to be serialized.
    }
}
#nullable disable
