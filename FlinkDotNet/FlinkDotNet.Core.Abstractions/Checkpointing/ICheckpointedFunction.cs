namespace FlinkDotNet.Core.Abstractions.Checkpointing
{
    /// <summary>
    /// Interface for functions that need to participate in Apache Flink's checkpointing mechanism.
    /// This provides exactly-once processing guarantees by allowing functions to snapshot and restore state.
    /// 
    /// Apache Flink's checkpointing works by:
    /// 1. Coordinator triggers checkpoint on all operators
    /// 2. Each operator snapshots its state via SnapshotState()
    /// 3. State is persisted to distributed storage
    /// 4. Once all operators complete, checkpoint is committed
    /// 5. On recovery, RestoreState() is called with last successful checkpoint
    /// </summary>
    public interface ICheckpointedFunction
    {
        /// <summary>
        /// Called when a checkpoint is triggered to snapshot the function's state.
        /// This method should capture all necessary state for recovery and store it
        /// in a serializable format.
        /// </summary>
        /// <param name="checkpointId">Unique identifier for this checkpoint</param>
        /// <param name="checkpointTimestamp">Timestamp when checkpoint was triggered</param>
        void SnapshotState(long checkpointId, long checkpointTimestamp);

        /// <summary>
        /// Called when the function is restored from a checkpoint to recover its state.
        /// This method receives the state that was previously captured in SnapshotState().
        /// </summary>
        /// <param name="state">The state object that was previously snapshotted</param>
        void RestoreState(object state);

        /// <summary>
        /// Called when a checkpoint has been successfully completed and committed.
        /// This is useful for functions that need to perform cleanup or commit operations
        /// only after a checkpoint is fully complete (e.g., committing Kafka offsets).
        /// </summary>
        /// <param name="checkpointId">The ID of the completed checkpoint</param>
        void NotifyCheckpointComplete(long checkpointId);
    }
}