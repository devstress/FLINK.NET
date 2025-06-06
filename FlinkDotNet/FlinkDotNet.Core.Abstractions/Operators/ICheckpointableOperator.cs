#nullable enable
using System.Threading.Tasks;
using FlinkDotNet.Core.Abstractions.Models.State; // For OperatorStateSnapshot (to be created)

namespace FlinkDotNet.Core.Abstractions.Operators
{
    /// <summary>
    /// Interface for operators that can have their state checkpointed.
    /// </summary>
    public interface ICheckpointableOperator
    {
        /// <summary>
        /// Called when a checkpoint barrier is aligned for this operator instance.
        /// The operator should snapshot its current state.
        /// </summary>
        /// <param name="checkpointId">The ID of the checkpoint.</param>
        /// <param name="checkpointTimestamp">The timestamp associated with the checkpoint trigger.</param>
        /// <returns>A task that represents the asynchronous snapshot operation, returning the snapshot data.</returns>
        Task<OperatorStateSnapshot> SnapshotState(long checkpointId, long checkpointTimestamp);

        /// <summary>
        /// Called to restore the operator's state from a given snapshot.
        /// This is typically called when a task is recovering from a failure.
        /// </summary>
        /// <param name="snapshotDetails">The details of the snapshot from which to restore state.</param>
        /// <returns>A task that represents the asynchronous state restoration operation.</returns>
        Task RestoreState(OperatorStateSnapshot snapshotDetails);
    }
}
#nullable disable
