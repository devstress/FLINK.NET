using System.Threading.Tasks;

namespace FlinkDotNet.Core.Abstractions.Storage
{
    /// <summary>
    /// Interface for a service that stores and retrieves state snapshots.
    /// </summary>
    public interface IStateSnapshotStore
    {
        /// <summary>
        /// Stores the snapshot data for a specific part of a checkpoint.
        /// </summary>
        /// <param name="jobId">Identifier for the job.</param>
        /// <param name="checkpointId">Identifier for the checkpoint.</param>
        /// <param name="taskManagerId">Identifier for the TaskManager (or subtask).</param>
        /// <param name="operatorId">Identifier for the operator (or state name).</param>
        /// <param name="snapshotData">The serialized state data.</param>
        /// <returns>A handle to the stored snapshot.</returns>
        Task<SnapshotHandle> StoreSnapshot(
            string jobId,
            long checkpointId,
            string taskManagerId, // Or a more granular unique ID for the state owner
            string operatorId,    // Or state name
            byte[] snapshotData);

        /// <summary>
        /// Retrieves the snapshot data for a specific part of a checkpoint.
        /// </summary>
        /// <param name="handle">The handle of the snapshot to retrieve.</param>
        /// <returns>The snapshot data, or null if not found.</returns>
        Task<byte[]?> RetrieveSnapshot(SnapshotHandle handle);

        // Optional: Methods for listing, deleting, or managing metadata about snapshots.
    }
}
