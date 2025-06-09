using System.Threading.Tasks;
using FlinkDotNet.Core.Abstractions.Models.Checkpointing;

namespace FlinkDotNet.Core.Abstractions.Operators
{
    /// <summary>
    /// Implemented by operators that support Flink.NET style state checkpointing.
    /// </summary>
    public interface ICheckpointableOperator
    {
        /// <summary>
        /// Snapshots the state of the operator for the given checkpoint.
        /// </summary>
        Task<OperatorStateSnapshot> SnapshotState(long checkpointId, long checkpointTimestamp);

        /// <summary>
        /// Restores the operator's state from a previously taken snapshot.
        /// </summary>
        Task RestoreState(OperatorStateSnapshot snapshot);
    }
}
