#nullable enable
using System.Threading;
using System.Threading.Tasks;

namespace FlinkDotNet.TaskManager
{
    /// <summary>
    /// Non-generic interface for NetworkedCollector instances to allow storing them
    /// in a common list and calling them dynamically.
    /// </summary>
    public interface INetworkedCollector
    {
        /// <summary>
        /// Collects a record (which could be data or a CheckpointBarrier).
        /// The implementation will handle type casting to its specific generic type.
        /// </summary>
        Task CollectObject(object record, CancellationToken cancellationToken);

        /// <summary>
        /// Closes the underlying network stream.
        /// </summary>
        Task CloseStreamAsync();

        // Potentially add other common methods if needed, e.g., to identify the target.
        string TargetVertexId { get; }
    }
}
#nullable disable
