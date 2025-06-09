namespace FlinkDotNet.Core.Abstractions.Storage
{
    /// <summary>
    /// Represents a pluggable state backend which exposes a snapshot store.
    /// </summary>
    public interface IStateBackend
    {
        /// <summary>
        /// Gets the snapshot store used to persist operator state.
        /// </summary>
        IStateSnapshotStore SnapshotStore { get; }
    }
}
