namespace FlinkDotNet.Core.Abstractions.Storage
{
    /// <summary>
    /// Represents a handle to a stored state snapshot.
    /// Could be a file path, a URI, or an opaque ID.
    /// </summary>
    public record SnapshotHandle(string Value);
}
