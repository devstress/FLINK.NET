using System.IO;
using FlinkDotNet.Core.Abstractions.Storage;

namespace FlinkDotNet.Storage.FileSystem
{
    /// <summary>
    /// Simple state backend that stores snapshots on the local filesystem in a
    /// disaggregated layout. This is a minimal prototype for testing.
    /// </summary>
    public class DisaggregatedStateBackend : IStateBackend
    {
        public IStateSnapshotStore SnapshotStore { get; }

        public string BasePath { get; }

        public DisaggregatedStateBackend(string basePath)
        {
            BasePath = Path.GetFullPath(basePath);
            Directory.CreateDirectory(BasePath);
            SnapshotStore = new FileSystemSnapshotStore(BasePath);
        }
    }
}
