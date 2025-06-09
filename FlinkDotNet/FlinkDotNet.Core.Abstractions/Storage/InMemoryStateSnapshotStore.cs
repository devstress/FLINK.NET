using System.Collections.Generic;
using System.Threading.Tasks;

namespace FlinkDotNet.Core.Abstractions.Storage
{
    /// <summary>
    /// Simple in-memory snapshot store used for unit tests.
    /// </summary>
    public class InMemoryStateSnapshotStore : IStateSnapshotStore
    {
        private readonly Dictionary<string, byte[]> _store = new();

        public Task<SnapshotHandle> StoreSnapshot(string jobId, long checkpointId, string taskManagerId, string operatorId, byte[] snapshotData)
        {
            string handleValue = $"{jobId}_{checkpointId}_{taskManagerId}_{operatorId}";
            _store[handleValue] = snapshotData;
            return Task.FromResult(new SnapshotHandle(handleValue));
        }

        public Task<byte[]?> RetrieveSnapshot(SnapshotHandle handle)
        {
            _store.TryGetValue(handle.Value, out var data);
            return Task.FromResult<byte[]?>(data);
        }
    }
}
