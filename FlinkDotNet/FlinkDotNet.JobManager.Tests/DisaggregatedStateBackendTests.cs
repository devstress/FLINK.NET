using System.IO;
using System.Threading.Tasks;
using FlinkDotNet.Storage.FileSystem;
using Xunit;

namespace FlinkDotNet.JobManager.Tests
{
    public class DisaggregatedStateBackendTests
    {
        [Fact]
        public async Task SnapshotStore_RoundTrip()
        {
            var path = Path.Combine(Path.GetTempPath(), Path.GetRandomFileName());
            var backend = new DisaggregatedStateBackend(path);
            var handle = await backend.SnapshotStore.StoreSnapshot("job", 1, "tm", "op", new byte[] { 1, 2, 3 });
            var data = await backend.SnapshotStore.RetrieveSnapshot(handle);
            Assert.Equal(new byte[] { 1, 2, 3 }, data);
            Directory.Delete(path, true);
        }
    }
}
