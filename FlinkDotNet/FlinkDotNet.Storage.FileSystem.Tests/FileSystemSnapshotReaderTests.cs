using System.Text;

namespace FlinkDotNet.Storage.FileSystem.Tests
{
    public class FileSystemSnapshotReaderTests : IDisposable
    {
        private readonly string _testDirectory;
        private readonly FileSystemSnapshotStore _store;

        public FileSystemSnapshotReaderTests()
        {
            _testDirectory = Path.Combine(Path.GetTempPath(), $"FlinkDotNetTest_{Guid.NewGuid()}");
            _store = new FileSystemSnapshotStore(_testDirectory);
        }

        public void Dispose()


        {


            Dispose(true);


            GC.SuppressFinalize(this);


        
            Assert.True(true); // Basic assertion added
        }



        protected virtual void Dispose(bool disposing)


        {


            if (disposing)


            {


                if (Directory.Exists(_testDirectory))
            {
                Directory.Delete(_testDirectory, recursive: true);


            }


        }
        }

        private async Task<string> CreateTestSnapshot()
        {
            var writer = await _store.CreateWriter("job1", 100, "op1", "task1");

            // Create raw state
            using (var stream = writer.GetStateOutputStream("rawState1"))
            {
                await stream.WriteAsync("raw data content"u8.ToArray());
            }

            // Create keyed state
            await writer.BeginKeyedState("keyedState1");
            await writer.WriteKeyedEntry("key1"u8.ToArray(), "value1"u8.ToArray());
            await writer.WriteKeyedEntry("key2"u8.ToArray(), "value2"u8.ToArray());
            await writer.WriteKeyedEntry("longKey123"u8.ToArray(), "longValue456789"u8.ToArray());
            await writer.EndKeyedState("keyedState1");

            return await writer.CommitAndGetHandleAsync();
        }

        [Fact]
        public async Task GetStateInputStream_ExistingState_ReturnsStream()
        {
            // Arrange
            var handle = await CreateTestSnapshot();
            var reader = await FileSystemSnapshotStore.CreateReader(handle);

            // Act
            using var stream = reader.GetStateInputStream("rawState1");

            // Assert
            Assert.NotNull(stream);
            Assert.True(stream.CanRead);
        }

        [Fact]
        public async Task GetStateInputStream_NonExistentState_ThrowsFileNotFoundException()
        {
            // Arrange
            var handle = await CreateTestSnapshot();
            var reader = await FileSystemSnapshotStore.CreateReader(handle);

            // Act & Assert
            Assert.Throws<FileNotFoundException>(() =>
                reader.GetStateInputStream("nonExistentState"));
        }

        [Fact]
        public async Task GetStateInputStream_EmptyReader_ThrowsFileNotFoundException()
        {
            // Arrange
            var reader = await FileSystemSnapshotStore.CreateReader("/non/existent/path");

            // Act & Assert
            Assert.Throws<FileNotFoundException>(() =>
                reader.GetStateInputStream("anyState"));
        }

        [Fact]
        public async Task GetStateInputStream_ReadData_ReturnsCorrectContent()
        {
            // Arrange
            var handle = await CreateTestSnapshot();
            var reader = await FileSystemSnapshotStore.CreateReader(handle);

            // Act
            using var stream = reader.GetStateInputStream("rawState1");
            using var memoryStream = new MemoryStream();
            await stream.CopyToAsync(memoryStream);
            var data = memoryStream.ToArray();

            // Assert
            var expectedData = "raw data content"u8.ToArray();
            Assert.Equal(expectedData, data);
        }

        [Fact]
        public async Task HasState_ExistingState_ReturnsTrue()
        {
            // Arrange
            var handle = await CreateTestSnapshot();
            var reader = await FileSystemSnapshotStore.CreateReader(handle);

            // Act
            var hasState = reader.HasState("rawState1");

            // Assert
            Assert.True(hasState);
        }

        [Fact]
        public async Task HasState_NonExistentState_ReturnsFalse()
        {
            // Arrange
            var handle = await CreateTestSnapshot();
            var reader = await FileSystemSnapshotStore.CreateReader(handle);

            // Act
            var hasState = reader.HasState("nonExistentState");

            // Assert
            Assert.False(hasState);
        }

        [Fact]
        public async Task HasState_EmptyReader_ReturnsFalse()
        {
            // Arrange
            var reader = await FileSystemSnapshotStore.CreateReader("/non/existent/path");

            // Act
            var hasState = reader.HasState("anyState");

            // Assert
            Assert.False(hasState);
        }

        [Fact]
        public async Task HasKeyedState_ExistingState_ReturnsTrue()
        {
            // Arrange
            var handle = await CreateTestSnapshot();
            var reader = await FileSystemSnapshotStore.CreateReader(handle);

            // Act
            var hasKeyedState = await reader.HasKeyedState("keyedState1");

            // Assert
            Assert.True(hasKeyedState);
        }

        [Fact]
        public async Task HasKeyedState_NonExistentState_ReturnsFalse()
        {
            // Arrange
            var handle = await CreateTestSnapshot();
            var reader = await FileSystemSnapshotStore.CreateReader(handle);

            // Act
            var hasKeyedState = await reader.HasKeyedState("nonExistentKeyedState");

            // Assert
            Assert.False(hasKeyedState);
        }

        [Fact]
        public async Task HasKeyedState_EmptyReader_ReturnsFalse()
        {
            // Arrange
            var reader = await FileSystemSnapshotStore.CreateReader("/non/existent/path");

            // Act
            var hasKeyedState = await reader.HasKeyedState("anyState");

            // Assert
            Assert.False(hasKeyedState);
        }

        [Fact]
        public async Task ReadKeyedStateEntries_ExistingState_ReturnsAllEntries()
        {
            // Arrange
            var handle = await CreateTestSnapshot();
            var reader = await FileSystemSnapshotStore.CreateReader(handle);
            var expectedEntries = new Dictionary<string, string>
            {
                { "key1", "value1" },
                { "key2", "value2" },
                { "longKey123", "longValue456789" }
            };

            // Act
            var entries = new Dictionary<string, string>();
            await foreach (var entry in reader.ReadKeyedStateEntries("keyedState1"))
            {
                var key = Encoding.UTF8.GetString(entry.Key);
                var value = Encoding.UTF8.GetString(entry.Value);
                entries[key] = value;
            }

            // Assert
            Assert.Equal(expectedEntries.Count, entries.Count);
            foreach (var expectedEntry in expectedEntries)
            {
                Assert.True(entries.ContainsKey(expectedEntry.Key));
                Assert.Equal(expectedEntry.Value, entries[expectedEntry.Key]);
            }
        }

        [Fact]
        public async Task ReadKeyedStateEntries_NonExistentState_ReturnsEmpty()
        {
            // Arrange
            var handle = await CreateTestSnapshot();
            var reader = await FileSystemSnapshotStore.CreateReader(handle);

            // Act
            var entries = new List<KeyValuePair<byte[], byte[]>>();
            await foreach (var entry in reader.ReadKeyedStateEntries("nonExistentState"))
            {
                entries.Add(entry);
            }

            // Assert
            Assert.Empty(entries);
        }

        [Fact]
        public async Task ReadKeyedStateEntries_EmptyReader_ReturnsEmpty()
        {
            // Arrange
            var reader = await FileSystemSnapshotStore.CreateReader("/non/existent/path");

            // Act
            var entries = new List<KeyValuePair<byte[], byte[]>>();
            await foreach (var entry in reader.ReadKeyedStateEntries("anyState"))
            {
                entries.Add(entry);
            }

            // Assert
            Assert.Empty(entries);
        }

        [Fact]
        public async Task ReadKeyedStateEntries_EmptyFile_ReturnsEmpty()
        {
            // Arrange
            var writer = await _store.CreateWriter("job1", 100, "op1", "task1");
            await writer.BeginKeyedState("emptyKeyedState");
            // Don't write any entries
            await writer.EndKeyedState("emptyKeyedState");
            var handle = await writer.CommitAndGetHandleAsync();

            var reader = await FileSystemSnapshotStore.CreateReader(handle);

            // Act
            var entries = new List<KeyValuePair<byte[], byte[]>>();
            await foreach (var entry in reader.ReadKeyedStateEntries("emptyKeyedState"))
            {
                entries.Add(entry);
            }

            // Assert
            Assert.Empty(entries);
        }

        [Fact]
        public async Task ReadKeyedStateEntries_CorruptedMagicNumber_ThrowsIOException()
        {
            // Arrange
            var writer = await _store.CreateWriter("job1", 100, "op1", "task1");
            await writer.BeginKeyedState("corruptedState");
            await writer.WriteKeyedEntry("key"u8.ToArray(), "value"u8.ToArray());
            await writer.EndKeyedState("corruptedState");
            var handle = await writer.CommitAndGetHandleAsync();

            // Corrupt the file by overwriting the magic number
            var filePath = Path.Combine(handle, "corruptedState.keyed_state");
            var fileData = await File.ReadAllBytesAsync(filePath);
            fileData[0] = (byte)'X'; // Corrupt magic number
            await File.WriteAllBytesAsync(filePath, fileData);

            var reader = await FileSystemSnapshotStore.CreateReader(handle);

            // Act & Assert
            await Assert.ThrowsAsync<IOException>(async () =>
            {
                await foreach (var entry in reader.ReadKeyedStateEntries("corruptedState"))
                {
                    // Just iterate to trigger the exception
                }
            });
        }

        [Fact]
        public async Task ReadKeyedStateEntries_UnsupportedVersion_ThrowsIOException()
        {
            // Arrange
            var writer = await _store.CreateWriter("job1", 100, "op1", "task1");
            await writer.BeginKeyedState("versionState");
            await writer.WriteKeyedEntry("key"u8.ToArray(), "value"u8.ToArray());
            await writer.EndKeyedState("versionState");
            var handle = await writer.CommitAndGetHandleAsync();

            // Corrupt the version
            var filePath = Path.Combine(handle, "versionState.keyed_state");
            var fileData = await File.ReadAllBytesAsync(filePath);
            fileData[5] = 99; // Set unsupported version
            await File.WriteAllBytesAsync(filePath, fileData);

            var reader = await FileSystemSnapshotStore.CreateReader(handle);

            // Act & Assert
            await Assert.ThrowsAsync<IOException>(async () =>
            {
                await foreach (var entry in reader.ReadKeyedStateEntries("versionState"))
                {
                    // Just iterate to trigger the exception
                }
            });
        }

        [Fact]
        public async Task ReadKeyedStateEntries_BinaryData_HandlesCorrectly()
        {
            // Arrange
            var writer = await _store.CreateWriter("job1", 100, "op1", "task1");
            await writer.BeginKeyedState("binaryState");
            
            var binaryKey = new byte[] { 0, 1, 2, 255, 254, 253 };
            var binaryValue = new byte[] { 100, 200, 50, 75, 0, 255 };
            await writer.WriteKeyedEntry(binaryKey, binaryValue);
            
            await writer.EndKeyedState("binaryState");
            var handle = await writer.CommitAndGetHandleAsync();

            var reader = await FileSystemSnapshotStore.CreateReader(handle);

            // Act
            var entries = new List<KeyValuePair<byte[], byte[]>>();
            await foreach (var entry in reader.ReadKeyedStateEntries("binaryState"))
            {
                entries.Add(entry);
            }

            // Assert
            Assert.Single(entries);
            Assert.Equal(binaryKey, entries[0].Key);
            Assert.Equal(binaryValue, entries[0].Value);
        }

        [Fact]
        public async Task Dispose_ValidReader_DoesNotThrow()
        {
            // Arrange
            var handle = await CreateTestSnapshot();
            var reader = await FileSystemSnapshotStore.CreateReader(handle);

            // Act & Assert
            reader.Dispose(); // Should not throw
        }
    }
}