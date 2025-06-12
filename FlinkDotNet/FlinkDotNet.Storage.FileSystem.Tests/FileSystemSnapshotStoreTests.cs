using FlinkDotNet.Core.Abstractions.Storage;

namespace FlinkDotNet.Storage.FileSystem.Tests
{
    public class FileSystemSnapshotStoreTests : IDisposable
    {
        private readonly string _testDirectory;
        private readonly FileSystemSnapshotStore _store;

        public FileSystemSnapshotStoreTests()
        {
            _testDirectory = Path.Combine(Path.GetTempPath(), $"FlinkDotNetTest_{Guid.NewGuid()}");
            _store = new FileSystemSnapshotStore(_testDirectory);
        }

        public void Dispose()
        {
            Dispose(true);
            GC.SuppressFinalize(this);
        }

        protected virtual void Dispose(bool disposing)
        {
            if (disposing && Directory.Exists(_testDirectory))
            {
                Directory.Delete(_testDirectory, recursive: true);
            }
        }

        [Fact]
        public void Constructor_ValidPath_CreatesDirectory()
        {
            // Arrange
            var tempPath = Path.Combine(Path.GetTempPath(), $"FlinkDotNetTest_{Guid.NewGuid()}");

            try
            {
                // Act
                new FileSystemSnapshotStore(tempPath);

                // Assert
                Assert.True(Directory.Exists(tempPath));
            }
            finally
            {
                if (Directory.Exists(tempPath))
                {
                    Directory.Delete(tempPath, recursive: true);
                }
            }
        }

        [Fact]
        public void Constructor_RelativePath_ConvertsToAbsolutePath()
        {
            // Arrange
            var relativePath = "test_relative_store";
            var expectedPath = Path.GetFullPath(relativePath);

            try
            {
                // Act
                new FileSystemSnapshotStore(relativePath);

                // Assert
                Assert.True(Directory.Exists(expectedPath));
            }
            finally
            {
                if (Directory.Exists(expectedPath))
                {
                    Directory.Delete(expectedPath, recursive: true);
                }
            }
        }

        [Fact]
        public async Task CreateWriter_ValidParameters_ReturnsWriter()
        {
            // Act
            var writer = await _store.CreateWriter("job1", 100, "op1", "task1");

            // Assert
            Assert.NotNull(writer);
            Assert.IsAssignableFrom<IStateSnapshotWriter>(writer);
        }

        [Fact]
        public async Task CreateWriter_CreatesDirectoryStructure()
        {
            // Act
            await _store.CreateWriter("job1", 100, "op1", "task1");

            // Assert
            var expectedPath = Path.Combine(_testDirectory, "job1", "cp_100", "op1_task1");
            Assert.True(Directory.Exists(expectedPath));
        }

        [Fact]
        public async Task CreateWriter_SanitizesPathComponents()
        {
            // Arrange - Use forward slash which is invalid on all systems
            var jobId = "job/id";  // / is invalid
            var operatorId = "op/id";  // / is invalid
            var subtaskId = "task/id";  // / is invalid

            // Act
            await _store.CreateWriter(jobId, 100, operatorId, subtaskId);

            // Assert
            var expectedPath = Path.Combine(_testDirectory, "job_id", "cp_100", "op_id_task_id");
            Assert.True(Directory.Exists(expectedPath));
        }

        [Fact]
        public async Task StoreSnapshot_ValidData_StoresFile()
        {
            // Arrange
            var testData = "Hello, World!"u8.ToArray();
            var jobId = "job1";
            var checkpointId = 100L;
            var taskManagerId = "tm1";
            var operatorId = "op1";

            // Act
            var handle = await _store.StoreSnapshot(jobId, checkpointId, taskManagerId, operatorId, testData);

            // Assert
            Assert.NotNull(handle);
            Assert.False(string.IsNullOrEmpty(handle.Value));
            Assert.True(File.Exists(handle.Value));
            
            var storedData = await File.ReadAllBytesAsync(handle.Value);
            Assert.Equal(testData, storedData);
        }

        [Fact]
        public async Task StoreSnapshot_CreatesCorrectDirectoryStructure()
        {
            // Arrange
            var testData = "test"u8.ToArray();

            // Act
            var handle = await _store.StoreSnapshot("job1", 100, "tm1", "op1", testData);

            // Assert
            var expectedDir = Path.Combine(_testDirectory, "job1", "cp_100", "op1_tm1");
            Assert.True(Directory.Exists(expectedDir));
            Assert.StartsWith(expectedDir, handle.Value);
        }

        [Fact]
        public async Task StoreSnapshot_SanitizesFileNames()
        {
            // Arrange
            var testData = "test"u8.ToArray();
            var operatorId = "op/id";  // / is invalid

            // Act
            var handle = await _store.StoreSnapshot("job1", 100, "tm1", operatorId, testData);

            // Assert
            Assert.NotNull(handle);
            Assert.Contains("op_id.snapshot.dat", handle.Value);
        }

        [Fact]
        public async Task RetrieveSnapshot_ValidHandle_ReturnsData()
        {
            // Arrange
            var testData = "Hello, World!"u8.ToArray();
            var handle = await _store.StoreSnapshot("job1", 100, "tm1", "op1", testData);

            // Act
            var retrievedData = await _store.RetrieveSnapshot(handle);

            // Assert
            Assert.NotNull(retrievedData);
            Assert.Equal(testData, retrievedData);
        }

        [Fact]
        public async Task RetrieveSnapshot_NonExistentFile_ReturnsNull()
        {
            // Arrange
            var nonExistentHandle = new SnapshotHandle("/non/existent/path/file.dat");

            // Act
            var result = await _store.RetrieveSnapshot(nonExistentHandle);

            // Assert
            Assert.Null(result);
        }

        [Fact]
        public async Task RetrieveSnapshot_NullHandle_ReturnsNull()
        {
            // Act
            var result = await _store.RetrieveSnapshot(null!);

            // Assert
            Assert.Null(result);
        }

        [Fact]
        public async Task RetrieveSnapshot_EmptyHandle_ReturnsNull()
        {
            // Arrange
            var emptyHandle = new SnapshotHandle("");

            // Act
            var result = await _store.RetrieveSnapshot(emptyHandle);

            // Assert
            Assert.Null(result);
        }

        [Fact]
        public async Task CreateReader_ValidDirectory_ReturnsReader()
        {
            // Arrange
            var testData = "test"u8.ToArray();
            var handle = await _store.StoreSnapshot("job1", 100, "tm1", "op1", testData);
            var directory = Path.GetDirectoryName(handle.Value)!;

            // Act
            var reader = await FileSystemSnapshotStore.CreateReader(directory);

            // Assert
            Assert.NotNull(reader);
            Assert.IsAssignableFrom<IStateSnapshotReader>(reader);
        }

        [Fact]
        public async Task CreateReader_NonExistentDirectory_ReturnsEmptyReader()
        {
            // Arrange
            var nonExistentPath = "/non/existent/directory";

            // Act
            var reader = await FileSystemSnapshotStore.CreateReader(nonExistentPath);

            // Assert
            Assert.NotNull(reader);
            Assert.IsAssignableFrom<IStateSnapshotReader>(reader);
        }

        [Fact]
        public async Task CreateReader_EmptyHandle_ReturnsEmptyReader()
        {
            // Act
            var reader = await FileSystemSnapshotStore.CreateReader("");

            // Assert
            Assert.NotNull(reader);
            Assert.IsAssignableFrom<IStateSnapshotReader>(reader);
        }

        [Fact]
        public async Task CreateReader_NullHandle_ReturnsEmptyReader()
        {
            // Act
            var reader = await FileSystemSnapshotStore.CreateReader(null!);

            // Assert
            Assert.NotNull(reader);
            Assert.IsAssignableFrom<IStateSnapshotReader>(reader);
        }

        [Fact]
        public async Task StoreAndRetrieve_LargeData_WorksCorrectly()
        {
            // Arrange
            var largeData = new byte[1024 * 1024]; // 1MB
            new Random().NextBytes(largeData);

            // Act
            var handle = await _store.StoreSnapshot("job1", 100, "tm1", "op1", largeData);
            var retrievedData = await _store.RetrieveSnapshot(handle);

            // Assert
            Assert.NotNull(retrievedData);
            Assert.Equal(largeData.Length, retrievedData.Length);
            Assert.Equal(largeData, retrievedData);
        }

        [Fact]
        public async Task StoreAndRetrieve_EmptyData_WorksCorrectly()
        {
            // Arrange
            var emptyData = Array.Empty<byte>();

            // Act
            var handle = await _store.StoreSnapshot("job1", 100, "tm1", "op1", emptyData);
            var retrievedData = await _store.RetrieveSnapshot(handle);

            // Assert
            Assert.NotNull(retrievedData);
            Assert.Empty(retrievedData);
        }
    }
}