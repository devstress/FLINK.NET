namespace FlinkDotNet.Storage.FileSystem.Tests
{
    public class FileSystemSnapshotWriterTests : IDisposable
    {
        private readonly string _testDirectory;
        private readonly FileSystemSnapshotStore _store;

        public FileSystemSnapshotWriterTests()
        {
            _testDirectory = Path.Combine(Path.GetTempPath(), $"FlinkDotNetTest_{Guid.NewGuid()}");
            _store = new FileSystemSnapshotStore(_testDirectory);
        }

        public void Dispose()
        {
            if (Directory.Exists(_testDirectory))
            {
                Directory.Delete(_testDirectory, recursive: true);
            }
        }

        [Fact]
        public async Task GetStateOutputStream_ValidStateName_ReturnsStream()
        {
            // Arrange
            var writer = await _store.CreateWriter("job1", 100, "op1", "task1");

            // Act
            using var stream = writer.GetStateOutputStream("testState");

            // Assert
            Assert.NotNull(stream);
            Assert.True(stream.CanWrite);
        }

        [Fact]
        public async Task GetStateOutputStream_WriteData_CreatesFile()
        {
            // Arrange
            var writer = await _store.CreateWriter("job1", 100, "op1", "task1");
            var testData = "Hello, World!"u8.ToArray();

            // Act
            using (var stream = writer.GetStateOutputStream("testState"))
            {
                await stream.WriteAsync(testData);
            }
            var handle = await writer.CommitAndGetHandleAsync();

            // Assert
            var expectedFile = Path.Combine(handle, "testState.raw_state");
            Assert.True(File.Exists(expectedFile));
            var fileData = await File.ReadAllBytesAsync(expectedFile);
            Assert.Equal(testData, fileData);
        }

        [Fact]
        public async Task BeginKeyedState_ValidStateName_StartsKeyedState()
        {
            // Arrange
            var writer = await _store.CreateWriter("job1", 100, "op1", "task1");

            // Act & Assert
            await writer.BeginKeyedState("keyedState1");
            // Should not throw
        }

        [Fact]
        public async Task BeginKeyedState_DuplicateState_ThrowsException()
        {
            // Arrange
            var writer = await _store.CreateWriter("job1", 100, "op1", "task1");
            await writer.BeginKeyedState("keyedState1");

            // Act & Assert
            await Assert.ThrowsAsync<InvalidOperationException>(() =>
                writer.BeginKeyedState("keyedState1"));
        }

        [Fact]
        public async Task BeginKeyedState_TwoStatesSimultaneously_ThrowsException()
        {
            // Arrange
            var writer = await _store.CreateWriter("job1", 100, "op1", "task1");
            await writer.BeginKeyedState("keyedState1");

            // Act & Assert
            await Assert.ThrowsAsync<InvalidOperationException>(() =>
                writer.BeginKeyedState("keyedState2"));
        }

        [Fact]
        public async Task WriteKeyedEntry_NoActiveState_ThrowsException()
        {
            // Arrange
            var writer = await _store.CreateWriter("job1", 100, "op1", "task1");

            // Act & Assert
            await Assert.ThrowsAsync<InvalidOperationException>(() =>
                writer.WriteKeyedEntry("key"u8.ToArray(), "value"u8.ToArray()));
        }

        [Fact]
        public async Task WriteKeyedEntry_ValidData_WritesEntry()
        {
            // Arrange
            var writer = await _store.CreateWriter("job1", 100, "op1", "task1");
            await writer.BeginKeyedState("keyedState1");

            // Act
            await writer.WriteKeyedEntry("key1"u8.ToArray(), "value1"u8.ToArray());
            await writer.WriteKeyedEntry("key2"u8.ToArray(), "value2"u8.ToArray());

            // Assert - Should not throw
            await writer.EndKeyedState("keyedState1");
        }

        [Fact]
        public async Task EndKeyedState_NonActiveState_ThrowsException()
        {
            // Arrange
            var writer = await _store.CreateWriter("job1", 100, "op1", "task1");

            // Act & Assert
            await Assert.ThrowsAsync<InvalidOperationException>(() =>
                writer.EndKeyedState("nonExistentState"));
        }

        [Fact]
        public async Task EndKeyedState_ValidState_EndsState()
        {
            // Arrange
            var writer = await _store.CreateWriter("job1", 100, "op1", "task1");
            await writer.BeginKeyedState("keyedState1");
            await writer.WriteKeyedEntry("key"u8.ToArray(), "value"u8.ToArray());

            // Act
            await writer.EndKeyedState("keyedState1");

            // Assert - Should be able to begin new state
            await writer.BeginKeyedState("keyedState2");
        }

        [Fact]
        public async Task CommitAndGetHandleAsync_WritesKeyedStateFile()
        {
            // Arrange
            var writer = await _store.CreateWriter("job1", 100, "op1", "task1");
            await writer.BeginKeyedState("keyedState1");
            await writer.WriteKeyedEntry("key1"u8.ToArray(), "value1"u8.ToArray());
            await writer.WriteKeyedEntry("key2"u8.ToArray(), "value2"u8.ToArray());
            await writer.EndKeyedState("keyedState1");

            // Act
            var handle = await writer.CommitAndGetHandleAsync();

            // Assert
            var expectedFile = Path.Combine(handle, "keyedState1.keyed_state");
            Assert.True(File.Exists(expectedFile));
            
            // Verify file content structure (magic number + version)
            var fileData = await File.ReadAllBytesAsync(expectedFile);
            Assert.True(fileData.Length > 6); // At least magic (4) + version (2)
            
            // Check magic number
            Assert.Equal((byte)'F', fileData[0]);
            Assert.Equal((byte)'N', fileData[1]);
            Assert.Equal((byte)'K', fileData[2]);
            Assert.Equal((byte)'S', fileData[3]);
        }

        [Fact]
        public async Task CommitAndGetHandleAsync_UnclosedKeyedState_AutoCloses()
        {
            // Arrange
            var writer = await _store.CreateWriter("job1", 100, "op1", "task1");
            await writer.BeginKeyedState("keyedState1");
            await writer.WriteKeyedEntry("key"u8.ToArray(), "value"u8.ToArray());
            // Intentionally not calling EndKeyedState

            // Act
            var handle = await writer.CommitAndGetHandleAsync();

            // Assert
            var expectedFile = Path.Combine(handle, "keyedState1.keyed_state");
            Assert.True(File.Exists(expectedFile));
        }

        [Fact]
        public async Task CommitAndGetHandleAsync_ReturnsCorrectHandle()
        {
            // Arrange
            var writer = await _store.CreateWriter("job1", 100, "op1", "task1");

            // Act
            var handle = await writer.CommitAndGetHandleAsync();

            // Assert
            Assert.NotNull(handle);
            Assert.True(Directory.Exists(handle));
            Assert.Contains("job1", handle);
            Assert.Contains("cp_100", handle);
            Assert.Contains("op1_task1", handle);
        }

        [Fact]
        public async Task DisposeAsync_UnclosedKeyedState_ClosesStates()
        {
            // Arrange
            var writer = await _store.CreateWriter("job1", 100, "op1", "task1");
            await writer.BeginKeyedState("keyedState1");
            await writer.WriteKeyedEntry("key"u8.ToArray(), "value"u8.ToArray());

            // Act & Assert - Should not throw
            if (writer is IAsyncDisposable asyncDisposable)
            {
                await asyncDisposable.DisposeAsync();
            }
            else
            {
                writer.Dispose();
            }
        }

        [Fact]
        public async Task Dispose_UnclosedKeyedState_ClosesStates()
        {
            // Arrange
            var writer = await _store.CreateWriter("job1", 100, "op1", "task1");
            await writer.BeginKeyedState("keyedState1");
            await writer.WriteKeyedEntry("key"u8.ToArray(), "value"u8.ToArray());

            // Act & Assert - Should not throw
            writer.Dispose();
        }

        [Fact]
        public async Task CompleteWorkflow_RawAndKeyedState_CreatesAllFiles()
        {
            // Arrange
            var writer = await _store.CreateWriter("job1", 100, "op1", "task1");

            // Act - Write raw state
            using (var stream = writer.GetStateOutputStream("rawState1"))
            {
                await stream.WriteAsync("raw data 1"u8.ToArray());
            }
            using (var stream = writer.GetStateOutputStream("rawState2"))
            {
                await stream.WriteAsync("raw data 2"u8.ToArray());
            }

            // Act - Write keyed state
            await writer.BeginKeyedState("keyedState1");
            await writer.WriteKeyedEntry("key1"u8.ToArray(), "value1"u8.ToArray());
            await writer.WriteKeyedEntry("key2"u8.ToArray(), "value2"u8.ToArray());
            await writer.EndKeyedState("keyedState1");

            await writer.BeginKeyedState("keyedState2");
            await writer.WriteKeyedEntry("keyA"u8.ToArray(), "valueA"u8.ToArray());
            await writer.EndKeyedState("keyedState2");

            var handle = await writer.CommitAndGetHandleAsync();

            // Assert
            Assert.True(File.Exists(Path.Combine(handle, "rawState1.raw_state")));
            Assert.True(File.Exists(Path.Combine(handle, "rawState2.raw_state")));
            Assert.True(File.Exists(Path.Combine(handle, "keyedState1.keyed_state")));
            Assert.True(File.Exists(Path.Combine(handle, "keyedState2.keyed_state")));
        }

        [Fact]
        public async Task GetStateOutputStream_SanitizesStateName()
        {
            // Arrange
            var writer = await _store.CreateWriter("job1", 100, "op1", "task1");
            var invalidStateName = "state/name";  // / is invalid

            // Act
            using (var stream = writer.GetStateOutputStream(invalidStateName))
            {
                await stream.WriteAsync("test"u8.ToArray());
            }
            var handle = await writer.CommitAndGetHandleAsync();

            // Assert
            var expectedFile = Path.Combine(handle, "state_name.raw_state");
            Assert.True(File.Exists(expectedFile));
        }
    }
}