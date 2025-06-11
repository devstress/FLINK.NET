namespace FlinkDotNet.Storage.FileSystem.Tests
{
    public class DisaggregatedStateBackendTests : IDisposable
    {
        private readonly string _testDirectory;

        public DisaggregatedStateBackendTests()
        {
            _testDirectory = Path.Combine(Path.GetTempPath(), $"FlinkDotNetTest_{Guid.NewGuid()}");
        }

        public void Dispose()
        {
            if (Directory.Exists(_testDirectory))
            {
                Directory.Delete(_testDirectory, recursive: true);
            }
        }

        [Fact]
        public void Constructor_ValidPath_CreatesDirectoryAndInitializesProperties()
        {
            // Act
            var backend = new DisaggregatedStateBackend(_testDirectory);

            // Assert
            Assert.True(Directory.Exists(_testDirectory));
            Assert.Equal(Path.GetFullPath(_testDirectory), backend.BasePath);
            Assert.NotNull(backend.SnapshotStore);
            Assert.IsType<FileSystemSnapshotStore>(backend.SnapshotStore);
        }

        [Fact]
        public void Constructor_RelativePath_ConvertsToAbsolutePath()
        {
            // Arrange
            var relativePath = "test_relative_path";
            var expectedPath = Path.GetFullPath(relativePath);

            // Act
            var backend = new DisaggregatedStateBackend(relativePath);

            // Assert
            Assert.Equal(expectedPath, backend.BasePath);
            Assert.True(Directory.Exists(expectedPath));

            // Cleanup
            if (Directory.Exists(expectedPath))
            {
                Directory.Delete(expectedPath, recursive: true);
            }
        }

        [Fact]
        public void Constructor_ExistingDirectory_DoesNotThrow()
        {
            // Arrange
            Directory.CreateDirectory(_testDirectory);
            Assert.True(Directory.Exists(_testDirectory));

            // Act & Assert
            var backend = new DisaggregatedStateBackend(_testDirectory);
            Assert.Equal(Path.GetFullPath(_testDirectory), backend.BasePath);
        }

        [Fact]
        public void Constructor_NestedPath_CreatesNestedDirectories()
        {
            // Arrange
            var nestedPath = Path.Combine(_testDirectory, "level1", "level2", "level3");

            // Act
            var backend = new DisaggregatedStateBackend(nestedPath);

            // Assert
            Assert.True(Directory.Exists(nestedPath));
            Assert.Equal(Path.GetFullPath(nestedPath), backend.BasePath);
        }

        [Fact]
        public void BasePath_IsReadOnly()
        {
            // Arrange
            var backend = new DisaggregatedStateBackend(_testDirectory);
            var originalPath = backend.BasePath;

            // Act & Assert - BasePath should be get-only
            Assert.Equal(originalPath, backend.BasePath);
            
            // Verify it's always the same reference
            Assert.Same(originalPath, backend.BasePath);
        }

        [Fact]
        public void SnapshotStore_IsReadOnly()
        {
            // Arrange
            var backend = new DisaggregatedStateBackend(_testDirectory);
            var originalStore = backend.SnapshotStore;

            // Act & Assert - SnapshotStore should be get-only
            Assert.Same(originalStore, backend.SnapshotStore);
        }

        [Theory]
        [InlineData(" ")]
        [InlineData("\t")]
        public void Constructor_WhitespaceBasePath_CreatesDirectoryInCurrentLocation(string basePath)
        {
            // Arrange
            string expectedPath;
            try
            {
                expectedPath = Path.GetFullPath(basePath);
            }
            catch (ArgumentException)
            {
                // On Windows, Path.GetFullPath can fail with whitespace-only paths
                // Skip test on platforms where this is not supported
                return;
            }

            try
            {
                // Act
                var backend = new DisaggregatedStateBackend(basePath);

                // Assert
                Assert.Equal(expectedPath, backend.BasePath);
                Assert.True(Directory.Exists(expectedPath));
            }
            finally
            {
                // Cleanup - be careful with empty paths
                if (!string.IsNullOrWhiteSpace(expectedPath) && 
                    expectedPath != Environment.CurrentDirectory &&
                    Directory.Exists(expectedPath))
                {
                    try
                    {
                        Directory.Delete(expectedPath, recursive: true);
                    }
                    catch
                    {
                        // Ignore cleanup errors for edge cases
                    }
                }
            }
        }

        [Fact]
        public void Constructor_EmptyBasePath_ThrowsArgumentException()
        {
            // Act & Assert
            Assert.Throws<ArgumentException>(() => new DisaggregatedStateBackend(""));
        }
    }
}