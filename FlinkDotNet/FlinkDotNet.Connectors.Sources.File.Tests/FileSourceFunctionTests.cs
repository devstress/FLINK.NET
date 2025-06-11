using FlinkDotNet.Connectors.Sources.File;
using FlinkDotNet.Core.Abstractions.Sources;
using FlinkDotNet.Core.Abstractions.Serializers;
using Moq;
using System.Text;

namespace FlinkDotNet.Connectors.Sources.File.Tests
{
    public class FileSourceFunctionTests : IDisposable
    {
        private readonly StringWriter _stringWriter;
        private readonly TextWriter _originalOut;
        private readonly string _testDirectory;

        public FileSourceFunctionTests()
        {
            _originalOut = System.Console.Out;
            _stringWriter = new StringWriter();
            System.Console.SetOut(_stringWriter);
            _testDirectory = Path.Combine(Path.GetTempPath(), $"FlinkDotNetFileSourceTest_{Guid.NewGuid()}");
            Directory.CreateDirectory(_testDirectory);
        }

        public void Dispose()
        {
            System.Console.SetOut(_originalOut);
            _stringWriter.Dispose();
            if (Directory.Exists(_testDirectory))
            {
                Directory.Delete(_testDirectory, recursive: true);
            }
        }

        private string GetConsoleOutput()
        {
            return _stringWriter.ToString();
        }

        private void ClearConsoleOutput()
        {
            _stringWriter.GetStringBuilder().Clear();
        }

        private string CreateTestFile(string fileName, params string[] lines)
        {
            var filePath = Path.Combine(_testDirectory, fileName);
            System.IO.File.WriteAllLines(filePath, lines);
            return filePath;
        }

        [Fact]
        public void Constructor_ValidParameters_CreatesInstance()
        {
            // Arrange
            var filePath = CreateTestFile("test.txt", "line1", "line2");
            var mockSerializer = new Mock<ITypeSerializer<string>>();

            // Act
            var source = new FileSourceFunction<string>(filePath, mockSerializer.Object);

            // Assert
            Assert.NotNull(source);
        }

        [Fact]
        public void Constructor_NullFilePath_ThrowsArgumentNullException()
        {
            // Arrange
            var mockSerializer = new Mock<ITypeSerializer<string>>();

            // Act & Assert
            Assert.Throws<ArgumentNullException>(() => new FileSourceFunction<string>(null!, mockSerializer.Object));
        }

        [Fact]
        public void Constructor_EmptyFilePath_ThrowsArgumentNullException()
        {
            // Arrange
            var mockSerializer = new Mock<ITypeSerializer<string>>();

            // Act & Assert
            Assert.Throws<ArgumentNullException>(() => new FileSourceFunction<string>("", mockSerializer.Object));
        }

        [Fact]
        public void Constructor_NullSerializer_ThrowsArgumentNullException()
        {
            // Arrange
            var filePath = CreateTestFile("test.txt", "line1");

            // Act & Assert
            Assert.Throws<ArgumentNullException>(() => new FileSourceFunction<string>(filePath, null!));
        }

        [Fact]
        public void Run_ValidFile_ReadsAllLines()
        {
            // Arrange
            var filePath = CreateTestFile("test.txt", "Hello", "World", "Test");
            var mockSerializer = new Mock<ITypeSerializer<string>>();
            var mockSourceContext = new Mock<ISourceContext<string>>();
            var collectedRecords = new List<string>();

            mockSerializer.Setup(s => s.Deserialize(It.IsAny<byte[]>()))
                .Returns((byte[] bytes) => Encoding.UTF8.GetString(bytes));
            
            mockSourceContext.Setup(c => c.Collect(It.IsAny<string>()))
                .Callback<string>(record => collectedRecords.Add(record));

            var source = new FileSourceFunction<string>(filePath, mockSerializer.Object);

            // Act
            source.Run(mockSourceContext.Object);

            // Assert
            Assert.Equal(3, collectedRecords.Count);
            Assert.Contains("Hello", collectedRecords);
            Assert.Contains("World", collectedRecords);
            Assert.Contains("Test", collectedRecords);

            // Verify serializer was called for each line
            mockSerializer.Verify(s => s.Deserialize(It.IsAny<byte[]>()), Times.Exactly(3));
            mockSourceContext.Verify(c => c.Collect(It.IsAny<string>()), Times.Exactly(3));
        }

        [Fact]
        public void Run_EmptyFile_DoesNotCollectAnything()
        {
            // Arrange
            var filePath = CreateTestFile("empty.txt");
            var mockSerializer = new Mock<ITypeSerializer<string>>();
            var mockSourceContext = new Mock<ISourceContext<string>>();

            var source = new FileSourceFunction<string>(filePath, mockSerializer.Object);

            // Act
            source.Run(mockSourceContext.Object);

            // Assert
            mockSourceContext.Verify(c => c.Collect(It.IsAny<string>()), Times.Never);
            mockSerializer.Verify(s => s.Deserialize(It.IsAny<byte[]>()), Times.Never);
        }

        [Fact]
        public void Run_NonExistentFile_HandlesGracefully()
        {
            // Arrange
            var nonExistentPath = Path.Combine(_testDirectory, "nonexistent.txt");
            var mockSerializer = new Mock<ITypeSerializer<string>>();
            var mockSourceContext = new Mock<ISourceContext<string>>();

            var source = new FileSourceFunction<string>(nonExistentPath, mockSerializer.Object);

            // Act
            source.Run(mockSourceContext.Object);

            // Assert
            var output = GetConsoleOutput();
            Assert.Contains("Error - File not found", output);
            Assert.Contains(nonExistentPath, output);
            mockSourceContext.Verify(c => c.Collect(It.IsAny<string>()), Times.Never);
        }

        [Fact]
        public void Run_SerializerReturnsNull_SkipsRecord()
        {
            // Arrange
            var filePath = CreateTestFile("test.txt", "line1", "line2", "line3");
            var mockSerializer = new Mock<ITypeSerializer<string>>();
            var mockSourceContext = new Mock<ISourceContext<string>>();
            var collectedRecords = new List<string>();

            // Setup serializer to return null for second line
            mockSerializer.SetupSequence(s => s.Deserialize(It.IsAny<byte[]>()))
                .Returns("line1")
                .Returns((string?)null)  // This should be skipped
                .Returns("line3");

            mockSourceContext.Setup(c => c.Collect(It.IsAny<string>()))
                .Callback<string>(record => collectedRecords.Add(record));

            var source = new FileSourceFunction<string>(filePath, mockSerializer.Object);

            // Act
            source.Run(mockSourceContext.Object);

            // Assert
            Assert.Equal(2, collectedRecords.Count);
            Assert.Contains("line1", collectedRecords);
            Assert.Contains("line3", collectedRecords);
            Assert.DoesNotContain(null, collectedRecords);

            var output = GetConsoleOutput();
            Assert.Contains("Deserialized a null record", output);
        }

        [Fact]
        public void Run_SerializerThrowsException_ContinuesProcessing()
        {
            // Arrange
            var filePath = CreateTestFile("test.txt", "line1", "badline", "line3");
            var mockSerializer = new Mock<ITypeSerializer<string>>();
            var mockSourceContext = new Mock<ISourceContext<string>>();
            var collectedRecords = new List<string>();

            // Setup serializer to throw for second line
            mockSerializer.SetupSequence(s => s.Deserialize(It.IsAny<byte[]>()))
                .Returns("line1")
                .Throws(new FormatException("Bad format"))
                .Returns("line3");

            mockSourceContext.Setup(c => c.Collect(It.IsAny<string>()))
                .Callback<string>(record => collectedRecords.Add(record));

            var source = new FileSourceFunction<string>(filePath, mockSerializer.Object);

            // Act
            source.Run(mockSourceContext.Object);

            // Assert
            Assert.Equal(2, collectedRecords.Count);
            Assert.Contains("line1", collectedRecords);
            Assert.Contains("line3", collectedRecords);

            var output = GetConsoleOutput();
            Assert.Contains("Error deserializing line 'badline'", output);
            Assert.Contains("Bad format", output);
        }

        [Fact]
        public void Run_CancelledBeforeStart_DoesNotProcess()
        {
            // Arrange
            var filePath = CreateTestFile("test.txt", "line1", "line2");
            var mockSerializer = new Mock<ITypeSerializer<string>>();
            var mockSourceContext = new Mock<ISourceContext<string>>();

            var source = new FileSourceFunction<string>(filePath, mockSerializer.Object);
            source.Cancel(); // Cancel before running

            // Act
            source.Run(mockSourceContext.Object);

            // Assert
            mockSourceContext.Verify(c => c.Collect(It.IsAny<string>()), Times.Never);
        }

        [Fact]
        public void Run_CancelledDuringProcess_StopsProcessing()
        {
            // Arrange
            var filePath = CreateTestFile("test.txt", "line1", "line2", "line3", "line4", "line5");
            var mockSerializer = new Mock<ITypeSerializer<string>>();
            var mockSourceContext = new Mock<ISourceContext<string>>();
            var collectedRecords = new List<string>();
            var source = new FileSourceFunction<string>(filePath, mockSerializer.Object);

            mockSerializer.Setup(s => s.Deserialize(It.IsAny<byte[]>()))
                .Returns((byte[] bytes) => Encoding.UTF8.GetString(bytes));

            mockSourceContext.Setup(c => c.Collect(It.IsAny<string>()))
                .Callback<string>(record =>
                {
                    collectedRecords.Add(record);
                    if (collectedRecords.Count == 2)
                    {
                        // Cancel after processing 2 records
                        source.Cancel();
                    }
                });

            // Act
            source.Run(mockSourceContext.Object);

            // Assert
            // Should have processed at least 2 records, but not all 5
            Assert.True(collectedRecords.Count >= 2);
            Assert.True(collectedRecords.Count < 5);

            var output = GetConsoleOutput();
            Assert.Contains("Cancel called", output);
        }

        [Fact]
        public void Cancel_LogsMessage()
        {
            // Arrange
            var filePath = CreateTestFile("test.txt", "line1");
            var mockSerializer = new Mock<ITypeSerializer<string>>();
            var source = new FileSourceFunction<string>(filePath, mockSerializer.Object);

            // Act
            source.Cancel();

            // Assert
            var output = GetConsoleOutput();
            Assert.Contains("Cancel called", output);
            Assert.Contains(filePath, output);
        }

        [Fact]
        public void Run_LogsStartAndFinishMessages()
        {
            // Arrange
            var filePath = CreateTestFile("test.txt", "line1");
            var mockSerializer = new Mock<ITypeSerializer<string>>();
            var mockSourceContext = new Mock<ISourceContext<string>>();

            mockSerializer.Setup(s => s.Deserialize(It.IsAny<byte[]>()))
                .Returns("line1");

            var source = new FileSourceFunction<string>(filePath, mockSerializer.Object);

            // Act
            source.Run(mockSourceContext.Object);

            // Assert
            var output = GetConsoleOutput();
            Assert.Contains("Starting to read from", output);
            Assert.Contains("Finished reading from", output);
            Assert.Contains(filePath, output);
        }

        [Fact]
        public void Run_WithIntegerData_WorksCorrectly()
        {
            // Arrange
            var filePath = CreateTestFile("numbers.txt", "123", "456", "789");
            var mockSerializer = new Mock<ITypeSerializer<int>>();
            var mockSourceContext = new Mock<ISourceContext<int>>();
            var collectedRecords = new List<int>();

            mockSerializer.Setup(s => s.Deserialize(It.IsAny<byte[]>()))
                .Returns((byte[] bytes) => int.Parse(Encoding.UTF8.GetString(bytes)));

            mockSourceContext.Setup(c => c.Collect(It.IsAny<int>()))
                .Callback<int>(record => collectedRecords.Add(record));

            var source = new FileSourceFunction<int>(filePath, mockSerializer.Object);

            // Act
            source.Run(mockSourceContext.Object);

            // Assert
            Assert.Equal(3, collectedRecords.Count);
            Assert.Contains(123, collectedRecords);
            Assert.Contains(456, collectedRecords);
            Assert.Contains(789, collectedRecords);
        }

        [Fact]
        public void Run_FileWithSpecialCharacters_HandlesCorrectly()
        {
            // Arrange
            var filePath = CreateTestFile("special.txt", "Hello, 世界!", "Café", "Naïve résumé");
            var mockSerializer = new Mock<ITypeSerializer<string>>();
            var mockSourceContext = new Mock<ISourceContext<string>>();
            var collectedRecords = new List<string>();

            mockSerializer.Setup(s => s.Deserialize(It.IsAny<byte[]>()))
                .Returns((byte[] bytes) => Encoding.UTF8.GetString(bytes));

            mockSourceContext.Setup(c => c.Collect(It.IsAny<string>()))
                .Callback<string>(record => collectedRecords.Add(record));

            var source = new FileSourceFunction<string>(filePath, mockSerializer.Object);

            // Act
            source.Run(mockSourceContext.Object);

            // Assert
            Assert.Equal(3, collectedRecords.Count);
            Assert.Contains("Hello, 世界!", collectedRecords);
            Assert.Contains("Café", collectedRecords);
            Assert.Contains("Naïve résumé", collectedRecords);
        }

        [Fact]
        public void Run_LargeFile_ProcessesAllLines()
        {
            // Arrange
            var lines = Enumerable.Range(1, 1000).Select(i => $"Line {i}").ToArray();
            var filePath = CreateTestFile("large.txt", lines);
            var mockSerializer = new Mock<ITypeSerializer<string>>();
            var mockSourceContext = new Mock<ISourceContext<string>>();
            var collectedRecords = new List<string>();

            mockSerializer.Setup(s => s.Deserialize(It.IsAny<byte[]>()))
                .Returns((byte[] bytes) => Encoding.UTF8.GetString(bytes));

            mockSourceContext.Setup(c => c.Collect(It.IsAny<string>()))
                .Callback<string>(record => collectedRecords.Add(record));

            var source = new FileSourceFunction<string>(filePath, mockSerializer.Object);

            // Act
            source.Run(mockSourceContext.Object);

            // Assert
            Assert.Equal(1000, collectedRecords.Count);
            Assert.Contains("Line 1", collectedRecords);
            Assert.Contains("Line 500", collectedRecords);
            Assert.Contains("Line 1000", collectedRecords);
        }
    }
}