using FlinkDotNet.Connectors.Sinks.Console;
using FlinkDotNet.Core.Abstractions.Context;
using FlinkDotNet.Core.Abstractions.Sinks;
using Moq;
using System.Text;

namespace FlinkDotNet.Connectors.Sinks.Console.Tests
{
    public class ConsoleSinkFunctionTests : IDisposable
    {
        private readonly StringWriter _stringWriter;
        private readonly TextWriter _originalOut;

        public ConsoleSinkFunctionTests()
        {
            _originalOut = System.Console.Out;
            _stringWriter = new StringWriter();
            System.Console.SetOut(_stringWriter);
        }

        public void Dispose()
        {
            System.Console.SetOut(_originalOut);
            _stringWriter.Dispose();
        }

        private string GetConsoleOutput()
        {
            return _stringWriter.ToString();
        }

        private void ClearConsoleOutput()
        {
            _stringWriter.GetStringBuilder().Clear();
        }

        [Fact]
        public void Constructor_CreatesInstance()
        {
            // Act
            var sink = new ConsoleSinkFunction<string>();

            // Assert
            Assert.NotNull(sink);
        }

        [Fact]
        public void Open_WithValidContext_UpdatesTaskNameAndLogsMessage()
        {
            // Arrange
            var sink = new ConsoleSinkFunction<string>();
            var mockContext = new Mock<IRuntimeContext>();
            mockContext.Setup(c => c.TaskName).Returns("TestTask");

            // Act
            sink.Open(mockContext.Object);

            // Assert
            var output = GetConsoleOutput();
            Assert.Contains("[TestTask] ConsoleSinkFunction opened.", output);
        }

        [Fact]
        public void Open_WithDefaultContext_UsesDefaultTaskName()
        {
            // Arrange
            var sink = new ConsoleSinkFunction<string>();
            var mockContext = new Mock<IRuntimeContext>();
            mockContext.Setup(c => c.TaskName).Returns("ConsoleSink");

            // Act
            sink.Open(mockContext.Object);

            // Assert
            var output = GetConsoleOutput();
            Assert.Contains("[ConsoleSink] ConsoleSinkFunction opened.", output);
        }

        [Fact]
        public void Invoke_WithStringRecord_LogsRecordWithTimestamp()
        {
            // Arrange
            var sink = new ConsoleSinkFunction<string>();
            var mockRuntimeContext = new Mock<IRuntimeContext>();
            mockRuntimeContext.Setup(c => c.TaskName).Returns("TestTask");
            var mockSinkContext = new Mock<ISinkContext>();
            mockSinkContext.Setup(c => c.CurrentProcessingTimeMillis()).Returns(12345L);

            sink.Open(mockRuntimeContext.Object);
            ClearConsoleOutput();

            // Act
            sink.Invoke("Hello, World!", mockSinkContext.Object);

            // Assert
            var output = GetConsoleOutput();
            Assert.Contains("[TestTask @ 12345ms] Hello, World!", output);
        }

        [Fact]
        public void Invoke_WithIntegerRecord_LogsRecordAsString()
        {
            // Arrange
            var sink = new ConsoleSinkFunction<int>();
            var mockRuntimeContext = new Mock<IRuntimeContext>();
            mockRuntimeContext.Setup(c => c.TaskName).Returns("IntTask");
            var mockSinkContext = new Mock<ISinkContext>();
            mockSinkContext.Setup(c => c.CurrentProcessingTimeMillis()).Returns(54321L);

            sink.Open(mockRuntimeContext.Object);
            ClearConsoleOutput();

            // Act
            sink.Invoke(42, mockSinkContext.Object);

            // Assert
            var output = GetConsoleOutput();
            Assert.Contains("[IntTask @ 54321ms] 42", output);
        }

        [Fact]
        public void Invoke_WithNullRecord_LogsNullString()
        {
            // Arrange
            var sink = new ConsoleSinkFunction<string>();
            var mockRuntimeContext = new Mock<IRuntimeContext>();
            mockRuntimeContext.Setup(c => c.TaskName).Returns("NullTask");
            var mockSinkContext = new Mock<ISinkContext>();
            mockSinkContext.Setup(c => c.CurrentProcessingTimeMillis()).Returns(99999L);

            sink.Open(mockRuntimeContext.Object);
            ClearConsoleOutput();

            // Act
            sink.Invoke(null!, mockSinkContext.Object);

            // Assert
            var output = GetConsoleOutput();
            Assert.Contains("[NullTask @ 99999ms] null", output);
        }

        [Fact]
        public void Invoke_WithComplexObject_LogsObjectToString()
        {
            // Arrange
            var sink = new ConsoleSinkFunction<TestRecord>();
            var mockRuntimeContext = new Mock<IRuntimeContext>();
            mockRuntimeContext.Setup(c => c.TaskName).Returns("ObjectTask");
            var mockSinkContext = new Mock<ISinkContext>();
            mockSinkContext.Setup(c => c.CurrentProcessingTimeMillis()).Returns(11111L);

            var testRecord = new TestRecord { Id = 123, Name = "Test", Value = 45.67 };

            sink.Open(mockRuntimeContext.Object);
            ClearConsoleOutput();

            // Act
            sink.Invoke(testRecord, mockSinkContext.Object);

            // Assert
            var output = GetConsoleOutput();
            Assert.Contains("[ObjectTask @ 11111ms]", output);
            Assert.Contains("TestRecord", output);
            Assert.Contains("123", output);
            Assert.Contains("Test", output);
            Assert.Contains("45.67", output);
        }

        [Fact]
        public void Invoke_MultipleRecords_LogsAllRecords()
        {
            // Arrange
            var sink = new ConsoleSinkFunction<string>();
            var mockRuntimeContext = new Mock<IRuntimeContext>();
            mockRuntimeContext.Setup(c => c.TaskName).Returns("MultiTask");
            var mockSinkContext = new Mock<ISinkContext>();
            mockSinkContext.SetupSequence(c => c.CurrentProcessingTimeMillis())
                .Returns(1000L)
                .Returns(2000L)
                .Returns(3000L);

            sink.Open(mockRuntimeContext.Object);
            ClearConsoleOutput();

            // Act
            sink.Invoke("First", mockSinkContext.Object);
            sink.Invoke("Second", mockSinkContext.Object);
            sink.Invoke("Third", mockSinkContext.Object);

            // Assert
            var output = GetConsoleOutput();
            Assert.Contains("[MultiTask @ 1000ms] First", output);
            Assert.Contains("[MultiTask @ 2000ms] Second", output);
            Assert.Contains("[MultiTask @ 3000ms] Third", output);
        }

        [Fact]
        public void Close_LogsCloseMessage()
        {
            // Arrange
            var sink = new ConsoleSinkFunction<string>();
            var mockContext = new Mock<IRuntimeContext>();
            mockContext.Setup(c => c.TaskName).Returns("CloseTask");

            sink.Open(mockContext.Object);
            ClearConsoleOutput();

            // Act
            sink.Close();

            // Assert
            var output = GetConsoleOutput();
            Assert.Contains("[CloseTask] ConsoleSinkFunction closed.", output);
        }

        [Fact]
        public void Close_WithoutOpen_UsesDefaultTaskName()
        {
            // Arrange
            var sink = new ConsoleSinkFunction<string>();

            // Act
            sink.Close();

            // Assert
            var output = GetConsoleOutput();
            Assert.Contains("[ConsoleSink] ConsoleSinkFunction closed.", output);
        }

        [Fact]
        public void LifecycleTest_OpenInvokeClose_WorksCorrectly()
        {
            // Arrange
            var sink = new ConsoleSinkFunction<string>();
            var mockRuntimeContext = new Mock<IRuntimeContext>();
            mockRuntimeContext.Setup(c => c.TaskName).Returns("LifecycleTask");
            var mockSinkContext = new Mock<ISinkContext>();
            mockSinkContext.Setup(c => c.CurrentProcessingTimeMillis()).Returns(5555L);

            // Act
            sink.Open(mockRuntimeContext.Object);
            sink.Invoke("LifecycleTest", mockSinkContext.Object);
            sink.Close();

            // Assert
            var output = GetConsoleOutput();
            Assert.Contains("[LifecycleTask] ConsoleSinkFunction opened.", output);
            Assert.Contains("[LifecycleTask @ 5555ms] LifecycleTest", output);
            Assert.Contains("[LifecycleTask] ConsoleSinkFunction closed.", output);
        }

        [Fact]
        public void Invoke_BeforeOpen_UsesDefaultTaskName()
        {
            // Arrange
            var sink = new ConsoleSinkFunction<string>();
            var mockSinkContext = new Mock<ISinkContext>();
            mockSinkContext.Setup(c => c.CurrentProcessingTimeMillis()).Returns(7777L);

            // Act
            sink.Invoke("BeforeOpen", mockSinkContext.Object);

            // Assert
            var output = GetConsoleOutput();
            Assert.Contains("[ConsoleSink @ 7777ms] BeforeOpen", output);
        }

        [Fact]
        public void Invoke_WithEmptyString_LogsEmptyRecord()
        {
            // Arrange
            var sink = new ConsoleSinkFunction<string>();
            var mockRuntimeContext = new Mock<IRuntimeContext>();
            mockRuntimeContext.Setup(c => c.TaskName).Returns("EmptyTask");
            var mockSinkContext = new Mock<ISinkContext>();
            mockSinkContext.Setup(c => c.CurrentProcessingTimeMillis()).Returns(8888L);

            sink.Open(mockRuntimeContext.Object);
            ClearConsoleOutput();

            // Act
            sink.Invoke("", mockSinkContext.Object);

            // Assert
            var output = GetConsoleOutput();
            Assert.Contains("[EmptyTask @ 8888ms] ", output);
            // The empty string should result in just the timestamp and brackets
            var lines = output.Split(Environment.NewLine, StringSplitOptions.RemoveEmptyEntries);
            var logLine = lines.FirstOrDefault(l => l.Contains("@ 8888ms"));
            Assert.NotNull(logLine);
            Assert.EndsWith("] ", logLine);
        }

        [Fact]
        public void TaskName_ChangesAfterMultipleOpens()
        {
            // Arrange
            var sink = new ConsoleSinkFunction<string>();
            var mockContext1 = new Mock<IRuntimeContext>();
            mockContext1.Setup(c => c.TaskName).Returns("FirstTask");
            var mockContext2 = new Mock<IRuntimeContext>();
            mockContext2.Setup(c => c.TaskName).Returns("SecondTask");
            var mockSinkContext = new Mock<ISinkContext>();
            mockSinkContext.Setup(c => c.CurrentProcessingTimeMillis()).Returns(1111L);

            // Act
            sink.Open(mockContext1.Object);
            ClearConsoleOutput();
            sink.Invoke("First", mockSinkContext.Object);

            sink.Open(mockContext2.Object);
            sink.Invoke("Second", mockSinkContext.Object);

            // Assert
            var output = GetConsoleOutput();
            Assert.Contains("[FirstTask @ 1111ms] First", output);
            Assert.Contains("[SecondTask] ConsoleSinkFunction opened.", output);
            Assert.Contains("[SecondTask @ 1111ms] Second", output);
        }

        private class TestRecord
        {
            public int Id { get; set; }
            public string? Name { get; set; }
            public double Value { get; set; }

            public override string ToString()
            {
                return $"TestRecord(Id={Id}, Name={Name}, Value={Value})";
            }
        }
    }
}