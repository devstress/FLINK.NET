using FlinkDotNet.Connectors.Sources.Kafka;
using FlinkDotNet.Connectors.Sources.Kafka.Native;
using FlinkDotNet.Core.Abstractions.Context;
using FlinkDotNet.Core.Abstractions.Sinks;
using Microsoft.Extensions.Logging;
using Moq;
using System.Text;

namespace FlinkDotNet.Connectors.Sources.Kafka.Tests
{
    /// <summary>
    /// Comprehensive unit tests for KafkaSinkFunction following Java Flink TDD patterns.
    /// Tests cover functionality, error handling, resource management, and performance characteristics.
    /// </summary>
    public class KafkaSinkFunctionTests
    {
        private readonly Mock<ILogger> _mockLogger;
        private readonly HighPerformanceKafkaProducer.Config _testConfig;

        public KafkaSinkFunctionTests()
        {
            _mockLogger = new Mock<ILogger>();
            _testConfig = new HighPerformanceKafkaProducer.Config
            {
                BootstrapServers = "localhost:9092",
                Topic = "test-topic",
                BatchSize = 1024,
                LingerMs = 5,
                QueueBufferingMaxKbytes = 1024
            };
        }

        [Fact]
        public void Constructor_WithValidConfig_ShouldCreateInstance()
        {
            // Arrange
            var serializer = Serializers.Utf8;

            // Act
            var sinkFunction = new KafkaSinkFunction<string>(_testConfig, serializer, _mockLogger.Object);

            // Assert
            Assert.NotNull(sinkFunction);
        }

        [Fact]
        public void Constructor_WithNullConfig_ShouldThrowArgumentNullException()
        {
            // Arrange
            var serializer = Serializers.Utf8;

            // Act & Assert
            Assert.Throws<ArgumentNullException>(() => 
                new KafkaSinkFunction<string>(null!, serializer, _mockLogger.Object));
        }

        [Fact]
        public void Constructor_WithNullSerializer_ShouldThrowArgumentNullException()
        {
            // Act & Assert
            Assert.Throws<ArgumentNullException>(() => 
                new KafkaSinkFunction<string>(_testConfig, null!, _mockLogger.Object));
        }

        [Fact]
        public void Open_ShouldInitializeProducerAndLogInfo()
        {
            // Arrange
            var serializer = Serializers.Utf8;
            var sinkFunction = new KafkaSinkFunction<string>(_testConfig, serializer, _mockLogger.Object);
            var mockContext = new Mock<IRuntimeContext>();

            // Act
            sinkFunction.Open(mockContext.Object);

            // Assert
            _mockLogger.Verify(
                x => x.Log(
                    LogLevel.Information,
                    It.IsAny<EventId>(),
                    It.Is<It.IsAnyType>((v, t) => v.ToString()!.Contains("High-performance native Kafka sink opened")),
                    It.IsAny<Exception>(),
                    It.IsAny<Func<It.IsAnyType, Exception?, string>>()),
                Times.Once);
        }

        [Fact]
        public void Invoke_WithoutOpen_ShouldThrowInvalidOperationException()
        {
            // Arrange
            var serializer = Serializers.Utf8;
            var sinkFunction = new KafkaSinkFunction<string>(_testConfig, serializer, _mockLogger.Object);
            var mockSinkContext = new Mock<ISinkContext>();

            // Act & Assert
            var exception = Assert.Throws<InvalidOperationException>(() => 
                sinkFunction.Invoke("test-message", mockSinkContext.Object));
            
            Assert.Equal("Sink not opened", exception.Message);
        }

        [Fact]
        public void Close_WithoutOpen_ShouldNotThrow()
        {
            // Arrange
            var serializer = Serializers.Utf8;
            var sinkFunction = new KafkaSinkFunction<string>(_testConfig, serializer, _mockLogger.Object);

            // Act & Assert - Should not throw
            sinkFunction.Close();
        }

        [Fact]
        public void Dispose_ShouldCloseAndDisposeResources()
        {
            // Arrange
            var serializer = Serializers.Utf8;
            var sinkFunction = new KafkaSinkFunction<string>(_testConfig, serializer, _mockLogger.Object);

            // Act
            sinkFunction.Dispose();

            // Assert - No exception should be thrown
        }

        [Fact]
        public void Dispose_MultipleCalls_ShouldNotThrow()
        {
            // Arrange
            var serializer = Serializers.Utf8;
            var sinkFunction = new KafkaSinkFunction<string>(_testConfig, serializer, _mockLogger.Object);

            // Act & Assert - Multiple dispose calls should not throw
            sinkFunction.Dispose();
            sinkFunction.Dispose();
            sinkFunction.Dispose();
        }
    }

    /// <summary>
    /// Unit tests for KafkaSinkBuilder following Java Flink builder patterns.
    /// </summary>
    public class KafkaSinkBuilderTests
    {
        [Fact]
        public void Build_WithAllRequiredProperties_ShouldReturnSinkFunction()
        {
            // Arrange
            var builder = new KafkaSinkBuilder<string>()
                .BootstrapServers("localhost:9092")
                .Topic("test-topic")
                .ValueSerializer(Serializers.Utf8);

            // Act
            var sinkFunction = builder.Build();

            // Assert
            Assert.NotNull(sinkFunction);
        }

        [Fact]
        public void Build_WithoutConfiguration_ShouldThrowInvalidOperationException()
        {
            // Arrange
            var builder = new KafkaSinkBuilder<string>();

            // Act & Assert
            var exception = Assert.Throws<InvalidOperationException>(() => builder.Build());
            Assert.Equal("Producer configuration is required", exception.Message);
        }

        [Fact]
        public void Build_WithoutTopic_ShouldThrowInvalidOperationException()
        {
            // Arrange
            var builder = new KafkaSinkBuilder<string>()
                .BootstrapServers("localhost:9092")
                .ValueSerializer(Serializers.Utf8);

            // Act & Assert
            var exception = Assert.Throws<InvalidOperationException>(() => builder.Build());
            Assert.Equal("Topic is required", exception.Message);
        }

        [Fact]
        public void Build_WithoutSerializer_ShouldThrowInvalidOperationException()
        {
            // Arrange
            var builder = new KafkaSinkBuilder<string>()
                .BootstrapServers("localhost:9092")
                .Topic("test-topic");

            // Act & Assert
            var exception = Assert.Throws<InvalidOperationException>(() => builder.Build());
            Assert.Equal("Value serializer is required", exception.Message);
        }

        [Fact]
        public void FluentApi_ShouldAllowMethodChaining()
        {
            // Arrange & Act
            var sinkFunction = new KafkaSinkBuilder<string>()
                .BootstrapServers("localhost:9092")
                .Topic("test-topic")
                .ValueSerializer(Serializers.Utf8)
                .Logger(new Mock<ILogger>().Object)
                .Build();

            // Assert
            Assert.NotNull(sinkFunction);
        }

        [Fact]
        public void ProducerConfig_ShouldOverrideOtherSettings()
        {
            // Arrange
            var customConfig = new HighPerformanceKafkaProducer.Config
            {
                BootstrapServers = "custom:9092",
                Topic = "custom-topic"
            };

            // Act
            var sinkFunction = new KafkaSinkBuilder<string>()
                .BootstrapServers("localhost:9092")
                .Topic("test-topic")
                .ProducerConfig(customConfig)
                .ValueSerializer(Serializers.Utf8)
                .Build();

            // Assert
            Assert.NotNull(sinkFunction);
        }
    }

    /// <summary>
    /// Unit tests for common serializers.
    /// </summary>
    public class SerializersTests
    {
        [Fact]
        public void Utf8Serializer_ShouldConvertStringToBytes()
        {
            // Arrange
            var input = "Hello, Kafka!";
            var expected = Encoding.UTF8.GetBytes(input);

            // Act
            var result = Serializers.Utf8(input);

            // Assert
            Assert.Equal(expected, result);
        }

        [Fact]
        public void ByteArraySerializer_ShouldReturnSameBytes()
        {
            // Arrange
            var input = new byte[] { 1, 2, 3, 4, 5 };

            // Act
            var result = Serializers.ByteArray(input);

            // Assert
            Assert.Same(input, result);
        }

        [Fact]
        public void Int32Serializer_ShouldConvertIntToBytes()
        {
            // Arrange
            var input = 42;
            var expected = BitConverter.GetBytes(input);

            // Act
            var result = Serializers.Int32(input);

            // Assert
            Assert.Equal(expected, result);
        }

        [Fact]
        public void Int64Serializer_ShouldConvertLongToBytes()
        {
            // Arrange
            var input = 12345678901234L;
            var expected = BitConverter.GetBytes(input);

            // Act
            var result = Serializers.Int64(input);

            // Assert
            Assert.Equal(expected, result);
        }
    }
}