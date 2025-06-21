using FlinkDotNet.Connectors.Sources.Kafka;
using FlinkDotNet.Connectors.Sources.Kafka.Native;
using FlinkDotNet.Core.Abstractions.Sources;
using Microsoft.Extensions.Logging;
using Moq;

namespace FlinkDotNet.Connectors.Sources.Kafka.Tests
{
    /// <summary>
    /// Comprehensive unit tests for KafkaSourceFunction following Java Flink TDD patterns.
    /// Tests cover basic functionality, cancellation, and resource management.
    /// </summary>
    public class KafkaSourceFunctionTests
    {
        private readonly Mock<ILogger> _mockLogger;
        private readonly HighPerformanceKafkaProducer.Config _testConfig;

        public KafkaSourceFunctionTests()
        {
            _mockLogger = new Mock<ILogger>();
            _testConfig = new HighPerformanceKafkaProducer.Config
            {
                BootstrapServers = "localhost:9092",
                Topic = "test-topic"
            };
        }

        [Fact]
        public void Constructor_WithValidConfig_ShouldCreateInstance()
        {
            // Arrange
            var deserializer = KafkaDeserializers.Utf8;

            // Act
            var sourceFunction = new KafkaSourceFunction<string>(_testConfig, deserializer, _mockLogger.Object);

            // Assert
            Assert.NotNull(sourceFunction);
        }

        [Fact]
        public void Constructor_WithNullConfig_ShouldThrowArgumentNullException()
        {
            // Arrange
            var deserializer = KafkaDeserializers.Utf8;

            // Act & Assert
            Assert.Throws<ArgumentNullException>(() => 
                new KafkaSourceFunction<string>(null!, deserializer, _mockLogger.Object));
        }

        [Fact]
        public void Constructor_WithNullDeserializer_ShouldThrowArgumentNullException()
        {
            // Act & Assert
            Assert.Throws<ArgumentNullException>(() => 
                new KafkaSourceFunction<string>(_testConfig, null!, _mockLogger.Object));
        }

        [Fact]
        public void IsBounded_WithUnboundedSource_ShouldReturnFalse()
        {
            // Arrange
            var sourceFunction = new KafkaSourceFunction<string>(_testConfig, KafkaDeserializers.Utf8, _mockLogger.Object, bounded: false);

            // Act
            var result = sourceFunction.IsBounded;

            // Assert
            Assert.False(result);
        }

        [Fact]
        public void IsBounded_WithBoundedSource_ShouldReturnTrue()
        {
            // Arrange
            var sourceFunction = new KafkaSourceFunction<string>(_testConfig, KafkaDeserializers.Utf8, _mockLogger.Object, bounded: true);

            // Act
            var result = sourceFunction.IsBounded;

            // Assert
            Assert.True(result);
        }

        [Fact]
        public void Cancel_ShouldStopExecution()
        {
            // Arrange
            var sourceFunction = new KafkaSourceFunction<string>(_testConfig, KafkaDeserializers.Utf8, _mockLogger.Object);

            // Act
            sourceFunction.Cancel();

            // Assert - Cancellation should be logged
            _mockLogger.Verify(
                x => x.Log(
                    LogLevel.Debug,
                    It.IsAny<EventId>(),
                    It.Is<It.IsAnyType>((v, t) => v.ToString()!.Contains("cancellation requested")),
                    It.IsAny<Exception>(),
                    It.IsAny<Func<It.IsAnyType, Exception?, string>>()),
                Times.Once);
        }
    }

    /// <summary>
    /// Unit tests for KafkaSourceBuilder following Java Flink builder patterns.
    /// </summary>
    public class KafkaSourceBuilderTests
    {
        [Fact]
        public void Build_WithAllRequiredProperties_ShouldReturnSourceFunction()
        {
            // Arrange
            var builder = new KafkaSourceBuilder<string>()
                .BootstrapServers("localhost:9092")
                .Topic("test-topic")
                .ValueDeserializer(KafkaDeserializers.Utf8);

            // Act
            var sourceFunction = builder.Build();

            // Assert
            Assert.NotNull(sourceFunction);
        }

        [Fact]
        public void Build_WithoutConfiguration_ShouldThrowInvalidOperationException()
        {
            // Arrange
            var builder = new KafkaSourceBuilder<string>();

            // Act & Assert
            var exception = Assert.Throws<InvalidOperationException>(() => builder.Build());
            Assert.Equal("Configuration is required", exception.Message);
        }

        [Fact]
        public void Build_WithoutTopic_ShouldThrowInvalidOperationException()
        {
            // Arrange
            var builder = new KafkaSourceBuilder<string>()
                .BootstrapServers("localhost:9092")
                .ValueDeserializer(KafkaDeserializers.Utf8);

            // Act & Assert
            var exception = Assert.Throws<InvalidOperationException>(() => builder.Build());
            Assert.Equal("Topic is required", exception.Message);
        }

        [Fact]
        public void Build_WithoutDeserializer_ShouldThrowInvalidOperationException()
        {
            // Arrange
            var builder = new KafkaSourceBuilder<string>()
                .BootstrapServers("localhost:9092")
                .Topic("test-topic");

            // Act & Assert
            var exception = Assert.Throws<InvalidOperationException>(() => builder.Build());
            Assert.Equal("Value deserializer is required", exception.Message);
        }

        [Fact]
        public void FluentApi_ShouldAllowMethodChaining()
        {
            // Arrange & Act
            var sourceFunction = new KafkaSourceBuilder<string>()
                .BootstrapServers("localhost:9092")
                .Topic("test-topic")
                .GroupId("test-group")
                .ValueDeserializer(KafkaDeserializers.Utf8)
                .Logger(new Mock<ILogger>().Object)
                .Bounded(true)
                .Build();

            // Assert
            Assert.NotNull(sourceFunction);
            Assert.True(sourceFunction.IsBounded);
        }
    }

    /// <summary>
    /// Unit tests for Kafka deserializers.
    /// </summary>
    public class KafkaDeserializersTests
    {
        [Fact]
        public void Utf8Deserializer_ShouldConvertBytesToString()
        {
            // Arrange
            var input = "Hello, Kafka!"u8.ToArray();
            var expected = "Hello, Kafka!";

            // Act
            var result = KafkaDeserializers.Utf8(input);

            // Assert
            Assert.Equal(expected, result);
        }

        [Fact]
        public void ByteArrayDeserializer_ShouldReturnSameBytes()
        {
            // Arrange
            var input = new byte[] { 1, 2, 3, 4, 5 };

            // Act
            var result = KafkaDeserializers.ByteArray(input);

            // Assert
            Assert.Same(input, result);
        }

        [Fact]
        public void Int32Deserializer_ShouldConvertBytesToInt()
        {
            // Arrange
            var expected = 42;
            var input = BitConverter.GetBytes(expected);

            // Act
            var result = KafkaDeserializers.Int32(input);

            // Assert
            Assert.Equal(expected, result);
        }

        [Fact]
        public void Int64Deserializer_ShouldConvertBytesToLong()
        {
            // Arrange
            var expected = 12345678901234L;
            var input = BitConverter.GetBytes(expected);

            // Act
            var result = KafkaDeserializers.Int64(input);

            // Assert
            Assert.Equal(expected, result);
        }

        [Fact]
        public void Utf8Deserializer_WithEmptyBytes_ShouldReturnEmptyString()
        {
            // Arrange
            var input = Array.Empty<byte>();

            // Act
            var result = KafkaDeserializers.Utf8(input);

            // Assert
            Assert.Equal(string.Empty, result);
        }
    }
}