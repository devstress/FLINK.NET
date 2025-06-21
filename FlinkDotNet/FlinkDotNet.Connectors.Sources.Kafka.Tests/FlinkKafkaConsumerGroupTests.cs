using FlinkDotNet.Connectors.Sources.Kafka;
using Microsoft.Extensions.Logging;
using Moq;

namespace FlinkDotNet.Connectors.Sources.Kafka.Tests
{
    /// <summary>
    /// Comprehensive unit tests for FlinkKafkaConsumerGroup following Java Flink TDD patterns.
    /// Tests cover initialization, subscription, checkpoint management, and resource disposal.
    /// </summary>
    public class FlinkKafkaConsumerGroupTests
    {
        private readonly Mock<ILogger> _mockLogger;

        public FlinkKafkaConsumerGroupTests()
        {
            _mockLogger = new Mock<ILogger>();
        }

        [Fact]
        public void Constructor_WithBootstrapServersAndGroupId_ShouldCreateInstance()
        {
            // Arrange & Act
            var consumerGroup = new FlinkKafkaConsumerGroup("localhost:9092", "test-group", _mockLogger.Object);

            // Assert
            Assert.NotNull(consumerGroup);
            Assert.Equal("test-group", consumerGroup.GetConsumerGroupId());
        }

        [Fact]
        public void Constructor_WithConsumerConfigObject_ShouldCreateInstance()
        {
            // Arrange
            var mockConfig = new { BootstrapServers = "localhost:9092", GroupId = "test-group" };

            // Act
            var consumerGroup = new FlinkKafkaConsumerGroup(mockConfig, _mockLogger.Object);

            // Assert
            Assert.NotNull(consumerGroup);
            Assert.Equal("test-group", consumerGroup.GetConsumerGroupId());
        }

        [Fact]
        public void Constructor_WithConfigObjectWithoutGroupId_ShouldUseDefaultGroup()
        {
            // Arrange
            var mockConfig = new { BootstrapServers = "localhost:9092" };

            // Act
            var consumerGroup = new FlinkKafkaConsumerGroup(mockConfig, _mockLogger.Object);

            // Assert
            Assert.Equal("default-group", consumerGroup.GetConsumerGroupId());
        }

        [Fact]
        public void Subscribe_WithTopics_ShouldCreateAssignment()
        {
            // Arrange
            var consumerGroup = new FlinkKafkaConsumerGroup("localhost:9092", "test-group", _mockLogger.Object);
            var topics = new[] { "topic1", "topic2" };

            // Act
            consumerGroup.Subscribe(topics);

            // Assert
            var assignment = consumerGroup.GetAssignment();
            Assert.Equal(2, assignment.Count);
            Assert.Contains(assignment, tp => tp.Topic == "topic1");
            Assert.Contains(assignment, tp => tp.Topic == "topic2");
        }

        [Fact]
        public async Task InitializeAsync_WithTopics_ShouldCompleteSuccessfully()
        {
            // Arrange
            var consumerGroup = new FlinkKafkaConsumerGroup("localhost:9092", "test-group", _mockLogger.Object);
            var topics = new[] { "test-topic" };

            // Act
            await consumerGroup.InitializeAsync(topics);

            // Assert
            var assignment = consumerGroup.GetAssignment();
            Assert.Single(assignment);
            Assert.Equal("test-topic", assignment[0].Topic);

            _mockLogger.Verify(
                x => x.Log(
                    LogLevel.Information,
                    It.IsAny<EventId>(),
                    It.Is<It.IsAnyType>((v, t) => v.ToString()!.Contains("FlinkKafkaConsumerGroup initialized")),
                    It.IsAny<Exception>(),
                    It.IsAny<Func<It.IsAnyType, Exception?, string>>()),
                Times.Once);
        }

        [Fact]
        public void ConsumeMessage_ShouldReturnNull()
        {
            // Arrange
            var consumerGroup = new FlinkKafkaConsumerGroup("localhost:9092", "test-group", _mockLogger.Object);

            // Act
            var result = consumerGroup.ConsumeMessage(TimeSpan.FromMilliseconds(100));

            // Assert
            Assert.Null(result);
        }

        [Fact]
        public void GetConsecutiveFailureCount_InitialValue_ShouldBeZero()
        {
            // Arrange
            var consumerGroup = new FlinkKafkaConsumerGroup("localhost:9092", "test-group", _mockLogger.Object);

            // Act
            var result = consumerGroup.GetConsecutiveFailureCount();

            // Assert
            Assert.Equal(0, result);
        }

        [Fact]
        public void IsInRecoveryMode_InitialValue_ShouldBeFalse()
        {
            // Arrange
            var consumerGroup = new FlinkKafkaConsumerGroup("localhost:9092", "test-group", _mockLogger.Object);

            // Act
            var result = consumerGroup.IsInRecoveryMode();

            // Assert
            Assert.False(result);
        }

        [Fact]
        public void GetCheckpointState_InitialState_ShouldBeEmpty()
        {
            // Arrange
            var consumerGroup = new FlinkKafkaConsumerGroup("localhost:9092", "test-group", _mockLogger.Object);

            // Act
            var result = consumerGroup.GetCheckpointState();

            // Assert
            Assert.Empty(result);
        }

        [Fact]
        public void Subscribe_ShouldInitializeCheckpointState()
        {
            // Arrange
            var consumerGroup = new FlinkKafkaConsumerGroup("localhost:9092", "test-group", _mockLogger.Object);
            var topics = new[] { "test-topic" };

            // Act
            consumerGroup.Subscribe(topics);

            // Assert
            var checkpointState = consumerGroup.GetCheckpointState();
            Assert.Single(checkpointState);
            Assert.Equal(0, checkpointState.First().Value); // Initial offset should be 0
        }

        [Fact]
        public async Task CommitCheckpointOffsetsAsync_ShouldCompleteSuccessfully()
        {
            // Arrange
            var consumerGroup = new FlinkKafkaConsumerGroup("localhost:9092", "test-group", _mockLogger.Object);
            var checkpointId = 123L;

            // Act & Assert - Should not throw
            await consumerGroup.CommitCheckpointOffsetsAsync(checkpointId);

            _mockLogger.Verify(
                x => x.Log(
                    LogLevel.Debug,
                    It.IsAny<EventId>(),
                    It.Is<It.IsAnyType>((v, t) => v.ToString()!.Contains("Committing checkpoint offsets")),
                    It.IsAny<Exception>(),
                    It.IsAny<Func<It.IsAnyType, Exception?, string>>()),
                Times.Once);
        }

        [Fact]
        public void Dispose_ShouldNotThrow()
        {
            // Arrange
            var consumerGroup = new FlinkKafkaConsumerGroup("localhost:9092", "test-group", _mockLogger.Object);

            // Act & Assert - Should not throw
            consumerGroup.Dispose();

            _mockLogger.Verify(
                x => x.Log(
                    LogLevel.Debug,
                    It.IsAny<EventId>(),
                    It.Is<It.IsAnyType>((v, t) => v.ToString()!.Contains("Disposing native Kafka consumer group")),
                    It.IsAny<Exception>(),
                    It.IsAny<Func<It.IsAnyType, Exception?, string>>()),
                Times.Once);
        }

        [Fact]
        public void Dispose_MultipleCalls_ShouldNotThrow()
        {
            // Arrange
            var consumerGroup = new FlinkKafkaConsumerGroup("localhost:9092", "test-group", _mockLogger.Object);

            // Act & Assert - Multiple dispose calls should not throw
            consumerGroup.Dispose();
            consumerGroup.Dispose();
            consumerGroup.Dispose();
        }
    }

    /// <summary>
    /// Unit tests for FlinkTopicPartition class.
    /// </summary>
    public class FlinkTopicPartitionTests
    {
        [Fact]
        public void Constructor_WithTopicAndPartition_ShouldSetProperties()
        {
            // Arrange
            var topic = "test-topic";
            var partition = 42;

            // Act
            var topicPartition = new FlinkTopicPartition(topic, partition);

            // Assert
            Assert.Equal(topic, topicPartition.Topic);
            Assert.Equal(partition, topicPartition.Partition);
        }

        [Fact]
        public void ToString_ShouldReturnFormattedString()
        {
            // Arrange
            var topicPartition = new FlinkTopicPartition("test-topic", 42);

            // Act
            var result = topicPartition.ToString();

            // Assert
            Assert.Equal("test-topic:42", result);
        }

        [Fact]
        public void Equality_WithSameTopicAndPartition_ShouldBeEqual()
        {
            // Arrange
            var tp1 = new FlinkTopicPartition("test-topic", 42);
            var tp2 = new FlinkTopicPartition("test-topic", 42);

            // Act & Assert
            Assert.Equal(tp1.Topic, tp2.Topic);
            Assert.Equal(tp1.Partition, tp2.Partition);
        }

        [Fact]
        public void Equality_WithDifferentTopic_ShouldNotBeEqual()
        {
            // Arrange
            var tp1 = new FlinkTopicPartition("topic1", 42);
            var tp2 = new FlinkTopicPartition("topic2", 42);

            // Act & Assert
            Assert.NotEqual(tp1.Topic, tp2.Topic);
        }

        [Fact]
        public void Equality_WithDifferentPartition_ShouldNotBeEqual()
        {
            // Arrange
            var tp1 = new FlinkTopicPartition("test-topic", 1);
            var tp2 = new FlinkTopicPartition("test-topic", 2);

            // Act & Assert
            Assert.NotEqual(tp1.Partition, tp2.Partition);
        }

        [Theory]
        [InlineData("", 0)]
        [InlineData("single-char-topic", 1)]
        [InlineData("very-long-topic-name-with-many-characters", 999)]
        public void Constructor_WithVariousInputs_ShouldWork(string topic, int partition)
        {
            // Act
            var topicPartition = new FlinkTopicPartition(topic, partition);

            // Assert
            Assert.Equal(topic, topicPartition.Topic);
            Assert.Equal(partition, topicPartition.Partition);
        }
    }
}