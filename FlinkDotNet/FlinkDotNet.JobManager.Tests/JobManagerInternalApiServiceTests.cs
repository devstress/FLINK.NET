using Moq;
using Microsoft.Extensions.Logging;
using Grpc.Core; // Required for ServerCallContext

#nullable enable
using FlinkDotNet.JobManager.Services; // The gRPC service implementation
using FlinkDotNet.Proto.Internal; // The generated gRPC C# classes

namespace FlinkDotNet.JobManager.Tests
{
    public class JobManagerInternalApiServiceTests
    {
        private readonly Mock<ILogger<JobManagerInternalApiService>> _mockLogger;
        private readonly JobManagerInternalApiService _service;

        public JobManagerInternalApiServiceTests()
        {
            _mockLogger = new Mock<ILogger<JobManagerInternalApiService>>();
            _service = new JobManagerInternalApiService(_mockLogger.Object);
        }

        // Helper to create a mock ServerCallContext
        private static ServerCallContext MockServerCallContext()
        {
            var mockContext = new Mock<ServerCallContext>();
            return mockContext.Object;
        }

        [Fact]
        public async Task ReportStateCompletion_ReturnsAckTrue()
        {
            // Arrange
            var request = new ReportStateCompletionRequest { CheckpointId = 1, OperatorInstanceId = "op1" };

            // Act
            var reply = await _service.ReportStateCompletion(request, MockServerCallContext());

            // Assert
            Assert.NotNull(reply);
            Assert.True(reply.Ack);
            // Verify logger was called (optional, but good for placeholder)
            _mockLogger.Verify(
                x => x.Log(
                    LogLevel.Information,
                    It.IsAny<EventId>(),
                    It.Is<It.IsAnyType>((v, t) => v.ToString()!.Contains($"ReportStateCompletion called for CheckpointId: {request.CheckpointId}")),
                    null,
                    It.IsAny<System.Func<It.IsAnyType, System.Exception?, string>>()),
                Times.Once);
        }

        [Fact]
        public async Task RequestCheckpoint_ReturnsAcceptedTrue()
        {
            // Arrange
            var request = new RequestCheckpointRequest { CheckpointId = 100 };

            // Act
            var reply = await _service.RequestCheckpoint(request, MockServerCallContext());

            // Assert
            Assert.NotNull(reply);
            Assert.True(reply.Accepted);
            _mockLogger.Verify(
                x => x.Log(
                    LogLevel.Information,
                    It.IsAny<EventId>(),
                    It.Is<It.IsAnyType>((v, t) => v.ToString()!.Contains($"RequestCheckpoint called for CheckpointId: {request.CheckpointId}")),
                    null,
                    It.IsAny<System.Func<It.IsAnyType, System.Exception?, string>>()),
                Times.Once);
        }

        [Fact]
        public async Task RequestRecovery_ReturnsRecoveryInitiatedTrue()
        {
            // Arrange
            var request = new RequestRecoveryRequest { JobId = "job1", CheckpointId = 200 };

            // Act
            var reply = await _service.RequestRecovery(request, MockServerCallContext());

            // Assert
            Assert.NotNull(reply);
            Assert.True(reply.RecoveryInitiated);
            _mockLogger.Verify(
                x => x.Log(
                    LogLevel.Information,
                    It.IsAny<EventId>(),
                    It.Is<It.IsAnyType>((v, t) => v.ToString()!.Contains($"RequestRecovery called for JobId: {request.JobId}")),
                    null,
                    It.IsAny<System.Func<It.IsAnyType, System.Exception?, string>>()),
                Times.Once);
        }

        [Fact]
        public async Task Heartbeat_ReturnsAckTrue()
        {
            // Arrange
            var request = new JobManagerHeartbeatRequest
            {
                JobId = "job2",
                OperatorInstanceId = "opHeartbeat",
                HealthStatus = "HEALTHY"
            };

            // Act
            var reply = await _service.Heartbeat(request, MockServerCallContext());

            // Assert
            Assert.NotNull(reply);
            Assert.True(reply.Ack);
            _mockLogger.Verify(
                x => x.Log(
                    LogLevel.Information,
                    It.IsAny<EventId>(),
                    It.Is<It.IsAnyType>((v, t) => v.ToString()!.Contains($"Heartbeat received from JobId: {request.JobId}")),
                    null,
                    It.IsAny<System.Func<It.IsAnyType, System.Exception?, string>>()),
                Times.Once);
        }
    }
}
