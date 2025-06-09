using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using Xunit;

#nullable enable
using Moq;
using Microsoft.AspNetCore.Mvc;
using FlinkDotNet.JobManager.Controllers;
using FlinkDotNet.JobManager.Interfaces;
using FlinkDotNet.JobManager.Models;

namespace FlinkDotNet.JobManager.Tests
{
    public class JobManagerControllerTests
    {
        private readonly Mock<IJobRepository> _mockRepo;
        private readonly JobManagerController _controller;

        public JobManagerControllerTests()
        {
            _mockRepo = new Mock<IJobRepository>();
            _controller = new JobManagerController(_mockRepo.Object);
        }

        // Tests for SubmitJob (6 tests)
        [Fact]
        public async Task SubmitJob_ValidJob_ReturnsOkWithJobStatus()
        {
            // Arrange
            var jobDefinition = new JobDefinitionDto { JobName = "Test Job" };
            _mockRepo.Setup(repo => repo.CreateJobAsync(It.IsAny<string>(), jobDefinition, It.IsAny<JobStatusDto>()))
                .ReturnsAsync(true);

            // Act
            var result = await _controller.SubmitJob(jobDefinition);

            // Assert
            var okResult = Assert.IsType<OkObjectResult>(result);
            var jobStatus = Assert.IsType<JobStatusDto>(okResult.Value);
            Assert.NotNull(jobStatus.JobId);
            Assert.False(string.IsNullOrWhiteSpace(jobStatus.JobId));
            Assert.Equal("SUBMITTED", jobStatus.Status);
        }

        [Fact]
        public async Task SubmitJob_NullJobDefinition_ReturnsBadRequest()
        {
            // Act
            var result = await _controller.SubmitJob(null!);

            // Assert
            var badRequestResult = Assert.IsType<BadRequestObjectResult>(result);
            Assert.Equal("Job definition or job name cannot be empty.", badRequestResult.Value);
        }

        [Theory]
        [InlineData(null)]
        [InlineData("")]
        [InlineData(" ")]
        public async Task SubmitJob_InvalidJobName_ReturnsBadRequest(string? jobName)
        {
            // Arrange
            var jobDefinition = new JobDefinitionDto { JobName = jobName! };

            // Act
            var result = await _controller.SubmitJob(jobDefinition);

            // Assert
            var badRequestResult = Assert.IsType<BadRequestObjectResult>(result);
            Assert.Equal("Job definition or job name cannot be empty.", badRequestResult.Value);
        }

        [Fact]
        public async Task SubmitJob_RepositoryCreateFails_ReturnsStatusCode500()
        {
            // Arrange
            var jobDefinition = new JobDefinitionDto { JobName = "Test Job That Fails Storage" };
            _mockRepo.Setup(repo => repo.CreateJobAsync(It.IsAny<string>(), jobDefinition, It.IsAny<JobStatusDto>()))
                .ReturnsAsync(false);

            // Act
            var result = await _controller.SubmitJob(jobDefinition);

            // Assert
            var statusCodeResult = Assert.IsType<ObjectResult>(result);
            Assert.Equal(500, statusCodeResult.StatusCode);
            Assert.Equal("Failed to create and store job.", statusCodeResult.Value);
        }

        // Tests for GetJobStatus (5 tests)
        [Fact]
        public async Task GetJobStatus_ExistingJobId_ReturnsOkWithJobStatus()
        {
            // Arrange
            var jobId = "existing-job-123";
            var expectedStatus = new JobStatusDto { JobId = jobId, Status = "RUNNING", LastUpdated = DateTime.UtcNow };
            _mockRepo.Setup(repo => repo.GetJobStatusAsync(jobId))
                .ReturnsAsync(expectedStatus);

            // Act
            var result = await _controller.GetJobStatus(jobId);

            // Assert
            var okResult = Assert.IsType<OkObjectResult>(result);
            var actualStatus = Assert.IsType<JobStatusDto>(okResult.Value);
            Assert.Equal(expectedStatus.JobId, actualStatus.JobId);
            Assert.Equal(expectedStatus.Status, actualStatus.Status);
        }

        [Fact]
        public async Task GetJobStatus_NonExistentJobId_ReturnsNotFound()
        {
            // Arrange
            var jobId = "non-existent-job-456";
            _mockRepo.Setup(repo => repo.GetJobStatusAsync(jobId))
                .ReturnsAsync((JobStatusDto?)null);

            // Act
            var result = await _controller.GetJobStatus(jobId);

            // Assert
            var notFoundResult = Assert.IsType<NotFoundObjectResult>(result);
            Assert.Equal($"Job with ID {jobId} not found.", notFoundResult.Value);
        }

        [Theory]
        [InlineData(null)]
        [InlineData("")]
        [InlineData(" ")]
        public async Task GetJobStatus_InvalidJobId_ReturnsBadRequest(string? jobId)
        {
            // Act
            var result = await _controller.GetJobStatus(jobId!);

            // Assert
            var badRequestResult = Assert.IsType<BadRequestObjectResult>(result);
            Assert.Equal("Job ID cannot be empty.", badRequestResult.Value);
        }

        // Tests for StopJob (6 tests)
        [Fact]
        public async Task StopJob_ExistingJobId_UpdatesStatusAndReturnsOk()
        {
            // Arrange
            var jobId = "existing-job-to-stop-123";
            var initialStatus = new JobStatusDto { JobId = jobId, Status = "RUNNING", LastUpdated = DateTime.UtcNow.AddMinutes(-5) };

            _mockRepo.Setup(repo => repo.GetJobStatusAsync(jobId))
                .ReturnsAsync(initialStatus);
            _mockRepo.Setup(repo => repo.UpdateJobStatusAsync(jobId, It.Is<JobStatusDto>(s => s.Status == "STOPPING")))
                .ReturnsAsync(true);

            // Act
            var result = await _controller.StopJob(jobId);

            // Assert
            var okResult = Assert.IsType<OkObjectResult>(result);
            var actualStatus = Assert.IsType<JobStatusDto>(okResult.Value);
            Assert.Equal(jobId, actualStatus.JobId);
            Assert.Equal("STOPPING", actualStatus.Status);
            _mockRepo.Verify(repo => repo.UpdateJobStatusAsync(jobId, actualStatus), Times.Once);
        }

        [Fact]
        public async Task StopJob_NonExistentJobId_ReturnsNotFound()
        {
            // Arrange
            var jobId = "non-existent-job-to-stop-456";
            _mockRepo.Setup(repo => repo.GetJobStatusAsync(jobId))
                .ReturnsAsync((JobStatusDto?)null);

            // Act
            var result = await _controller.StopJob(jobId);

            // Assert
            var notFoundResult = Assert.IsType<NotFoundObjectResult>(result);
            Assert.Equal($"Job with ID {jobId} not found.", notFoundResult.Value);
        }

        [Theory]
        [InlineData(null)]
        [InlineData("")]
        [InlineData(" ")]
        public async Task StopJob_InvalidJobId_ReturnsBadRequest(string? jobId)
        {
            // Act
            var result = await _controller.StopJob(jobId!);

            // Assert
            var badRequestResult = Assert.IsType<BadRequestObjectResult>(result);
            Assert.Equal("Job ID cannot be empty.", badRequestResult.Value);
        }

        [Fact]
        public async Task StopJob_RepositoryUpdateFails_ReturnsStatusCode500()
        {
            // Arrange
            var jobId = "existing-job-update-fails-789";
            var initialStatus = new JobStatusDto { JobId = jobId, Status = "RUNNING", LastUpdated = DateTime.UtcNow.AddMinutes(-5) };

            _mockRepo.Setup(repo => repo.GetJobStatusAsync(jobId))
                .ReturnsAsync(initialStatus);
            _mockRepo.Setup(repo => repo.UpdateJobStatusAsync(jobId, It.Is<JobStatusDto>(s => s.Status == "STOPPING")))
                .ReturnsAsync(false);

            // Act
            var result = await _controller.StopJob(jobId);

            // Assert
            var statusCodeResult = Assert.IsType<ObjectResult>(result);
            Assert.Equal(500, statusCodeResult.StatusCode);
            Assert.Equal($"Failed to update status for job {jobId} to STOPPING.", statusCodeResult.Value);
        }

        // Tests for CancelJob (6 tests)
        [Fact]
        public async Task CancelJob_ExistingJobId_UpdatesStatusAndReturnsOk()
        {
            // Arrange
            var jobId = "existing-job-to-cancel-123";
            var initialStatus = new JobStatusDto { JobId = jobId, Status = "RUNNING", LastUpdated = DateTime.UtcNow.AddMinutes(-10) };

            _mockRepo.Setup(repo => repo.GetJobStatusAsync(jobId))
                .ReturnsAsync(initialStatus);
            _mockRepo.Setup(repo => repo.UpdateJobStatusAsync(jobId, It.Is<JobStatusDto>(s => s.Status == "CANCELLING")))
                .ReturnsAsync(true);

            // Act
            var result = await _controller.CancelJob(jobId);

            // Assert
            var okResult = Assert.IsType<OkObjectResult>(result);
            var actualStatus = Assert.IsType<JobStatusDto>(okResult.Value);
            Assert.Equal(jobId, actualStatus.JobId);
            Assert.Equal("CANCELLING", actualStatus.Status);
            _mockRepo.Verify(repo => repo.UpdateJobStatusAsync(jobId, actualStatus), Times.Once);
        }

        [Fact]
        public async Task CancelJob_NonExistentJobId_ReturnsNotFound()
        {
            // Arrange
            var jobId = "non-existent-job-to-cancel-456";
            _mockRepo.Setup(repo => repo.GetJobStatusAsync(jobId))
                .ReturnsAsync((JobStatusDto?)null);

            // Act
            var result = await _controller.CancelJob(jobId);

            // Assert
            var notFoundResult = Assert.IsType<NotFoundObjectResult>(result);
            Assert.Equal($"Job with ID {jobId} not found.", notFoundResult.Value);
        }

        [Theory]
        [InlineData(null)]
        [InlineData("")]
        [InlineData(" ")]
        public async Task CancelJob_InvalidJobId_ReturnsBadRequest(string? jobId)
        {
            // Act
            var result = await _controller.CancelJob(jobId!);

            // Assert
            var badRequestResult = Assert.IsType<BadRequestObjectResult>(result);
            Assert.Equal("Job ID cannot be empty.", badRequestResult.Value);
        }

        [Fact]
        public async Task CancelJob_RepositoryUpdateFails_ReturnsStatusCode500()
        {
            // Arrange
            var jobId = "existing-job-cancel-update-fails-789";
            var initialStatus = new JobStatusDto { JobId = jobId, Status = "RUNNING", LastUpdated = DateTime.UtcNow.AddMinutes(-5) };

            _mockRepo.Setup(repo => repo.GetJobStatusAsync(jobId))
                .ReturnsAsync(initialStatus);
            _mockRepo.Setup(repo => repo.UpdateJobStatusAsync(jobId, It.Is<JobStatusDto>(s => s.Status == "CANCELLING")))
                .ReturnsAsync(false);

            // Act
            var result = await _controller.CancelJob(jobId);

            // Assert
            var statusCodeResult = Assert.IsType<ObjectResult>(result);
            Assert.Equal(500, statusCodeResult.StatusCode);
            Assert.Equal($"Failed to update status for job {jobId} to CANCELLING.", statusCodeResult.Value);
        }

        // Tests for ScaleJob (9 tests)
        [Fact]
        public async Task ScaleJob_ValidParameters_UpdatesStatusAndReturnsOk()
        {
            // Arrange
            var jobId = "existing-job-to-scale-123";
            var scaleParameters = new ScaleParametersDto { DesiredParallelism = 4 };
            var initialStatus = new JobStatusDto { JobId = jobId, Status = "RUNNING", LastUpdated = DateTime.UtcNow.AddMinutes(-10) };

            _mockRepo.Setup(repo => repo.GetJobStatusAsync(jobId))
                .ReturnsAsync(initialStatus);
            _mockRepo.Setup(repo => repo.UpdateJobStatusAsync(jobId, It.Is<JobStatusDto>(s => s.Status == "SCALING_REQUESTED")))
                .ReturnsAsync(true);

            // Act
            var result = await _controller.ScaleJob(jobId, scaleParameters);

            // Assert
            var okResult = Assert.IsType<OkObjectResult>(result);
            var actualStatus = Assert.IsType<JobStatusDto>(okResult.Value);
            Assert.Equal(jobId, actualStatus.JobId);
            Assert.Equal("SCALING_REQUESTED", actualStatus.Status);
            _mockRepo.Verify(repo => repo.UpdateJobStatusAsync(jobId, actualStatus), Times.Once);
        }

        [Fact]
        public async Task ScaleJob_NonExistentJobId_ReturnsNotFound()
        {
            // Arrange
            var jobId = "non-existent-job-to-scale-456";
            var scaleParameters = new ScaleParametersDto { DesiredParallelism = 2 };
            _mockRepo.Setup(repo => repo.GetJobStatusAsync(jobId))
                .ReturnsAsync((JobStatusDto?)null);

            // Act
            var result = await _controller.ScaleJob(jobId, scaleParameters);

            // Assert
            var notFoundResult = Assert.IsType<NotFoundObjectResult>(result);
            Assert.Equal($"Job with ID {jobId} not found.", notFoundResult.Value);
        }

        [Theory]
        [InlineData(null)]
        [InlineData("")]
        [InlineData(" ")]
        public async Task ScaleJob_InvalidJobId_ReturnsBadRequest(string? jobId)
        {
            // Arrange
            var scaleParameters = new ScaleParametersDto { DesiredParallelism = 3 };

            // Act
            var result = await _controller.ScaleJob(jobId!, scaleParameters);

            // Assert
            var badRequestResult = Assert.IsType<BadRequestObjectResult>(result);
            Assert.Equal("Job ID cannot be empty.", badRequestResult.Value);
        }

        [Fact]
        public async Task ScaleJob_NullScaleParameters_ReturnsBadRequest()
        {
            // Arrange
            var jobId = "job-scale-null-params-001";

            // Act
            var result = await _controller.ScaleJob(jobId, null!);

            // Assert
            var badRequestResult = Assert.IsType<BadRequestObjectResult>(result);
            Assert.Equal("Scale parameters must be provided and desired parallelism must be greater than 0.", badRequestResult.Value);
        }

        [Theory]
        [InlineData(0)]
        [InlineData(-1)]
        public async Task ScaleJob_InvalidDesiredParallelism_ReturnsBadRequest(int desiredParallelism)
        {
            // Arrange
            var jobId = "job-scale-invalid-parallelism-002";
            var scaleParameters = new ScaleParametersDto { DesiredParallelism = desiredParallelism };

            // Act
            var result = await _controller.ScaleJob(jobId, scaleParameters);

            // Assert
            var badRequestResult = Assert.IsType<BadRequestObjectResult>(result);
            Assert.Equal("Scale parameters must be provided and desired parallelism must be greater than 0.", badRequestResult.Value);
        }

        [Fact]
        public async Task ScaleJob_RepositoryUpdateFails_ReturnsStatusCode500()
        {
            // Arrange
            var jobId = "existing-job-scale-update-fails-789";
            var scaleParameters = new ScaleParametersDto { DesiredParallelism = 5 };
            var initialStatus = new JobStatusDto { JobId = jobId, Status = "RUNNING", LastUpdated = DateTime.UtcNow.AddMinutes(-5) };

            _mockRepo.Setup(repo => repo.GetJobStatusAsync(jobId))
                .ReturnsAsync(initialStatus);
            _mockRepo.Setup(repo => repo.UpdateJobStatusAsync(jobId, It.Is<JobStatusDto>(s => s.Status == "SCALING_REQUESTED")))
                .ReturnsAsync(false);

            // Act
            var result = await _controller.ScaleJob(jobId, scaleParameters);

            // Assert
            var statusCodeResult = Assert.IsType<ObjectResult>(result);
            Assert.Equal(500, statusCodeResult.StatusCode);
            Assert.Equal($"Failed to update status for job {jobId} to SCALING_REQUESTED.", statusCodeResult.Value);
        }

        // Tests for GetJobCheckpoints (6 tests)
        [Fact]
        public async Task GetJobCheckpoints_JobExistsWithCheckpoints_ReturnsOkWithCheckpoints()
        {
            // Arrange
            var jobId = "job-with-checkpoints-123";
            var expectedCheckpoints = new List<CheckpointInfoDto>
            {
                new CheckpointInfoDto { CheckpointId = "chk-1", Status = "COMPLETED", Timestamp = DateTime.UtcNow.AddMinutes(-5) },
                new CheckpointInfoDto { CheckpointId = "chk-2", Status = "COMPLETED", Timestamp = DateTime.UtcNow }
            };
            _mockRepo.Setup(repo => repo.GetCheckpointsAsync(jobId))
                .ReturnsAsync(expectedCheckpoints);

            // Act
            var result = await _controller.GetJobCheckpoints(jobId);

            // Assert
            var okResult = Assert.IsType<OkObjectResult>(result);
            var actualCheckpoints = Assert.IsAssignableFrom<IEnumerable<CheckpointInfoDto>>(okResult.Value);
            Assert.Equal(expectedCheckpoints.Count, actualCheckpoints.Count());
        }

        [Fact]
        public async Task GetJobCheckpoints_JobExistsWithoutCheckpoints_ReturnsOkWithEmptyList()
        {
            // Arrange
            var jobId = "job-without-checkpoints-456";
            _mockRepo.Setup(repo => repo.GetCheckpointsAsync(jobId))
                .ReturnsAsync(new List<CheckpointInfoDto>());

            // Act
            var result = await _controller.GetJobCheckpoints(jobId);

            // Assert
            var okResult = Assert.IsType<OkObjectResult>(result);
            var actualCheckpoints = Assert.IsAssignableFrom<IEnumerable<CheckpointInfoDto>>(okResult.Value);
            Assert.Empty(actualCheckpoints);
        }

        [Fact]
        public async Task GetJobCheckpoints_NonExistentJobId_ReturnsNotFound()
        {
            // Arrange
            var jobId = "non-existent-job-for-checkpoints-789";
            _mockRepo.Setup(repo => repo.GetCheckpointsAsync(jobId))
                .ReturnsAsync((IEnumerable<CheckpointInfoDto>?)null);

            // Act
            var result = await _controller.GetJobCheckpoints(jobId);

            // Assert
            var notFoundResult = Assert.IsType<NotFoundObjectResult>(result);
            Assert.Equal($"Job with ID {jobId} not found, or no checkpoint information available.", notFoundResult.Value);
        }

        [Theory]
        [InlineData(null)]
        [InlineData("")]
        [InlineData(" ")]
        public async Task GetJobCheckpoints_InvalidJobId_ReturnsBadRequest(string? jobId)
        {
            // Act
            var result = await _controller.GetJobCheckpoints(jobId!);

            // Assert
            var badRequestResult = Assert.IsType<BadRequestObjectResult>(result);
            Assert.Equal("Job ID cannot be empty.", badRequestResult.Value);
        }

        // Tests for RestartJob (6 tests)
        [Fact]
        public async Task RestartJob_ExistingJobId_UpdatesStatusAndReturnsOk()
        {
            // Arrange
            var jobId = "existing-job-to-restart-123";
            var initialStatus = new JobStatusDto {
                JobId = jobId,
                Status = "FAILED",
                ErrorMessage = "Something went wrong",
                LastUpdated = DateTime.UtcNow.AddMinutes(-10)
            };

            _mockRepo.Setup(repo => repo.GetJobStatusAsync(jobId))
                .ReturnsAsync(initialStatus);
            _mockRepo.Setup(repo => repo.UpdateJobStatusAsync(jobId, It.Is<JobStatusDto>(s => s.Status == "RESTARTING" && s.ErrorMessage == null)))
                .ReturnsAsync(true);

            // Act
            var result = await _controller.RestartJob(jobId);

            // Assert
            var okResult = Assert.IsType<OkObjectResult>(result);
            var actualStatus = Assert.IsType<JobStatusDto>(okResult.Value);
            Assert.Equal(jobId, actualStatus.JobId);
            Assert.Equal("RESTARTING", actualStatus.Status);
            Assert.Null(actualStatus.ErrorMessage);
            _mockRepo.Verify(repo => repo.UpdateJobStatusAsync(jobId, actualStatus), Times.Once);
        }

        [Fact]
        public async Task RestartJob_NonExistentJobId_ReturnsNotFound()
        {
            // Arrange
            var jobId = "non-existent-job-to-restart-456";
            _mockRepo.Setup(repo => repo.GetJobStatusAsync(jobId))
                .ReturnsAsync((JobStatusDto?)null);

            // Act
            var result = await _controller.RestartJob(jobId);

            // Assert
            var notFoundResult = Assert.IsType<NotFoundObjectResult>(result);
            Assert.Equal($"Job with ID {jobId} not found.", notFoundResult.Value);
        }

        [Theory]
        [InlineData(null)]
        [InlineData("")]
        [InlineData(" ")]
        public async Task RestartJob_InvalidJobId_ReturnsBadRequest(string? jobId)
        {
            // Act
            var result = await _controller.RestartJob(jobId!);

            // Assert
            var badRequestResult = Assert.IsType<BadRequestObjectResult>(result);
            Assert.Equal("Job ID cannot be empty.", badRequestResult.Value);
        }

        [Fact]
        public async Task RestartJob_RepositoryUpdateFails_ReturnsStatusCode500()
        {
            // Arrange
            var jobId = "existing-job-restart-update-fails-789";
            var initialStatus = new JobStatusDto { JobId = jobId, Status = "STOPPED", LastUpdated = DateTime.UtcNow.AddMinutes(-5) };

            _mockRepo.Setup(repo => repo.GetJobStatusAsync(jobId))
                .ReturnsAsync(initialStatus);
            _mockRepo.Setup(repo => repo.UpdateJobStatusAsync(jobId, It.Is<JobStatusDto>(s => s.Status == "RESTARTING")))
                .ReturnsAsync(false);

            // Act
            var result = await _controller.RestartJob(jobId);

            // Assert
            var statusCodeResult = Assert.IsType<ObjectResult>(result);
            Assert.Equal(500, statusCodeResult.StatusCode);
            Assert.Equal($"Failed to update status for job {jobId} to RESTARTING.", statusCodeResult.Value);
        }

        // Tests for ResubmitDlqMessages (5 tests)
        [Fact]
        public async Task ResubmitDlqMessages_ValidJobId_ReturnsAccepted()
        {
            // Arrange
            var jobId = "job-with-dlq-123";
            _mockRepo.Setup(repo => repo.RequestBatchDlqResubmissionAsync(jobId))
                .ReturnsAsync(true);

            // Act
            var result = await _controller.ResubmitDlqMessages(jobId);

            // Assert
            var acceptedResult = Assert.IsType<AcceptedResult>(result);
            Assert.Equal($"Request to resubmit DLQ messages for job {jobId} has been accepted.", acceptedResult.Location ?? acceptedResult.Value!.ToString());
        }

        [Fact]
        public async Task ResubmitDlqMessages_JobNotFoundOrRequestRejected_ReturnsNotFound()
        {
            // Arrange
            var jobId = "job-not-found-for-dlq-456";
            _mockRepo.Setup(repo => repo.RequestBatchDlqResubmissionAsync(jobId))
                .ReturnsAsync(false);

            // Act
            var result = await _controller.ResubmitDlqMessages(jobId);

            // Assert
            var notFoundResult = Assert.IsType<NotFoundObjectResult>(result);
            Assert.Equal($"Failed to request DLQ resubmission for job {jobId}, or job not found.", notFoundResult.Value);
        }

        [Theory]
        [InlineData(null)]
        [InlineData("")]
        [InlineData(" ")]
        public async Task ResubmitDlqMessages_InvalidJobId_ReturnsBadRequest(string? jobId)
        {
            // Act
            var result = await _controller.ResubmitDlqMessages(jobId!);

            // Assert
            var badRequestResult = Assert.IsType<BadRequestObjectResult>(result);
            Assert.Equal("Job ID cannot be empty.", badRequestResult.Value);
        }

        // New tests for ModifyDlqMessage (8 tests)
        [Fact]
        public async Task ModifyDlqMessage_ValidInput_UpdatesAndReturnsMessage()
        {
            // Arrange
            var jobId = "dlq-job-1";
            var messageId = "dlq-msg-1";
            var newPayload = new byte[] { 1, 2, 3 };
            var messageData = new DlqMessageDto { NewPayload = newPayload };

            var originalDlqMessage = new DlqMessageDto {
                JobId = jobId, MessageId = messageId, Payload = new byte[] { 0 }, LastUpdated = DateTime.UtcNow.AddMinutes(-1)
            };
            // This is what the GetDlqMessageAsync should return *after* UpdateDlqMessagePayloadAsync was successful
            var updatedDlqMessageFromRepo = new DlqMessageDto {
                JobId = jobId, MessageId = messageId, Payload = newPayload, LastUpdated = DateTime.UtcNow
            };

            _mockRepo.Setup(repo => repo.GetDlqMessageAsync(jobId, messageId))
                .ReturnsAsync(originalDlqMessage);
            _mockRepo.Setup(repo => repo.UpdateDlqMessagePayloadAsync(jobId, messageId, newPayload))
                .ReturnsAsync(true);
            // After update, the next GetDlqMessageAsync should return the updated message
            _mockRepo.SetupSequence(repo => repo.GetDlqMessageAsync(jobId, messageId))
                .ReturnsAsync(originalDlqMessage) // First call for existence check
                .ReturnsAsync(updatedDlqMessageFromRepo); // Second call to get the updated message

            // Act
            var result = await _controller.ModifyDlqMessage(jobId, messageId, messageData);

            // Assert
            var okResult = Assert.IsType<OkObjectResult>(result);
            var actualMessage = Assert.IsType<DlqMessageDto>(okResult.Value);
            Assert.Equal(newPayload, actualMessage.Payload);
            // We cannot reliably check LastUpdated against originalDlqMessage.LastUpdated
            // because the updatedDlqMessageFromRepo provides its own DateTime.UtcNow.
            // We can check that it's recent, or that it's the one from updatedDlqMessageFromRepo.
            Assert.Equal(updatedDlqMessageFromRepo.LastUpdated, actualMessage.LastUpdated);
            _mockRepo.Verify(repo => repo.UpdateDlqMessagePayloadAsync(jobId, messageId, newPayload), Times.Once);
        }

        [Fact]
        public async Task ModifyDlqMessage_MessageNotFound_ReturnsNotFound()
        {
            // Arrange
            var jobId = "dlq-job-2";
            var messageId = "dlq-msg-non-existent";
            var messageData = new DlqMessageDto { NewPayload = new byte[] { 1 } };
            _mockRepo.Setup(repo => repo.GetDlqMessageAsync(jobId, messageId))
                .ReturnsAsync((DlqMessageDto?)null);

            // Act
            var result = await _controller.ModifyDlqMessage(jobId, messageId, messageData);

            // Assert
            Assert.IsType<NotFoundObjectResult>(result);
        }

        [Theory]
        [InlineData(null, "msg1")]
        [InlineData("job1", null)]
        [InlineData(" ", "msg1")]
        [InlineData("job1", " ")]
        public async Task ModifyDlqMessage_InvalidJobOrMessageId_ReturnsBadRequest(string? jobId, string? messageId)
        {
            // Arrange
            var messageData = new DlqMessageDto { NewPayload = new byte[] { 1 } };

            // Act
            var result = await _controller.ModifyDlqMessage(jobId!, messageId!, messageData);

            // Assert
            var badRequestResult = Assert.IsType<BadRequestObjectResult>(result);
            Assert.Equal("Job ID and Message ID cannot be empty.", badRequestResult.Value);
        }

        [Fact]
        public async Task ModifyDlqMessage_NullMessageData_ReturnsBadRequest()
        {
            // Arrange
            var jobId = "dlq-job-3";
            var messageId = "dlq-msg-3";

            // Act
            var result = await _controller.ModifyDlqMessage(jobId, messageId, null!);

            // Assert
            var badRequestResult = Assert.IsType<BadRequestObjectResult>(result);
            Assert.Equal("Message data with new payload must be provided.", badRequestResult.Value);
        }

        [Fact]
        public async Task ModifyDlqMessage_NullNewPayload_ReturnsBadRequest()
        {
            // Arrange
            var jobId = "dlq-job-4";
            var messageId = "dlq-msg-4";
            var messageData = new DlqMessageDto { NewPayload = null };

            // Act
            var result = await _controller.ModifyDlqMessage(jobId, messageId, messageData);

            // Assert
            var badRequestResult = Assert.IsType<BadRequestObjectResult>(result);
            Assert.Equal("Message data with new payload must be provided.", badRequestResult.Value);
        }

        [Fact]
        public async Task ModifyDlqMessage_RepositoryUpdateFails_ReturnsStatusCode500()
        {
            // Arrange
            var jobId = "dlq-job-5";
            var messageId = "dlq-msg-5";
            var newPayload = new byte[] { 1, 2, 3 };
            var messageData = new DlqMessageDto { NewPayload = newPayload };
            var originalDlqMessage = new DlqMessageDto { JobId = jobId, MessageId = messageId, Payload = new byte[] {0} };

            _mockRepo.Setup(repo => repo.GetDlqMessageAsync(jobId, messageId))
                .ReturnsAsync(originalDlqMessage);
            _mockRepo.Setup(repo => repo.UpdateDlqMessagePayloadAsync(jobId, messageId, newPayload))
                .ReturnsAsync(false);

            // Act
            var result = await _controller.ModifyDlqMessage(jobId, messageId, messageData);

            // Assert
            var statusCodeResult = Assert.IsType<ObjectResult>(result);
            Assert.Equal(500, statusCodeResult.StatusCode);
            Assert.Equal($"Failed to update DLQ message {messageId} for job {jobId}.", statusCodeResult.Value);
        }
    }
}
