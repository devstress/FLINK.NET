using System;
using System.Threading.Tasks;
using Microsoft.AspNetCore.Mvc;
using FlinkDotNet.JobManager.Interfaces;
using FlinkDotNet.JobManager.Models;
using System.Linq;
using System.Collections.Concurrent; // Added
using FlinkDotNet.JobManager.Models.JobGraph; // Added
using FlinkDotNet.JobManager.Services; // For TaskManagerRegistrationServiceImpl
using FlinkDotNet.JobManager.Checkpointing; // Corrected namespace For CheckpointCoordinator
using Grpc.Net.Client; // Added for GrpcChannel
using System.Text.Json; // Added for JsonSerializer
using Proto = FlinkDotNet.Proto.Internal; // Alias for generated gRPC types


namespace FlinkDotNet.JobManager.Controllers
{
    [ApiController]
    [Route("api/jobmanager")] // Changed route
    public class JobManagerController : ControllerBase // Removed IJobManagerApi for now as methods change
    {
        // Temporary storage for JobGraphs - made public static for TaskManagerRegistrationService access (TEMPORARY DESIGN)
        internal static readonly ConcurrentDictionary<Guid, JobGraph> JobGraphs = new();
        private readonly IJobRepository _jobRepository;
        private readonly ILogger<JobManagerController> _logger;

        private const string JobIdEmptyMessage = "Job ID cannot be empty.";

        public JobManagerController(IJobRepository jobRepository, ILogger<JobManagerController> logger)
        {
            _jobRepository = jobRepository;
            _logger = logger;
        }

        // Convenience constructor used in unit tests
        public JobManagerController(IJobRepository jobRepository)
            : this(jobRepository,
                   Microsoft.Extensions.Logging.Abstractions.NullLogger<JobManagerController>.Instance)
        {
        }

        [HttpGet("taskmanagers")]
        public IActionResult GetTaskManagers()
        {
            var taskManagers = TaskManagerTracker.RegisteredTaskManagers.Values.ToList();
            return Ok(taskManagers);
        }

        [HttpGet("jobs")]
        public IActionResult GetJobs()
        {
            var jobOverviews = JobGraphs.Values.Select(jg => new JobOverviewDto
            {
                JobId = jg.JobId.ToString(),
                JobName = jg.JobName,
                SubmissionTime = jg.SubmissionTime,
                Status = jg.Status,
                Duration = DateTime.UtcNow - jg.SubmissionTime // Calculate duration as if currently running
            }).ToList();

            return Ok(jobOverviews);
        }

        [HttpGet("jobs/{jobId}")]
        public IActionResult GetJobDetails(string jobId)
        {
            if (!Guid.TryParse(jobId, out var parsedGuid))
            {
                return BadRequest("Invalid Job ID format. Job ID must be a valid GUID.");
            }

            if (JobGraphs.TryGetValue(parsedGuid, out var jobGraph))
            {
                return Ok(jobGraph);
            }
            else
            {
                return NotFound($"Job with ID {jobId} not found.");
            }
        }

        [HttpGet("jobs/{jobId}/metrics")]
        public IActionResult GetJobMetrics(string jobId)
        {
            // Placeholder: Basic check for Job ID format and existence (optional for a placeholder)
            if (!Guid.TryParse(jobId, out var parsedGuid))
            {
                return BadRequest("Invalid Job ID format. Job ID must be a valid GUID.");
            }

            if (!JobGraphs.ContainsKey(parsedGuid))
            {
                return NotFound($"Job with ID {jobId} not found. Cannot retrieve metrics.");
            }

            // Actual implementation:
            if (!JobGraphs.TryGetValue(parsedGuid, out var jobGraph))
            {
                return NotFound($"Job with ID {jobId} not found.");
            }

            var vertexMetricsList = new List<VertexMetricsDto>();
            foreach (var vertex in jobGraph.Vertices)
            {
                long totalRecordsIn = 0;
                long totalRecordsOut = 0;

                // Aggregate from TaskInstanceMetrics
                // The key in TaskInstanceMetrics is "JobVertexId_SubtaskIndex"
                foreach (var subtaskIndex in Enumerable.Range(0, vertex.Parallelism))
                {
                    string taskInstanceId = $"{vertex.Id}_{subtaskIndex}";
                    if (jobGraph.TaskInstanceMetrics.TryGetValue(taskInstanceId, out var taskMetrics))
                    {
                        totalRecordsIn += taskMetrics.RecordsIn;
                        totalRecordsOut += taskMetrics.RecordsOut;
                    }
                }

                // Update JobVertex aggregate properties (optional, could also just calculate for DTO)
                // For simplicity, let's assume the JobVertex properties are the single source of truth for aggregated values
                // and they would be updated by a background process or directly by heartbeat handler if not using TaskInstanceMetrics for aggregation here.
                // For this subtask, we'll populate VertexMetricsDto directly from aggregation of TaskInstanceMetrics.
                // The JobVertex.AggregatedRecordsIn/Out properties would ideally be updated by the heartbeat processing logic itself.
                // Let's assume for now the heartbeat logic is responsible for updating JobVertex.AggregatedRecordsIn/Out directly
                // or we read from there. Given the current setup, it's easier to aggregate here for the DTO.

                vertexMetricsList.Add(new VertexMetricsDto
                {
                    VertexId = vertex.Id.ToString(),
                    VertexName = vertex.Name,
                    RecordsIn = totalRecordsIn,  // Use aggregated values
                    RecordsOut = totalRecordsOut // Use aggregated values
                });
            }

            _logger.LogDebug("Returning {MetricCount} VertexMetricsDto for Job ID {JobId}", vertexMetricsList.Count, jobId);
            return Ok(vertexMetricsList);
        }

        // ... (existing methods like GetStatus, GetJobs etc. might need adjustment or removal if IJobManagerApi is removed/changed)

        [HttpPost("submit")]
        public async Task<IActionResult> SubmitJob([FromBody] JobDefinitionDto jobDefinition)
        {
            if (jobDefinition == null || string.IsNullOrWhiteSpace(jobDefinition.JobName))
            {
                return BadRequest("Job definition or job name cannot be empty.");
            }

            var jobId = Guid.NewGuid().ToString();
            var status = new JobStatusDto
            {
                JobId = jobId,
                Status = "SUBMITTED",
                LastUpdated = DateTime.UtcNow
            };

            bool created = await _jobRepository.CreateJobAsync(jobId, jobDefinition, status);
            if (!created)
            {
                return StatusCode(500, "Failed to create and store job.");
            }

            return Ok(status);
        }


        // GET /jobs/{jobId} // This and other methods below might need to be adapted or removed if IJobManagerApi is no longer implemented or if their logic is incompatible
        [HttpGet("/jobs/{jobId}")]
        public async Task<IActionResult> GetJobStatus(string jobId)
        {
            if (string.IsNullOrWhiteSpace(jobId))
            {
                return BadRequest(JobIdEmptyMessage);
            }

            var jobStatus = await _jobRepository.GetJobStatusAsync(jobId);

            if (jobStatus == null)
            {
                return NotFound($"Job with ID {jobId} not found.");
            }
            return Ok(jobStatus);
        }

        // PUT /jobs/{jobId}/scale
        [HttpPut("/jobs/{jobId}/scale")]
        public async Task<IActionResult> ScaleJob(string jobId, [FromBody] ScaleParametersDto scaleParameters)
        {
            if (string.IsNullOrWhiteSpace(jobId))
            {
                return BadRequest(JobIdEmptyMessage);
            }

            if (scaleParameters == null || scaleParameters.DesiredParallelism <= 0)
            {
                return BadRequest("Scale parameters must be provided and desired parallelism must be greater than 0.");
            }

            var jobStatus = await _jobRepository.GetJobStatusAsync(jobId);
            if (jobStatus == null)
            {
                return NotFound($"Job with ID {jobId} not found.");
            }

            jobStatus.Status = "SCALING_REQUESTED";
            jobStatus.LastUpdated = DateTime.UtcNow;

            bool updated = await _jobRepository.UpdateJobStatusAsync(jobId, jobStatus);
            if (!updated)
            {
                return StatusCode(500, $"Failed to update status for job {jobId} to SCALING_REQUESTED.");
            }
            return Ok(jobStatus);
        }

        // POST /jobs/{jobId}/stop
        [HttpPost("/jobs/{jobId}/stop")]
        public async Task<IActionResult> StopJob(string jobId)
        {
            if (string.IsNullOrWhiteSpace(jobId))
            {
                return BadRequest(JobIdEmptyMessage);
            }

            var jobStatus = await _jobRepository.GetJobStatusAsync(jobId);
            if (jobStatus == null)
            {
                return NotFound($"Job with ID {jobId} not found.");
            }

            jobStatus.Status = "STOPPING";
            jobStatus.LastUpdated = DateTime.UtcNow;

            bool updated = await _jobRepository.UpdateJobStatusAsync(jobId, jobStatus);
            if (!updated)
            {
                return StatusCode(500, $"Failed to update status for job {jobId} to STOPPING.");
            }
            return Ok(jobStatus);
        }

        // POST /jobs/{jobId}/cancel
        [HttpPost("/jobs/{jobId}/cancel")]
        public async Task<IActionResult> CancelJob(string jobId)
        {
            if (string.IsNullOrWhiteSpace(jobId))
            {
                return BadRequest(JobIdEmptyMessage);
            }

            var jobStatus = await _jobRepository.GetJobStatusAsync(jobId);
            if (jobStatus == null)
            {
                return NotFound($"Job with ID {jobId} not found.");
            }

            jobStatus.Status = "CANCELLING";
            jobStatus.LastUpdated = DateTime.UtcNow;

            bool updated = await _jobRepository.UpdateJobStatusAsync(jobId, jobStatus);
            if (!updated)
            {
                return StatusCode(500, $"Failed to update status for job {jobId} to CANCELLING.");
            }
            return Ok(jobStatus);
        }

        // GET /jobs/{jobId}/checkpoints
        [HttpGet("jobs/{jobId}/checkpoints")]
        public async Task<IActionResult> GetJobCheckpoints(string jobId)
        {
            if (string.IsNullOrWhiteSpace(jobId))
            {
                return BadRequest(JobIdEmptyMessage);
            }

            var checkpoints = await _jobRepository.GetCheckpointsAsync(jobId);
            if (checkpoints == null)
            {
                return NotFound($"Job with ID {jobId} not found, or no checkpoint information available.");
            }

            return Ok(checkpoints);
        }

        [HttpGet("jobs/{jobId}/logs")]
        public IActionResult GetJobLogs(string jobId)
        {
            if (!Guid.TryParse(jobId, out var parsedGuid))
            {
                _logger.LogWarning("GetJobLogs called with invalid Job ID format: {JobId}", jobId);
                return BadRequest("Invalid Job ID format. Job ID must be a valid GUID.");
            }

            if (!JobGraphs.ContainsKey(parsedGuid))
            {
                _logger.LogWarning("GetJobLogs called for non-existent Job ID: {JobId}", jobId);
                return NotFound($"Job with ID {jobId} not found.");
            }

            _logger.LogInformation("Log retrieval request for Job ID {JobId}. Job-specific log aggregation from console output is not implemented.", jobId);

            return Ok(new { Message = "Job-specific log retrieval from the current logging setup (e.g., console) is not yet implemented. Check centralized JobManager console/logs for diagnostics.", Logs = new List<LogEntryDto>() });
        }

        // POST /jobs/{jobId}/restart
        [HttpPost("/jobs/{jobId}/restart")]
        public async Task<IActionResult> RestartJob(string jobId)
        {
            if (string.IsNullOrWhiteSpace(jobId))
            {
                return BadRequest(JobIdEmptyMessage);
            }

            var jobStatus = await _jobRepository.GetJobStatusAsync(jobId);
            if (jobStatus == null)
            {
                return NotFound($"Job with ID {jobId} not found.");
            }

            jobStatus.Status = "RESTARTING";
            jobStatus.LastUpdated = DateTime.UtcNow;
            jobStatus.ErrorMessage = null;

            bool updated = await _jobRepository.UpdateJobStatusAsync(jobId, jobStatus);
            if (!updated)
            {
                return StatusCode(500, $"Failed to update status for job {jobId} to RESTARTING.");
            }
            return Ok(jobStatus);
        }

        // POST /dlq/{jobId}/resubmit
        [HttpPost("/dlq/{jobId}/resubmit")]
        public async Task<IActionResult> ResubmitDlqMessages(string jobId)
        {
            if (string.IsNullOrWhiteSpace(jobId))
            {
                return BadRequest(JobIdEmptyMessage);
            }

            bool accepted = await _jobRepository.RequestBatchDlqResubmissionAsync(jobId);
            if (!accepted)
            {
                return NotFound($"Failed to request DLQ resubmission for job {jobId}, or job not found.");
            }

            return Accepted($"Request to resubmit DLQ messages for job {jobId} has been accepted.");
        }

        // PUT /dlq/{jobId}/messages/{messageId}
        [HttpPut("/dlq/{jobId}/messages/{messageId}")]
        public async Task<IActionResult> ModifyDlqMessage(string jobId, string messageId, [FromBody] DlqMessageDto messageData)
        {
            if (string.IsNullOrWhiteSpace(jobId) || string.IsNullOrWhiteSpace(messageId))
            {
                return BadRequest("Job ID and Message ID cannot be empty.");
            }

            if (messageData == null || messageData.NewPayload == null)
            {
                return BadRequest("Message data with new payload must be provided.");
            }

            var existingMessage = await _jobRepository.GetDlqMessageAsync(jobId, messageId);
            if (existingMessage == null)
            {
                return NotFound($"DLQ message with ID {messageId} for job {jobId} not found.");
            }

            bool updated = await _jobRepository.UpdateDlqMessagePayloadAsync(jobId, messageId, messageData.NewPayload);
            if (!updated)
            {
                return StatusCode(500, $"Failed to update DLQ message {messageId} for job {jobId}.");
            }

            var updatedMessage = await _jobRepository.GetDlqMessageAsync(jobId, messageId);
            return Ok(updatedMessage);
        }
        // The private ObjectResult NotImplemented(string message) method has been removed.
    }
}
