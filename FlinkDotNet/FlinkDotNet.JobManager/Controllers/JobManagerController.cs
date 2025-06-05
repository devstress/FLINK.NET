using System;
using System.Threading.Tasks;
using Microsoft.AspNetCore.Mvc;
using FlinkDotNet.JobManager.Interfaces;
using FlinkDotNet.JobManager.Models;
using System.Linq;

namespace FlinkDotNet.JobManager.Controllers
{
    [ApiController]
    [Route("[controller]")] // Standard controller route
    public class JobManagerController : ControllerBase, IJobManagerApi
    {
        private readonly IJobRepository _jobRepository;

        public JobManagerController(IJobRepository jobRepository)
        {
            _jobRepository = jobRepository;
        }

        // POST /jobs
        [HttpPost("/jobs")]
        public async Task<IActionResult> SubmitJob([FromBody] JobDefinitionDto jobDefinition)
        {
            if (jobDefinition == null || string.IsNullOrWhiteSpace(jobDefinition.JobName))
            {
                return BadRequest("Job definition or job name cannot be empty.");
            }

            var jobId = Guid.NewGuid().ToString();
            var jobStatus = new JobStatusDto
            {
                JobId = jobId,
                Status = "SUBMITTED",
                LastUpdated = DateTime.UtcNow,
                ErrorMessage = null
            };

            bool created = await _jobRepository.CreateJobAsync(jobId, jobDefinition, jobStatus);

            if (!created)
            {
                return StatusCode(500, "Failed to create and store job.");
            }
            return Ok(jobStatus);
        }

        // GET /jobs/{jobId}
        [HttpGet("/jobs/{jobId}")]
        public async Task<IActionResult> GetJobStatus(string jobId)
        {
            if (string.IsNullOrWhiteSpace(jobId))
            {
                return BadRequest("Job ID cannot be empty.");
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
                return BadRequest("Job ID cannot be empty.");
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
                return BadRequest("Job ID cannot be empty.");
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
                return BadRequest("Job ID cannot be empty.");
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
        [HttpGet("/jobs/{jobId}/checkpoints")]
        public async Task<IActionResult> GetJobCheckpoints(string jobId)
        {
            if (string.IsNullOrWhiteSpace(jobId))
            {
                return BadRequest("Job ID cannot be empty.");
            }

            var checkpoints = await _jobRepository.GetCheckpointsAsync(jobId);

            if (checkpoints == null)
            {
                return NotFound($"Job with ID {jobId} not found, or no checkpoint information available.");
            }
            return Ok(checkpoints);
        }

        // POST /jobs/{jobId}/restart
        [HttpPost("/jobs/{jobId}/restart")]
        public async Task<IActionResult> RestartJob(string jobId)
        {
            if (string.IsNullOrWhiteSpace(jobId))
            {
                return BadRequest("Job ID cannot be empty.");
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
                return BadRequest("Job ID cannot be empty.");
            }

            bool accepted = await _jobRepository.RequestBatchDlqResubmissionAsync(jobId);
            if (!accepted)
            {
                return NotFound($"Failed to request DLQ resubmission for job {jobId}, or job not found.");
            }

            Console.WriteLine($"Controller: DLQ resubmission requested for job {jobId}");
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
