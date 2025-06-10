using FlinkDotNet.JobManager.Models;

namespace FlinkDotNet.JobManager.Interfaces
{
    /// <summary>
    /// Defines the contract for a repository that handles persistence of job definitions and statuses.
    /// </summary>
    public interface IJobRepository
    {
        /// <summary>
        /// Stores a new job definition and its initial status.
        /// </summary>
        Task<bool> CreateJobAsync(string jobId, JobDefinitionDto jobDefinition, JobStatusDto initialStatus);

        /// <summary>
        /// Retrieves the status of a specific job.
        /// </summary>
        Task<JobStatusDto?> GetJobStatusAsync(string jobId);

        /// <summary>
        /// Retrieves the definition of a specific job.
        /// </summary>
        Task<JobDefinitionDto?> GetJobDefinitionAsync(string jobId);

        /// <summary>
        /// Updates the status of an existing job.
        /// </summary>
        Task<bool> UpdateJobStatusAsync(string jobId, JobStatusDto newStatus);

        /// <summary>
        /// Retrieves all checkpoint information for a specific job.
        /// </summary>
        Task<IEnumerable<CheckpointInfoDto>?> GetCheckpointsAsync(string jobId);

        /// <summary>
        /// Adds a new checkpoint information entry for a specific job.
        /// </summary>
        Task AddCheckpointAsync(string jobId, CheckpointInfoDto checkpointInfo);

        // DLQ Methods
        /// <summary>
        /// Conceptually requests resubmission of all DLQ messages for a given job.
        /// </summary>
        Task<bool> RequestBatchDlqResubmissionAsync(string jobId);

        /// <summary>
        /// Retrieves a specific DLQ message.
        /// </summary>
        Task<DlqMessageDto?> GetDlqMessageAsync(string jobId, string messageId);

        /// <summary>
        /// Updates the payload of a specific DLQ message.
        /// </summary>
        Task<bool> UpdateDlqMessagePayloadAsync(string jobId, string messageId, byte[] newPayload);
    }
}
