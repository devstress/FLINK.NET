using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using FlinkDotNet.JobManager.Interfaces;
using FlinkDotNet.JobManager.Models;

namespace FlinkDotNet.JobManager.Services
{
    /// <summary>
    /// Mock implementation of IJobRepository using in-memory ConcurrentDictionaries.
    /// </summary>
    public class InMemoryJobRepository : IJobRepository
    {
        private readonly ConcurrentDictionary<string, JobDefinitionDto> _jobDefinitions = new();
        private readonly ConcurrentDictionary<string, JobStatusDto> _jobStatuses = new();
        private readonly ConcurrentDictionary<string, List<CheckpointInfoDto>> _jobCheckpoints = new();
        private readonly ConcurrentDictionary<string, DlqMessageDto> _dlqMessages = new(); // Key: "jobId_messageId"

        public Task<bool> CreateJobAsync(string jobId, JobDefinitionDto jobDefinition, JobStatusDto initialStatus)
        {
            if (string.IsNullOrWhiteSpace(jobId))
                return Task.FromResult(false);

            bool definitionAdded = _jobDefinitions.TryAdd(jobId, jobDefinition);
            bool statusAdded = _jobStatuses.TryAdd(jobId, initialStatus);

            return Task.FromResult(definitionAdded && statusAdded);
        }

        public Task<JobDefinitionDto?> GetJobDefinitionAsync(string jobId)
        {
            _jobDefinitions.TryGetValue(jobId, out var jobDefinition);
            return Task.FromResult(jobDefinition);
        }

        public Task<JobStatusDto?> GetJobStatusAsync(string jobId)
        {
            _jobStatuses.TryGetValue(jobId, out var jobStatus);
            return Task.FromResult(jobStatus);
        }

        public Task<bool> UpdateJobStatusAsync(string jobId, JobStatusDto newStatus)
        {
            if (!_jobStatuses.ContainsKey(jobId))
                return Task.FromResult(false);

            _jobStatuses[jobId] = newStatus;
            return Task.FromResult(true);
        }

        public Task<IEnumerable<CheckpointInfoDto>?> GetCheckpointsAsync(string jobId)
        {
            if (!_jobStatuses.ContainsKey(jobId))
            {
                return Task.FromResult<IEnumerable<CheckpointInfoDto>?>(null);
            }

            if (_jobCheckpoints.TryGetValue(jobId, out var checkpoints))
            {
                return Task.FromResult<IEnumerable<CheckpointInfoDto>?>(checkpoints.AsEnumerable());
            }

            return Task.FromResult<IEnumerable<CheckpointInfoDto>?>(new List<CheckpointInfoDto>());
        }

        public void AddMockCheckpoint(string jobId, CheckpointInfoDto checkpoint)
        {
            if (!_jobDefinitions.ContainsKey(jobId))
            {
                 throw new InvalidOperationException($"Job with ID {jobId} does not exist. Cannot add checkpoint.");
            }
            var checkpoints = _jobCheckpoints.GetOrAdd(jobId, _ => new List<CheckpointInfoDto>());
            checkpoints.Add(checkpoint);
        }

        // DLQ Methods
        public Task<bool> RequestBatchDlqResubmissionAsync(string jobId)
        {
            if (!_jobStatuses.ContainsKey(jobId))
            {
                return Task.FromResult(false); // Job not found
            }
            // Simulate acceptance of the request
            // In a real system, this would trigger an actual process.
            // For this mock, simply acknowledging is enough.
            Console.WriteLine($"Mock: Batch DLQ resubmission requested for job {jobId}");
            return Task.FromResult(true);
        }

        public Task<DlqMessageDto?> GetDlqMessageAsync(string jobId, string messageId)
        {
            _dlqMessages.TryGetValue($"{jobId}_{messageId}", out var message);
            return Task.FromResult(message);
        }

        public Task<bool> UpdateDlqMessagePayloadAsync(string jobId, string messageId, byte[] newPayload)
        {
            var key = $"{jobId}_{messageId}";
            if (_dlqMessages.TryGetValue(key, out var message))
            {
                message.Payload = newPayload;
                message.NewPayload = null; // Clear the NewPayload field as it has been applied
                message.LastUpdated = DateTime.UtcNow;
                return Task.FromResult(true);
            }
            return Task.FromResult(false); // Message not found
        }

        // Helper for tests to add mock DLQ messages
        public void AddMockDlqMessage(DlqMessageDto message)
        {
            if (string.IsNullOrWhiteSpace(message.JobId) || string.IsNullOrWhiteSpace(message.MessageId))
            {
                throw new ArgumentNullException(nameof(message), "JobId and MessageId must be set for DLQ message.");
            }
            var key = $"{message.JobId}_{message.MessageId}";
            _dlqMessages.TryAdd(key, message);
        }
    }
}
