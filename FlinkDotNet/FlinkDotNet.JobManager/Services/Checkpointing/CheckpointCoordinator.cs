// TODO: Reduce cognitive complexity
using System.Collections.Concurrent;
using System.Globalization;
using FlinkDotNet.JobManager.Interfaces; // Added for IJobRepository
using FlinkDotNet.JobManager.Models; // For CheckpointMetadata, TaskManagerInfo (assuming TaskManagerInfo is also in Models or accessible)
using FlinkDotNet.Proto.Internal;    // For gRPC client (TaskManagerCheckpointing.TaskManagerCheckpointingClient)
using Grpc.Net.Client;               // For GrpcChannel
using FlinkDotNet.JobManager.Services; // Assuming TaskManagerTracker is in FlinkDotNet.JobManager.Services
using FlinkDotNet.Storage.RocksDB;

namespace FlinkDotNet.JobManager.Checkpointing
{
    /// <summary>
    /// Apache Flink 2.0 enhanced checkpoint coordinator with RocksDB state backend management
    /// </summary>
    public class CheckpointCoordinator
    {
        private readonly string _jobId; // For which job this coordinator is responsible
        private readonly ConcurrentDictionary<long, CheckpointMetadata> _checkpoints;
        private readonly ConcurrentDictionary<string, RocksDBStateBackend> _registeredStateBackends;
        private long _nextCheckpointId = 1;
        private Timer? _checkpointTriggerTimer;
        private readonly TimeSpan _checkpointInterval;
        private readonly JobManagerConfig _config; // Placeholder for config like checkpoint timeout
        private readonly IJobRepository _jobRepository;
        private readonly ILogger<CheckpointCoordinator> _logger; // Added
        // For now, it might fetch from TaskManagerTracker, but this coupling isn't ideal long-term.
        // A better approach would be an ITaskManagerProxy or similar abstraction.

        public CheckpointCoordinator(string jobId, IJobRepository jobRepository, ILogger<CheckpointCoordinator> logger, JobManagerConfig? config = null)
        {
            _jobId = jobId;
            _jobRepository = jobRepository;
            _logger = logger; // Added
            _checkpoints = new ConcurrentDictionary<long, CheckpointMetadata>();
            _registeredStateBackends = new ConcurrentDictionary<string, RocksDBStateBackend>();
            _config = config ?? new JobManagerConfig(); // Use default config if none provided
            _checkpointInterval = TimeSpan.FromSeconds(_config.CheckpointIntervalSecs);

            _logger.LogInformation("CheckpointCoordinator for job {JobId} initialized with interval {CheckpointIntervalSeconds}s.", _jobId, _checkpointInterval.TotalSeconds);
        }

        /// <summary>
        /// Registers a state backend for coordinated checkpointing
        /// </summary>
        public async Task RegisterStateBackendAsync(string stateBackendId, RocksDBStateBackend stateBackend)
        {
            _registeredStateBackends.TryAdd(stateBackendId, stateBackend);
            _logger.LogInformation("Registered state backend {StateBackendId} for coordinated checkpointing", stateBackendId);
            await Task.CompletedTask;
        }

        /// <summary>
        /// Unregisters a state backend from coordinated checkpointing
        /// </summary>
        public async Task UnregisterStateBackendAsync(string stateBackendId)
        {
            _registeredStateBackends.TryRemove(stateBackendId, out _);
            _logger.LogInformation("Unregistered state backend {StateBackendId} from coordinated checkpointing", stateBackendId);
            await Task.CompletedTask;
        }

        public void Start()
        {
            _logger.LogInformation("CheckpointCoordinator for job {JobId} starting. Scheduling periodic triggers.", _jobId);
            _checkpointTriggerTimer?.Dispose();
            _checkpointTriggerTimer = new Timer(
                async _ => await TriggerCheckpoint(),
                null,
                TimeSpan.FromSeconds(5), // Initial delay before first checkpoint
                _checkpointInterval);
        }

        public void Stop()
        {
            _logger.LogInformation("CheckpointCoordinator for job {JobId} stopping.", _jobId);
            _checkpointTriggerTimer?.Dispose();
            _checkpointTriggerTimer = null;
        }

        private async Task TriggerCheckpoint()
        {
            var checkpointId = Interlocked.Increment(ref _nextCheckpointId);
            var timestamp = DateTimeOffset.UtcNow.ToUnixTimeMilliseconds();

            _logger.LogInformation("Job {JobId}: Triggering Apache Flink 2.0 style checkpoint {CheckpointId} with {StateBackendCount} state backends", 
                _jobId, checkpointId, _registeredStateBackends.Count);

            // First, trigger checkpoints on all registered state backends
            var stateBackendCheckpointTasks = _registeredStateBackends.Select(async kvp =>
            {
                var (stateBackendId, stateBackend) = kvp;
                try
                {
                    await stateBackend.CreateCheckpointAsync(checkpointId);
                    _logger.LogDebug("Job {JobId}, Checkpoint {CheckpointId}: State backend {StateBackendId} checkpoint created", 
                        _jobId, checkpointId, stateBackendId);
                    return true;
                }
                catch (Exception ex)
                {
                    _logger.LogError(ex, "Job {JobId}, Checkpoint {CheckpointId}: Failed to create checkpoint for state backend {StateBackendId}", 
                        _jobId, checkpointId, stateBackendId);
                    return false;
                }
            });

            var stateBackendResults = await Task.WhenAll(stateBackendCheckpointTasks);
            var successfulStateBackends = stateBackendResults.Count(r => r);

            if (successfulStateBackends < _registeredStateBackends.Count)
            {
                _logger.LogWarning("Job {JobId}, Checkpoint {CheckpointId}: Only {SuccessfulCount}/{TotalCount} state backends completed checkpoint successfully", 
                    _jobId, checkpointId, successfulStateBackends, _registeredStateBackends.Count);
            }

            // Get list of *expected* TaskManagers for this job.
            // For now, assume all registered TMs are part of this job. This needs refinement with actual job-to-TM assignment.
            // Ensure TaskManagerTracker is accessible here. It's in FlinkDotNet.JobManager.Services namespace.
            var participatingTaskManagers = TaskManagerTracker.RegisteredTaskManagers.Values.ToList();
            if (!participatingTaskManagers.Any())
            {
                _logger.LogWarning("Job {JobId}: No TaskManagers registered. Skipping checkpoint trigger for ID {CheckpointId}.", _jobId, checkpointId);
                return;
            }

            var expectedTmIds = participatingTaskManagers.Select(tm => tm.TaskManagerId).ToList();
            var checkpoint = new CheckpointMetadata(_jobId, checkpointId, timestamp, expectedTmIds);
            if (!_checkpoints.TryAdd(checkpointId, checkpoint))
            {
                _logger.LogError("Job {JobId}: Failed to add checkpoint {CheckpointId} to internal tracking. This should not happen.", _jobId, checkpointId);
                return; // Skip this attempt
            }

            _logger.LogInformation("Job {JobId}: Triggering checkpoint {CheckpointId} for {NumTaskManagers} TaskManagers.", _jobId, checkpointId, participatingTaskManagers.Count);

            var triggerTasks = participatingTaskManagers.Select(async tmInfo =>
            {
                try
                {
                    var channelAddress = $"http://{tmInfo.Address}:{tmInfo.Port}";
                    _logger.LogDebug("Job {JobId}, Checkpoint {CheckpointId}: Attempting to connect to TM {TaskManagerId} at {ChannelAddress} for TriggerTaskCheckpoint.", _jobId, checkpointId, tmInfo.TaskManagerId, channelAddress);

                    using var channel = GrpcChannel.ForAddress(channelAddress);
                    var client = new TaskManagerCheckpointing.TaskManagerCheckpointingClient(channel);
                    var request = new TriggerCheckpointRequest
                    {
                        JobManagerId = _config.JobManagerId, // Use JobManagerId from config
                        JobId = _jobId,
                        CheckpointId = checkpointId,
                        CheckpointTimestamp = timestamp
                    };

                    var response = await client.TriggerTaskCheckpointAsync(request, deadline: DateTime.UtcNow.AddSeconds(10));

                    if (!response.Acknowledged)
                    {
                        _logger.LogWarning("Job {JobId}, Checkpoint {CheckpointId}: TM {TaskManagerId} did not acknowledge trigger immediately (response.Acknowledged = false).", _jobId, checkpointId, tmInfo.TaskManagerId);
                        if (checkpoint.TaskSnapshots.TryGetValue(tmInfo.TaskManagerId, out var taskCpInfo))
                        {
                             taskCpInfo.Fail(); // Assuming this method updates internal status
                        }
                    }
                    else
                    {
                         _logger.LogDebug("Job {JobId}, Checkpoint {CheckpointId}: TM {TaskManagerId} acknowledged trigger.", _jobId, checkpointId, tmInfo.TaskManagerId);
                    }
                }
                catch (Exception ex)
                {
                    _logger.LogError(ex, "Job {JobId}, Checkpoint {CheckpointId}: Failed to send TriggerTaskCheckpoint to TM {TaskManagerId}.", _jobId, checkpointId, tmInfo.TaskManagerId);
                    if (checkpoint.TaskSnapshots.TryGetValue(tmInfo.TaskManagerId, out var taskCpInfo))
                    {
                         taskCpInfo.Fail(); // Assuming this method updates internal status
                    }
                }
            });

            await Task.WhenAll(triggerTasks);
            _logger.LogInformation("Job {JobId}: All TriggerTaskCheckpoint messages sent for checkpoint {CheckpointId}.", _jobId, checkpointId);
            // Start a timer here for 'checkpointId'. If it fires before the checkpoint is fully acknowledged,
            // call 'RecordFailedCheckpointAsync(checkpointId, "Timeout")' and potentially cancel ongoing efforts for this CP.
            Console.WriteLine($"[CheckpointCoordinator] Placeholder for starting timeout timer for checkpoint {checkpointId}.");
        }

        public void AcknowledgeCheckpoint(string jobId, long checkpointId, string taskManagerId, string snapshotHandle, long size = 0, long duration = 0)
        {
            if (jobId != _jobId)
            {
                _logger.LogWarning("CheckpointCoordinator for job {MyJobId} received ack for wrong job {ReportedJobId}. Ignoring.", _jobId, jobId);
                return;
            }

            if (_checkpoints.TryGetValue(checkpointId, out var checkpoint))
            {
                _logger.LogInformation("Job {JobId}, Checkpoint {CheckpointId}: Received ACK from TM {TaskManagerId}. Handle: {SnapshotHandle}", _jobId, checkpointId, taskManagerId, snapshotHandle);
                checkpoint.MarkTaskAcknowledged(taskManagerId, snapshotHandle, unchecked((ulong)size), unchecked((ulong)duration));

                if (checkpoint.IsFullyAcknowledged())
                {
                    checkpoint.MarkCompleted();
                    _logger.LogInformation("Job {JobId}: Checkpoint {CheckpointId} is fully acknowledged and completed.", _jobId, checkpoint.CheckpointId);

                    var checkpointDto = new CheckpointInfoDto
                    {
                        CheckpointId = checkpoint.CheckpointId.ToString(CultureInfo.InvariantCulture),
                        Timestamp = DateTimeOffset.FromUnixTimeMilliseconds(checkpoint.Timestamp).UtcDateTime,
                        Status = checkpoint.Status.ToString().ToUpperInvariant(), // COMPLETED, FAILED, IN_PROGRESS
                        DurationMs = checkpoint.TaskSnapshots.Values.Any() ? checkpoint.TaskSnapshots.Values.Max(s => (long)s.DurationMs) : 0,
                        SizeBytes = checkpoint.TaskSnapshots.Values.Sum(s => (long)s.SnapshotSize)
                    };
                    _jobRepository.AddCheckpointAsync(_jobId, checkpointDto).ConfigureAwait(false); // Fire and forget for now
                    // and potentially from the _jobRepository / durable storage based on retention policy.
                    Console.WriteLine($"[CheckpointCoordinator] Placeholder for cleaning up old checkpoints after CP {checkpoint.CheckpointId} completed.");
                }
                // Optional: Check if checkpoint has failed due to some tasks failing
                // This might be more complex if some tasks succeed and some fail for the same checkpoint ID.
                // For now, explicit MarkFailed() in other parts of the coordinator would handle this.
            }
            else
            {
                _logger.LogWarning("Job {JobId}: Received ACK for unknown checkpoint ID {CheckpointId} from TM {TaskManagerId}. Ignoring.", _jobId, checkpointId, taskManagerId);
            }
        }

        // Call this method if a checkpoint is determined to have failed overall
        public async Task RecordFailedCheckpointAsync(long checkpointId, string reason = "Unknown")
        {
            if (_checkpoints.TryGetValue(checkpointId, out var checkpoint))
            {
                checkpoint.MarkFailed();
                var checkpointDto = new CheckpointInfoDto
                {
                    CheckpointId = checkpoint.CheckpointId.ToString(CultureInfo.InvariantCulture),
                    Timestamp = DateTimeOffset.FromUnixTimeMilliseconds(checkpoint.Timestamp).UtcDateTime,
                    Status = checkpoint.Status.ToString().ToUpperInvariant(), // Should be FAILED
                    DurationMs = (long)(DateTime.UtcNow - DateTimeOffset.FromUnixTimeMilliseconds(checkpoint.Timestamp)).TotalMilliseconds, // Duration until failure
                    SizeBytes = 0 // Or sum of any partial data if applicable
                };
                await _jobRepository.AddCheckpointAsync(_jobId, checkpointDto).ConfigureAwait(false);
                _logger.LogInformation("Job {JobId}: Checkpoint {CheckpointId} marked as FAILED (Reason: {Reason}) and recorded.", _jobId, checkpointId, reason);
            }
        }
    }

    // Placeholder for JobManager configuration
    public class JobManagerConfig
    {
        public string JobManagerId { get; set; } = $"JM-{Guid.NewGuid()}"; // Each JM instance gets a unique ID
        public int CheckpointIntervalSecs { get; set; } = 30; // Default checkpoint interval
        public int CheckpointTimeoutSecs { get; set; } = 120;  // Default checkpoint timeout
    }
}
#nullable disable
