#nullable enable
using System;
using System.Collections.Concurrent;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using FlinkDotNet.JobManager.Models; // For CheckpointMetadata, TaskManagerInfo (assuming TaskManagerInfo is also in Models or accessible)
using FlinkDotNet.Proto.Internal;    // For gRPC client (TaskManagerCheckpointing.TaskManagerCheckpointingClient)
using Grpc.Net.Client;               // For GrpcChannel
using FlinkDotNet.JobManager.Services; // Assuming TaskManagerTracker is in FlinkDotNet.JobManager.Services

namespace FlinkDotNet.JobManager.Checkpointing // Changed namespace to match new folder
{
    public class CheckpointCoordinator
    {
        private readonly string _jobId; // For which job this coordinator is responsible
        private readonly ConcurrentDictionary<long, CheckpointMetadata> _checkpoints;
        private long _nextCheckpointId = 1;
        private Timer? _checkpointTriggerTimer;
        private readonly TimeSpan _checkpointInterval;
        private readonly JobManagerConfig _config; // Placeholder for config like checkpoint timeout

        // TODO: This needs a way to get gRPC channels to TaskManagers.
        // For now, it might fetch from TaskManagerTracker, but this coupling isn't ideal long-term.
        // A better approach would be an ITaskManagerProxy or similar abstraction.

        public CheckpointCoordinator(string jobId, JobManagerConfig? config = null)
        {
            _jobId = jobId;
            _checkpoints = new ConcurrentDictionary<long, CheckpointMetadata>();
            _config = config ?? new JobManagerConfig(); // Use default config if none provided
            _checkpointInterval = TimeSpan.FromSeconds(_config.CheckpointIntervalSecs);

            Console.WriteLine($"CheckpointCoordinator for job {_jobId} initialized with interval {_checkpointInterval.TotalSeconds}s.");
        }

        public void Start()
        {
            Console.WriteLine($"CheckpointCoordinator for job {_jobId} starting. Scheduling periodic triggers.");
            _checkpointTriggerTimer?.Dispose();
            _checkpointTriggerTimer = new Timer(
                async _ => await TriggerCheckpoint(),
                null,
                TimeSpan.FromSeconds(5), // Initial delay before first checkpoint
                _checkpointInterval);
        }

        public void Stop()
        {
            Console.WriteLine($"CheckpointCoordinator for job {_jobId} stopping.");
            _checkpointTriggerTimer?.Dispose();
            _checkpointTriggerTimer = null;
            // TODO: Abort any in-progress checkpoints?
        }

        private async Task TriggerCheckpoint()
        {
            var checkpointId = Interlocked.Increment(ref _nextCheckpointId);
            var timestamp = DateTimeOffset.UtcNow.ToUnixTimeMilliseconds();

            // Get list of *expected* TaskManagers for this job.
            // For now, assume all registered TMs are part of this job. This needs refinement with actual job-to-TM assignment.
            // Ensure TaskManagerTracker is accessible here. It's in FlinkDotNet.JobManager.Services namespace.
            var participatingTaskManagers = TaskManagerTracker.RegisteredTaskManagers.Values.ToList();
            if (!participatingTaskManagers.Any())
            {
                Console.WriteLine($"Job '{_jobId}': No TaskManagers registered. Skipping checkpoint trigger for ID {checkpointId}.");
                return;
            }

            var expectedTmIds = participatingTaskManagers.Select(tm => tm.TaskManagerId).ToList();
            var checkpoint = new CheckpointMetadata(_jobId, checkpointId, timestamp, expectedTmIds);
            if (!_checkpoints.TryAdd(checkpointId, checkpoint))
            {
                Console.WriteLine($"Job '{_jobId}': Failed to add checkpoint {checkpointId} to internal tracking. This should not happen.");
                return; // Skip this attempt
            }

            Console.WriteLine($"Job '{_jobId}': Triggering checkpoint {checkpointId} for {participatingTaskManagers.Count} TaskManagers.");

            var triggerTasks = participatingTaskManagers.Select(async tmInfo =>
            {
                try
                {
                    // TODO: Channel management should be improved (cache channels per TM endpoint)
                    var channelAddress = $"http://{tmInfo.Address}:{tmInfo.Port}";
                    // Ensure tmInfo.Address and tmInfo.Port are correctly populated from registration.
                    // The TaskManager now registers with "localhost" and its GrpcPort.
                    // If JobManager and TaskManager are on different machines, "localhost" for TM address won't work.
                    // This assumes local testing for now.

                    Console.WriteLine($"Job '{_jobId}', Checkpoint {checkpointId}: Attempting to connect to TM {tmInfo.TaskManagerId} at {channelAddress} for TriggerTaskCheckpoint.");

                    using var channel = GrpcChannel.ForAddress(channelAddress); // Inefficient, but for PoC
                    var client = new TaskManagerCheckpointing.TaskManagerCheckpointingClient(channel);
                    var request = new TriggerCheckpointRequest
                    {
                        JobManagerId = _config.JobManagerId, // Use JobManagerId from config
                        JobId = _jobId,
                        CheckpointId = checkpointId,
                        CheckpointTimestamp = timestamp
                    };

                    var response = await client.TriggerTaskCheckpointAsync(request, deadline: DateTime.UtcNow.AddSeconds(10)); // Add a deadline

                    if (!response.Acknowledged)
                    {
                        Console.WriteLine($"Job '{_jobId}', Checkpoint {checkpointId}: TM {tmInfo.TaskManagerId} did not acknowledge trigger immediately (response.Acknowledged = false).");
                        if (checkpoint.TaskSnapshots.TryGetValue(tmInfo.TaskManagerId, out var taskCpInfo))
                        {
                             taskCpInfo.Fail();
                        }
                    }
                    else
                    {
                         Console.WriteLine($"Job '{_jobId}', Checkpoint {checkpointId}: TM {tmInfo.TaskManagerId} acknowledged trigger.");
                    }
                }
                catch (Exception ex)
                {
                    Console.WriteLine($"Job '{_jobId}', Checkpoint {checkpointId}: Failed to send TriggerTaskCheckpoint to TM {tmInfo.TaskManagerId}: {ex.GetType().Name} - {ex.Message}");
                    if (checkpoint.TaskSnapshots.TryGetValue(tmInfo.TaskManagerId, out var taskCpInfo))
                    {
                         taskCpInfo.Fail();
                    }
                }
            });

            await Task.WhenAll(triggerTasks);
            Console.WriteLine($"Job '{_jobId}': All TriggerTaskCheckpoint messages sent for checkpoint {checkpointId}.");

            // TODO: Start a timer for checkpoint timeout for this specific checkpointId.
            // If it times out before all TMs ack, mark as failed.
        }

        public void AcknowledgeCheckpoint(string jobId, long checkpointId, string taskManagerId, string snapshotHandle, long size = 0, long duration = 0)
        {
            if (jobId != _jobId)
            {
                Console.WriteLine($"CheckpointCoordinator for job {_jobId} received ack for wrong job {jobId}. Ignoring.");
                return;
            }

            if (_checkpoints.TryGetValue(checkpointId, out var checkpoint))
            {
                Console.WriteLine($"Job '{_jobId}', Checkpoint {checkpointId}: Received ACK from TM {taskManagerId}. Handle: {snapshotHandle}");
                checkpoint.MarkTaskAcknowledged(taskManagerId, snapshotHandle, size, duration);

                if (checkpoint.IsFullyAcknowledged())
                {
                    checkpoint.MarkCompleted(); // This method already checks if all tasks are 'Completed'
                    Console.WriteLine($"Job '{_jobId}': Checkpoint {checkpointId} is fully acknowledged and completed.");
                    // TODO: Clean up old checkpoints
                }
            }
            else
            {
                Console.WriteLine($"Job '{_jobId}': Received ACK for unknown checkpoint ID {checkpointId} from TM {taskManagerId}. Ignoring.");
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
