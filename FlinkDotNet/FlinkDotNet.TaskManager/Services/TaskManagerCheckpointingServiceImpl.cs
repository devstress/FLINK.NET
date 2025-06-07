using System;
using System.Threading.Tasks;
using Grpc.Core;
using FlinkDotNet.Proto.Internal; // From .proto file
using System.IO; // For Path, File
using FlinkDotNet.Storage.FileSystem; // Added for FileSystemSnapshotStore
using System.Text; // Added for Encoding.UTF8

namespace FlinkDotNet.TaskManager.Services
{
    public record SnapshotResult(bool Success, string SnapshotHandle, long SnapshotSize, long Duration); // Added record

    public class TaskManagerCheckpointingServiceImpl : TaskManagerCheckpointing.TaskManagerCheckpointingBase
    {
        private readonly string _taskManagerId;
        private static readonly FileSystemSnapshotStore _snapshotStore = new FileSystemSnapshotStore(); // Simple static instance for now

        // Inject TaskManagerId or get it from a shared service/config
        public TaskManagerCheckpointingServiceImpl(string taskManagerId)
        {
            _taskManagerId = taskManagerId;
        }

        public override async Task<TriggerCheckpointResponse> TriggerTaskCheckpoint( // Made async
            TriggerCheckpointRequest request, ServerCallContext context)
        {
            Console.WriteLine($"TaskManager [{_taskManagerId}]: Received TriggerCheckpoint request for JobID '{request.JobId}', CheckpointID {request.CheckpointId} from JM '{request.JobManagerId}'.");

            // Placeholder for actual snapshot logic
            string snapshotHandle = string.Empty;
            long snapshotSize = 0;
            long duration = 0;
            // bool snapshotSuccess = await PerformLocalSnapshotAsync(request.JobId, request.CheckpointId, snapshotHandle: out snapshotHandle, snapshotSize: out snapshotSize, duration: out duration);
            var snapshotResult = await PerformLocalSnapshotAsync(request.JobId, request.CheckpointId);


            if (snapshotResult.Success)
            {
                Console.WriteLine($"TaskManager [{_taskManagerId}]: Local snapshot for checkpoint {request.CheckpointId} completed. Handle: {snapshotResult.SnapshotHandle}");

                // Send AcknowledgeCheckpoint back to the JobManager
                if (Program.CoreServiceInstance != null)
                {
                    // Fire and forget for now, or await if critical path
                    _ = Program.CoreServiceInstance.SendAcknowledgeCheckpointAsync(
                            request.JobId,
                            request.CheckpointId,
                            snapshotResult.SnapshotHandle,
                            snapshotResult.SnapshotSize,
                            snapshotResult.Duration);
                }
                else
                {
                    Console.WriteLine($"TaskManager [{_taskManagerId}]: CRITICAL - TaskManagerCoreService instance not available to send AcknowledgeCheckpoint.");
                }
            }
            else
            {
                Console.WriteLine($"TaskManager [{_taskManagerId}]: Local snapshot for checkpoint {request.CheckpointId} FAILED. No acknowledgement will be sent.");
                // Note: JobManager will eventually time out this checkpoint if it doesn't receive all acks.
            }

            return new TriggerCheckpointResponse { Acknowledged = true }; // Changed for async method
        }

        private async Task<SnapshotResult> PerformLocalSnapshotAsync(string jobId, long checkpointId) // Changed signature
        {
            Console.WriteLine($"TaskManager [{_taskManagerId}]: Performing local snapshot for JobID '{jobId}', CheckpointID {checkpointId}.");

            var snapshotData = Encoding.UTF8.GetBytes($"Snapshot data for CP {checkpointId} from TM {_taskManagerId} for Job {jobId}"); // Dummy data

            try
            {
                // Use a consistent operatorId/stateName for the dummy snapshot
                var handleRecord = await _snapshotStore.StoreSnapshot(jobId, checkpointId, _taskManagerId, "operator_default_state", snapshotData);
                long snapshotSize = snapshotData.Length;
                long duration = 50; // dummy, ideally measure actual time
                Console.WriteLine($"TaskManager [{_taskManagerId}]: Snapshot stored via FileSystemSnapshotStore. Handle: {handleRecord.Value}");
                return new SnapshotResult(true, handleRecord.Value, snapshotSize, duration);
            }
            catch (Exception ex)
            {
                Console.WriteLine($"TaskManager [{_taskManagerId}]: Error storing snapshot using FileSystemSnapshotStore: {ex.Message}");
                return new SnapshotResult(false, string.Empty, 0, 0);
            }
        }
    }
}
#nullable disable
