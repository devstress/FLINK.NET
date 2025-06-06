#nullable enable
using System;
using System.Threading.Tasks;
using Grpc.Core;
using FlinkDotNet.Proto.Internal; // From .proto file
using System.IO; // For Path, File
// using FlinkDotNet.Storage.FileSystem; // No longer used directly here
using System.Text; // Added for Encoding.UTF8
using System.Threading.Channels; // Required for ChannelClosedException
using FlinkDotNet.Core.Abstractions.Models.State; // Added for OperatorStateSnapshot
using FlinkDotNet.TaskManager; // Added for ActiveTaskRegistry and BarrierInjectionRequest

namespace FlinkDotNet.TaskManager.Services
{
    // public record SnapshotResult(bool Success, string SnapshotHandle, long SnapshotSize, long Duration); // No longer needed here

    public class TaskManagerCheckpointingServiceImpl : TaskManagerCheckpointing.TaskManagerCheckpointingBase
    {
        private readonly string _taskManagerId;
        private readonly ActiveTaskRegistry _activeTaskRegistry;
        // private static readonly FileSystemSnapshotStore _snapshotStore = new FileSystemSnapshotStore(); // Removed

        // Inject TaskManagerId or get it from a shared service/config
        public TaskManagerCheckpointingServiceImpl(string taskManagerId, ActiveTaskRegistry activeTaskRegistry)
        {
            _taskManagerId = taskManagerId;
            _activeTaskRegistry = activeTaskRegistry;
        }

        public override async Task<TriggerCheckpointResponse> TriggerTaskCheckpoint( // Made async
            TriggerCheckpointRequest request, ServerCallContext context)
        {
            Console.WriteLine($"TaskManager [{_taskManagerId}]: Received TriggerCheckpoint request for JobID '{request.JobId}', CheckpointID {request.CheckpointId} from JM '{request.JobManagerId}'.");

            // --- New Barrier Injection Logic ---
            var sourcesForJob = _activeTaskRegistry.GetSourcesForJob(request.JobId);
            int injectionCount = 0;
            foreach (var source in sourcesForJob)
            {
                try
                {
                    var barrierRequest = new BarrierInjectionRequest(request.CheckpointId, request.CheckpointTimestamp);
                    // Asynchronously send the request. If the channel is full or closed, it might throw.
                    // Consider using TryWrite if immediate failure handling is needed, but WriteAsync is fine for now.
                    await source.BarrierChannelWriter.WriteAsync(barrierRequest, context.CancellationToken); // Use context.CancellationToken
                    injectionCount++;
                    Console.WriteLine($"TaskManager [{_taskManagerId}]: Queued barrier {request.CheckpointId} for source {source.JobVertexId}_{source.SubtaskIndex}");
                }
                catch (ChannelClosedException cce)
                {
                    Console.WriteLine($"TaskManager [{_taskManagerId}]: Failed to queue barrier for source {source.JobVertexId}_{source.SubtaskIndex} because channel was closed: {cce.Message}");
                    // This might happen if the source task completed and unregistered concurrently.
                }
                catch (OperationCanceledException oce)
                {
                    Console.WriteLine($"TaskManager [{_taskManagerId}]: Failed to queue barrier for source {source.JobVertexId}_{source.SubtaskIndex} due to cancellation: {oce.Message}");
                    // This might happen if the gRPC call itself is cancelled.
                }
                catch (Exception ex)
                {
                    Console.WriteLine($"TaskManager [{_taskManagerId}]: Error queueing barrier for source {source.JobVertexId}_{source.SubtaskIndex}: {ex.Message}");
                    // Log other potential errors
                }
            }
            Console.WriteLine($"TaskManager [{_taskManagerId}]: Attempted to queue barrier {request.CheckpointId} for {injectionCount} sources for job {request.JobId}.");
            // --- End of New Barrier Injection Logic ---

            // The method now primarily just ensures barriers are injected into sources.
            // Acknowledgements will be sent by ReportOperatorSnapshotComplete when operators (or sources acting as checkpointable) complete their snapshots.
            Console.WriteLine($"TaskManager [{_taskManagerId}]: Finished processing TriggerCheckpoint for CP {request.CheckpointId}. Barriers injected for sources. Operators will acknowledge upon their snapshot completion.");
            return new TriggerCheckpointResponse { Acknowledged = true }; // Acknowledges the trigger itself
        }

        public async Task ReportOperatorSnapshotComplete(
            string jobId,
            long checkpointId,
            string jobVertexId, // From TDD
            int subtaskIndex,   // From TDD
            OperatorStateSnapshot snapshotDetail, // The result from operator.SnapshotState()
            long durationMs) // Duration of the snapshot operation for this operator
        {
            Console.WriteLine($"TaskManager [{_taskManagerId}]: Operator {jobVertexId}_{subtaskIndex} completed snapshot for CP {checkpointId}. Handle: {snapshotDetail.StateHandle}");

            if (Program.CoreServiceInstance != null)
            {
                // Assuming Program.CoreServiceInstance.SendAcknowledgeCheckpointAsync exists and
                // can take these parameters or be adapted.
                // The AcknowledgeCheckpointRequest proto has fields for most of these.
                // It expects snapshotHandle, snapshotSize, duration.
                // It also has jobVertexId and subtaskIndex.
                await Program.CoreServiceInstance.SendAcknowledgeCheckpointAsync(
                    jobId,
                    checkpointId,
                    snapshotDetail.StateHandle ?? string.Empty, // Ensure null safety
                    (ulong)snapshotDetail.StateSize, // Cast to ulong for proto
                    (ulong)durationMs, // Cast to ulong for proto
                    jobVertexId,    // Pass through identifying info
                    subtaskIndex    // Pass through identifying info
                    // TODO: Add source_offsets if snapshotDetail.SourceOffsets is populated (for sources)
                );
                 Console.WriteLine($"TaskManager [{_taskManagerId}]: Sent AcknowledgeCheckpoint for {jobVertexId}_{subtaskIndex}, CP {checkpointId}.");
            }
            else
            {
                Console.WriteLine($"TaskManager [{_taskManagerId}]: CRITICAL - TaskManagerCoreService instance not available to send AcknowledgeCheckpoint for {jobVertexId}_{subtaskIndex}, CP {checkpointId}.");
            }
        }
    }
}
#nullable disable
