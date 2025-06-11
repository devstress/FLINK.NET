using Grpc.Core;
using FlinkDotNet.Proto.Internal; // From .proto file
using FlinkDotNet.TaskManager.Models; // Added

namespace FlinkDotNet.TaskManager.Services
{

    public class TaskManagerCheckpointingServiceImpl : TaskManagerCheckpointing.TaskManagerCheckpointingBase
    {
        private readonly string _taskManagerId;

        // Inject TaskManagerId or get it from a shared service/config
        public TaskManagerCheckpointingServiceImpl(string taskManagerId)
        {
            _taskManagerId = taskManagerId;
        }

        public override Task<TriggerCheckpointResponse> TriggerTaskCheckpoint(
            TriggerCheckpointRequest request, ServerCallContext context)
        {
            Console.WriteLine($"TaskManager [{_taskManagerId}]: Received TriggerCheckpoint request for JobID '{request.JobId}', CheckpointID {request.CheckpointId}, Timestamp {request.CheckpointTimestamp} from JM '{request.JobManagerId}'.");

            var sourcesForJob = ActiveTaskRegistry.GetAllSources()
                                .Where(s => s.JobId == request.JobId)
                                .ToList();

            if (!sourcesForJob.Any())
            {
                Console.WriteLine($"TaskManager [{_taskManagerId}]: No active sources found for JobID '{request.JobId}' to inject barrier for CheckpointID {request.CheckpointId}.");
                return Task.FromResult(new TriggerCheckpointResponse { Acknowledged = true });
            }

            var barrierRequest = new BarrierInjectionRequest(request.CheckpointId, request.CheckpointTimestamp);
            int injectionCount = 0;

            foreach (var source in sourcesForJob)
            {
                try
                {
                    if (source.BarrierChannelWriter.TryWrite(barrierRequest))
                    {
                        Console.WriteLine($"TaskManager [{_taskManagerId}]: Successfully enqueued barrier injection request for CP {request.CheckpointId} to Source {source.JobVertexId}_{source.SubtaskIndex}.");
                        injectionCount++;
                    }
                    else
                    {
                        Console.WriteLine($"TaskManager [{_taskManagerId}]: Failed to enqueue barrier injection request for CP {request.CheckpointId} to Source {source.JobVertexId}_{source.SubtaskIndex}. Channel might be full or closed.");
                    }
                }
                catch (Exception ex)
                {
                    Console.WriteLine($"TaskManager [{_taskManagerId}]: Exception enqueuing barrier injection for CP {request.CheckpointId} to Source {source.JobVertexId}_{source.SubtaskIndex}: {ex.Message}");
                }
            }

            Console.WriteLine($"TaskManager [{_taskManagerId}]: Enqueued barrier injection requests for {injectionCount}/{sourcesForJob.Count} sources for JobID '{request.JobId}', CP {request.CheckpointId}.");

            return Task.FromResult(new TriggerCheckpointResponse { Acknowledged = true });
        }

    }
}
#nullable disable
