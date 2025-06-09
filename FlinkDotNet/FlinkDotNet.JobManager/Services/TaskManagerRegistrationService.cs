using System;
using System.Collections.Concurrent;
using System.Threading.Tasks;
using Grpc.Core;
using Proto = FlinkDotNet.Proto.Internal;
using FlinkDotNet.JobManager.Models; // For TaskManagerInfo
using FlinkDotNet.JobManager.Controllers;
using FlinkDotNet.JobManager.Checkpointing; // For CheckpointCoordinator
using Microsoft.Extensions.Logging; // Added for ILogger

// Simple tracker for registered TaskManagers (replace with a more robust solution later)
// This could be moved to its own file or an infrastructure layer.
public static class TaskManagerTracker
{
    public static ConcurrentDictionary<string, TaskManagerInfo> RegisteredTaskManagers { get; } = new();
}

// TaskManagerInfo class has been moved to FlinkDotNet.JobManager.Models/TaskManagerInfo.cs

public class TaskManagerRegistrationServiceImpl : Proto.TaskManagerRegistration.TaskManagerRegistrationBase
{
    private readonly string _jobManagerId = $"JM-{Guid.NewGuid()}"; // Example JobManager ID
    public static ConcurrentDictionary<string, CheckpointCoordinator> JobCoordinators { get; } = new();
    private readonly ILogger<TaskManagerRegistrationServiceImpl> _logger;

    public TaskManagerRegistrationServiceImpl(ILogger<TaskManagerRegistrationServiceImpl> logger)
    {
        _logger = logger;
    }

    public override Task<Proto.RegisterTaskManagerResponse> RegisterTaskManager(
        Proto.RegisterTaskManagerRequest request, ServerCallContext context)
    {
        _logger.LogInformation("Received registration request from TaskManager ID: {TaskManagerId}, Address: {Address}:{Port}", request.TaskManagerId, request.Address, request.Port);

        var tmInfo = new TaskManagerInfo
        {
            TaskManagerId = request.TaskManagerId,
            Address = request.Address,
            Port = request.Port,
            LastHeartbeat = DateTime.UtcNow
        };

        bool added = TaskManagerTracker.RegisteredTaskManagers.TryAdd(request.TaskManagerId, tmInfo);

        if (added)
        {
            _logger.LogInformation("TaskManager {TaskManagerId} registered successfully.", request.TaskManagerId);
            return Task.FromResult(new Proto.RegisterTaskManagerResponse
            {
                Success = true,
                JobManagerId = _jobManagerId
            });
        }
        else
        {
            // Could be a duplicate registration attempt or a concurrency issue
            // For now, treat as success if it's already there and update heartbeat
            if (TaskManagerTracker.RegisteredTaskManagers.TryGetValue(request.TaskManagerId, out var existingTmInfo))
            {
                 existingTmInfo.LastHeartbeat = DateTime.UtcNow; // Update heartbeat on re-registration
                 _logger.LogInformation("TaskManager {TaskManagerId} was already registered. Updated heartbeat.", request.TaskManagerId);
                 return Task.FromResult(new Proto.RegisterTaskManagerResponse
                 {
                     Success = true,
                     JobManagerId = _jobManagerId
                 });
            }
            _logger.LogWarning("Failed to add TaskManager {TaskManagerId} to tracker. Concurrent add issue?", request.TaskManagerId);
            return Task.FromResult(new Proto.RegisterTaskManagerResponse { Success = false });
        }
    }

    public override Task<Proto.HeartbeatResponse> SendHeartbeat(
        Proto.HeartbeatRequest request, ServerCallContext context)
    {
        _logger.LogDebug("Received heartbeat from TaskManager ID: {TaskManagerId}", request.TaskManagerId);

        if (TaskManagerTracker.RegisteredTaskManagers.TryGetValue(request.TaskManagerId, out var tmInfo))
        {
            tmInfo.LastHeartbeat = DateTime.UtcNow;

            if (request.TaskMetrics.Any())
            {
                _logger.LogDebug("Processing {NumMetrics} task metrics from TaskManager {TaskManagerId}", request.TaskMetrics.Count, request.TaskManagerId);
            }

            foreach (var metricData in request.TaskMetrics)
            {
                // Example TaskId format: "JobGUID_JobVertexGUID_SubtaskIndex"
                // For this PoC, let's assume TaskId from TM is "JobVertexGUID_SubtaskIndex"
                // and we need to find which JobGraph it belongs to by iterating or a lookup table.
                // This is inefficient. A better TaskId would include JobGUID directly.
                // For now, we parse what we expect: JobVertexId_SubtaskIndex.
                // The JobVertexId itself is a Guid.

                string jobVertexIdStr = "";
                // int subtaskIndex = -1; // Not directly used for storing raw TaskInstanceMetrics, but vital for aggregation.

                // Find the JobGraph that contains this vertex. This is the tricky part without a direct JobId in TaskMetricData.
                // We iterate all known JobGraphs. This is NOT scalable.
                // A better approach: TaskManager should report JobId with metrics, or TaskId should be globally unique and include JobId.
                global::FlinkDotNet.JobManager.Models.JobGraph.JobGraph? targetJobGraph = null;
                Guid jobVertexGuid = Guid.Empty;

                // Assuming metricData.TaskId is JobVertexId_SubtaskIndex
                // We need to extract JobVertexId part and find which JobGraph contains it.
                // For simplicity, let's assume metricData.TaskId IS the JobVertexId for now for updating TaskInstanceMetrics,
                // and the actual JobVertexId_SubtaskIndex is the key for TaskInstanceMetrics.
                // This needs a more robust TaskId scheme.

                // Let's refine: Assume TaskManager sends TaskId as "JobGUID/JobVertexGUID_SubtaskIndex"
                // Or, for now, that TaskMetricData includes a JobId field (needs proto change).
                // Given current proto, TaskId is "JobVertexId_SubtaskIndex".
                // We must iterate JobGraphs to find which one contains this JobVertexId.

                string[] parts = metricData.TaskId.Split('_');
                if (parts.Length >= 1) // At least JobVertexId should be there
                {
                    jobVertexIdStr = parts[0];
                    // if (parts.Length > 1) int.TryParse(parts[1], out subtaskIndex);
                }

                if (Guid.TryParse(jobVertexIdStr, out jobVertexGuid))
                {
                    foreach (var jobGraphPair in JobManagerController._jobGraphs) // Accessing static member
                    {
                        if (jobGraphPair.Value.Vertices.Any(v => v.Id == jobVertexGuid))
                        {
                            targetJobGraph = jobGraphPair.Value;
                            break;
                        }
                    }
                }

                if (targetJobGraph != null)
                {
                    // Store the raw TaskMetric from TaskManager.
                    // Key for TaskInstanceMetrics is "JobVertexId_SubtaskIndex" which is metricData.TaskId
                    targetJobGraph.TaskInstanceMetrics.AddOrUpdate(
                        metricData.TaskId, // Key: "JobVertexId_SubtaskIndex"
                        addValueFactory: _ => new FlinkDotNet.JobManager.Models.TaskMetrics
                        {
                            TaskId = metricData.TaskId,
                            RecordsIn = metricData.RecordsIn,
                            RecordsOut = metricData.RecordsOut
                        },
                        updateValueFactory: (_, existingMetrics) =>
                        {
                            existingMetrics.RecordsIn = metricData.RecordsIn; // Assume cumulative from TM
                            existingMetrics.RecordsOut = metricData.RecordsOut;
                            return existingMetrics;
                        }
                    );
                    _logger.LogTrace("Updated metrics for task {TaskId} in Job {JobId}: In={RecordsIn}, Out={RecordsOut}", metricData.TaskId, targetJobGraph.JobId, metricData.RecordsIn, metricData.RecordsOut);
                }
                else
                {
                    _logger.LogDebug("Could not find JobGraph for task metric: {TaskId}", metricData.TaskId);
                }
            }
            return Task.FromResult(new Proto.HeartbeatResponse { Acknowledged = true });
        }
        else
        {
            _logger.LogWarning("Heartbeat from unknown TaskManager ID: {TaskManagerId}. Registration might be lost.", request.TaskManagerId);
            return Task.FromResult(new Proto.HeartbeatResponse { Acknowledged = false }); // Ask TM to re-register
        }
    }

    public override Task<Proto.AcknowledgeCheckpointResponse> AcknowledgeCheckpoint(
        Proto.AcknowledgeCheckpointRequest request, ServerCallContext context)
    {
        _logger.LogInformation("Received AcknowledgeCheckpoint for JobID: {JobId}, CheckpointID: {CheckpointId}, TM: {TaskManagerId}", request.JobId, request.CheckpointId, request.TaskManagerId);
        if (JobCoordinators.TryGetValue(request.JobId, out var coordinator))
        {
            coordinator.AcknowledgeCheckpoint(request.JobId, request.CheckpointId, request.TaskManagerId, request.SnapshotHandle /*, pass other details like size, duration */);
            return Task.FromResult(new Proto.AcknowledgeCheckpointResponse { Success = true });
        }
        else
        {
            _logger.LogWarning("No CheckpointCoordinator found for JobID: {JobId}. Cannot acknowledge checkpoint for CP {CheckpointId} from TM {TaskManagerId}.", request.JobId, request.CheckpointId, request.TaskManagerId);
            return Task.FromResult(new Proto.AcknowledgeCheckpointResponse { Success = false });
        }
    }
}
#nullable disable
