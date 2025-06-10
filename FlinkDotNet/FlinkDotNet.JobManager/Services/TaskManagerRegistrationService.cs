using System.Collections.Concurrent;
using Grpc.Core;
using FlinkDotNet.Proto.Internal;
using FlinkDotNet.JobManager.Models; // For TaskManagerInfo
using FlinkDotNet.JobManager.Controllers;
using FlinkDotNet.JobManager.Checkpointing; // For CheckpointCoordinator

namespace FlinkDotNet.JobManager.Services
{
    // Simple tracker for registered TaskManagers (replace with a more robust solution later)
    // This could be moved to its own file or an infrastructure layer.
    public static class TaskManagerTracker
    {
        public static ConcurrentDictionary<string, TaskManagerInfo> RegisteredTaskManagers { get; } = new();
    }

    // TaskManagerInfo class has been moved to FlinkDotNet.JobManager.Models/TaskManagerInfo.cs

    public class TaskManagerRegistrationServiceImpl : TaskManagerRegistration.TaskManagerRegistrationBase
    {
    private readonly string _jobManagerId = $"JM-{Guid.NewGuid()}"; // Example JobManager ID
    public static ConcurrentDictionary<string, CheckpointCoordinator> JobCoordinators { get; } = new();
    private readonly ILogger<TaskManagerRegistrationServiceImpl> _logger;

    public TaskManagerRegistrationServiceImpl(ILogger<TaskManagerRegistrationServiceImpl> logger)
    {
        _logger = logger;
    }

    public override Task<RegisterTaskManagerResponse> RegisterTaskManager(
        RegisterTaskManagerRequest request, ServerCallContext context)
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
            return Task.FromResult(new RegisterTaskManagerResponse
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
                 return Task.FromResult(new RegisterTaskManagerResponse
                 {
                     Success = true,
                     JobManagerId = _jobManagerId
                 });
            }
            _logger.LogWarning("Failed to add TaskManager {TaskManagerId} to tracker. Concurrent add issue?", request.TaskManagerId);
            return Task.FromResult(new RegisterTaskManagerResponse { Success = false });
        }
    }

    public override Task<HeartbeatResponse> SendHeartbeat(
        HeartbeatRequest request, ServerCallContext context)
    {
        _logger.LogDebug("Received heartbeat from TaskManager ID: {TaskManagerId}", request.TaskManagerId);

        if (!TaskManagerTracker.RegisteredTaskManagers.TryGetValue(request.TaskManagerId, out var tmInfo))
        {
            _logger.LogWarning("Heartbeat from unknown TaskManager ID: {TaskManagerId}. Registration might be lost.", request.TaskManagerId);
            return Task.FromResult(new HeartbeatResponse { Acknowledged = false });
        }

        tmInfo.LastHeartbeat = DateTime.UtcNow;

        if (request.TaskMetrics.Any())
        {
            _logger.LogDebug("Processing {NumMetrics} task metrics from TaskManager {TaskManagerId}", request.TaskMetrics.Count, request.TaskManagerId);
        }

        foreach (var metricData in request.TaskMetrics)
        {
            var jobGraph = FindJobGraph(metricData.TaskId);
            if (jobGraph != null)
            {
                UpdateMetrics(jobGraph, metricData);
            }
            else
            {
                _logger.LogDebug("Could not find JobGraph for task metric: {TaskId}", metricData.TaskId);
            }
        }

        return Task.FromResult(new HeartbeatResponse { Acknowledged = true });
    }

    private static global::FlinkDotNet.JobManager.Models.JobGraph.JobGraph? FindJobGraph(string taskId)
    {
        var jobVertexIdStr = taskId.Split('_').FirstOrDefault();
        if (Guid.TryParse(jobVertexIdStr, out var jobVertexGuid))
        {
            return JobManagerController.JobGraphs.Values
                .FirstOrDefault(jg => jg.Vertices.Any(v => v.Id == jobVertexGuid));
        }

        return null;
    }

    private void UpdateMetrics(global::FlinkDotNet.JobManager.Models.JobGraph.JobGraph jobGraph, TaskMetricData metricData)
    {
        jobGraph.TaskInstanceMetrics.AddOrUpdate(
            metricData.TaskId,
            _ => new FlinkDotNet.JobManager.Models.TaskMetrics
            {
                TaskId = metricData.TaskId,
                RecordsIn = metricData.RecordsIn,
                RecordsOut = metricData.RecordsOut
            },
            (_, existingMetrics) =>
            {
                existingMetrics.RecordsIn = metricData.RecordsIn;
                existingMetrics.RecordsOut = metricData.RecordsOut;
                return existingMetrics;
            });

        _logger.LogTrace(
            "Updated metrics for task {TaskId} in Job {JobId}: In={RecordsIn}, Out={RecordsOut}",
            metricData.TaskId,
            jobGraph.JobId,
            metricData.RecordsIn,
            metricData.RecordsOut);
    }

    public override Task<AcknowledgeCheckpointResponse> AcknowledgeCheckpoint(
        AcknowledgeCheckpointRequest request, ServerCallContext context)
    {
        _logger.LogInformation("Received AcknowledgeCheckpoint for JobID: {JobId}, CheckpointID: {CheckpointId}, TM: {TaskManagerId}", request.JobId, request.CheckpointId, request.TaskManagerId);
        if (JobCoordinators.TryGetValue(request.JobId, out var coordinator))
        {
            coordinator.AcknowledgeCheckpoint(request.JobId, request.CheckpointId, request.TaskManagerId, request.SnapshotHandle /*, pass other details like size, duration */);
            return Task.FromResult(new AcknowledgeCheckpointResponse { Success = true });
        }
        else
        {
            _logger.LogWarning("No CheckpointCoordinator found for JobID: {JobId}. Cannot acknowledge checkpoint for CP {CheckpointId} from TM {TaskManagerId}.", request.JobId, request.CheckpointId, request.TaskManagerId);
            return Task.FromResult(new AcknowledgeCheckpointResponse { Success = false });
        }
    }
    }
}
#nullable disable
