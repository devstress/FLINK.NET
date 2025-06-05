#nullable enable
using System;
using System.Collections.Concurrent;
using System.Threading.Tasks;
using Grpc.Core;
using FlinkDotNet.Proto.Internal; // Namespace from your .proto file
using FlinkDotNet.JobManager.Models; // For TaskManagerInfo
using FlinkDotNet.JobManager.Checkpointing; // For CheckpointCoordinator

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


    public override Task<RegisterTaskManagerResponse> RegisterTaskManager(
        RegisterTaskManagerRequest request, ServerCallContext context)
    {
        Console.WriteLine($"Received registration request from TaskManager ID: {request.TaskManagerId}, Address: {request.Address}:{request.Port}");

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
            Console.WriteLine($"TaskManager {request.TaskManagerId} registered successfully.");
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
                 Console.WriteLine($"TaskManager {request.TaskManagerId} was already registered. Updated heartbeat.");
                 return Task.FromResult(new RegisterTaskManagerResponse
                 {
                     Success = true,
                     JobManagerId = _jobManagerId
                 });
            }
            Console.WriteLine($"Failed to add TaskManager {request.TaskManagerId} to tracker. Concurrent add issue?");
            return Task.FromResult(new RegisterTaskManagerResponse { Success = false });
        }
    }

    public override Task<HeartbeatResponse> SendHeartbeat(
        HeartbeatRequest request, ServerCallContext context)
    {
        // Console.WriteLine($"Received heartbeat from TaskManager ID: {request.TaskManagerId}"); // Can be too verbose

        if (TaskManagerTracker.RegisteredTaskManagers.TryGetValue(request.TaskManagerId, out var tmInfo))
        {
            tmInfo.LastHeartbeat = DateTime.UtcNow;
            return Task.FromResult(new HeartbeatResponse { Acknowledged = true });
        }
        else
        {
            Console.WriteLine($"Heartbeat from unknown TaskManager ID: {request.TaskManagerId}. Registration might be lost.");
            return Task.FromResult(new HeartbeatResponse { Acknowledged = false }); // Ask TM to re-register
        }
    }

    public override Task<AcknowledgeCheckpointResponse> AcknowledgeCheckpoint(
        AcknowledgeCheckpointRequest request, ServerCallContext context)
    {
        Console.WriteLine($"Received AcknowledgeCheckpoint for JobID: {request.JobId}, CheckpointID: {request.CheckpointId}, TM: {request.TaskManagerId}");
        if (JobCoordinators.TryGetValue(request.JobId, out var coordinator))
        {
            coordinator.AcknowledgeCheckpoint(request.JobId, request.CheckpointId, request.TaskManagerId, request.SnapshotHandle /*, pass other details like size, duration */);
            return Task.FromResult(new AcknowledgeCheckpointResponse { Success = true });
        }
        else
        {
            Console.WriteLine($"No CheckpointCoordinator found for JobID: {request.JobId}. Cannot acknowledge checkpoint.");
            return Task.FromResult(new AcknowledgeCheckpointResponse { Success = false });
        }
    }
}
#nullable disable
