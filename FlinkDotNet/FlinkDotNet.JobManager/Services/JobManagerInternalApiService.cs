using System.Threading.Tasks;
using Grpc.Core;
using Microsoft.Extensions.Logging; // For optional logging
using FlinkDotNet.JobManager.Interfaces; // For IJobRepository
using FlinkDotNet.JobManager.Controllers; // For JobManagerController._jobGraphs (temporary)
using FlinkDotNet.JobManager.Models.JobGraph; // For JobGraph
using FlinkDotNet.JobManager.Checkpointing; // For CheckpointCoordinator
using FlinkDotNet.JobManager.Models; // For TaskManagerInfo, JobManagerConfig
using System.Linq; // For Linq operations
using System.Collections.Generic; // For Dictionary
using Grpc.Net.Client; // For GrpcChannel
using System.Text.Json; // For JsonSerializer

// Assuming the generated base class is global::FlinkDotNet.Proto.Internal.JobManagerInternalService.JobManagerInternalServiceBase
// The actual name depends on the .proto service definition and Grpc.Tools generation.

namespace FlinkDotNet.JobManager.Services
{
    public class JobManagerInternalApiService : global::FlinkDotNet.Proto.Internal.JobManagerInternalService.JobManagerInternalServiceBase
    {
        private readonly ILogger<JobManagerInternalApiService> _logger;
        private readonly IJobRepository _jobRepository;
        private readonly ILoggerFactory _loggerFactory;

        public JobManagerInternalApiService(
            ILogger<JobManagerInternalApiService> logger,
            IJobRepository jobRepository,
            ILoggerFactory loggerFactory)
        {
            _logger = logger;
            _jobRepository = jobRepository;
            _loggerFactory = loggerFactory;
        }

        // Convenience constructor used in unit tests
        public JobManagerInternalApiService(ILogger<JobManagerInternalApiService> logger)
            : this(logger,
                   new InMemoryJobRepository(Microsoft.Extensions.Logging.Abstractions.NullLogger<InMemoryJobRepository>.Instance),
                   Microsoft.Extensions.Logging.Abstractions.NullLoggerFactory.Instance)
        {
        }

        public override Task<global::FlinkDotNet.Proto.Internal.SubmitJobReply> SubmitJob(global::FlinkDotNet.Proto.Internal.SubmitJobRequest request, ServerCallContext context)
        {
            _logger.LogInformation("gRPC: SubmitJob called.");

            if (request.JobGraph == null)
            {
                _logger.LogWarning("Received global::FlinkDotNet.Proto.Internal.SubmitJobRequest with null JobGraph.");
                return Task.FromResult(new global::FlinkDotNet.Proto.Internal.SubmitJobReply
                {
                    Success = false,
                    Message = "JobGraph cannot be null.",
                    JobId = ""
                });
            }

            try
            {
                // Convert global::FlinkDotNet.Proto.Internal.Internal.JobGraph to C# model JobGraph
                // This exercises the FromProto methods.
                var jobGraphModel = Models.JobGraph.JobGraph.FromProto(request.JobGraph);

                _logger.LogInformation($"Successfully converted submitted JobGraph. JobName: '{jobGraphModel.JobName}', JobId (model): {jobGraphModel.JobId}");
                _logger.LogInformation($"Number of vertices: {jobGraphModel.Vertices.Count}");
                _logger.LogInformation($"Number of edges: {jobGraphModel.Edges.Count}");
                foreach(var vertex in jobGraphModel.Vertices)
                {
                    _logger.LogInformation($"Vertex: {vertex.Name} (ID: {vertex.Id}, Type: {vertex.Type}, Op: {vertex.OperatorDefinition.FullyQualifiedName}, Parallelism: {vertex.Parallelism})");
                }
                 foreach(var edge in jobGraphModel.Edges)
                {
                    _logger.LogInformation($"Edge: {edge.Id} from {edge.SourceVertexId} to {edge.TargetVertexId} (Mode: {edge.ShuffleMode})");
                }
                bool stored = JobManagerController._jobGraphs.TryAdd(jobGraphModel.JobId, jobGraphModel);
                if (stored)
                {
                    _logger.LogInformation($"JobGraph for '{jobGraphModel.JobName}' (ID: {jobGraphModel.JobId}) stored in static dictionary.");
                }
                else
                {
                    _logger.LogWarning($"Failed to store JobGraph for '{jobGraphModel.JobName}' (ID: {jobGraphModel.JobId}) in static dictionary. It might already exist.");
                    // Potentially return error if this is unexpected
                }
                // --- Replicated Task Deployment Logic ---
                _logger.LogInformation($"Job '{jobGraphModel.JobName}' (ID: {jobGraphModel.JobId}): Preparing for task deployment...");

                var coordinatorConfig = new JobManagerConfig { /* Populate as needed, e.g., from request or global config */ };
                var coordinatorLogger = _loggerFactory.CreateLogger<CheckpointCoordinator>();
                var checkpointCoordinator = new CheckpointCoordinator(jobGraphModel.JobId.ToString(), _jobRepository, coordinatorLogger, coordinatorConfig);

                // Using TaskManagerRegistrationServiceImpl.JobCoordinators as per existing pattern in JobManagerController
                if (TaskManagerRegistrationServiceImpl.JobCoordinators.TryAdd(jobGraphModel.JobId.ToString(), checkpointCoordinator))
                {
                    checkpointCoordinator.Start();
                    _logger.LogInformation("CheckpointCoordinator for job {JobId} created, added to static list, and started.", jobGraphModel.JobId);
                }
                else
                {
                    _logger.LogWarning($"Could not add/start CheckpointCoordinator for job {jobGraphModel.JobId}. It might already exist.");
                }

                var taskAssignments = new Dictionary<string, (TaskManagerInfo tm, int subtaskIndex)>();
                var tmAssignmentIndex = 0;
                var availableTaskManagers = TaskManagerTracker.RegisteredTaskManagers.Values.ToList();

                if (!availableTaskManagers.Any())
                {
                    _logger.LogWarning($"No TaskManagers available to deploy job {jobGraphModel.JobName}. Job submitted but not deployed.");
                    // Decide on reply: success true but with warning, or success false?
                    // For now, let's consider it a partial success as the job is "submitted" but not deployed.
                    return Task.FromResult(new global::FlinkDotNet.Proto.Internal.SubmitJobReply
                    {
                        Success = true, // Or false, depending on desired strictness
                        Message = "Job submitted but no TaskManagers available for deployment.",
                        JobId = jobGraphModel.JobId.ToString()
                    });
                }

                foreach (var jobVertex in jobGraphModel.Vertices)
                {
                    for (int i = 0; i < jobVertex.Parallelism; i++)
                    {
                        var assignedTm = availableTaskManagers[tmAssignmentIndex % availableTaskManagers.Count];
                        string taskInstanceId = $"{jobVertex.Id}_{i}";
                        taskAssignments[taskInstanceId] = (assignedTm, i);
                        tmAssignmentIndex++;
                    }
                }
                _logger.LogInformation("Pre-assigned all task instances to TaskManagers for job {JobId}.", jobGraphModel.JobId);

                foreach (var vertex in jobGraphModel.Vertices)
                {
                    for (int i = 0; i < vertex.Parallelism; i++)
                    {
                        string currentTaskInstanceId = $"{vertex.Id}_{i}";
                        var (targetTm, subtaskIdx) = taskAssignments[currentTaskInstanceId];

                        var tdd = new global::FlinkDotNet.Proto.Internal.TaskDeploymentDescriptor
                        {
                            JobGraphJobId = jobGraphModel.JobId.ToString(),
                            JobVertexId = vertex.Id.ToString(),
                            SubtaskIndex = i,
                            TaskName = $"{vertex.Name} ({i + 1}/{vertex.Parallelism})",
                            FullyQualifiedOperatorName = vertex.OperatorDefinition.FullyQualifiedName,
                            OperatorConfiguration = Google.Protobuf.ByteString.CopyFromUtf8(
                                JsonSerializer.Serialize(vertex.OperatorDefinition.ConfigurationJson) // Assuming ConfigurationJson is the string property
                            ),
                            InputTypeName = vertex.InputTypeName ?? "",
                            OutputTypeName = vertex.OutputTypeName ?? "",
                            InputSerializerTypeName = vertex.InputSerializerTypeName ?? "",
                            OutputSerializerTypeName = vertex.OutputSerializerTypeName ?? ""
                        };

                        _logger.LogInformation($"Deploying task '{tdd.TaskName}' for vertex {vertex.Name} (ID: {vertex.Id}) to TaskManager {targetTm.TaskManagerId} ({targetTm.Address}:{targetTm.Port}) for job {jobGraphModel.JobId}");

                        try
                        {
                            var channelAddress = $"http://{targetTm.Address}:{targetTm.Port}";
                            using var channel = GrpcChannel.ForAddress(channelAddress);
                            var client = new global::FlinkDotNet.Proto.Internal.TaskExecution.TaskExecutionClient(channel);
                            _ = client.DeployTaskAsync(tdd, deadline: System.DateTime.UtcNow.AddSeconds(10)); // Fire and forget for now
                            _logger.LogDebug($"DeployTask call initiated for '{tdd.TaskName}' to TM {targetTm.TaskManagerId}.");
                        }
                        catch (System.Exception ex)
                        {
                            _logger.LogError(ex, $"Failed to send DeployTask for '{tdd.TaskName}' to TM {targetTm.TaskManagerId}: {ex.Message}");
                            // Potentially collect failures and report them
                        }
                    }
                }
                _logger.LogInformation("All tasks for job {JobName} (ID: {JobId}) have been (attempted) deployed.", jobGraphModel.JobName, jobGraphModel.JobId);
                // --- End of Replicated Task Deployment Logic ---

                return Task.FromResult(new global::FlinkDotNet.Proto.Internal.SubmitJobReply
                {
                    Success = true,
                    Message = $"Job '{jobGraphModel.JobName}' submitted and deployment initiated.",
                    JobId = jobGraphModel.JobId.ToString()
                });
            }
            catch (System.Exception ex)
            {
                _logger.LogError(ex, "Error processing global::FlinkDotNet.Proto.Internal.SubmitJobRequest.");
                return Task.FromResult(new global::FlinkDotNet.Proto.Internal.SubmitJobReply
                {
                    Success = false,
                    Message = $"Error processing job: {ex.Message}",
                    JobId = ""
                });
            }
        }


        public override Task<global::FlinkDotNet.Proto.Internal.ReportStateCompletionReply> ReportStateCompletion(global::FlinkDotNet.Proto.Internal.ReportStateCompletionRequest request, ServerCallContext context)
        {
            _logger.LogInformation($"gRPC: ReportStateCompletion called for CheckpointId: {request.CheckpointId}, Operator: {request.OperatorInstanceId}");
            // In a real implementation, process the state completion.
            return Task.FromResult(new global::FlinkDotNet.Proto.Internal.ReportStateCompletionReply
            {
                Ack = true
            });
        }

        public override Task<global::FlinkDotNet.Proto.Internal.RequestCheckpointReply> RequestCheckpoint(global::FlinkDotNet.Proto.Internal.RequestCheckpointRequest request, ServerCallContext context)
        {
            _logger.LogInformation($"gRPC: RequestCheckpoint called for CheckpointId: {request.CheckpointId}");
            // In a real implementation, this would trigger checkpointing logic on a TaskManager.
            return Task.FromResult(new global::FlinkDotNet.Proto.Internal.RequestCheckpointReply
            {
                Accepted = true
            });
        }

        public override Task<global::FlinkDotNet.Proto.Internal.RequestRecoveryReply> RequestRecovery(global::FlinkDotNet.Proto.Internal.RequestRecoveryRequest request, ServerCallContext context)
        {
            _logger.LogInformation($"gRPC: RequestRecovery called for JobId: {request.JobId}, CheckpointId: {request.CheckpointId}");
            // In a real implementation, this would initiate recovery procedures.
            return Task.FromResult(new global::FlinkDotNet.Proto.Internal.RequestRecoveryReply
            {
                RecoveryInitiated = true
            });
        }

        public override Task<global::FlinkDotNet.Proto.Internal.JobManagerHeartbeatReply> Heartbeat(global::FlinkDotNet.Proto.Internal.JobManagerHeartbeatRequest request, ServerCallContext context) // Types updated
        {
            _logger.LogInformation($"gRPC: Heartbeat received from JobId: {request.JobId}, Operator: {request.OperatorInstanceId}, Status: {request.HealthStatus}");
            // In a real implementation, update heartbeat status, check for timeouts, etc.
            return Task.FromResult(new global::FlinkDotNet.Proto.Internal.JobManagerHeartbeatReply // Type updated
            {
                Ack = true
            });
        }

        public override Task<global::FlinkDotNet.Proto.Internal.ReportFailedCheckpointResponse> ReportFailedCheckpoint(global::FlinkDotNet.Proto.Internal.ReportFailedCheckpointRequest request, ServerCallContext context)
        {
            _logger.LogWarning($"gRPC: ReportFailedCheckpoint called for JobId: {request.JobId}, CheckpointId: {request.CheckpointId}, Task: {request.JobVertexId}_{request.SubtaskIndex}, TM: {request.TaskManagerId}, Reason: {request.FailureReason}");

            // In a real implementation, this would trigger checkpoint cancellation or recovery logic.
            // For now, just acknowledge the report.
            // Example: Find the CheckpointCoordinator for the job and notify it.
            // if (TaskManagerRegistrationServiceImpl.JobCoordinators.TryGetValue(request.JobId, out var coordinator))
            // {
            //     coordinator.HandleFailedCheckpoint(request.CheckpointId, request.JobVertexId, request.SubtaskIndex, request.FailureReason);
            // }
            // else
            // {
            //     _logger.LogError($"Could not find CheckpointCoordinator for JobId {request.JobId} to report failed checkpoint {request.CheckpointId}.");
            // }

            return Task.FromResult(new global::FlinkDotNet.Proto.Internal.ReportFailedCheckpointResponse
            {
                Acknowledged = true
            });
        }

        public override Task<global::FlinkDotNet.Proto.Internal.ReportTaskStartupFailureResponse> ReportTaskStartupFailure(
            global::FlinkDotNet.Proto.Internal.ReportTaskStartupFailureRequest request, ServerCallContext context)
        {
            _logger.LogWarning($"[JobManager] Received task startup failure report from TM {request.TaskManagerId} for Job {request.JobId}, Task {request.JobVertexId}_{request.SubtaskIndex}. Reason: {request.FailureReason}");
            // For now, just log and acknowledge.
            // Example: _jobRepository.UpdateTaskStatus(request.JobId, request.JobVertexId, request.SubtaskIndex, JobStatus.Failed, request.FailureReason);
            return Task.FromResult(new global::FlinkDotNet.Proto.Internal.ReportTaskStartupFailureResponse { Acknowledged = true });
        }
    }
}
