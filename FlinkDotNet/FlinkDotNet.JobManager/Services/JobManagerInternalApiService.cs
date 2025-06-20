using Grpc.Core;
using FlinkDotNet.JobManager.Interfaces; // For IJobRepository
using FlinkDotNet.JobManager.Controllers; // For JobManagerController.JobGraphs
using FlinkDotNet.JobManager.Models.JobGraph; // For JobGraph
using FlinkDotNet.JobManager.Checkpointing; // For CheckpointCoordinator
using FlinkDotNet.JobManager.Models; // For TaskManagerInfo, JobManagerConfig
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

        private void LogGraphDetails(JobGraph jobGraph)
        {
            foreach (var vertex in jobGraph.Vertices)
            {
                _logger.LogDebug(
                    "Vertex: {Name} (ID: {Id}, Type: {Type}, Op: {Operator}, Parallelism: {Parallelism})",
                    vertex.Name,
                    vertex.Id,
                    vertex.Type,
                    vertex.OperatorDefinition.FullyQualifiedName,
                    vertex.Parallelism);
            }

            foreach (var edge in jobGraph.Edges)
            {
                _logger.LogDebug(
                    "Edge: {EdgeId} from {Source} to {Target} (Mode: {Mode})",
                    edge.Id,
                    edge.SourceVertexId,
                    edge.TargetVertexId,
                    edge.ShuffleMode);
            }
        }

        private static Dictionary<string, (TaskManagerInfo tm, int subtaskIndex)> AssignTasks(JobGraph jobGraph, List<TaskManagerInfo> availableTaskManagers)
        {
            var taskAssignments = new Dictionary<string, (TaskManagerInfo tm, int subtaskIndex)>();
            var tmAssignmentIndex = 0;

            foreach (var jobVertex in jobGraph.Vertices)
            {
                for (int i = 0; i < jobVertex.Parallelism; i++)
                {
                    var assignedTm = availableTaskManagers[tmAssignmentIndex % availableTaskManagers.Count];
                    string taskInstanceId = $"{jobVertex.Id}_{i}";
                    taskAssignments[taskInstanceId] = (assignedTm, i);
                    tmAssignmentIndex++;
                }
            }

            return taskAssignments;
        }

        private void DeployTasks(JobGraph jobGraph, Dictionary<string, (TaskManagerInfo tm, int subtaskIndex)> taskAssignments)
        {
            foreach (var vertex in jobGraph.Vertices)
            {
                for (int i = 0; i < vertex.Parallelism; i++)
                {
                    string currentTaskInstanceId = $"{vertex.Id}_{i}";
                    var (targetTm, _) = taskAssignments[currentTaskInstanceId];

                    var tdd = new global::FlinkDotNet.Proto.Internal.TaskDeploymentDescriptor
                    {
                        JobGraphJobId = jobGraph.JobId.ToString(),
                        JobVertexId = vertex.Id.ToString(),
                        SubtaskIndex = i,
                        TaskName = $"{vertex.Name} ({i + 1}/{vertex.Parallelism})",
                        FullyQualifiedOperatorName = vertex.OperatorDefinition.FullyQualifiedName,
                        OperatorConfiguration = Google.Protobuf.ByteString.CopyFromUtf8(
                            JsonSerializer.Serialize(vertex.OperatorDefinition.ConfigurationJson)),
                        InputTypeName = vertex.InputTypeName ?? string.Empty,
                        OutputTypeName = vertex.OutputTypeName ?? string.Empty,
                        InputSerializerTypeName = vertex.InputSerializerTypeName ?? string.Empty,
                        OutputSerializerTypeName = vertex.OutputSerializerTypeName ?? string.Empty
                    };

                    _logger.LogDebug(
                        "Deploying task '{TaskName}' for vertex {VertexName} (ID: {VertexId}) to TaskManager {TaskManagerId} ({Address}:{Port}) for job {JobId}",
                        tdd.TaskName,
                        vertex.Name,
                        vertex.Id,
                        targetTm.TaskManagerId,
                        targetTm.Address,
                        targetTm.Port,
                        jobGraph.JobId);

                    try
                    {
                        // Use Aspire service discovery for TaskManager communication
                        var channelAddress = ResolveTaskManagerAddress(targetTm);
                        using var channel = GrpcChannel.ForAddress(channelAddress);
                        var client = new global::FlinkDotNet.Proto.Internal.TaskExecution.TaskExecutionClient(channel);
                        _ = client.DeployTaskAsync(tdd, deadline: System.DateTime.UtcNow.AddSeconds(10));
                        _logger.LogDebug(
                            "DeployTask call initiated for '{TaskName}' to TM {TaskManagerId} at {ChannelAddress}.",
                            tdd.TaskName,
                            targetTm.TaskManagerId,
                            channelAddress);
                    }
                    catch (System.Exception ex)
                    {
                        _logger.LogError(
                            ex,
                            "Failed to send DeployTask for '{TaskName}' to TM {TaskManagerId}: {ErrorMessage}",
                            tdd.TaskName,
                            targetTm.TaskManagerId,
                            ex.Message);
                    }
                }
            }

            _logger.LogInformation(
                "All tasks for job {JobName} (ID: {JobId}) have been (attempted) deployed.",
                jobGraph.JobName,
                jobGraph.JobId);
        }

        /// <summary>
        /// Resolve TaskManager address using Aspire service discovery
        /// </summary>
        private string ResolveTaskManagerAddress(TaskManagerInfo taskManager)
        {
            try
            {
                // In Aspire environment, use service name from registered address
                var serviceName = taskManager.Address;
                
                // Check if this is a service name pattern (taskmanager1, taskmanager2, etc.)
                if (serviceName.StartsWith("taskmanager") && char.IsDigit(serviceName.Last()))
                {
                    // Try Aspire service discovery environment variables
                    var aspireHttpsUrl = Environment.GetEnvironmentVariable($"services__{serviceName}__https__0");
                    if (!string.IsNullOrEmpty(aspireHttpsUrl))
                    {
                        _logger.LogDebug("Using Aspire HTTPS service discovery for {ServiceName}: {Url}", serviceName, aspireHttpsUrl);
                        return aspireHttpsUrl;
                    }

                    var aspireHttpUrl = Environment.GetEnvironmentVariable($"services__{serviceName}__http__0");
                    if (!string.IsNullOrEmpty(aspireHttpUrl))
                    {
                        _logger.LogDebug("Using Aspire HTTP service discovery for {ServiceName}: {Url}", serviceName, aspireHttpUrl);
                        return aspireHttpUrl;
                    }

                    // Try alternative Aspire patterns
                    var aspireConnectionString = Environment.GetEnvironmentVariable($"ConnectionStrings__{serviceName}");
                    if (!string.IsNullOrEmpty(aspireConnectionString))
                    {
                        _logger.LogDebug("Using Aspire connection string for {ServiceName}: {Url}", serviceName, aspireConnectionString);
                        return aspireConnectionString;
                    }
                }

                // Determine protocol based on environment
                var allowUnsecured = Environment.GetEnvironmentVariable("ASPIRE_ALLOW_UNSECURED_TRANSPORT");
                var protocol = string.Equals(allowUnsecured, "true", StringComparison.OrdinalIgnoreCase) ? "http" : "https";
                
                // Fallback to registered address and port
                var fallbackAddress = $"{protocol}://{taskManager.Address}:{taskManager.Port}";
                _logger.LogDebug("Using fallback address for TaskManager {TaskManagerId}: {Address}", taskManager.TaskManagerId, fallbackAddress);
                return fallbackAddress;
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "Error resolving TaskManager address for {TaskManagerId}, using fallback", taskManager.TaskManagerId);
                
                // Final fallback
                var allowUnsecured = Environment.GetEnvironmentVariable("ASPIRE_ALLOW_UNSECURED_TRANSPORT");
                var protocol = string.Equals(allowUnsecured, "true", StringComparison.OrdinalIgnoreCase) ? "http" : "https";
                return $"{protocol}://{taskManager.Address}:{taskManager.Port}";
            }
        }

        public override Task<global::FlinkDotNet.Proto.Internal.SubmitJobReply> SubmitJob(global::FlinkDotNet.Proto.Internal.SubmitJobRequest request, ServerCallContext context)
        {
            _logger.LogInformation("gRPC: SubmitJob called.");

            if (request.JobGraph == null)
            {
                return Task.FromResult(CreateErrorReply("JobGraph cannot be null.", ""));
            }

            try
            {
                var jobGraphModel = ConvertAndStoreJobGraph(request.JobGraph);
                var checkpointCoordinator = CreateAndStartCheckpointCoordinator(jobGraphModel);
                _logger.LogDebug("CheckpointCoordinator created for job {JobId}: {CoordinatorId}", 
                    jobGraphModel.JobId, checkpointCoordinator.GetHashCode());
                var deploymentResult = AttemptJobDeployment(jobGraphModel);
                
                return Task.FromResult(deploymentResult);
            }
            catch (System.Exception ex)
            {
                _logger.LogError(ex, "Error processing global::FlinkDotNet.Proto.Internal.SubmitJobRequest.");
                return Task.FromResult(CreateErrorReply($"Error processing job: {ex.Message}", ""));
            }
        }

        private global::FlinkDotNet.Proto.Internal.SubmitJobReply CreateErrorReply(string message, string jobId)
        {
            _logger.LogWarning("Creating error reply: {Message}", message);
            return new global::FlinkDotNet.Proto.Internal.SubmitJobReply
            {
                Success = false,
                Message = message,
                JobId = jobId
            };
        }

        private Models.JobGraph.JobGraph ConvertAndStoreJobGraph(global::FlinkDotNet.Proto.Internal.JobGraph protoJobGraph)
        {
            var jobGraphModel = Models.JobGraph.JobGraph.FromProto(protoJobGraph);

            _logger.LogInformation(
                "Converted JobGraph '{JobName}' (ID: {JobId}) with {VertexCount} vertices and {EdgeCount} edges.",
                jobGraphModel.JobName,
                jobGraphModel.JobId,
                jobGraphModel.Vertices.Count,
                jobGraphModel.Edges.Count);
            
            LogGraphDetails(jobGraphModel);
            
            bool stored = JobManagerController.JobGraphs.TryAdd(jobGraphModel.JobId, jobGraphModel);
            if (stored)
            {
                _logger.LogInformation(
                    "JobGraph for '{JobName}' (ID: {JobId}) stored in static dictionary.",
                    jobGraphModel.JobName,
                    jobGraphModel.JobId);
            }
            else
            {
                _logger.LogWarning("Failed to store JobGraph for '{JobName}' (ID: {JobId}) in static dictionary. It might already exist.", jobGraphModel.JobName, jobGraphModel.JobId);
            }

            return jobGraphModel;
        }

        private CheckpointCoordinator CreateAndStartCheckpointCoordinator(Models.JobGraph.JobGraph jobGraphModel)
        {
            _logger.LogDebug(
                "Job '{JobName}' (ID: {JobId}): Preparing for task deployment...",
                jobGraphModel.JobName,
                jobGraphModel.JobId);

            var coordinatorConfig = new JobManagerConfig { /* Populate as needed, e.g., from request or global config */ };
            var coordinatorLogger = _loggerFactory.CreateLogger<CheckpointCoordinator>();
            var checkpointCoordinator = new CheckpointCoordinator(jobGraphModel.JobId.ToString(), _jobRepository, coordinatorLogger, coordinatorConfig);

            if (TaskManagerRegistrationServiceImpl.JobCoordinators.TryAdd(jobGraphModel.JobId.ToString(), checkpointCoordinator))
            {
                checkpointCoordinator.Start();
                _logger.LogInformation("CheckpointCoordinator for job {JobId} created, added to static list, and started.", jobGraphModel.JobId);
            }
            else
            {
                _logger.LogWarning("Could not add/start CheckpointCoordinator for job {JobId}. It might already exist.", jobGraphModel.JobId);
            }

            return checkpointCoordinator;
        }

        private global::FlinkDotNet.Proto.Internal.SubmitJobReply AttemptJobDeployment(Models.JobGraph.JobGraph jobGraphModel)
        {
            var availableTaskManagers = TaskManagerTracker.RegisteredTaskManagers.Values.ToList();

            if (!availableTaskManagers.Any())
            {
                _logger.LogWarning("No TaskManagers available to deploy job {JobName}. Job submitted but not deployed.", jobGraphModel.JobName);
                return new global::FlinkDotNet.Proto.Internal.SubmitJobReply
                {
                    Success = true,
                    Message = "Job submitted but no TaskManagers available for deployment.",
                    JobId = jobGraphModel.JobId.ToString()
                };
            }

            var taskAssignments = AssignTasks(jobGraphModel, availableTaskManagers);
            _logger.LogInformation(
                "Pre-assigned all task instances to TaskManagers for job {JobId}.",
                jobGraphModel.JobId);
            
            DeployTasks(jobGraphModel, taskAssignments);

            return new global::FlinkDotNet.Proto.Internal.SubmitJobReply
            {
                Success = true,
                Message = $"Job '{jobGraphModel.JobName}' submitted and deployment initiated.",
                JobId = jobGraphModel.JobId.ToString()
            };
        }


        public override Task<global::FlinkDotNet.Proto.Internal.ReportStateCompletionReply> ReportStateCompletion(global::FlinkDotNet.Proto.Internal.ReportStateCompletionRequest request, ServerCallContext context)
        {
            _logger.LogInformation(
                "gRPC: ReportStateCompletion called for CheckpointId: {CheckpointId}, Operator: {Operator}",
                request.CheckpointId,
                request.OperatorInstanceId);
            // In a real implementation, process the state completion.
            return Task.FromResult(new global::FlinkDotNet.Proto.Internal.ReportStateCompletionReply
            {
                Ack = true
            });
        }

        public override Task<global::FlinkDotNet.Proto.Internal.RequestCheckpointReply> RequestCheckpoint(global::FlinkDotNet.Proto.Internal.RequestCheckpointRequest request, ServerCallContext context)
        {
            _logger.LogInformation(
                "gRPC: RequestCheckpoint called for CheckpointId: {CheckpointId}",
                request.CheckpointId);
            // In a real implementation, this would trigger checkpointing logic on a TaskManager.
            return Task.FromResult(new global::FlinkDotNet.Proto.Internal.RequestCheckpointReply
            {
                Accepted = true
            });
        }

        public override Task<global::FlinkDotNet.Proto.Internal.RequestRecoveryReply> RequestRecovery(global::FlinkDotNet.Proto.Internal.RequestRecoveryRequest request, ServerCallContext context)
        {
            _logger.LogInformation(
                "gRPC: RequestRecovery called for JobId: {JobId}, CheckpointId: {CheckpointId}",
                request.JobId,
                request.CheckpointId);
            // In a real implementation, this would initiate recovery procedures.
            return Task.FromResult(new global::FlinkDotNet.Proto.Internal.RequestRecoveryReply
            {
                RecoveryInitiated = true
            });
        }

        public override Task<global::FlinkDotNet.Proto.Internal.JobManagerHeartbeatReply> Heartbeat(global::FlinkDotNet.Proto.Internal.JobManagerHeartbeatRequest request, ServerCallContext context) // Types updated
        {
            _logger.LogInformation(
                "gRPC: Heartbeat received from JobId: {JobId}, Operator: {OperatorInstanceId}, Status: {HealthStatus}",
                request.JobId,
                request.OperatorInstanceId,
                request.HealthStatus);
            // In a real implementation, update heartbeat status, check for timeouts, etc.
            return Task.FromResult(new global::FlinkDotNet.Proto.Internal.JobManagerHeartbeatReply // Type updated
            {
                Ack = true
            });
        }

        public override Task<global::FlinkDotNet.Proto.Internal.ReportFailedCheckpointResponse> ReportFailedCheckpoint(global::FlinkDotNet.Proto.Internal.ReportFailedCheckpointRequest request, ServerCallContext context)
        {
            _logger.LogWarning(
                "gRPC: ReportFailedCheckpoint called for JobId: {JobId}, CheckpointId: {CheckpointId}, Task: {Task}, TM: {TaskManagerId}, Reason: {FailureReason}",
                request.JobId,
                request.CheckpointId,
                $"{request.JobVertexId}_{request.SubtaskIndex}",
                request.TaskManagerId,
                request.FailureReason);

            // In a real implementation, this would trigger checkpoint cancellation or recovery logic.
            // For now, just acknowledge the report.

            return Task.FromResult(new global::FlinkDotNet.Proto.Internal.ReportFailedCheckpointResponse
            {
                Acknowledged = true
            });
        }

        public override Task<global::FlinkDotNet.Proto.Internal.ReportTaskStartupFailureResponse> ReportTaskStartupFailure(
            global::FlinkDotNet.Proto.Internal.ReportTaskStartupFailureRequest request, ServerCallContext context)
        {
            _logger.LogWarning(
                "[JobManager] Received task startup failure report from TM {TaskManagerId} for Job {JobId}, Task {TaskId}. Reason: {Reason}",
                request.TaskManagerId,
                request.JobId,
                $"{request.JobVertexId}_{request.SubtaskIndex}",
                request.FailureReason);
            // For now, just log and acknowledge.
            return Task.FromResult(new global::FlinkDotNet.Proto.Internal.ReportTaskStartupFailureResponse { Acknowledged = true });
        }
    }
}
