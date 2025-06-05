using System;
using System.Threading.Tasks;
using Microsoft.AspNetCore.Mvc;
using FlinkDotNet.JobManager.Interfaces;
using FlinkDotNet.JobManager.Models;
using System.Linq;
using System.Collections.Concurrent; // Added
using FlinkDotNet.JobManager.Models.JobGraph; // Added
using FlinkDotNet.JobManager.Services; // For TaskManagerRegistrationServiceImpl
using FlinkDotNet.JobManager.Checkpointing; // Corrected namespace For CheckpointCoordinator
using Grpc.Net.Client; // Added for GrpcChannel
using System.Text.Json; // Added for JsonSerializer
using FlinkDotNet.Proto.Internal; // Added for TaskExecutionClient & TDD


namespace FlinkDotNet.JobManager.Controllers
{
    [ApiController]
    [Route("api/jobmanager")] // Changed route
    public class JobManagerController : ControllerBase // Removed IJobManagerApi for now as methods change
    {
        // Temporary storage for JobGraphs - replace with a proper JobRepository/Service
        private static readonly ConcurrentDictionary<Guid, JobGraph> _jobGraphs = new();
        // Corrected key type from Guid to string to match TaskManagerRegistrationServiceImpl.JobCoordinators and CheckpointCoordinator's expectation of string JobId
        private static readonly ConcurrentDictionary<string, CheckpointCoordinator> _jobCoordinators = TaskManagerRegistrationServiceImpl.JobCoordinators;

        // Removed IJobRepository for now to simplify with new SubmitJob
        // public JobManagerController(IJobRepository jobRepository)
        // {
        //     _jobRepository = jobRepository;
        // }

        // ... (existing methods like GetStatus, GetJobs etc. might need adjustment or removal if IJobManagerApi is removed/changed)

        [HttpPost("submit")]
        public IActionResult SubmitJob([FromBody] JobDefinitionDto jobDefinition)
        {
            if (jobDefinition == null)
            {
                return BadRequest("Job definition is null.");
            }

            Console.WriteLine($"Received job submission: {jobDefinition.JobName}");

            // --- 1. Create JobGraph ---
            var jobGraph = new JobGraph(jobDefinition.JobName);
            Console.WriteLine($"Created JobGraph with JobId: {jobGraph.JobId}, Name: {jobGraph.JobName}");

            JobVertex? previousVertex = null;
            string? currentOutputType = null;
            string? currentOutputSerializer = null;

            // --- 2. Process Source ---
            if (jobDefinition.Source == null)
            {
                return BadRequest("Job definition must include a source.");
            }

            var sourceVertex = new JobVertex(
                jobDefinition.Source.Name,
                VertexType.Source,
                jobDefinition.Source.TypeName)
            {
                OutputTypeName = jobDefinition.Source.OutputTypeName,
                OutputSerializerTypeName = jobDefinition.Source.SerializerTypeName
                // Parallelism can be set from DTO if added there
            };
            jobDefinition.Source.Properties.ToList().ForEach(p => sourceVertex.Properties.Add(p.Key, p.Value));
            jobGraph.AddVertex(sourceVertex);
            Console.WriteLine($"Added SourceVertex: {sourceVertex.Name}, Type: {sourceVertex.TypeName}, Output: {sourceVertex.OutputTypeName}");

            previousVertex = sourceVertex;
            currentOutputType = sourceVertex.OutputTypeName;
            currentOutputSerializer = sourceVertex.OutputSerializerTypeName;

            // --- 3. Process Operators (currently a linear chain) ---
            if (jobDefinition.Operators != null)
            {
                foreach (var opDef in jobDefinition.Operators)
                {
                    if (previousVertex == null || string.IsNullOrEmpty(currentOutputType))
                    {
                        return BadRequest("Operators must follow a source or another operator with a defined output type.");
                    }

                    var opVertex = new JobVertex(
                        opDef.Name,
                        VertexType.Operator,
                        opDef.TypeName)
                    {
                        InputTypeName = currentOutputType, // Output of previous becomes input of current
                        InputSerializerTypeName = currentOutputSerializer,
                        // OutputTypeName and OutputSerializerTypeName for operator needs to be defined in DTO
                        // For now, let's assume it transforms to string if not specified, or same as input for generic pass-through
                        OutputTypeName = opDef.Properties.GetValueOrDefault("outputTypeName", currentOutputType),
                        OutputSerializerTypeName = opDef.Properties.GetValueOrDefault("outputSerializerTypeName", currentOutputSerializer)
                    };
                    opDef.Properties.ToList().ForEach(p => opVertex.Properties.Add(p.Key, p.Value));
                    jobGraph.AddVertex(opVertex);
                    jobGraph.AddEdge(previousVertex, opVertex, currentOutputType, currentOutputSerializer); // Add edge from previous to current
                    Console.WriteLine($"Added OperatorVertex: {opVertex.Name}, Type: {opVertex.TypeName}, Input: {opVertex.InputTypeName}, Output: {opVertex.OutputTypeName}");

                    previousVertex = opVertex;
                    currentOutputType = opVertex.OutputTypeName;
                    currentOutputSerializer = opVertex.OutputSerializerTypeName;
                }
            }

            // --- 4. Process Sink ---
            if (jobDefinition.Sink == null)
            {
                return BadRequest("Job definition must include a sink.");
            }
            if (previousVertex == null || string.IsNullOrEmpty(currentOutputType))
            {
                return BadRequest("Sink must follow a source or an operator.");
            }

            var sinkVertex = new JobVertex(
                jobDefinition.Sink.Name,
                VertexType.Sink,
                jobDefinition.Sink.TypeName)
            {
                InputTypeName = currentOutputType, // Output of previous becomes input of sink
                InputSerializerTypeName = currentOutputSerializer
            };
            jobDefinition.Sink.Properties.ToList().ForEach(p => sinkVertex.Properties.Add(p.Key, p.Value));
            jobGraph.AddVertex(sinkVertex);
            jobGraph.AddEdge(previousVertex, sinkVertex, currentOutputType, currentOutputSerializer); // Add edge from previous to sink
            Console.WriteLine($"Added SinkVertex: {sinkVertex.Name}, Type: {sinkVertex.TypeName}, Input: {sinkVertex.InputTypeName}");

            // --- 5. Store and (conceptually) Deploy Job ---
            if (!_jobGraphs.TryAdd(jobGraph.JobId, jobGraph))
            {
                // Highly unlikely with GUIDs but good practice
                return Conflict($"Job with generated ID {jobGraph.JobId} already exists.");
            }
            Console.WriteLine($"JobGraph for '{jobGraph.JobName}' (ID: {jobGraph.JobId}) successfully created with {jobGraph.Vertices.Count} vertices and {jobGraph.Edges.Count} edges.");

            // TODO: Initiate checkpoint coordinator for this job
            var coordinatorConfig = new JobManagerConfig { /* Populate from jobDefinition or global config */ };
            var checkpointCoordinator = new CheckpointCoordinator(jobGraph.JobId.ToString(), coordinatorConfig); // JobId for CC is string
            if (_jobCoordinators.TryAdd(jobGraph.JobId.ToString(), checkpointCoordinator)) // Use string key here
            {
                // checkpointCoordinator.Start(); // Start triggering checkpoints
                Console.WriteLine($"CheckpointCoordinator started for job {jobGraph.JobId}. (Currently commented out)");
            }
            else
            {
                 Console.WriteLine($"Warning: Could not add/start CheckpointCoordinator for job {jobGraph.JobId}. It might already exist if using shared dictionary.");
            }


            // TODO: Trigger actual deployment of the job based on the JobGraph.
            // This will be covered in "Task Deployment from JobGraph" (Step 10).
            // Console.WriteLine($"Job '{jobGraph.JobName}' submitted. ID: {jobGraph.JobId}. Next step would be deployment."); // Moved this log

            // --- 6. Deploy Tasks based on JobGraph ---
            Console.WriteLine($"Job '{jobGraph.JobName}' (ID: {jobGraph.JobId}): Preparing for task deployment...");

            // --- Pre-assign all subtasks to TaskManagers ---
            var taskAssignments = new Dictionary<string, (TaskManagerInfo tm, int subtaskIndex)>(); // Simplified: Key: JobVertexId_SubtaskIndex, Value: (TM, original subtask index on that TM)
            var tmAssignmentIndex = 0;
            var availableTaskManagers = TaskManagerTracker.RegisteredTaskManagers.Values.ToList();
            if (!availableTaskManagers.Any())
            {
                Console.WriteLine($"Warning: No TaskManagers available to deploy job {jobGraph.JobName}. Job submitted but not deployed.");
                return Ok(new { Message = "Job submitted but no TaskManagers available for deployment.", JobId = jobGraph.JobId });
            }

            foreach (var jobVertex in jobGraph.Vertices) // Renamed for clarity in this loop
            {
                for (int i = 0; i < jobVertex.Parallelism; i++)
                {
                    var assignedTm = availableTaskManagers[tmAssignmentIndex % availableTaskManagers.Count];
                    string taskInstanceId = $"{jobVertex.Id}_{i}";
                    taskAssignments[taskInstanceId] = (assignedTm, i); // Assign TM and subtask index for this instance
                    tmAssignmentIndex++;
                }
            }
            Console.WriteLine("Pre-assigned all task instances to TaskManagers.");


            // --- Iterate through vertices to create and deploy TDDs ---
            foreach (var vertex in jobGraph.Vertices)
            {
                for (int i = 0; i < vertex.Parallelism; i++) // Handle parallelism
                {
                    string currentTaskInstanceId = $"{vertex.Id}_{i}";
                    var (targetTm, subtaskIdx) = taskAssignments[currentTaskInstanceId]; // subtaskIdx is the same as i here

                    var tdd = new TaskDeploymentDescriptor
                    {
                        JobGraphJobId = jobGraph.JobId.ToString(),
                        JobVertexId = vertex.Id.ToString(),
                        SubtaskIndex = i,
                        TaskName = $"{vertex.Name} ({i + 1}/{vertex.Parallelism})",
                        FullyQualifiedOperatorName = vertex.TypeName,
                        OperatorConfiguration = Google.Protobuf.ByteString.CopyFromUtf8(
                            System.Text.Json.JsonSerializer.Serialize(vertex.Properties)
                        ),
                        InputTypeName = vertex.InputTypeName ?? "",
                        OutputTypeName = vertex.OutputTypeName ?? "",
                        InputSerializerTypeName = vertex.InputSerializerTypeName ?? "",
                        OutputSerializerTypeName = vertex.OutputSerializerTypeName ?? ""
                    };

                    // Populate tdd.Inputs based on vertex.InputEdges
                    foreach (var edge in vertex.InputEdges)
                    {
                        tdd.Inputs.Add(new OperatorInput { SourceVertexId = edge.SourceVertex.Id.ToString() });
                    }

                    // Populate tdd.Outputs with resolved target endpoints
                    foreach (var edge in vertex.OutputEdges)
                    {
                        // For each output edge, find assignments for ALL subtasks of the target vertex
                        for (int targetSubtaskParallelIndex = 0; targetSubtaskParallelIndex < edge.TargetVertex.Parallelism; targetSubtaskParallelIndex++)
                        {
                            string targetTaskInstanceId = $"{edge.TargetVertex.Id}_{targetSubtaskParallelIndex}";
                            if (taskAssignments.TryGetValue(targetTaskInstanceId, out var targetAssignmentInfo))
                            {
                                var (downstreamTm, assignedDownstreamSubtaskIndex) = targetAssignmentInfo;

                                tdd.Outputs.Add(new OperatorOutput {
                                    TargetVertexId = edge.TargetVertex.Id.ToString(),
                                    TargetTaskEndpoint = $"http://{downstreamTm.Address}:{downstreamTm.Port}",
                                    TargetSpecificSubtaskIndex = assignedDownstreamSubtaskIndex
                                });
                                 Console.WriteLine($"  Output from {vertex.Name}_{i} to {edge.TargetVertex.Name}_{assignedDownstreamSubtaskIndex} on TM {downstreamTm.TaskManagerId} at {tdd.Outputs.Last().TargetTaskEndpoint}");
                            }
                            else
                            {
                                Console.WriteLine($"WARNING: Could not find TM assignment for downstream task {targetTaskInstanceId}. Output from {vertex.Name}_{i} might be lost.");
                            }
                        }
                    }

                    Console.WriteLine($"Deploying task '{tdd.TaskName}' for vertex {vertex.Name} (ID: {vertex.Id}) to TaskManager {targetTm.TaskManagerId} ({targetTm.Address}:{targetTm.Port})");

                    try
                    {
                        var channelAddress = $"http://{targetTm.Address}:{targetTm.Port}";
                        using var channel = GrpcChannel.ForAddress(channelAddress);
                        var client = new TaskExecution.TaskExecutionClient(channel);

                        _ = client.DeployTaskAsync(tdd, deadline: DateTime.UtcNow.AddSeconds(10));
                         Console.WriteLine($"DeployTask call initiated for '{tdd.TaskName}' to TM {targetTm.TaskManagerId}.");
                    }
                    catch (Exception ex)
                    {
                        Console.WriteLine($"Failed to send DeployTask for '{tdd.TaskName}' to TM {targetTm.TaskManagerId}: {ex.Message}");
                    }
                }
            }
            Console.WriteLine($"All tasks for job '{jobGraph.JobName}' have been (attempted) deployed.");

            return Ok(new { Message = "Job submitted successfully and task deployment initiated.", JobId = jobGraph.JobId });
        }

        // GET /jobs/{jobId} // This and other methods below might need to be adapted or removed if IJobManagerApi is no longer implemented or if their logic is incompatible
        [HttpGet("/jobs/{jobId}")]
        public async Task<IActionResult> GetJobStatus(string jobId)
        {
            // if (string.IsNullOrWhiteSpace(jobId))
            // {
            //     return BadRequest("Job ID cannot be empty.");
            // }

            // var jobStatus = await _jobRepository.GetJobStatusAsync(jobId);

            // if (jobStatus == null)
            // {
            //     return NotFound($"Job with ID {jobId} not found.");
            // }
            // return Ok(jobStatus);
            return StatusCode(501, "GetJobStatus not implemented with JobGraph storage yet.");
        }

        // PUT /jobs/{jobId}/scale
        [HttpPut("/jobs/{jobId}/scale")]
        public async Task<IActionResult> ScaleJob(string jobId, [FromBody] ScaleParametersDto scaleParameters)
        {
            // if (string.IsNullOrWhiteSpace(jobId))
            // {
            //     return BadRequest("Job ID cannot be empty.");
            // }

            // if (scaleParameters == null || scaleParameters.DesiredParallelism <= 0)
            // {
            //     return BadRequest("Scale parameters must be provided and desired parallelism must be greater than 0.");
            // }

            // var jobStatus = await _jobRepository.GetJobStatusAsync(jobId);
            // if (jobStatus == null)
            // {
            //     return NotFound($"Job with ID {jobId} not found.");
            // }

            // jobStatus.Status = "SCALING_REQUESTED";
            // jobStatus.LastUpdated = DateTime.UtcNow;

            // bool updated = await _jobRepository.UpdateJobStatusAsync(jobId, jobStatus);
            // if (!updated)
            // {
            //     return StatusCode(500, $"Failed to update status for job {jobId} to SCALING_REQUESTED.");
            // }
            // return Ok(jobStatus);
            return StatusCode(501, "ScaleJob not implemented with JobGraph storage yet.");
        }

        // POST /jobs/{jobId}/stop
        [HttpPost("/jobs/{jobId}/stop")]
        public async Task<IActionResult> StopJob(string jobId)
        {
            // if (string.IsNullOrWhiteSpace(jobId))
            // {
            //     return BadRequest("Job ID cannot be empty.");
            // }

            // var jobStatus = await _jobRepository.GetJobStatusAsync(jobId);
            // if (jobStatus == null)
            // {
            //     return NotFound($"Job with ID {jobId} not found.");
            // }

            // jobStatus.Status = "STOPPING";
            // jobStatus.LastUpdated = DateTime.UtcNow;

            // bool updated = await _jobRepository.UpdateJobStatusAsync(jobId, jobStatus);
            // if (!updated)
            // {
            //     return StatusCode(500, $"Failed to update status for job {jobId} to STOPPING.");
            // }
            // return Ok(jobStatus);
            return StatusCode(501, "StopJob not implemented with JobGraph storage yet.");
        }

        // POST /jobs/{jobId}/cancel
        [HttpPost("/jobs/{jobId}/cancel")]
        public async Task<IActionResult> CancelJob(string jobId)
        {
            // if (string.IsNullOrWhiteSpace(jobId))
            // {
            //     return BadRequest("Job ID cannot be empty.");
            // }

            // var jobStatus = await _jobRepository.GetJobStatusAsync(jobId);
            // if (jobStatus == null)
            // {
            //     return NotFound($"Job with ID {jobId} not found.");
            // }

            // jobStatus.Status = "CANCELLING";
            // jobStatus.LastUpdated = DateTime.UtcNow;

            // bool updated = await _jobRepository.UpdateJobStatusAsync(jobId, jobStatus);
            // if (!updated)
            // {
            //     return StatusCode(500, $"Failed to update status for job {jobId} to CANCELLING.");
            // }
            // return Ok(jobStatus);
            return StatusCode(501, "CancelJob not implemented with JobGraph storage yet.");
        }

        // GET /jobs/{jobId}/checkpoints
        [HttpGet("/jobs/{jobId}/checkpoints")]
        public async Task<IActionResult> GetJobCheckpoints(string jobId)
        {
            // if (string.IsNullOrWhiteSpace(jobId))
            // {
            //     return BadRequest("Job ID cannot be empty.");
            // }

            // var checkpoints = await _jobRepository.GetCheckpointsAsync(jobId);

            // if (checkpoints == null)
            // {
            //     return NotFound($"Job with ID {jobId} not found, or no checkpoint information available.");
            // }
            // return Ok(checkpoints);
            return StatusCode(501, "GetJobCheckpoints not implemented with JobGraph storage yet.");
        }

        // POST /jobs/{jobId}/restart
        [HttpPost("/jobs/{jobId}/restart")]
        public async Task<IActionResult> RestartJob(string jobId)
        {
            // if (string.IsNullOrWhiteSpace(jobId))
            // {
            //     return BadRequest("Job ID cannot be empty.");
            // }

            // var jobStatus = await _jobRepository.GetJobStatusAsync(jobId);
            // if (jobStatus == null)
            // {
            //     return NotFound($"Job with ID {jobId} not found.");
            // }

            // jobStatus.Status = "RESTARTING";
            // jobStatus.LastUpdated = DateTime.UtcNow;
            // jobStatus.ErrorMessage = null;

            // bool updated = await _jobRepository.UpdateJobStatusAsync(jobId, jobStatus);
            // if (!updated)
            // {
            //     return StatusCode(500, $"Failed to update status for job {jobId} to RESTARTING.");
            // }
            // return Ok(jobStatus);
            return StatusCode(501, "RestartJob not implemented with JobGraph storage yet.");
        }

        // POST /dlq/{jobId}/resubmit
        [HttpPost("/dlq/{jobId}/resubmit")]
        public async Task<IActionResult> ResubmitDlqMessages(string jobId)
        {
            // if (string.IsNullOrWhiteSpace(jobId))
            // {
            //     return BadRequest("Job ID cannot be empty.");
            // }

            // bool accepted = await _jobRepository.RequestBatchDlqResubmissionAsync(jobId);
            // if (!accepted)
            // {
            //     return NotFound($"Failed to request DLQ resubmission for job {jobId}, or job not found.");
            // }

            // Console.WriteLine($"Controller: DLQ resubmission requested for job {jobId}");
            // return Accepted($"Request to resubmit DLQ messages for job {jobId} has been accepted.");
            return StatusCode(501, "ResubmitDlqMessages not implemented with JobGraph storage yet.");
        }

        // PUT /dlq/{jobId}/messages/{messageId}
        [HttpPut("/dlq/{jobId}/messages/{messageId}")]
        public async Task<IActionResult> ModifyDlqMessage(string jobId, string messageId, [FromBody] DlqMessageDto messageData)
        {
            // if (string.IsNullOrWhiteSpace(jobId) || string.IsNullOrWhiteSpace(messageId))
            // {
            //     return BadRequest("Job ID and Message ID cannot be empty.");
            // }

            // if (messageData == null || messageData.NewPayload == null)
            // {
            //     return BadRequest("Message data with new payload must be provided.");
            // }

            // var existingMessage = await _jobRepository.GetDlqMessageAsync(jobId, messageId);
            // if (existingMessage == null)
            // {
            //     return NotFound($"DLQ message with ID {messageId} for job {jobId} not found.");
            // }

            // bool updated = await _jobRepository.UpdateDlqMessagePayloadAsync(jobId, messageId, messageData.NewPayload);
            // if (!updated)
            // {
            //     return StatusCode(500, $"Failed to update DLQ message {messageId} for job {jobId}.");
            // }

            // var updatedMessage = await _jobRepository.GetDlqMessageAsync(jobId, messageId);
            // return Ok(updatedMessage);
            return StatusCode(501, "ModifyDlqMessage not implemented with JobGraph storage yet.");
        }
        // The private ObjectResult NotImplemented(string message) method has been removed.
    }
}
