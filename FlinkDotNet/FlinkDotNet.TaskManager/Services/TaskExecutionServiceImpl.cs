#nullable enable
using System;
using System.Collections.Generic; // Added for Dictionary
using System.Text.Json; // For deserializing OperatorConfiguration
using System.Threading.Tasks;
using Grpc.Core;
using FlinkDotNet.Proto.Internal; // For TaskDeploymentDescriptor, DeployTaskResponse
// Assuming TaskExecutor is in FlinkDotNet.TaskManager namespace

namespace FlinkDotNet.TaskManager.Services
{
    public class TaskExecutionServiceImpl : TaskExecution.TaskExecutionBase
    {
        private readonly string _taskManagerId;
        private readonly TaskExecutor _taskExecutor; // Instance to execute the tasks

        public TaskExecutionServiceImpl(string taskManagerId, TaskExecutor taskExecutor)
        {
            _taskManagerId = taskManagerId;
            _taskExecutor = taskExecutor;
        }

        public override async Task<DeployTaskResponse> DeployTask(
            TaskDeploymentDescriptor request, ServerCallContext context)
        {
            Console.WriteLine($"TaskManager [{_taskManagerId}]: Received DeployTask request for Task '{request.TaskName}', Operator '{request.FullyQualifiedOperatorName}'.");
            Console.WriteLine($"    JobGraphJobId: {request.JobGraphJobId}, JobVertexId: {request.JobVertexId}, SubtaskIndex: {request.SubtaskIndex}");
            Console.WriteLine($"    InputType: {request.InputTypeName}, OutputType: {request.OutputTypeName}");
            Console.WriteLine($"    InputSerializer: {request.InputSerializerTypeName}, OutputSerializer: {request.OutputSerializerTypeName}");
            Console.WriteLine($"    Inputs: {request.Inputs.Count}, Outputs: {request.Outputs.Count}");


            try
            {
                // Deserialize OperatorConfiguration (assuming JSON)
                var operatorProperties = JsonSerializer.Deserialize<Dictionary<string, string>>(
                                             request.OperatorConfiguration.ToStringUtf8())
                                         ?? new Dictionary<string, string>();

                Console.WriteLine($"TaskManager [{_taskManagerId}]: Launching TaskExecutor for '{request.TaskName}'.");

                // Create a CancellationTokenSource for this specific task execution
                var taskCts = new CancellationTokenSource();

                // Fire and forget the actual task execution. The response is for acknowledging deployment.
                // Note: Awaiting this would block the gRPC response until the task completes, which is usually not desired for DeployTask.
                // Proper error handling for asynchronous task startup failures would involve more complex mechanisms (e.g., callback to JM).
                _ = _taskExecutor.ExecuteFromDescriptor(request, operatorProperties, taskCts.Token);

                return new DeployTaskResponse
                {
                    Success = true,
                    Message = $"Task '{request.TaskName}' deployment initiated on TM {_taskManagerId}."
                };
            }
            catch (Exception ex)
            {
                Console.WriteLine($"TaskManager [{_taskManagerId}]: Error processing DeployTask for '{request.TaskName}': {ex.Message}");
                return new DeployTaskResponse { Success = false, Message = ex.Message };
            }
        }
    }
}
#nullable disable
