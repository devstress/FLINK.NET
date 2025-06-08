using System;
using System.Threading;
using System.Threading.Tasks;
using Grpc.Net.Client;
using FlinkDotNet.Proto.Internal; // Namespace from your .proto file
using System.IO; // For File.WriteAllLines in test code
using System.Collections.Generic; // For Dictionary in test code
using FlinkDotNet.TaskManager; // For TaskExecutor
using Microsoft.Extensions.Hosting; // For IHostedService

// public class TaskManagerService // old
public class TaskManagerCoreService : IHostedService // new
{
    // LOGGING_PLACEHOLDER:
    // private readonly Microsoft.Extensions.Logging.ILogger<TaskManagerCoreService> _logger; // Inject via constructor, ensure using Microsoft.Extensions.Logging;

    public record Config(string TaskManagerId, string JobManagerGrpcAddress);

    private readonly Config _config;
    private Timer? _heartbeatTimer;
    private TaskManagerRegistration.TaskManagerRegistrationClient? _client;
    private JobManagerInternalService.JobManagerInternalServiceClient? _jobManagerInternalClient; // Added for ReportFailedCheckpoint
    private bool _registered = false;
    private CancellationTokenSource? _internalCts;
    private string _jobManagerId = string.Empty; // Added to store JM Id

    // public TaskManagerService(string taskManagerId, string jobManagerAddress) // old
    public TaskManagerCoreService(Config config) // new
    {
        _config = config;
    }

    // public async Task StartAsync(CancellationToken cancellationToken) // old signature
    public async Task StartAsync(CancellationToken hostCancellationToken) // new signature from IHostedService
    {
        _internalCts = CancellationTokenSource.CreateLinkedTokenSource(hostCancellationToken);
        var linkedToken = _internalCts.Token;

        Console.WriteLine($"TaskManagerCoreService {_config.TaskManagerId} starting...");
        var channel = GrpcChannel.ForAddress(_config.JobManagerGrpcAddress); // _jobManagerChannel could be a field
        _client = new TaskManagerRegistration.TaskManagerRegistrationClient(channel); // _jmRegistrationClient is _client
        _jobManagerInternalClient = new JobManagerInternalService.JobManagerInternalServiceClient(channel); // Initialize the new client

        try
        {
            var request = new RegisterTaskManagerRequest
            {
                TaskManagerId = _config.TaskManagerId,
                Address = "localhost", // Simplification: Assuming localhost. K8s would use pod IP.
                Port = Program.GrpcPort // Use the port defined in Program.cs
            };
            Console.WriteLine($"Attempting to register TaskManager {_config.TaskManagerId} (gRPC on port {Program.GrpcPort}) with JobManager at {_config.JobManagerGrpcAddress}...");
            var response = await _client.RegisterTaskManagerAsync(request, cancellationToken: linkedToken);
            if (response.Success)
            {
                _registered = true;
                _jobManagerId = response.JobManagerId; // Store JobManagerId
                Console.WriteLine($"TaskManager {_config.TaskManagerId} registered successfully with JobManager {response.JobManagerId}.");
                StartHeartbeat(linkedToken);

                // Test execution logic (from previous step) - This is now removed/commented out
                // if (_registered) {
                //     _ = Task.Run(async () => {
                //         await Task.Delay(2000, linkedToken); // Give it a moment
                //         if (linkedToken.IsCancellationRequested) return;
                //         var executor = new TaskExecutor();
                //         var sourceProps = new Dictionary<string, string> { { "filePath", "testfile.txt" } };
                //         File.WriteAllLines("testfile.txt", new[] { "hello world", "flink dot net", "test data" });

                //         // await executor.ExecuteTask( // This old method signature is gone or deprecated
                //         //     taskName: "MyTestTask-1",
                //         //     sourceTypeName: "FlinkDotNet.Connectors.Sources.File.FileSourceFunction`1, FlinkDotNet.Connectors.Sources.File",
                //         //     sourceProperties: sourceProps,
                //         //     sourceOutputTypeName: "System.String, System.Private.CoreLib",
                //         //     sourceSerializerTypeName: "FlinkDotNet.Core.Abstractions.Serializers.StringSerializer, FlinkDotNet.Core.Abstractions",
                //         //     operatorTypeName: "FlinkDotNet.TaskManager.SimpleStringToUpperMapOperator, FlinkDotNet.TaskManager",
                //         //     operatorProperties: new Dictionary<string, string>(),
                //         //     sinkTypeName: "FlinkDotNet.Connectors.Sinks.Console.ConsoleSinkFunction`1, FlinkDotNet.Connectors.Sinks.Console",
                //         //     sinkProperties: new Dictionary<string, string>(),
                //         //     linkedToken);
                //     }, linkedToken);
                // }
            }
            else
            {
                Console.WriteLine($"TaskManager {_config.TaskManagerId} registration failed.");
                // Handle registration failure (e.g., retry, shutdown)
            }
        }
        catch (Exception ex) when (ex is not OperationCanceledException)
        {
            Console.WriteLine($"Error during TaskManager registration: {ex.Message}");
            // Handle exception
        }
        // For IHostedService, StartAsync should complete, and background work happens in ExecuteAsync or via timers/threads.
        // The previous while(!cancellationToken.IsCancellationRequested) loop is removed from here.
        // The host itself will keep running.
         Console.WriteLine($"TaskManagerCoreService {_config.TaskManagerId} started and registered (or failed). Background work (heartbeat, task execution) initiated.");
    }

    private void StartHeartbeat(CancellationToken cancellationToken) // Renamed parameter for clarity
    {
        if (_client == null) return;

        _heartbeatTimer = new Timer(async _ =>
        {
            if (cancellationToken.IsCancellationRequested || !_registered) // Use the passed token
            {
                _heartbeatTimer?.Dispose();
                return;
            }

            try
            {
                var heartbeatRequest = new HeartbeatRequest { TaskManagerId = _config.TaskManagerId }; // Use _config

                // METRICS_PLACEHOLDER:
                // foreach (var tm in TaskMetricsRegistry.AllTaskMetrics.Values) // Assuming TaskMetricsRegistry from TaskExecutor
                // {
                //    heartbeatRequest.TaskMetrics.Add(new TaskMetricData
                //    {
                //        TaskId = tm.TaskId,
                //        RecordsIn = tm.RecordsIn,
                //        RecordsOut = tm.RecordsOut
                //    });
                // }

                // Console.WriteLine($"TaskManager {_config.TaskManagerId}: Sending heartbeat...");
                var response = await _client.SendHeartbeatAsync(heartbeatRequest, cancellationToken: cancellationToken); // Use passed token
                if (!response.Acknowledged)
                {
                    Console.WriteLine($"TaskManager {_config.TaskManagerId}: Heartbeat not acknowledged.");
                    _registered = false;
                    _heartbeatTimer?.Dispose();
                }
            }
            catch (Exception ex)
            {
                if (!cancellationToken.IsCancellationRequested) // Use passed token
                {
                    Console.WriteLine($"TaskManager {_config.TaskManagerId}: Error sending heartbeat: {ex.Message}");
                     _registered = false;
                    _heartbeatTimer?.Dispose();
                }
            }
        }, null, TimeSpan.Zero, TimeSpan.FromSeconds(15));
    }

    // public async Task StopAsync(CancellationToken cancellationToken) // old signature
    public Task StopAsync(CancellationToken hostCancellationToken) // new signature from IHostedService
    {
        Console.WriteLine($"TaskManagerCoreService {_config.TaskManagerId} stopping...");
        _internalCts?.Cancel();
        _heartbeatTimer?.Dispose();
        _registered = false;
        return Task.CompletedTask;
    }

    public async Task SendAcknowledgeCheckpointAsync(
        string jobId,
        long checkpointId,
        string snapshotHandle,
        long snapshotSize,
        long duration,
        string jobVertexId,
        int subtaskIndex,
        Dictionary<string, long> sourceOffsets)
    {
        if (!_registered || _client == null) // _client is _jmRegistrationClient
        {
            Console.WriteLine($"TaskManagerCoreService: JobManager client not initialized or TM not registered. Cannot send AcknowledgeCheckpoint for CP {checkpointId}.");
            return;
        }

        var request = new AcknowledgeCheckpointRequest
        {
            JobManagerId = _jobManagerId, // The ID of the JM this TM is registered with
            JobId = jobId,
            CheckpointId = checkpointId,
            TaskManagerId = _config.TaskManagerId, // This TM's ID from _config
            JobVertexId = jobVertexId,
            SubtaskIndex = subtaskIndex,
            SnapshotHandle = snapshotHandle ?? string.Empty,
            SnapshotSize = (ulong)snapshotSize,
            Duration = (ulong)duration
        };
        request.SourceOffsets.Add(sourceOffsets);

        try
        {
            Console.WriteLine($"TaskManagerCoreService: Sending AcknowledgeCheckpoint for Job {jobId}, CP {checkpointId}, Task {jobVertexId}_{subtaskIndex}, Handle: {snapshotHandle}");
            var response = await _client.AcknowledgeCheckpointAsync(request); // Removed deadline for now, can be added back from config
            if (response.Success)
            {
                Console.WriteLine($"TaskManagerCoreService: AcknowledgeCheckpoint for CP {checkpointId}, Task {jobVertexId}_{subtaskIndex} successfully sent to JobManager.");
            }
            else
            {
                Console.WriteLine($"TaskManagerCoreService: JobManager did not confirm AcknowledgeCheckpoint for CP {checkpointId}, Task {jobVertexId}_{subtaskIndex}.");
            }
        }
        catch (Exception ex)
        {
            Console.WriteLine($"TaskManagerCoreService: Error sending AcknowledgeCheckpoint for CP {checkpointId}, Task {jobVertexId}_{subtaskIndex}: {ex.Message}");
        }
    }

    public async Task SendFailedCheckpointAsync(
        string jobId,
        long checkpointId,
        string jobVertexId,
        int subtaskIndex,
        string failureReason)
    {
        if (!_registered || _jobManagerInternalClient == null)
        {
            Console.WriteLine($"TaskManagerCoreService: JobManagerInternal client not initialized or TM not registered. Cannot send FailedCheckpoint for CP {checkpointId}.");
            return;
        }

        var request = new ReportFailedCheckpointRequest
        {
            JobId = jobId,
            CheckpointId = checkpointId,
            JobVertexId = jobVertexId,
            SubtaskIndex = subtaskIndex,
            FailureReason = failureReason ?? string.Empty,
            TaskManagerId = _config.TaskManagerId
        };

        try
        {
            Console.WriteLine($"TaskManagerCoreService: Sending FailedCheckpoint for Job {jobId}, CP {checkpointId}, Task {jobVertexId}_{subtaskIndex}, Reason: {failureReason}");
            var response = await _jobManagerInternalClient.ReportFailedCheckpointAsync(request);
            if (response.Acknowledged)
            {
                Console.WriteLine($"TaskManagerCoreService: FailedCheckpoint for CP {checkpointId}, Task {jobVertexId}_{subtaskIndex} successfully sent to JobManager.");
            }
            else
            {
                Console.WriteLine($"TaskManagerCoreService: JobManager did not acknowledge FailedCheckpoint for CP {checkpointId}, Task {jobVertexId}_{subtaskIndex}.");
            }
        }
        catch (Exception ex)
        {
            Console.WriteLine($"TaskManagerCoreService: Error sending FailedCheckpoint for CP {checkpointId}, Task {jobVertexId}_{subtaskIndex}: {ex.Message}");
        }
    }

    public async Task SendTaskStartupFailureAsync(string jobId, string jobVertexId, int subtaskIndex, string failureReason)
    {
        if (_jobManagerInternalClient == null)
        {
            Console.WriteLine($"[TaskManagerCoreService] JobManagerInternal client not available. Cannot send task startup failure for {jobVertexId}_{subtaskIndex}.");
            return;
        }

        if (!_registered)
        {
            Console.WriteLine($"[TaskManagerCoreService] TaskManager not registered. Cannot send task startup failure for {jobVertexId}_{subtaskIndex}.");
            return;
        }

        var request = new ReportTaskStartupFailureRequest
        {
            JobId = jobId,
            JobVertexId = jobVertexId,
            SubtaskIndex = subtaskIndex,
            TaskManagerId = _config.TaskManagerId,
            FailureReason = failureReason ?? string.Empty
        };

        try
        {
            Console.WriteLine($"[TaskManagerCoreService] Sending task startup failure report for Job {jobId}, Task {jobVertexId}_{subtaskIndex}. Reason: {failureReason}");
            var response = await _jobManagerInternalClient.ReportTaskStartupFailureAsync(request);
            if (response.Acknowledged)
            {
                Console.WriteLine($"[TaskManagerCoreService] Task startup failure report for {jobVertexId}_{subtaskIndex} successfully acknowledged by JobManager.");
            }
            else
            {
                Console.WriteLine($"[TaskManagerCoreService] JobManager did not acknowledge task startup failure report for {jobVertexId}_{subtaskIndex}.");
            }
        }
        catch (Exception ex)
        {
            Console.WriteLine($"[TaskManagerCoreService] Failed to send task startup failure report for {jobVertexId}_{subtaskIndex}: {ex.Message}");
        }
    }
}
#nullable disable
