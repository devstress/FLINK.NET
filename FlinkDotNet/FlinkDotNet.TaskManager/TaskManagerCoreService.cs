#nullable enable
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
    public record Config(string TaskManagerId, string JobManagerGrpcAddress);

    private readonly Config _config;
    private Timer? _heartbeatTimer;
    private TaskManagerRegistration.TaskManagerRegistrationClient? _client;
    private bool _registered = false;
    private CancellationTokenSource? _internalCts;

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
        var channel = GrpcChannel.ForAddress(_config.JobManagerGrpcAddress);
        _client = new TaskManagerRegistration.TaskManagerRegistrationClient(channel);

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

    public async Task SendAcknowledgeCheckpointAsync(string jobId, long checkpointId, string snapshotHandle, long snapshotSize, long duration)
    {
        if (!_registered || _client == null)
        {
            Console.WriteLine($"TaskManager [{_config.TaskManagerId}]: Not registered or client not available. Cannot send AcknowledgeCheckpoint for CP {checkpointId}.");
            return;
        }

        try
        {
            var ackRequest = new AcknowledgeCheckpointRequest
            {
                JobManagerId = "", // JobManagerId is known by the JM, TM doesn't need to echo it back unless proto requires for routing
                JobId = jobId,
                CheckpointId = checkpointId,
                TaskManagerId = _config.TaskManagerId,
                SnapshotHandle = snapshotHandle,
                // SnapshotSize = snapshotSize, // Add if/when proto supports these details directly
                // Duration = duration        // Add if/when proto supports these details directly
            };

            Console.WriteLine($"TaskManager [{_config.TaskManagerId}]: Sending AcknowledgeCheckpoint for Job '{jobId}', CP {checkpointId}, Handle '{snapshotHandle}' to JobManager.");
            var response = await _client.AcknowledgeCheckpointAsync(ackRequest, deadline: DateTime.UtcNow.AddSeconds(10));

            if (response.Success)
            {
                Console.WriteLine($"TaskManager [{_config.TaskManagerId}]: AcknowledgeCheckpoint for CP {checkpointId} successfully processed by JobManager.");
            }
            else
            {
                Console.WriteLine($"TaskManager [{_config.TaskManagerId}]: JobManager did not successfully process AcknowledgeCheckpoint for CP {checkpointId}.");
                // Handle this failure, e.g., retry, mark checkpoint as failed locally.
            }
        }
        catch (Exception ex)
        {
            Console.WriteLine($"TaskManager [{_config.TaskManagerId}]: Error sending AcknowledgeCheckpoint for CP {checkpointId}: {ex.GetType().Name} - {ex.Message}");
            // Handle failure
        }
    }
}
#nullable disable
