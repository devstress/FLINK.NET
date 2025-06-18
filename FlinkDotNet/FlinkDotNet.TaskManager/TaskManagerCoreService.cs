using Grpc.Net.Client;
using FlinkDotNet.Proto.Internal; // Namespace from your .proto file
using Microsoft.Extensions.Hosting; // For IHostedService
using FlinkDotNet.Common.Constants;
using Microsoft.AspNetCore.Hosting.Server;
using Microsoft.AspNetCore.Hosting.Server.Features;

namespace FlinkDotNet.TaskManager
{
    public class TaskManagerCoreService : IHostedService
{
    // LOGGING_PLACEHOLDER:

    public record Config(string TaskManagerId, string JobManagerGrpcAddress);

    public record CheckpointAcknowledgment(
        string JobId,
        long CheckpointId,
        string SnapshotHandle,
        long SnapshotSize,
        long Duration,
        string JobVertexId,
        int SubtaskIndex,
        Dictionary<string, long> SourceOffsets);

    private readonly Config _config;
    private readonly IServer _server;
    private Timer? _heartbeatTimer;
    private TaskManagerRegistration.TaskManagerRegistrationClient? _client;
    private JobManagerInternalService.JobManagerInternalServiceClient? _jobManagerInternalClient; // Added for ReportFailedCheckpoint
    private bool _registered = false;
    private CancellationTokenSource? _internalCts;
    private string _jobManagerId = string.Empty; // Added to store JM Id

    // public TaskManagerService(string taskManagerId, string jobManagerAddress) // old
    public TaskManagerCoreService(Config config, IServer server) // new - added IServer for port discovery
    {
        _config = config;
        _server = server;
    }

    public async Task StartAsync(CancellationToken cancellationToken)
    {
        _internalCts = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken);
        var linkedToken = _internalCts.Token;

        Console.WriteLine($"TaskManagerCoreService {_config.TaskManagerId} starting...");
        
        // Resolve JobManager address for Aspire environments
        var resolvedJobManagerAddress = await ResolveJobManagerAddressAsync();
        Console.WriteLine($"Resolved JobManager address: {resolvedJobManagerAddress}");
        
        // Discover the actual assigned port when using dynamic allocation
        var actualGrpcPort = GetActualGrpcPort();
        Console.WriteLine($"TaskManager {_config.TaskManagerId} actual gRPC port: {actualGrpcPort}");
        
        var channel = GrpcChannel.ForAddress(resolvedJobManagerAddress); // Use resolved address
        _client = new TaskManagerRegistration.TaskManagerRegistrationClient(channel); // _jmRegistrationClient is _client
        _jobManagerInternalClient = new JobManagerInternalService.JobManagerInternalServiceClient(channel); // Initialize the new client

        try
        {
            var request = new RegisterTaskManagerRequest
            {
                TaskManagerId = _config.TaskManagerId,
                Address = GetTaskManagerAddress(), // Use dynamic address discovery
                Port = actualGrpcPort // Use the actual assigned port instead of Program.GrpcPort
            };
            Console.WriteLine($"Attempting to register TaskManager {_config.TaskManagerId} (gRPC on port {actualGrpcPort}) with JobManager at {resolvedJobManagerAddress}...");
            var response = await _client.RegisterTaskManagerAsync(request, cancellationToken: linkedToken);
            if (response.Success)
            {
                _registered = true;
                _jobManagerId = response.JobManagerId; // Store JobManagerId
                Console.WriteLine($"TaskManager {_config.TaskManagerId} registered successfully with JobManager {response.JobManagerId}.");
                StartHeartbeat(linkedToken);
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

    public Task StopAsync(CancellationToken cancellationToken)
    {
        Console.WriteLine($"TaskManagerCoreService {_config.TaskManagerId} stopping...");
        _internalCts?.Cancel();
        _internalCts?.Dispose();
        _heartbeatTimer?.Dispose();
        _registered = false;
        return Task.CompletedTask;
    }

    /// <summary>
    /// Resolve JobManager address for Aspire environments with service discovery
    /// </summary>
    private Task<string> ResolveJobManagerAddressAsync()
    {
        try
        {
            // First try Aspire service discovery patterns
            var aspireJobManagerUrl = Environment.GetEnvironmentVariable("services__jobmanager__https__0");
            if (!string.IsNullOrEmpty(aspireJobManagerUrl))
            {
                Console.WriteLine($"Using Aspire HTTPS service discovery: {aspireJobManagerUrl}");
                return Task.FromResult(aspireJobManagerUrl);
            }

            var aspireJobManagerGrpc = Environment.GetEnvironmentVariable("services__jobmanager__grpc__0");
            if (!string.IsNullOrEmpty(aspireJobManagerGrpc))
            {
                Console.WriteLine($"Using Aspire gRPC service discovery: {aspireJobManagerGrpc}");
                return Task.FromResult(aspireJobManagerGrpc);
            }

            // Try HTTP endpoint as fallback
            var aspireJobManagerHttp = Environment.GetEnvironmentVariable("services__jobmanager__http__0");
            if (!string.IsNullOrEmpty(aspireJobManagerHttp))
            {
                Console.WriteLine($"Using Aspire HTTP service discovery: {aspireJobManagerHttp}");
                return Task.FromResult(aspireJobManagerHttp);
            }

            // Try alternative Aspire patterns
            var aspireJobManager = Environment.GetEnvironmentVariable("ConnectionStrings__jobmanager");
            if (!string.IsNullOrEmpty(aspireJobManager))
            {
                Console.WriteLine($"Using Aspire connection string: {aspireJobManager}");
                return Task.FromResult(aspireJobManager);
            }

            // Fallback to the original config address
            Console.WriteLine($"Using fallback JobManager address: {_config.JobManagerGrpcAddress}");
            return Task.FromResult(_config.JobManagerGrpcAddress);
        }
        catch (Exception ex)
        {
            Console.WriteLine($"Error resolving JobManager address: {ex.Message}");
            return Task.FromResult(_config.JobManagerGrpcAddress);
        }
    }

    /// <summary>
    /// Discover the actual assigned gRPC port when using dynamic port allocation
    /// </summary>
    private int GetActualGrpcPort()
    {
        try
        {
            // If using fixed port, return it
            if (Program.GrpcPort != 0)
            {
                return Program.GrpcPort;
            }

            // For dynamic ports, discover the actual assigned port
            var discoveredPort = DiscoverDynamicPort();
            return discoveredPort > 0 ? discoveredPort : Program.GrpcPort;
        }
        catch (Exception ex)
        {
            Console.WriteLine($"Error discovering actual gRPC port: {ex.Message}");
            return Program.GrpcPort;
        }
    }

    /// <summary>
    /// Attempts to discover the dynamically assigned port from server features
    /// </summary>
    private int DiscoverDynamicPort()
    {
        const int maxAttempts = 10;
        const int delayMs = 500;
        
        for (int attempt = 1; attempt <= maxAttempts; attempt++)
        {
            var discoveredPort = TryGetPortFromServerAddresses(attempt, maxAttempts);
            if (discoveredPort > 0)
            {
                return discoveredPort;
            }

            if (attempt < maxAttempts)
            {
                Thread.Sleep(delayMs);
            }
        }

        Console.WriteLine($"Could not discover actual port after {maxAttempts} attempts, falling back to Program.GrpcPort: {Program.GrpcPort}");
        return -1;
    }

    /// <summary>
    /// Attempts to extract port from server addresses feature
    /// </summary>
    private int TryGetPortFromServerAddresses(int attempt, int maxAttempts)
    {
        var serverAddressesFeature = _server.Features.Get<IServerAddressesFeature>();
        if (serverAddressesFeature?.Addresses == null || !serverAddressesFeature.Addresses.Any())
        {
            Console.WriteLine($"Server addresses not available yet (attempt {attempt}/{maxAttempts}), waiting...");
            return -1;
        }

        foreach (var address in serverAddressesFeature.Addresses)
        {
            Console.WriteLine($"Server address (attempt {attempt}): {address}");
            
            var port = ExtractPortFromAddress(address);
            if (port > 0)
            {
                return port;
            }
        }

        return -1;
    }

    /// <summary>
    /// Extracts port number from server address string
    /// </summary>
    private static int ExtractPortFromAddress(string address)
    {
        // Parse port from address like "http://[::]:5000" or "http://0.0.0.0:5000"
        if (Uri.TryCreate(address, UriKind.Absolute, out var uri) && uri.Port > 0)
        {
            Console.WriteLine($"Discovered actual gRPC port: {uri.Port}");
            return uri.Port;
        }

        return -1;
    }

    /// <summary>
    /// Get the TaskManager address for registration - supports Aspire/K8s environments
    /// </summary>
    private static string GetTaskManagerAddress()
    {
        try
        {
            // In Aspire/K8s environments, use the pod/container IP if available
            var aspireAddress = Environment.GetEnvironmentVariable("ASPIRE_TASKMANAGER_ADDRESS");
            if (!string.IsNullOrEmpty(aspireAddress))
            {
                Console.WriteLine($"Using Aspire TaskManager address: {aspireAddress}");
                return aspireAddress;
            }

            // Try to get pod IP for Kubernetes
            var podIP = Environment.GetEnvironmentVariable("POD_IP");
            if (!string.IsNullOrEmpty(podIP))
            {
                Console.WriteLine($"Using Kubernetes pod IP: {podIP}");
                return podIP;
            }

            // Try to get hostname for Docker environments
            var hostname = Environment.GetEnvironmentVariable("HOSTNAME");
            if (!string.IsNullOrEmpty(hostname) && hostname != "localhost")
            {
                Console.WriteLine($"Using container hostname: {hostname}");
                return hostname;
            }

            // Default to localhost for local development
            Console.WriteLine($"Using default address: {ServiceHosts.Localhost}");
            return ServiceHosts.Localhost;
        }
        catch (Exception ex)
        {
            Console.WriteLine($"Error determining TaskManager address: {ex.Message}");
            return ServiceHosts.Localhost;
        }
    }

    public async Task SendAcknowledgeCheckpointAsync(CheckpointAcknowledgment acknowledgment)
    {
        if (!_registered || _client == null) // _client is _jmRegistrationClient
        {
            Console.WriteLine($"TaskManagerCoreService: JobManager client not initialized or TM not registered. Cannot send AcknowledgeCheckpoint for CP {acknowledgment.CheckpointId}.");
            return;
        }

        var request = new AcknowledgeCheckpointRequest
        {
            JobManagerId = _jobManagerId, // The ID of the JM this TM is registered with
            JobId = acknowledgment.JobId,
            CheckpointId = acknowledgment.CheckpointId,
            TaskManagerId = _config.TaskManagerId, // This TM's ID from _config
            JobVertexId = acknowledgment.JobVertexId,
            SubtaskIndex = acknowledgment.SubtaskIndex,
            SnapshotHandle = acknowledgment.SnapshotHandle ?? string.Empty,
            SnapshotSize = (ulong)acknowledgment.SnapshotSize,
            Duration = (ulong)acknowledgment.Duration
        };
        request.SourceOffsets.Add(acknowledgment.SourceOffsets);

        try
        {
            Console.WriteLine($"TaskManagerCoreService: Sending AcknowledgeCheckpoint for Job {acknowledgment.JobId}, CP {acknowledgment.CheckpointId}, Task {acknowledgment.JobVertexId}_{acknowledgment.SubtaskIndex}, Handle: {acknowledgment.SnapshotHandle}");
            var response = await _client.AcknowledgeCheckpointAsync(request); // Removed deadline for now, can be added back from config
            if (response.Success)
            {
                Console.WriteLine($"TaskManagerCoreService: AcknowledgeCheckpoint for CP {acknowledgment.CheckpointId}, Task {acknowledgment.JobVertexId}_{acknowledgment.SubtaskIndex} successfully sent to JobManager.");
            }
            else
            {
                Console.WriteLine($"TaskManagerCoreService: JobManager did not confirm AcknowledgeCheckpoint for CP {acknowledgment.CheckpointId}, Task {acknowledgment.JobVertexId}_{acknowledgment.SubtaskIndex}.");
            }
        }
        catch (Exception ex)
        {
            Console.WriteLine($"TaskManagerCoreService: Error sending AcknowledgeCheckpoint for CP {acknowledgment.CheckpointId}, Task {acknowledgment.JobVertexId}_{acknowledgment.SubtaskIndex}: {ex.Message}");
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
}
#nullable disable
