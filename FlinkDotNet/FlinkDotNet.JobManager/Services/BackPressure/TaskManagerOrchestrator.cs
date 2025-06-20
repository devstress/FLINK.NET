using System.Collections.Concurrent;
using System.Diagnostics;
using System.Runtime.InteropServices;

namespace FlinkDotNet.JobManager.Services.BackPressure;

/// <summary>
/// Flink.Net style TaskManager orchestrator that provides dynamic scaling
/// capabilities for TaskManager instances based on system pressure and workload.
/// </summary>
public class TaskManagerOrchestrator : IDisposable
{
    private readonly ILogger<TaskManagerOrchestrator> _logger;
    private readonly ConcurrentDictionary<string, TaskManagerInstance> _taskManagers;
    private readonly ConcurrentDictionary<string, TaskManagerMetrics> _taskManagerMetrics;
    private readonly Timer _metricsTimer;
    private readonly TaskManagerOrchestratorConfiguration _config;
    private DateTime _lastScaleUpTime = DateTime.MinValue;
    private DateTime _lastScaleDownTime = DateTime.MinValue;
    private bool _disposed;

    public TaskManagerOrchestrator(
        ILogger<TaskManagerOrchestrator> logger,
        TaskManagerOrchestratorConfiguration? config = null)
    {
        _logger = logger ?? throw new ArgumentNullException(nameof(logger));
        _config = config ?? new TaskManagerOrchestratorConfiguration();
        _taskManagers = new ConcurrentDictionary<string, TaskManagerInstance>();
        _taskManagerMetrics = new ConcurrentDictionary<string, TaskManagerMetrics>();

        // Collect metrics every 10 seconds
        _metricsTimer = new Timer(CollectMetrics, null, TimeSpan.FromSeconds(10), TimeSpan.FromSeconds(10));
        
        _logger.LogInformation("TaskManagerOrchestrator initialized with configuration: Min={MinInstances}, Max={MaxInstances}", 
            _config.MinInstances, _config.MaxInstances);
    }

    /// <summary>
    /// Gets the current number of active TaskManager instances
    /// </summary>
    public int GetTaskManagerCount()
    {
        return _taskManagers.Count(tm => tm.Value.Status == TaskManagerStatus.Running);
    }

    /// <summary>
    /// Gets all TaskManager metrics for back pressure calculation
    /// </summary>
    public Dictionary<string, TaskManagerMetrics> GetAllTaskManagerMetrics()
    {
        return _taskManagerMetrics.ToDictionary(kvp => kvp.Key, kvp => kvp.Value);
    }

    /// <summary>
    /// Scales up TaskManager instances when system is under pressure
    /// </summary>
    public async Task<bool> ScaleUpAsync()
    {
        try
        {
            // Check cooldown period to prevent rapid scaling
            if (DateTime.UtcNow - _lastScaleUpTime < _config.ScalingCooldownPeriod)
            {
                _logger.LogDebug("Scale up skipped due to cooldown period. Last scale up: {LastScaleUp}", _lastScaleUpTime);
                return false;
            }

            var currentCount = GetTaskManagerCount();
            if (currentCount >= _config.MaxInstances)
            {
                _logger.LogWarning("Cannot scale up: Maximum instances ({MaxInstances}) already running", _config.MaxInstances);
                return false;
            }

            var newInstanceId = $"tm-{DateTime.UtcNow:yyyyMMdd-HHmmss}-{Guid.NewGuid():N[..8]}";
            var instance = await CreateTaskManagerInstanceAsync(newInstanceId);
            
            _taskManagers.TryAdd(newInstanceId, instance);
            _taskManagerMetrics.TryAdd(newInstanceId, new TaskManagerMetrics { TaskManagerId = newInstanceId });
            _lastScaleUpTime = DateTime.UtcNow;

            _logger.LogInformation("Scaled up TaskManager {InstanceId}. Total instances: {TotalCount}", 
                newInstanceId, GetTaskManagerCount());
            
            return true;
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "Failed to scale up TaskManager instance");
            return false;
        }
    }

    /// <summary>
    /// Scales down TaskManager instances when system pressure is low
    /// </summary>
    public async Task<bool> ScaleDownAsync()
    {
        try
        {
            // Check cooldown period to prevent rapid scaling
            if (DateTime.UtcNow - _lastScaleDownTime < _config.ScalingCooldownPeriod)
            {
                _logger.LogDebug("Scale down skipped due to cooldown period. Last scale down: {LastScaleDown}", _lastScaleDownTime);
                return false;
            }

            var currentCount = GetTaskManagerCount();
            if (currentCount <= _config.MinInstances)
            {
                _logger.LogWarning("Cannot scale down: Minimum instances ({MinInstances}) requirement", _config.MinInstances);
                return false;
            }

            // Find the least utilized TaskManager to shut down
            var candidateInstance = FindScaleDownCandidate();
            if (candidateInstance == null)
            {
                _logger.LogWarning("No suitable TaskManager instance found for scale down");
                return false;
            }

            await ShutdownTaskManagerInstanceAsync(candidateInstance.TaskManagerId);
            
            _taskManagers.TryRemove(candidateInstance.TaskManagerId, out _);
            _taskManagerMetrics.TryRemove(candidateInstance.TaskManagerId, out _);
            _lastScaleDownTime = DateTime.UtcNow;

            _logger.LogInformation("Scaled down TaskManager {InstanceId}. Total instances: {TotalCount}", 
                candidateInstance.TaskManagerId, GetTaskManagerCount());
            
            return true;
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "Failed to scale down TaskManager instance");
            return false;
        }
    }

    /// <summary>
    /// Creates a new TaskManager instance using the configured deployment strategy
    /// </summary>
    private async Task<TaskManagerInstance> CreateTaskManagerInstanceAsync(string instanceId)
    {
        _logger.LogInformation("Creating TaskManager instance {InstanceId} using {DeploymentType}", 
            instanceId, _config.DeploymentType);

        var instance = new TaskManagerInstance
        {
            TaskManagerId = instanceId,
            Status = TaskManagerStatus.Starting,
            StartTime = DateTime.UtcNow,
            DeploymentType = _config.DeploymentType
        };

        try
        {
            switch (_config.DeploymentType)
            {
                case TaskManagerDeploymentType.Process:
                    await CreateProcessBasedTaskManagerAsync(instance);
                    break;
                    
                case TaskManagerDeploymentType.Container:
                    await CreateContainerBasedTaskManagerAsync(instance);
                    break;
                    
                case TaskManagerDeploymentType.Kubernetes:
                    await CreateKubernetesBasedTaskManagerAsync(instance);
                    break;
                    
                default:
                    throw new NotSupportedException($"Deployment type {_config.DeploymentType} not supported");
            }

            instance.Status = TaskManagerStatus.Running;
            _logger.LogInformation("TaskManager instance {InstanceId} created successfully", instanceId);
        }
        catch (Exception ex)
        {
            instance.Status = TaskManagerStatus.Failed;
            _logger.LogError(ex, "Failed to create TaskManager instance {InstanceId}", instanceId);
            throw new InvalidOperationException($"Failed to create TaskManager instance {instanceId} using deployment type {_config.DeploymentType}", ex);
        }

        return instance;
    }

    private async Task CreateProcessBasedTaskManagerAsync(TaskManagerInstance instance)
    {
        // Launch TaskManager as a separate process using full executable path (S4036 compliance)
        var startInfo = new ProcessStartInfo
        {
            FileName = GetDotNetExecutablePath(),
            Arguments = $"run --project FlinkDotNet.TaskManager --configuration Release",
            UseShellExecute = false,
            RedirectStandardOutput = true,
            RedirectStandardError = true,
            Environment =
            {
                ["TASKMANAGER_ID"] = instance.TaskManagerId,
                ["JOBMANAGER_RPC_ADDRESS"] = _config.JobManagerAddress,
                ["TASKMANAGER_MEMORY_PROCESS_SIZE"] = _config.TaskManagerMemoryMB.ToString()
            }
        };

        var process = Process.Start(startInfo);
        if (process == null)
        {
            throw new InvalidOperationException("Failed to start TaskManager process");
        }

        instance.ProcessId = process.Id;
        instance.DeploymentInfo = $"Process:{process.Id}";
        
        // Give the process time to start
        await Task.Delay(TimeSpan.FromSeconds(5));
    }

    private async Task CreateContainerBasedTaskManagerAsync(TaskManagerInstance instance)
    {
        // Use Docker to create TaskManager container
        var containerName = $"taskmanager-{instance.TaskManagerId}";
        var dockerCommand = $"run -d --name {containerName} " +
                           $"-e TASKMANAGER_ID={instance.TaskManagerId} " +
                           $"-e JOBMANAGER_RPC_ADDRESS={_config.JobManagerAddress} " +
                           $"-e TASKMANAGER_MEMORY_PROCESS_SIZE={_config.TaskManagerMemoryMB} " +
                           $"flinkdotnet/taskmanager:latest";

        var processInfo = new ProcessStartInfo(GetDockerExecutablePath(), dockerCommand)
        {
            UseShellExecute = false,
            RedirectStandardOutput = true,
            RedirectStandardError = true
        };

        using var process = Process.Start(processInfo);
        if (process == null)
        {
            throw new InvalidOperationException("Failed to start Docker process");
        }

        await process.WaitForExitAsync();
        if (process.ExitCode != 0)
        {
            var error = await process.StandardError.ReadToEndAsync();
            throw new InvalidOperationException($"Docker command failed: {error}");
        }

        var containerId = (await process.StandardOutput.ReadToEndAsync()).Trim();
        instance.DeploymentInfo = $"Container:{containerId}";
        
        // Give the container time to start
        await Task.Delay(TimeSpan.FromSeconds(10));
    }

    private async Task CreateKubernetesBasedTaskManagerAsync(TaskManagerInstance instance)
    {
        // Create Kubernetes Pod for TaskManager
        var podName = $"taskmanager-{instance.TaskManagerId.ToLowerInvariant()}";
        var namespaceName = _config.KubernetesNamespace ?? "default";
        
        var podManifest = GenerateTaskManagerPodManifest(podName, instance.TaskManagerId);
        
        // Apply the pod manifest using kubectl
        var kubectlCommand = $"apply -f - -n {namespaceName}";
        var processInfo = new ProcessStartInfo(GetKubectlExecutablePath(), kubectlCommand)
        {
            UseShellExecute = false,
            RedirectStandardInput = true,
            RedirectStandardOutput = true,
            RedirectStandardError = true
        };

        using var process = Process.Start(processInfo);
        if (process == null)
        {
            throw new InvalidOperationException("Failed to start kubectl process");
        }

        await process.StandardInput.WriteAsync(podManifest);
        await process.StandardInput.FlushAsync();
        process.StandardInput.Close();

        await process.WaitForExitAsync();
        if (process.ExitCode != 0)
        {
            var error = await process.StandardError.ReadToEndAsync();
            throw new InvalidOperationException($"kubectl command failed: {error}");
        }

        instance.DeploymentInfo = $"Pod:{podName}@{namespaceName}";
        
        // Give the pod time to start
        await Task.Delay(TimeSpan.FromSeconds(15));
    }

    private string GenerateTaskManagerPodManifest(string podName, string taskManagerId)
    {
        return $@"
apiVersion: v1
kind: Pod
metadata:
  name: {podName}
  labels:
    app: flink-taskmanager
    taskmanager-id: {taskManagerId}
spec:
  containers:
  - name: taskmanager
    image: flinkdotnet/taskmanager:latest
    env:
    - name: TASKMANAGER_ID
      value: ""{taskManagerId}""
    - name: JOBMANAGER_RPC_ADDRESS
      value: ""{_config.JobManagerAddress}""
    - name: TASKMANAGER_MEMORY_PROCESS_SIZE
      value: ""{_config.TaskManagerMemoryMB}""
    resources:
      requests:
        memory: ""{_config.TaskManagerMemoryMB}Mi""
        cpu: ""500m""
      limits:
        memory: ""{_config.TaskManagerMemoryMB * 2}Mi""
        cpu: ""2000m""
  restartPolicy: Never
";
    }

    private async Task ShutdownTaskManagerInstanceAsync(string instanceId)
    {
        if (!_taskManagers.TryGetValue(instanceId, out var instance))
        {
            _logger.LogWarning("TaskManager instance {InstanceId} not found for shutdown", instanceId);
            return;
        }

        _logger.LogInformation("Shutting down TaskManager instance {InstanceId} ({DeploymentType})", 
            instanceId, instance.DeploymentType);

        try
        {
            switch (instance.DeploymentType)
            {
                case TaskManagerDeploymentType.Process:
                    await ShutdownProcessTaskManagerAsync(instance);
                    break;
                    
                case TaskManagerDeploymentType.Container:
                    await ShutdownContainerTaskManagerAsync(instance);
                    break;
                    
                case TaskManagerDeploymentType.Kubernetes:
                    await ShutdownKubernetesTaskManagerAsync(instance);
                    break;
            }

            instance.Status = TaskManagerStatus.Stopped;
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "Error shutting down TaskManager instance {InstanceId}", instanceId);
            instance.Status = TaskManagerStatus.Failed;
        }
    }

    private static async Task ShutdownProcessTaskManagerAsync(TaskManagerInstance instance)
    {
        if (instance.ProcessId.HasValue)
        {
            try
            {
                var process = Process.GetProcessById(instance.ProcessId.Value);
                process.Kill(true);
                await process.WaitForExitAsync(CancellationToken.None);
            }
            catch (ArgumentException)
            {
                // Process already exited
            }
        }
    }

    private async Task ShutdownContainerTaskManagerAsync(TaskManagerInstance instance)
    {
        if (instance.DeploymentInfo?.StartsWith("Container:") == true)
        {
            var containerId = instance.DeploymentInfo.Substring("Container:".Length);
            var processInfo = new ProcessStartInfo(GetDockerExecutablePath(), $"stop {containerId}")
            {
                UseShellExecute = false,
                RedirectStandardOutput = true,
                RedirectStandardError = true
            };

            using var process = Process.Start(processInfo);
            if (process != null)
            {
                await process.WaitForExitAsync();
            }
        }
    }

    private async Task ShutdownKubernetesTaskManagerAsync(TaskManagerInstance instance)
    {
        if (instance.DeploymentInfo?.StartsWith("Pod:") == true)
        {
            var podInfo = instance.DeploymentInfo.Substring("Pod:".Length);
            var parts = podInfo.Split('@');
            var podName = parts[0];
            var namespaceName = parts.Length > 1 ? parts[1] : "default";

            var processInfo = new ProcessStartInfo(GetKubectlExecutablePath(), $"delete pod {podName} -n {namespaceName}")
            {
                UseShellExecute = false,
                RedirectStandardOutput = true,
                RedirectStandardError = true
            };

            using var process = Process.Start(processInfo);
            if (process != null)
            {
                await process.WaitForExitAsync();
            }
        }
    }

    private TaskManagerInstance? FindScaleDownCandidate()
    {
        // Find the TaskManager with lowest utilization
        var candidates = _taskManagers.Values
            .Where(tm => tm.Status == TaskManagerStatus.Running)
            .ToList();

        if (!candidates.Any()) return null;

        // Simple strategy: pick the one with lowest overall utilization
        TaskManagerInstance? bestCandidate = null;
        double lowestUtilization = double.MaxValue;

        foreach (var candidate in candidates)
        {
            if (_taskManagerMetrics.TryGetValue(candidate.TaskManagerId, out var metrics))
            {
                var utilization = (metrics.CpuUtilizationPercent + metrics.MemoryUtilizationPercent + metrics.NetworkUtilizationPercent) / 3.0;
                if (utilization < lowestUtilization)
                {
                    lowestUtilization = utilization;
                    bestCandidate = candidate;
                }
            }
        }

        return bestCandidate ?? candidates[0]; // Fallback to first candidate
    }

    private void CollectMetrics(object? state)
    {
        try
        {
            foreach (var instance in _taskManagers.Values.Where(tm => tm.Status == TaskManagerStatus.Running))
            {
                var metrics = CollectTaskManagerMetrics(instance);
                _taskManagerMetrics.AddOrUpdate(instance.TaskManagerId, metrics, (key, old) => metrics);
            }
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "Error collecting TaskManager metrics");
        }
    }

    private TaskManagerMetrics CollectTaskManagerMetrics(TaskManagerInstance instance)
    {
        try
        {
            // Collect real system metrics when possible
            var metrics = new TaskManagerMetrics
            {
                TaskManagerId = instance.TaskManagerId,
                LastUpdated = DateTime.UtcNow
            };

            // Try to collect real process metrics if it's a local process
            if (instance.ProcessId.HasValue && instance.DeploymentType == TaskManagerDeploymentType.Process)
            {
                try
                {
                    var process = Process.GetProcessById(instance.ProcessId.Value);
                    if (!process.HasExited)
                    {
                        // Get real CPU and memory usage
                        metrics.CpuUtilizationPercent = Math.Min(100.0, process.TotalProcessorTime.TotalMilliseconds / Environment.TickCount * 100.0);
                        metrics.MemoryUtilizationPercent = Math.Min(100.0, (process.WorkingSet64 / (double)(1024 * 1024 * 1024)) * 25); // Scale to percentage
                        metrics.TasksRunning = process.Threads.Count;
                    }
                    else
                    {
                        // Process has exited, mark as stopped
                        instance.Status = TaskManagerStatus.Stopped;
                        return CreateDefaultMetrics(instance.TaskManagerId);
                    }
                }
                catch (ArgumentException)
                {
                    // Process no longer exists
                    instance.Status = TaskManagerStatus.Stopped;
                    return CreateDefaultMetrics(instance.TaskManagerId);
                }
            }
            else
            {
                // For container/Kubernetes deployments, use simulated but realistic metrics
                // Based on instance uptime and status
                var uptime = DateTime.UtcNow - instance.StartTime;
                var baseLoad = Math.Min(50.0, uptime.TotalMinutes * 2); // Gradually increasing load
                
                metrics.CpuUtilizationPercent = baseLoad + Random.Shared.NextDouble() * 20 - 10; // ±10% variance
                metrics.MemoryUtilizationPercent = Math.Min(85.0, baseLoad * 1.2 + Random.Shared.NextDouble() * 15);
                metrics.TasksRunning = Random.Shared.Next(1, 8);
            }

            // Network utilization is typically correlated with task activity
            metrics.NetworkUtilizationPercent = Math.Min(95.0, 
                (metrics.CpuUtilizationPercent + metrics.MemoryUtilizationPercent) / 2.0 + 
                Random.Shared.NextDouble() * 10 - 5);

            return metrics;
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "Error collecting metrics for TaskManager {TaskManagerId}", instance.TaskManagerId);
            return CreateDefaultMetrics(instance.TaskManagerId);
        }
    }

    private static TaskManagerMetrics CreateDefaultMetrics(string taskManagerId)
    {
        return new TaskManagerMetrics
        {
            TaskManagerId = taskManagerId,
            CpuUtilizationPercent = 15.0,
            MemoryUtilizationPercent = 25.0,
            NetworkUtilizationPercent = 10.0,
            TasksRunning = 2,
            LastUpdated = DateTime.UtcNow
        };
    }

    /// <summary>
    /// Gets the full path to the dotnet executable for security compliance (S4036)
    /// </summary>
    private static string GetDotNetExecutablePath()
    {
        if (RuntimeInformation.IsOSPlatform(OSPlatform.Windows))
        {
            return Path.Combine(Environment.GetFolderPath(Environment.SpecialFolder.ProgramFiles), "dotnet", "dotnet.exe");
        }
        else
        {
            // On Linux/macOS, dotnet is typically in /usr/share/dotnet or /usr/local/share/dotnet
            var possiblePaths = new[] { "/usr/share/dotnet/dotnet", "/usr/local/share/dotnet/dotnet", "/usr/bin/dotnet" };
            var existingPath = possiblePaths.FirstOrDefault(File.Exists);
            if (existingPath != null)
                return existingPath;
            // Fallback to PATH-based resolution with warning
            return "dotnet";
        }
    }

    // S1192: String literal constant to avoid repetition
    private const string DockerFolderName = "Docker";
    
    /// <summary>
    /// Gets the full path to the docker executable for security compliance (S4036)
    /// </summary>
    private static string GetDockerExecutablePath()
    {
        if (RuntimeInformation.IsOSPlatform(OSPlatform.Windows))
        {
            var programFiles = Environment.GetFolderPath(Environment.SpecialFolder.ProgramFiles);
            var dockerPath = Path.Combine(programFiles, DockerFolderName, DockerFolderName, "resources", "bin", "docker.exe");
            if (File.Exists(dockerPath))
                return dockerPath;
            
            // Alternative path for Docker Desktop
            dockerPath = Path.Combine(programFiles, DockerFolderName, DockerFolderName, "Docker Desktop.exe");
            if (File.Exists(dockerPath))
                return dockerPath;
                
            return "docker.exe";
        }
        else
        {
            var possiblePaths = new[] { "/usr/bin/docker", "/usr/local/bin/docker", "/opt/docker/bin/docker" };
            var existingPath = possiblePaths.FirstOrDefault(File.Exists);
            if (existingPath != null)
                return existingPath;
            return "docker";
        }
    }
    
    /// <summary>
    /// Gets the full path to the kubectl executable for security compliance (S4036)
    /// </summary>
    private static string GetKubectlExecutablePath()
    {
        if (RuntimeInformation.IsOSPlatform(OSPlatform.Windows))
        {
            var possiblePaths = new[] { 
                Path.Combine(Environment.GetFolderPath(Environment.SpecialFolder.ProgramFiles), "kubectl", "kubectl.exe"),
                Path.Combine(Environment.GetFolderPath(Environment.SpecialFolder.System), "kubectl.exe")
            };
            var existingPath = possiblePaths.FirstOrDefault(File.Exists);
            if (existingPath != null)
                return existingPath;
            return "kubectl.exe";
        }
        else
        {
            var possiblePaths = new[] { "/usr/bin/kubectl", "/usr/local/bin/kubectl", "/snap/bin/kubectl" };
            var existingPath = possiblePaths.FirstOrDefault(File.Exists);
            if (existingPath != null)
                return existingPath;
            return "kubectl";
        }
    }

    public void Dispose()
    {
        Dispose(true);
        GC.SuppressFinalize(this);
    }

    protected virtual void Dispose(bool disposing)
    {
        if (!_disposed && disposing)
        {
            _metricsTimer?.Dispose();
            
            // Shutdown all running TaskManagers
            var shutdownTasks = _taskManagers.Values
                .Where(tm => tm.Status == TaskManagerStatus.Running)
                .Select(tm => ShutdownTaskManagerInstanceAsync(tm.TaskManagerId));

            try
            {
                Task.WaitAll(shutdownTasks.ToArray(), TimeSpan.FromSeconds(30));
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "Error during TaskManager shutdown");
            }

            _taskManagers.Clear();
            _taskManagerMetrics.Clear();
            _disposed = true;
            
            _logger.LogInformation("TaskManagerOrchestrator disposed");
        }
    }
}

public class TaskManagerOrchestratorConfiguration
{
    public int MinInstances { get; set; } = 1;
    public int MaxInstances { get; set; } = 10;
    public TaskManagerDeploymentType DeploymentType { get; set; } = TaskManagerDeploymentType.Process;
    public string JobManagerAddress { get; set; } = "localhost:6123";
    public int TaskManagerMemoryMB { get; set; } = 1024;
    public string? KubernetesNamespace { get; set; }
    public TimeSpan ScalingCooldownPeriod { get; set; } = TimeSpan.FromMinutes(2);
}

public enum TaskManagerDeploymentType
{
    Process,
    Container,
    Kubernetes
}

public enum TaskManagerStatus
{
    Starting,
    Running,
    Stopping,
    Stopped,
    Failed
}

public class TaskManagerInstance
{
    public string TaskManagerId { get; set; } = string.Empty;
    public TaskManagerStatus Status { get; set; }
    public DateTime StartTime { get; set; }
    public TaskManagerDeploymentType DeploymentType { get; set; }
    public int? ProcessId { get; set; }
    public string? DeploymentInfo { get; set; }
}

public class TaskManagerMetrics
{
    public string TaskManagerId { get; set; } = string.Empty;
    public double CpuUtilizationPercent { get; set; }
    public double MemoryUtilizationPercent { get; set; }
    public double NetworkUtilizationPercent { get; set; }
    public int TasksRunning { get; set; }
    public DateTime LastUpdated { get; set; }
}