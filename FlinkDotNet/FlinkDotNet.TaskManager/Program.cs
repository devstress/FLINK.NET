using Microsoft.AspNetCore.Builder;
using Microsoft.AspNetCore.Hosting;
using Microsoft.AspNetCore.Hosting.Server;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting; // Ensured for AddServiceDefaults
using FlinkDotNet.TaskManager.Services;
using FlinkDotNet.Core.Abstractions.Execution;
using FlinkDotNet.Core.Abstractions.Storage; // For TaskManagerCheckpointingServiceImpl
using FlinkDotNet.Common.Constants;

namespace FlinkDotNet.TaskManager
{
    public static class Program
    {
        public static string TaskManagerId { get; private set; } = $"TM-{Guid.NewGuid()}";
        public static int GrpcPort { get; private set; }
        public static string JobManagerAddress { get; private set; } = ServiceUris.Insecure.JobManagerGrpcHttp;
        public static TaskManagerCoreService? CoreServiceInstance { get; private set; }


        public static async Task Main(string[] args)
        {
            // Initialize dynamic port allocation for Aspire/Kubernetes environments
            InitializeDynamicPortAllocation();

            // Basic configuration - replace with actual config mechanism later
            ProcessCommandLineArguments(args);

            // Override settings from environment variables if provided (Aspire service discovery)
            ApplyEnvironmentVariables();

            DisplayStartupConfiguration();

            var host = CreateHostBuilder(args).Build();

            // Start the main TaskManagerCoreService (registration, heartbeats, task execution trigger)
            // This needs to be run as a background service or integrated differently if TM also hosts gRPC services.
            CoreServiceInstance = host.Services.GetRequiredService<TaskManagerCoreService>(); // Assign to static instance
            // var taskManagerCoreService = CoreServiceInstance; // Can use CoreServiceInstance directly if needed elsewhere in Main

            var hostRunTask = host.RunAsync(); // Runs the gRPC server etc.

            try
            {
                // await taskManagerCoreService.StartAsync(CancellationToken.None); // This was problematic
                // IHostedService's StartAsync is called by host.RunAsync() or host.StartAsync()
                // We just need to wait for the host to terminate or for taskManagerCoreService to indicate completion if it were managing its own main loop.
                // Since TaskManagerCoreService's StartAsync is now non-blocking for the main registration part,
                // and actual work (like task execution) is Task.Run, we primarily await the host.
                await hostRunTask;
            }
            catch (OperationCanceledException)
            {
                Console.WriteLine("TaskManager host operation was canceled.");
            }
            finally
            {
                // StopAsync for IHostedService is called automatically by the host on shutdown.
                // Calling it manually here might be redundant or even problematic if the host is also trying to stop it.
                // However, if taskManagerCoreService needs to be stopped before the host fully stops, this could be a place.
                // For typical IHostedService, manual StopAsync call here is not standard.
                Console.WriteLine("TaskManager has shut down.");
            }
        }

        /// <summary>
        /// Initialize dynamic port allocation based on environment settings
        /// </summary>
        private static void InitializeDynamicPortAllocation()
        {
            // Use port 0 to let the system assign an available port when ASPIRE_USE_DYNAMIC_PORTS is set
            if (Environment.GetEnvironmentVariable("ASPIRE_USE_DYNAMIC_PORTS")?.ToLowerInvariant() == "true")
            {
                GrpcPort = 0; // Let Kestrel/system assign an available port dynamically
                Console.WriteLine("🔄 ASPIRE MODE: Using dynamic port allocation (system will assign available port)");
            }
            else
            {
                // Default gRPC port for TaskManager services (JobManager will call this) - for non-Aspire scenarios
                GrpcPort = ServicePorts.TaskManagerGrpc;
            }
        }

        /// <summary>
        /// Process command line arguments for configuration overrides
        /// </summary>
        private static void ProcessCommandLineArguments(string[] args)
        {
            // Allow overriding TM ID and gRPC port via command line for multiple instances
            if (args.Length > 0) TaskManagerId = args[0];
            if (args.Length > 1 && int.TryParse(args[1], out int port)) GrpcPort = port;
            if (args.Length > 2) JobManagerAddress = args[2];
        }

        /// <summary>
        /// Apply configuration from environment variables (Aspire service discovery)
        /// </summary>
        private static void ApplyEnvironmentVariables()
        {
            ApplyTaskManagerIdFromEnvironment();
            ApplyJobManagerAddressFromEnvironment();
            ApplyGrpcPortFromEnvironment();
        }

        /// <summary>
        /// Apply TaskManager ID from environment variable if available
        /// </summary>
        private static void ApplyTaskManagerIdFromEnvironment()
        {
            var envTaskManagerId = Environment.GetEnvironmentVariable("TaskManagerId");
            if (!string.IsNullOrEmpty(envTaskManagerId))
            {
                TaskManagerId = envTaskManagerId;
            }
        }

        /// <summary>
        /// Apply JobManager address from environment variables with fallback chain
        /// </summary>
        private static void ApplyJobManagerAddressFromEnvironment()
        {
            // Try Aspire service discovery patterns first
            var aspireServiceAddress = Environment.GetEnvironmentVariable("services__jobmanager__grpc__0");
            if (!string.IsNullOrEmpty(aspireServiceAddress))
            {
                JobManagerAddress = aspireServiceAddress;
                Console.WriteLine($"Using JobManager address from Aspire service discovery: {JobManagerAddress}");
                return;
            }

            // Try alternative Aspire patterns
            var aspireConnectionString = Environment.GetEnvironmentVariable("ConnectionStrings__jobmanager");
            if (!string.IsNullOrEmpty(aspireConnectionString))
            {
                JobManagerAddress = aspireConnectionString;
                Console.WriteLine($"Using JobManager address from Aspire connection string: {JobManagerAddress}");
                return;
            }

            // Note: JOBMANAGER_GRPC_ADDRESS and DOTNET_JOBMANAGER_GRPC_ADDRESS environment variables
            // are no longer set by discover-aspire-ports.ps1 since JobManager runs as .NET project.
            // Service discovery should happen through Aspire configuration instead.
            Console.WriteLine($"No JobManager address found in environment, using default: {JobManagerAddress}");
        }

        /// <summary>
        /// Apply gRPC port from environment variable if available
        /// </summary>
        private static void ApplyGrpcPortFromEnvironment()
        {
            var envGrpcPort = Environment.GetEnvironmentVariable("TASKMANAGER_GRPC_PORT");
            if (!string.IsNullOrEmpty(envGrpcPort) && int.TryParse(envGrpcPort, out int envPort))
            {
                GrpcPort = envPort;
            }
        }

        /// <summary>
        /// Display startup configuration information
        /// </summary>
        private static void DisplayStartupConfiguration()
        {
            Console.WriteLine($"Starting TaskManager: {TaskManagerId}");
            Console.WriteLine($"JobManager Address: {JobManagerAddress}");
            if (GrpcPort == 0)
            {
                Console.WriteLine($"TaskManager gRPC services using dynamic port allocation (Aspire/K8s mode)");
            }
            else
            {
                Console.WriteLine($"TaskManager gRPC services listening on: http://{ServiceHosts.Localhost}:{GrpcPort}");
            }
        }

        public static IHostBuilder CreateHostBuilder(string[] args) =>
            Host.CreateDefaultBuilder(args)
                .ConfigureServices((hostContext, services) =>
                {
                    // Register TaskManagerCoreService (previously TaskManagerService)
                    // It needs to be an IHostedService to integrate with Generic Host lifecycle
                    services.AddSingleton(new TaskManagerCoreService.Config(TaskManagerId, JobManagerAddress));
                    services.AddSingleton<TaskManagerCoreService>(sp => 
                        new TaskManagerCoreService(
                            sp.GetRequiredService<TaskManagerCoreService.Config>(), 
                            sp.GetRequiredService<IServer>()));
                    services.AddHostedService(sp => sp.GetRequiredService<TaskManagerCoreService>());

                    // Register TaskExecutor
                    services.AddSingleton(sp => new TaskExecutor(
                        sp.GetRequiredService<ActiveTaskRegistry>(),
                        sp.GetRequiredService<TaskManagerCheckpointingServiceImpl>(),
                        sp.GetRequiredService<SerializerRegistry>(),
                        TaskManagerId, // Pass the TaskManagerId here
                        sp.GetRequiredService<IStateSnapshotStore>()
                    ));

                    // Register ActiveTaskRegistry
                    services.AddSingleton<ActiveTaskRegistry>();

                    // Register SerializerRegistry
                    services.AddSingleton<SerializerRegistry>();

                    // Register gRPC services
                    services.AddGrpc();
                    // Pass TaskManagerId to the service if needed for context
                    services.AddSingleton(sp => new TaskManagerCheckpointingServiceImpl(TaskManagerId));
                    services.AddSingleton(sp => new TaskExecutionServiceImpl(TaskManagerId, sp.GetRequiredService<TaskExecutor>()));
                    services.AddSingleton(sp => new DataExchangeServiceImpl(TaskManagerId)); // Register DataExchangeService
                })
                .ConfigureWebHostDefaults(webBuilder =>
                {
                    webBuilder.ConfigureKestrel(options =>
                    {
                        if (GrpcPort == 0)
                        {
                            // Dynamic port allocation - let Kestrel choose an available port
                            options.ListenAnyIP(0, o => o.Protocols = Microsoft.AspNetCore.Server.Kestrel.Core.HttpProtocols.Http2);
                        }
                        else
                        {
                            // Use specified port
                            options.ListenLocalhost(GrpcPort, o => o.Protocols = Microsoft.AspNetCore.Server.Kestrel.Core.HttpProtocols.Http2);
                        }
                    });
                    webBuilder.Configure(app =>
                    {
                        app.UseRouting();
                        app.UseEndpoints(endpoints =>
                        {
                            endpoints.MapGrpcService<TaskManagerCheckpointingServiceImpl>();
                            endpoints.MapGrpcService<TaskExecutionServiceImpl>();
                            endpoints.MapGrpcService<DataExchangeServiceImpl>(); // Map DataExchangeService
                            // Map other TaskManager gRPC services here if any
                        });
                    });
                });
    }
}
