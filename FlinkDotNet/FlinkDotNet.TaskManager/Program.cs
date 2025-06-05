#nullable enable
using System;
using System.IO; // For Path
using System.Threading;
using System.Threading.Tasks;
using Microsoft.AspNetCore.Builder;
using Microsoft.AspNetCore.Hosting;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using FlinkDotNet.TaskManager.Services; // For TaskManagerCheckpointingServiceImpl

namespace FlinkDotNet.TaskManager
{
    public class Program
    {
        public static string TaskManagerId { get; private set; } = $"TM-{Guid.NewGuid()}";
        // Default gRPC port for TaskManager services (JobManager will call this)
        public static int GrpcPort { get; private set; } = 50071;
        public static string JobManagerAddress { get; private set; } = "http://localhost:50070";
        public static TaskManagerCoreService? CoreServiceInstance { get; private set; }


        public static async Task Main(string[] args)
        {
            // Basic configuration - replace with actual config mechanism later
            // Allow overriding TM ID and gRPC port via command line for multiple instances
            if (args.Length > 0) TaskManagerId = args[0];
            if (args.Length > 1 && int.TryParse(args[1], out int port)) GrpcPort = port;
            if (args.Length > 2) JobManagerAddress = args[2];


            Console.WriteLine($"Starting TaskManager: {TaskManagerId}");
            Console.WriteLine($"JobManager Address: {JobManagerAddress}");
            Console.WriteLine($"TaskManager gRPC services listening on: http://localhost:{GrpcPort}");

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
                // await taskManagerCoreService.StopAsync(CancellationToken.None);
                Console.WriteLine("TaskManager has shut down.");
            }
        }

        public static IHostBuilder CreateHostBuilder(string[] args) =>
            Host.CreateDefaultBuilder(args)
                .ConfigureServices((hostContext, services) =>
                {
                    // Register TaskManagerCoreService (previously TaskManagerService)
                    // It needs to be an IHostedService to integrate with Generic Host lifecycle
                    services.AddSingleton(new TaskManagerCoreService.Config(TaskManagerId, JobManagerAddress));
                    services.AddHostedService<TaskManagerCoreService>();

                    // Register TaskExecutor
                    services.AddSingleton<TaskExecutor>();

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
                        options.ListenLocalhost(GrpcPort, o => o.Protocols = Microsoft.AspNetCore.Server.Kestrel.Core.HttpProtocols.Http2);
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
#nullable disable
