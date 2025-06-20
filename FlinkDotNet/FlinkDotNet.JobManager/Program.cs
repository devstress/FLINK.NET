using System.Net;
using FlinkDotNet.JobManager.Interfaces;
using FlinkDotNet.JobManager.Services;
using FlinkDotNet.JobManager.Services.BackPressure;
using FlinkDotNet.JobManager.Services.StateManagement;
using FlinkDotNet.JobManager.Checkpointing;
using FlinkDotNet.Common.Constants;
using FlinkDotNet.Storage.RocksDB;

namespace FlinkDotNet.JobManager;

public static class Program
{
    public static async Task Main(string[] args)
    {
        var builder = WebApplication.CreateBuilder(args);
        ConfigurePorts(builder);
        ConfigureServices(builder);

        var app = builder.Build();
        ConfigurePipeline(app);
        await app.RunAsync();
    }

    private static void ConfigurePorts(WebApplicationBuilder builder)
    {
        // Check if dynamic port allocation is enabled for Aspire environments
        var useDynamicPorts = Environment.GetEnvironmentVariable("ASPIRE_USE_DYNAMIC_PORTS")?.ToLowerInvariant() == "true";
        
        var httpPort = useDynamicPorts ? 0 : ServicePorts.JobManagerHttp;
        var grpcPort = useDynamicPorts ? 0 : ServicePorts.JobManagerGrpc;

        // Allow environment variable overrides
        if (int.TryParse(Environment.GetEnvironmentVariable(EnvironmentVariables.JobManagerGrpcPort), out var envGrpc))
        {
            grpcPort = envGrpc;
        }

        if (int.TryParse(Environment.GetEnvironmentVariable("JOBMANAGER_HTTP_PORT"), out var envHttp))
        {
            httpPort = envHttp;
        }

        builder.WebHost.ConfigureKestrel(options =>
        {
            if (useDynamicPorts)
            {
                Console.WriteLine("🔄 ASPIRE MODE: JobManager using dynamic port allocation");
                // Let Kestrel assign available ports dynamically
                options.ListenAnyIP(httpPort, o => o.Protocols = Microsoft.AspNetCore.Server.Kestrel.Core.HttpProtocols.Http1);
                options.ListenAnyIP(grpcPort, o => o.Protocols = Microsoft.AspNetCore.Server.Kestrel.Core.HttpProtocols.Http2);
            }
            else
            {
                Console.WriteLine($"🔧 FIXED PORT MODE: JobManager using HTTP port {httpPort}, gRPC port {grpcPort}");
                options.Listen(IPAddress.Any, httpPort, o => o.Protocols = Microsoft.AspNetCore.Server.Kestrel.Core.HttpProtocols.Http1);
                options.Listen(IPAddress.Any, grpcPort, o => o.Protocols = Microsoft.AspNetCore.Server.Kestrel.Core.HttpProtocols.Http2);
            }
        });
    }

    [System.Diagnostics.CodeAnalysis.SuppressMessage("Major Code Smell", "S3776", Justification = "Service wiring requires multiple registrations")]
    private static void ConfigureServices(WebApplicationBuilder builder)
    {
        builder.Services.AddControllers();
        builder.Services.AddEndpointsApiExplorer();
        builder.Services.AddSingleton<IJobRepository, InMemoryJobRepository>();
        builder.Services.AddSwaggerGen();
        builder.Services.AddGrpc();

        builder.Services.Configure<RocksDBOptions>(options =>
        {
            options.DataDirectory = Environment.GetEnvironmentVariable("ROCKSDB_DATA_DIR") ?? Path.Combine(Environment.GetFolderPath(Environment.SpecialFolder.LocalApplicationData), "flink-rocksdb");
            options.MaxBackgroundJobs = int.TryParse(Environment.GetEnvironmentVariable("ROCKSDB_MAX_BACKGROUND_JOBS"), out var jobs) ? jobs : 4;
            options.WriteBufferSize = ulong.TryParse(Environment.GetEnvironmentVariable("ROCKSDB_WRITE_BUFFER_SIZE"), out var bufferSize) ? bufferSize : 64 * 1024 * 1024;
        });

        builder.Services.AddSingleton<CheckpointCoordinator>(provider =>
        {
            var logger = provider.GetRequiredService<ILogger<CheckpointCoordinator>>();
            var jobRepository = provider.GetRequiredService<IJobRepository>();
            var config = new JobManagerConfig
            {
                JobManagerId = Environment.GetEnvironmentVariable("JOBMANAGER_ID") ?? $"JM-{Environment.MachineName}-{DateTime.UtcNow:yyyyMMdd-HHmmss}",
                CheckpointIntervalSecs = int.TryParse(Environment.GetEnvironmentVariable("CHECKPOINT_INTERVAL_SECS"), out var interval) ? interval : 30,
                CheckpointTimeoutSecs = int.TryParse(Environment.GetEnvironmentVariable("CHECKPOINT_TIMEOUT_SECS"), out var timeout) ? timeout : 120
            };
            return new CheckpointCoordinator("default-job", jobRepository, logger, config);
        });

        builder.Services.AddSingleton<StateCoordinator>();
        builder.Services.AddSingleton<TaskManagerOrchestrator>(provider =>
        {
            var logger = provider.GetRequiredService<ILogger<TaskManagerOrchestrator>>();
            var config = new TaskManagerOrchestratorConfiguration
            {
                MinInstances = int.TryParse(Environment.GetEnvironmentVariable("TASKMANAGER_MIN_INSTANCES"), out var min) ? min : 1,
                MaxInstances = int.TryParse(Environment.GetEnvironmentVariable("TASKMANAGER_MAX_INSTANCES"), out var max) ? max : 10,
                DeploymentType = Enum.TryParse<TaskManagerDeploymentType>(Environment.GetEnvironmentVariable("TASKMANAGER_DEPLOYMENT_TYPE"), out var deployType) ? deployType : TaskManagerDeploymentType.Process,
                JobManagerAddress = Environment.GetEnvironmentVariable("JOBMANAGER_ADDRESS") ?? "localhost:6123",
                TaskManagerMemoryMB = int.TryParse(Environment.GetEnvironmentVariable("TASKMANAGER_MEMORY_MB"), out var memory) ? memory : 1024,
                KubernetesNamespace = Environment.GetEnvironmentVariable("KUBERNETES_NAMESPACE")
            };
            return new TaskManagerOrchestrator(logger, config);
        });

        builder.Services.AddSingleton<BackPressureCoordinator>(provider =>
        {
            var logger = provider.GetRequiredService<ILogger<BackPressureCoordinator>>();
            var loggerFactory = provider.GetRequiredService<ILoggerFactory>();
            var stateCoordinator = provider.GetRequiredService<StateCoordinator>();
            var taskManagerOrchestrator = provider.GetRequiredService<TaskManagerOrchestrator>();
            var config = new BackPressureConfiguration
            {
                ScaleUpThreshold = double.TryParse(Environment.GetEnvironmentVariable("BACKPRESSURE_SCALE_UP_THRESHOLD"), out var upThreshold) ? upThreshold : 0.8,
                ScaleDownThreshold = double.TryParse(Environment.GetEnvironmentVariable("BACKPRESSURE_SCALE_DOWN_THRESHOLD"), out var downThreshold) ? downThreshold : 0.3,
                MinTaskManagers = int.TryParse(Environment.GetEnvironmentVariable("BACKPRESSURE_MIN_TASKMANAGERS"), out var minTm) ? minTm : 1,
                MaxTaskManagers = int.TryParse(Environment.GetEnvironmentVariable("BACKPRESSURE_MAX_TASKMANAGERS"), out var maxTm) ? maxTm : 10
            };
            return new BackPressureCoordinator(logger, stateCoordinator, taskManagerOrchestrator, config, loggerFactory);
        });

        // Register hosted service to ensure BackPressureCoordinator is started
        builder.Services.AddHostedService<BackPressureHostedService>();

        builder.Logging.ClearProviders();
        builder.Logging.AddConsole();
        builder.Logging.SetMinimumLevel(LogLevel.Information);
    }

    private static void ConfigurePipeline(WebApplication app)
    {
        if (app.Environment.IsDevelopment())
        {
            app.UseSwagger();
            app.UseSwaggerUI();
        }

        var allowUnsecured = Environment.GetEnvironmentVariable("ASPIRE_ALLOW_UNSECURED_TRANSPORT");
        if (!string.Equals(allowUnsecured, "true", StringComparison.OrdinalIgnoreCase))
        {
            app.UseHttpsRedirection();
        }

        app.MapGrpcService<JobManagerInternalApiService>();
        app.MapGrpcService<TaskManagerRegistrationServiceImpl>();

        var summaries = new[] { "Freezing", "Bracing", "Chilly", "Cool", "Mild", "Warm", "Balmy", "Hot", "Sweltering", "Scorching" };
        app.MapGet("/weatherforecast", () =>
        {
            var forecast = Enumerable.Range(1, 5).Select(index => new WeatherForecast(DateOnly.FromDateTime(DateTime.Now.AddDays(index)), Random.Shared.Next(-20, 55), summaries[Random.Shared.Next(summaries.Length)])).ToArray();
            return forecast;
        }).WithName("GetWeatherForecast").WithOpenApi();
    }

    public record WeatherForecast(DateOnly Date, int TemperatureC, string? Summary)
    {
        public int TemperatureF => 32 + (int)(TemperatureC / 0.5556);
    }
}
