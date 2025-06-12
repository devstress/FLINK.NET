// TODO: Reduce cognitive complexity
using FlinkDotNet.JobManager;
using FlinkDotNet.JobManager.Interfaces;
using FlinkDotNet.JobManager.Services;
using FlinkDotNet.JobManager.Services.StateManagement;
using FlinkDotNet.JobManager.Services.BackPressure;
using FlinkDotNet.JobManager.Checkpointing;
using FlinkDotNet.Common.Constants;
using FlinkDotNet.Storage.RocksDB;

var builder = WebApplication.CreateBuilder(args);

// Determine ports from environment variables with Apache Flink style defaults
int httpPort = ServicePorts.JobManagerHttp;
int grpcPort = ServicePorts.JobManagerGrpc;

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
    options.Listen(System.Net.IPAddress.Any, httpPort, o => o.Protocols = Microsoft.AspNetCore.Server.Kestrel.Core.HttpProtocols.Http1);
    options.Listen(System.Net.IPAddress.Any, grpcPort, o => o.Protocols = Microsoft.AspNetCore.Server.Kestrel.Core.HttpProtocols.Http2);
});
builder.Services.AddControllers(); // Added ServiceDefaults

// Add services to the container.
// Learn more about configuring Swagger/OpenAPI at https://aka.ms/aspnetcore/swashbuckle
builder.Services.AddEndpointsApiExplorer();
// IJobRepository is now registered like this due to ILogger dependency:
builder.Services.AddSingleton<IJobRepository, InMemoryJobRepository>();
// If InMemoryJobRepository itself needs to be resolved directly (it doesn't seem to be),
// it would also be covered by AddSingleton<IJobRepository, InMemoryJobRepository>() if it's the implementation.
builder.Services.AddSwaggerGen();
builder.Services.AddGrpc(); // Added for gRPC

// Apache Flink 2.0 enhanced components
builder.Services.Configure<RocksDBOptions>(options =>
{
    options.DataDirectory = Environment.GetEnvironmentVariable("ROCKSDB_DATA_DIR") ?? "/tmp/flink-rocksdb";
    options.MaxBackgroundJobs = int.TryParse(Environment.GetEnvironmentVariable("ROCKSDB_MAX_BACKGROUND_JOBS"), out var jobs) ? jobs : 4;
    options.WriteBufferSize = ulong.TryParse(Environment.GetEnvironmentVariable("ROCKSDB_WRITE_BUFFER_SIZE"), out var bufferSize) ? bufferSize : 64 * 1024 * 1024;
});

// Register Apache Flink 2.0 state management components
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
    var stateCoordinator = provider.GetRequiredService<StateCoordinator>();
    var taskManagerOrchestrator = provider.GetRequiredService<TaskManagerOrchestrator>();
    var config = new BackPressureConfiguration
    {
        ScaleUpThreshold = double.TryParse(Environment.GetEnvironmentVariable("BACKPRESSURE_SCALE_UP_THRESHOLD"), out var upThreshold) ? upThreshold : 0.8,
        ScaleDownThreshold = double.TryParse(Environment.GetEnvironmentVariable("BACKPRESSURE_SCALE_DOWN_THRESHOLD"), out var downThreshold) ? downThreshold : 0.3,
        MinTaskManagers = int.TryParse(Environment.GetEnvironmentVariable("BACKPRESSURE_MIN_TASKMANAGERS"), out var minTm) ? minTm : 1,
        MaxTaskManagers = int.TryParse(Environment.GetEnvironmentVariable("BACKPRESSURE_MAX_TASKMANAGERS"), out var maxTm) ? maxTm : 10
    };
    return new BackPressureCoordinator(logger, stateCoordinator, taskManagerOrchestrator, config);
});

// Configure logging
builder.Logging.ClearProviders(); // Optional: Remove other providers like EventLog
builder.Logging.AddConsole();
builder.Logging.SetMinimumLevel(LogLevel.Information); // Set desired minimum log level

var app = builder.Build();

// Configure the HTTP request pipeline.
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

// Map REST API controllers (ASP.NET Core default if using [ApiController] attribute with routing)
// If not using default conventional routing for controllers, ensure app.MapControllers(); is present.
// For this project, default controller routing via attributes is assumed.

// Map gRPC service
app.MapGrpcService<JobManagerInternalApiService>(); // Added for gRPC
app.MapGrpcService<TaskManagerRegistrationServiceImpl>(); // Added for new TM registration service

var summaries = new[]
{
    "Freezing", "Bracing", "Chilly", "Cool", "Mild", "Warm", "Balmy", "Hot", "Sweltering", "Scorching"
};

app.MapGet("/weatherforecast", () =>
{
    var forecast =  Enumerable.Range(1, 5).Select(index =>
        new WeatherForecast
        (
            DateOnly.FromDateTime(DateTime.Now.AddDays(index)),
            Random.Shared.Next(-20, 55),
            summaries[Random.Shared.Next(summaries.Length)]
        ))
        .ToArray();
    return forecast;
})
.WithName("GetWeatherForecast")
.WithOpenApi();

// Ensure controllers are mapped if not done by default by WebApplication type.
// For minimal APIs with controllers, builder.Services.AddControllers() and app.MapControllers() are usually needed.
// The `dotnet new webapi` template for .NET 6+ often includes this setup implicitly or via AddEndpointsApiExplorer.
// If API controllers are not found, explicit registration might be needed.
// For now, assuming JobManagerController (REST API) is correctly mapped by existing setup.


await app.RunAsync();

namespace FlinkDotNet.JobManager
{
    public record WeatherForecast(DateOnly Date, int TemperatureC, string? Summary)
    {
        public int TemperatureF => 32 + (int)(TemperatureC / 0.5556);
    }
}

