using FlinkDotNet.JobManager.Interfaces;
using FlinkDotNet.JobManager.Services;
using FlinkDotNet.JobManager.Checkpointing; // Added for CheckpointCoordinator
using FlinkDotNet.JobManager.Models; // Added for JobManagerConfig
using System;
using System.Linq; // Required for Enumerable
using Microsoft.Extensions.Hosting; // Added for AddServiceDefaults
using FlinkDotNet.Core.Abstractions.Storage;
using FlinkDotNet.Storage.FileSystem;
using FlinkDotNet.Storage.AzureBlob;
using FlinkDotNet.Storage.S3;
using FlinkDotNet.Storage.GCS;

var builder = WebApplication.CreateBuilder(args);
builder.AddServiceDefaults(); // Added ServiceDefaults

// Add services to the container.
// Learn more about configuring Swagger/OpenAPI at https://aka.ms/aspnetcore/swashbuckle
builder.Services.AddEndpointsApiExplorer();
// IJobRepository is now registered like this due to ILogger dependency:
builder.Services.AddSingleton<IJobRepository, InMemoryJobRepository>();
// If InMemoryJobRepository itself needs to be resolved directly (it doesn't seem to be),
// it would also be covered by AddSingleton<IJobRepository, InMemoryJobRepository>() if it's the implementation.
// For clarity, if it were ever resolved directly: builder.Services.AddSingleton<InMemoryJobRepository>();
builder.Services.AddSwaggerGen();
builder.Services.AddGrpc(); // Added for gRPC

// Configure logging
builder.Logging.ClearProviders(); // Optional: Remove other providers like EventLog
builder.Logging.AddConsole();
builder.Logging.SetMinimumLevel(LogLevel.Information); // Set desired minimum log level

// Configure State Snapshot Store
var stateStoreConfig = builder.Configuration.GetSection("StateSnapshotStore");
var storeType = stateStoreConfig.GetValue<string>("Type")?.ToLowerInvariant();

switch (storeType)
{
    case "azureblob":
        builder.Services.AddSingleton<AzureBlobStorageSnapshotStoreOptions>(sp =>
        {
            var options = new AzureBlobStorageSnapshotStoreOptions();
            var azureConfigSection = stateStoreConfig.GetSection("AzureBlob");

            azureConfigSection.Bind(options); // Assumes keys in appsettings.json match property names

            // Fallback to environment variables if specific properties are still empty after binding
            if (string.IsNullOrWhiteSpace(options.ConnectionString))
            {
                options.ConnectionString = Environment.GetEnvironmentVariable("FLINKDOTNET_AZURE_CONNECTION_STRING");

                if (string.IsNullOrWhiteSpace(options.ConnectionString))
                {
                    // Only try constructing from parts if ConnectionString itself wasn't found via direct config or its specific env var
                    if (string.IsNullOrWhiteSpace(options.AccountName))
                        options.AccountName = Environment.GetEnvironmentVariable("FLINKDOTNET_AZURE_ACCOUNT_NAME");
                    if (string.IsNullOrWhiteSpace(options.AccountKey))
                        options.AccountKey = Environment.GetEnvironmentVariable("FLINKDOTNET_AZURE_ACCOUNT_KEY");
                    if (string.IsNullOrWhiteSpace(options.BlobServiceEndpoint))
                        options.BlobServiceEndpoint = Environment.GetEnvironmentVariable("FLINKDOTNET_AZURE_BLOB_SERVICE_ENDPOINT");
                }
            }

            if (string.IsNullOrWhiteSpace(options.ContainerName))
            {
                options.ContainerName = Environment.GetEnvironmentVariable("FLINKDOTNET_AZURE_CONTAINER_NAME") ?? "flinkdotnet-default-container";
            }
            // BasePath can also have a fallback if needed:
            // if (string.IsNullOrWhiteSpace(options.BasePath))
            // {
            //    options.BasePath = Environment.GetEnvironmentVariable("FLINKDOTNET_AZURE_BASE_PATH") ?? "";
            // }
            return options;
        });
        builder.Services.AddSingleton<IStateSnapshotStore, AzureBlobStorageSnapshotStore>();
        Console.WriteLine("Using Azure Blob Storage for state snapshots (hybrid configuration).");
        break;

    case "s3":
        builder.Services.AddSingleton<S3SnapshotStoreOptions>(sp =>
        {
            var s3ConfigSection = stateStoreConfig.GetSection("S3");
            var options = new S3SnapshotStoreOptions();

            // Bind the whole section. Properties in S3SnapshotStoreOptions should match keys in appsettings.S3 for this to work well.
            // Since we want appsettings keys like "s3.endpoint", we need to map them.
            // Alternative: Manually read each Flink-named key.
            options.BucketName = s3ConfigSection.GetValue<string>("BucketName") ?? Environment.GetEnvironmentVariable("FLINKDOTNET_S3_BUCKET_NAME") ?? "";
            options.Region = s3ConfigSection.GetValue<string>("Region") ?? Environment.GetEnvironmentVariable("AWS_REGION"); // Standard AWS env var
            options.BasePath = s3ConfigSection.GetValue<string>("BasePath") ?? "";
            options.SessionToken = s3ConfigSection.GetValue<string>("SessionToken") ?? Environment.GetEnvironmentVariable("AWS_SESSION_TOKEN"); // Standard AWS env var

            options.Endpoint = s3ConfigSection.GetValue<string>("s3.endpoint") ?? Environment.GetEnvironmentVariable("FLINK_S3_ENDPOINT"); // Flink-like env var
            options.PathStyleAccess = s3ConfigSection.GetValue<bool?>("s3.path.style.access") ?? (bool.TryParse(Environment.GetEnvironmentVariable("FLINK_S3_PATH_STYLE_ACCESS"), out var ps) ? ps : false);
            options.AccessKey = s3ConfigSection.GetValue<string>("s3.access-key") ?? Environment.GetEnvironmentVariable("AWS_ACCESS_KEY_ID"); // Standard AWS env var
            options.SecretKey = s3ConfigSection.GetValue<string>("s3.secret-key") ?? Environment.GetEnvironmentVariable("AWS_SECRET_ACCESS_KEY"); // Standard AWS env var

            if(string.IsNullOrWhiteSpace(options.BucketName))
            {
                Console.Error.WriteLine("S3 BucketName is not configured. S3 backend may not function correctly.");
            }
            return options;
        });
        builder.Services.AddSingleton<IStateSnapshotStore, S3SnapshotStore>();
        Console.WriteLine("Using S3 for state snapshots (Flink-aligned configuration).");
        break;

    case "gcs":
        builder.Services.AddSingleton<GcsSnapshotStoreOptions>(sp =>
        {
            var gcsConfigSection = stateStoreConfig.GetSection("GCS");
            var options = new GcsSnapshotStoreOptions();
            gcsConfigSection.Bind(options); // Bind properties matching appsettings keys

            // Fallback for CredentialsFilePath from environment if not in appsettings
            if (string.IsNullOrWhiteSpace(options.CredentialsFilePath))
            {
                options.CredentialsFilePath = Environment.GetEnvironmentVariable("FLINKDOTNET_GCS_CREDENTIALS_FILE_PATH")
                                            ?? Environment.GetEnvironmentVariable("GOOGLE_APPLICATION_CREDENTIALS"); // Standard env var
            }
            // Fallback for Endpoint (emulator)
            if (string.IsNullOrWhiteSpace(options.Endpoint))
            {
                options.Endpoint = Environment.GetEnvironmentVariable("FLINKDOTNET_GCS_ENDPOINT");
            }
            // Fallback for ProjectId
            if (string.IsNullOrWhiteSpace(options.ProjectId))
            {
                options.ProjectId = Environment.GetEnvironmentVariable("FLINKDOTNET_GCS_PROJECT_ID")
                                    ?? Environment.GetEnvironmentVariable("GOOGLE_CLOUD_PROJECT"); // Standard env var
            }
            // Fallback for BucketName (critical)
            if (string.IsNullOrWhiteSpace(options.BucketName))
            {
                options.BucketName = Environment.GetEnvironmentVariable("FLINKDOTNET_GCS_BUCKET_NAME") ?? "";
                if(string.IsNullOrWhiteSpace(options.BucketName))
                {
                    Console.Error.WriteLine("[JobManager] GCS BucketName is not configured. GCS backend may not function.");
                }
            }
            // BasePath can also have a fallback if needed
            if (string.IsNullOrWhiteSpace(options.BasePath))
            {
                options.BasePath = Environment.GetEnvironmentVariable("FLINKDOTNET_GCS_BASE_PATH") ?? "";
            }

            Console.WriteLine($"[JobManager] GCS Options Loaded: Bucket='{options.BucketName}', Project='{options.ProjectId}', Endpoint='{options.Endpoint}', CredentialsFile='{!string.IsNullOrWhiteSpace(options.CredentialsFilePath)}'.");
            return options;
        });
        builder.Services.AddSingleton<IStateSnapshotStore, GcsSnapshotStore>();
        Console.WriteLine("[JobManager] Using Google Cloud Storage (GCS) for state snapshots.");
        break;

    case "filesystem":
    default:
        builder.Services.AddSingleton<FileSystemSnapshotStoreOptions>(sp =>
        {
            var options = new FileSystemSnapshotStoreOptions();
            stateStoreConfig.GetSection("FileSystem").Bind(options);
            return options;
        });
        builder.Services.AddSingleton<IStateSnapshotStore, FileSystemSnapshotStore>();
        Console.WriteLine("Using File System for state snapshots.");
        break;
}

var app = builder.Build();

// Configure the HTTP request pipeline.
if (app.Environment.IsDevelopment())
{
    app.UseSwagger();
    app.UseSwaggerUI();
}

app.UseHttpsRedirection();

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

// Test CheckpointCoordinator setup - This is now outdated due to constructor changes
// var testJobId = "test-job-001";
// var coordinatorConfig = new JobManagerConfig { CheckpointIntervalSecs = 15 };
// ILogger<CheckpointCoordinator> dummyCoordinatorLogger = app.Services.GetRequiredService<ILoggerFactory>().CreateLogger<CheckpointCoordinator>();
// IJobRepository dummyRepo = app.Services.GetRequiredService<IJobRepository>();
// var checkpointCoordinator = new CheckpointCoordinator(testJobId, dummyRepo, dummyCoordinatorLogger, coordinatorConfig);
// TaskManagerRegistrationServiceImpl.JobCoordinators.TryAdd(testJobId, checkpointCoordinator);
// checkpointCoordinator.Start();

app.Run();

record WeatherForecast(DateOnly Date, int TemperatureC, string? Summary)
{
    public int TemperatureF => 32 + (int)(TemperatureC / 0.5556);
}
