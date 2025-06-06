using FlinkDotNet.JobManager.Interfaces;
using FlinkDotNet.JobManager.Services;
using FlinkDotNet.JobManager.Checkpointing; // Added for CheckpointCoordinator
using FlinkDotNet.JobManager.Models; // Added for JobManagerConfig
using System;
using System.Linq; // Required for Enumerable

var builder = WebApplication.CreateBuilder(args);

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
