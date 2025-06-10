using FlinkDotNet.JobManager;
using FlinkDotNet.JobManager.Interfaces;
using FlinkDotNet.JobManager.Services;

var builder = WebApplication.CreateBuilder(args);

// Determine ports from environment variables with Apache Flink style defaults
int httpPort = 8088;
int grpcPort = 50051;

if (int.TryParse(Environment.GetEnvironmentVariable("JOBMANAGER_HTTP_PORT"), out var envHttp))
{
    httpPort = envHttp;
}

if (int.TryParse(Environment.GetEnvironmentVariable("JOBMANAGER_GRPC_PORT"), out var envGrpc))
{
    grpcPort = envGrpc;
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


await app.RunAsync();

namespace FlinkDotNet.JobManager
{
    public record WeatherForecast(DateOnly Date, int TemperatureC, string? Summary)
    {
        public int TemperatureF => 32 + (int)(TemperatureC / 0.5556);
    }
}

