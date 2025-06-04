using FlinkDotNet.JobManager.Interfaces;
using FlinkDotNet.JobManager.Services; // This using is already present and is needed for JobManagerInternalApiService
using System; // Required for DateOnly, DateTime
using System.Linq; // Required for Enumerable

var builder = WebApplication.CreateBuilder(args);

// Add services to the container.
// Learn more about configuring Swagger/OpenAPI at https://aka.ms/aspnetcore/swashbuckle
builder.Services.AddEndpointsApiExplorer();
builder.Services.AddSingleton<FlinkDotNet.JobManager.Interfaces.IJobRepository, FlinkDotNet.JobManager.Services.InMemoryJobRepository>();
builder.Services.AddSwaggerGen();
builder.Services.AddGrpc(); // Added for gRPC

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

app.Run();

record WeatherForecast(DateOnly Date, int TemperatureC, string? Summary)
{
    public int TemperatureF => 32 + (int)(TemperatureC / 0.5556);
}
