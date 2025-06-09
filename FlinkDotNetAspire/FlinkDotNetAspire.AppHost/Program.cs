// Existing using statements (implicit for DistributedApplication, Projects)
// using FlinkDotNet.JobManager; // Keep this if Projects.FlinkDotNet_JobManager is used, seems to be for Projects.*

var builder = DistributedApplication.CreateBuilder(args);

// Add resources
var redis = builder.AddRedis("redis");
var kafka = builder.AddKafka("kafka"); // Add Kafka resource

var jobManagerHttpEndpoint = "http://localhost:8088";
var jobManagerGrpcEndpoint = "http://localhost:50051";

var jobManager = builder.AddProject<Projects.FlinkDotNet_JobManager>("jobmanager")
    .WithHttpEndpoint(targetPort: 8080, name: "rest")
    .WithEndpoint(targetPort:50051, name: "grpc", scheme: "http", protocol: Microsoft.AspNetCore.Hosting.Server.Protocols.HttpProtocols.Http2)
    .WithEnvironment("ASPNETCORE_URLS", $"{jobManagerHttpEndpoint};{jobManagerGrpcEndpoint}")
    .WithEnvironment("DOTNET_ENVIRONMENT", "Development");

builder.AddProject<Projects.FlinkDotNet_TaskManager>("taskmanager1")
    .WithReference(jobManager.GetEndpoint("grpc"))
    .WithEnvironment("TaskManagerId", "tm-1")
    .WithEnvironment("DOTNET_ENVIRONMENT", "Development");

// Provide Redis and Kafka connection information to the FlinkJobSimulator
builder.AddProject<Projects.FlinkJobSimulator>("flinkjobsimulator")
    .WithReference(jobManager.GetEndpoint("grpc"))
    .WithReference(redis) // Makes "ConnectionStrings__redis" available
    .WithReference(kafka) // Makes "ConnectionStrings__kafka" available (or similar for bootstrap servers)
    .WithEnvironment("SIMULATOR_NUM_MESSAGES", "1000000") // Use 1M messages for integration test
    .WithEnvironment("SIMULATOR_REDIS_KEY", "flinkdotnet:sample:counter") // Default Redis key
    .WithEnvironment("SIMULATOR_KAFKA_TOPIC", "flinkdotnet.sample.topic") // Default Kafka topic
    .WithEnvironment("DOTNET_ENVIRONMENT", "Development");

builder.Build().Run();
