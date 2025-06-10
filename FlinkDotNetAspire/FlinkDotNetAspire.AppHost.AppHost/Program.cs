// Existing using statements (implicit for DistributedApplication, Projects)

var builder = DistributedApplication.CreateBuilder(args);

// Add resources
// Bind Redis and Kafka containers to their default ports so tests can reliably
// connect using localhost addresses without parsing the Aspire manifest.
var redis = builder.AddRedis("redis", port: 6379);
var kafka = builder.AddKafka("kafka", port: 9092); // Add Kafka resource

var jobManagerHttpPort = 8088;
var jobManagerGrpcPort = 50051;

var aspNetCoreUrls = $"http://0.0.0.0:{jobManagerHttpPort};http://0.0.0.0:{jobManagerGrpcPort}";

var jobManager = builder.AddProject<Projects.FlinkDotNet_JobManager>("jobmanager")
    .WithHttpEndpoint(targetPort: jobManagerHttpPort, name: "rest")
    .WithEndpoint(targetPort: jobManagerGrpcPort, name: "grpc", scheme: "http")
    .WithEnvironment("JOBMANAGER_HTTP_PORT", jobManagerHttpPort.ToString())
    .WithEnvironment("JOBMANAGER_GRPC_PORT", jobManagerGrpcPort.ToString())
    .WithEnvironment("ASPNETCORE_URLS", aspNetCoreUrls)
    .WithEnvironment("DOTNET_ENVIRONMENT", "Development");

builder.AddProject<Projects.FlinkDotNet_TaskManager>("taskmanager1")
    .WithReference(jobManager.GetEndpoint("grpc"))
    .WithEnvironment("TaskManagerId", "tm-1")
    .WithEnvironment("TASKMANAGER_GRPC_PORT", "50071")
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
