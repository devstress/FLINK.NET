// Existing using statements (implicit for DistributedApplication, Projects)

var builder = DistributedApplication.CreateBuilder(args);

// Add resources
// Bind Redis and Kafka containers to their default ports so tests can reliably
// connect using localhost addresses without parsing the Aspire manifest.
var redis = builder.AddRedis("redis", port: 6379);
var kafka = builder.AddKafka("kafka", port: 9092); // Add Kafka resource

// Set up for 1 million message high throughput test
var simulatorNumMessages = Environment.GetEnvironmentVariable("SIMULATOR_NUM_MESSAGES") ?? "1000000";

// Add JobManager (1 instance)
var jobManager = builder.AddProject<Projects.FlinkDotNet_JobManager>("jobmanager")
    .WithEnvironment("JOBMANAGER_HTTP_PORT", "8088")
    .WithEnvironment("JOBMANAGER_GRPC_PORT", "50051")
    .WithEnvironment("DOTNET_ENVIRONMENT", "Development");

// Add TaskManagers (10 instances as per requirements)
for (int i = 1; i <= 10; i++)
{
    builder.AddProject<Projects.FlinkDotNet_TaskManager>($"taskmanager{i}")
        .WithEnvironment("TaskManagerId", $"TM-{i.ToString("D2")}")
        .WithEnvironment("TASKMANAGER_GRPC_PORT", (50070 + i).ToString())
        .WithEnvironment("services__jobmanager__grpc__0", "http://localhost:50051")
        .WithEnvironment("DOTNET_ENVIRONMENT", "Development");
}

// Provide Redis and Kafka connection information to the FlinkJobSimulator
builder.AddProject<Projects.FlinkJobSimulator>("flinkjobsimulator")
    .WithReference(redis) // Makes "ConnectionStrings__redis" available
    .WithReference(kafka) // Makes "ConnectionStrings__kafka" available (or similar for bootstrap servers)
    .WithEnvironment("SIMULATOR_NUM_MESSAGES", simulatorNumMessages) // Use environment variable or 1M default
    .WithEnvironment("SIMULATOR_REDIS_KEY_SINK_COUNTER", "flinkdotnet:sample:processed_message_counter") // Redis sink counter key
    .WithEnvironment("SIMULATOR_REDIS_KEY_GLOBAL_SEQUENCE", "flinkdotnet:global_sequence_id") // Redis global sequence key
    .WithEnvironment("SIMULATOR_KAFKA_TOPIC", "flinkdotnet.sample.topic") // Default Kafka topic
    .WithEnvironment("ASPIRE_DASHBOARD_URL", "http://localhost:18888") // Default Aspire Dashboard URL
    .WithEnvironment("DOTNET_ENVIRONMENT", "Development");

await builder.Build().RunAsync();
