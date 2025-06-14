// Existing using statements (implicit for DistributedApplication, Projects)

namespace FlinkDotNetAspire.AppHost.AppHost;

public static class Program
{
    public static async Task Main(string[] args)
    {
        var builder = DistributedApplication.CreateBuilder(args);

        // Add resources with Aspire's default dynamic port allocation
        // Don't force specific ports - let Aspire handle port management
        var redis = builder.AddConnectionString("redis");
        var kafka = builder.AddKafka("kafka")
            .PublishAsContainer(); // Ensure Kafka is accessible from host

        // Set up for 1 million message high throughput test
        var simulatorNumMessages = Environment.GetEnvironmentVariable("SIMULATOR_NUM_MESSAGES") ?? "1000000";

        // Add JobManager (1 instance) - Using container instead of project
        var jobManager = builder.AddContainer("jobmanager", "flinkdotnet/jobmanager", "latest")
            .WithEnvironment("DOTNET_ENVIRONMENT", "Development")
            .WithEnvironment("ASPNETCORE_ENVIRONMENT", "Development")
            .WithHttpEndpoint(targetPort: 8080, name: "http")
            .WithEndpoint(targetPort: 8081, scheme: "http", name: "grpc");

        // Add TaskManagers (20 instances as per requirements) - Using containers instead of projects
        for (int i = 1; i <= 20; i++)
        {
            builder.AddContainer($"taskmanager{i}", "flinkdotnet/taskmanager", "latest")
                .WithEnvironment("TaskManagerId", $"TM-{i.ToString("D2")}")
                .WithEnvironment("DOTNET_ENVIRONMENT", "Development")
                .WithEnvironment("ASPNETCORE_ENVIRONMENT", "Development")
                .WithEnvironment("JOBMANAGER_GRPC_ADDRESS", "http://jobmanager:8081")
                .WithHttpEndpoint(targetPort: 8080, name: "grpc");
        }

        // Provide Redis and Kafka connection information to the FlinkJobSimulator
        builder.AddProject<Projects.FlinkJobSimulator>("flinkjobsimulator")
            .WithReference(redis) // Makes "ConnectionStrings__redis" available
            .WithReference(kafka) // Makes "ConnectionStrings__kafka" available (or similar for bootstrap servers)
            .WithEnvironment("SIMULATOR_NUM_MESSAGES", simulatorNumMessages) // Use environment variable or 1M default
            .WithEnvironment("SIMULATOR_REDIS_KEY_SINK_COUNTER", "flinkdotnet:sample:processed_message_counter") // Redis sink counter key
            .WithEnvironment("SIMULATOR_REDIS_KEY_GLOBAL_SEQUENCE", "flinkdotnet:global_sequence_id") // Redis global sequence key
            .WithEnvironment("SIMULATOR_KAFKA_TOPIC", "flinkdotnet.sample.topic") // Default Kafka topic
            .WithEnvironment("DOTNET_ENVIRONMENT", "Development");

        await builder.Build().RunAsync();
    }
}
