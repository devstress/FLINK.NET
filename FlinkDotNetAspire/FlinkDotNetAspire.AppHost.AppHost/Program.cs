// Existing using statements (implicit for DistributedApplication, Projects)
using FlinkDotNet.Common.Constants;

namespace FlinkDotNetAspire.AppHost.AppHost;

public static class Program
{
    public static async Task Main(string[] args)
    {
        var builder = DistributedApplication.CreateBuilder(args);

        // Add resources with Aspire's default dynamic port allocation
        // Don't force specific ports - let Aspire handle port management
        var redis = builder.AddRedis("redis")
            .PublishAsContainer(); // Ensure Redis is accessible from host
        var kafka = builder.AddKafka("kafka")
            .WithEnvironment("KAFKA_AUTO_CREATE_TOPICS_ENABLE", "true")
            .WithEnvironment("KAFKA_NUM_PARTITIONS", "1") 
            .WithEnvironment("KAFKA_DEFAULT_REPLICATION_FACTOR", "1")
            .PublishAsContainer(); // Ensure Kafka is accessible from host

        // Set up for 1 million message high throughput test
        var simulatorNumMessages = Environment.GetEnvironmentVariable("SIMULATOR_NUM_MESSAGES") ?? "1000000";

        // Add JobManager (1 instance)
        builder.AddProject<Projects.FlinkDotNet_JobManager>("jobmanager")
            .WithEnvironment("JOBMANAGER_HTTP_PORT", ServicePorts.JobManagerHttp.ToString())
            .WithEnvironment(EnvironmentVariables.JobManagerGrpcPort, ServicePorts.JobManagerGrpc.ToString())
            .WithEnvironment("DOTNET_ENVIRONMENT", "Development");

        // Add TaskManagers (20 instances as per requirements)
        for (int i = 1; i <= 20; i++)
        {
            builder.AddProject<Projects.FlinkDotNet_TaskManager>($"taskmanager{i}")
                .WithEnvironment("TaskManagerId", $"TM-{i.ToString("D2")}")
                .WithEnvironment("TASKMANAGER_GRPC_PORT", ServiceUris.GetTaskManagerAspirePort(i).ToString())
                .WithEnvironment("services__jobmanager__grpc__0", ServiceUris.Insecure.JobManagerGrpcHttp)
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
            .WithEnvironment("DOTNET_ENVIRONMENT", "Development");

        await builder.Build().RunAsync();
    }
}
