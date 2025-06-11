// Existing using statements (implicit for DistributedApplication, Projects)
using FlinkDotNet.Common.Constants;

public class Program
{
    public static async Task Main(string[] args)
    {
        var builder = DistributedApplication.CreateBuilder(args);

        // Add resources
        // Use configurable ports for Redis and Kafka to avoid conflicts in CI environments
        // Priority: DOTNET_REDIS_PORT > DOTNET_REDIS_URL parsing > default
        var redisPort = ServicePorts.Redis;
        var kafkaPort = ServicePorts.Kafka;
        
        // Try to get port from environment variables set by port discovery script
        if (int.TryParse(Environment.GetEnvironmentVariable("DOTNET_REDIS_PORT"), out var rPort))
        {
            redisPort = rPort;
        }
        else if (!string.IsNullOrEmpty(Environment.GetEnvironmentVariable("DOTNET_REDIS_URL")))
        {
            // Try to parse port from URL format like "localhost:6379"
            var redisUrl = Environment.GetEnvironmentVariable("DOTNET_REDIS_URL");
            if (redisUrl!.Contains(':') && int.TryParse(redisUrl.Split(':')[1], out var parsedRedisPort))
            {
                redisPort = parsedRedisPort;
            }
        }
        
        if (int.TryParse(Environment.GetEnvironmentVariable("DOTNET_KAFKA_PORT"), out var kPort))
        {
            kafkaPort = kPort;
        }
        else if (!string.IsNullOrEmpty(Environment.GetEnvironmentVariable("DOTNET_KAFKA_BOOTSTRAP_SERVERS")))
        {
            // Try to parse port from bootstrap servers format like "localhost:9092"
            var kafkaBootstrap = Environment.GetEnvironmentVariable("DOTNET_KAFKA_BOOTSTRAP_SERVERS");
            if (kafkaBootstrap!.Contains(':') && int.TryParse(kafkaBootstrap.Split(':')[1], out var parsedKafkaPort))
            {
                kafkaPort = parsedKafkaPort;
            }
        }
        
        Console.WriteLine($"Using Redis port: {redisPort}, Kafka port: {kafkaPort}");
        
        var redis = builder.AddRedis("redis", port: redisPort);
        var kafka = builder.AddKafka("kafka", port: kafkaPort); // Add Kafka resource

        // Set up for 1 million message high throughput test
        var simulatorNumMessages = Environment.GetEnvironmentVariable("SIMULATOR_NUM_MESSAGES") ?? "1000000";

        // Add JobManager (1 instance)
        builder.AddProject<Projects.FlinkDotNet_JobManager>("jobmanager")
            .WithEnvironment("JOBMANAGER_HTTP_PORT", ServicePorts.JobManagerHttp.ToString())
            .WithEnvironment(EnvironmentVariables.JobManagerGrpcPort, ServicePorts.JobManagerGrpc.ToString())
            .WithEnvironment("DOTNET_ENVIRONMENT", "Development");

        // Add TaskManagers (10 instances as per requirements)
        for (int i = 1; i <= 10; i++)
        {
            builder.AddProject<Projects.FlinkDotNet_TaskManager>($"taskmanager{i}")
                .WithEnvironment("TaskManagerId", $"TM-{i.ToString("D2")}")
                .WithEnvironment("TASKMANAGER_GRPC_PORT", ServiceUris.GetTaskManagerAspirePort(i).ToString())
                .WithEnvironment("services__jobmanager__grpc__0", ServiceUris.JobManagerGrpc)
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
            .WithEnvironment("DOTNET_REDIS_URL", $"{ServiceHosts.Localhost}:{redisPort.ToString()}") // For IntegrationTestVerifier compatibility
            .WithEnvironment("DOTNET_KAFKA_BOOTSTRAP_SERVERS", $"{ServiceHosts.Localhost}:{kafkaPort.ToString()}") // For IntegrationTestVerifier compatibility
            .WithEnvironment("DOTNET_ENVIRONMENT", "Development");

        await builder.Build().RunAsync();
    }
}
