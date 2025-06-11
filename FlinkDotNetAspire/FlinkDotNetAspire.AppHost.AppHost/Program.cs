// Existing using statements (implicit for DistributedApplication, Projects)

var builder = DistributedApplication.CreateBuilder(args);

// Add resources
// Bind Redis and Kafka containers to their default ports so tests can reliably
// connect using localhost addresses without parsing the Aspire manifest.
var redis = builder.AddRedis("redis", port: 6379);
var kafka = builder.AddKafka("kafka", port: 9092); // Add Kafka resource

// Provide Redis and Kafka connection information to the FlinkJobSimulator
var simulatorNumMessages = Environment.GetEnvironmentVariable("SIMULATOR_NUM_MESSAGES") ?? "10000";
builder.AddProject<Projects.FlinkJobSimulator>("flinkjobsimulator")
    .WithReference(redis) // Makes "ConnectionStrings__redis" available
    .WithReference(kafka) // Makes "ConnectionStrings__kafka" available (or similar for bootstrap servers)
    .WithEnvironment("SIMULATOR_NUM_MESSAGES", simulatorNumMessages) // Use environment variable or default to 10K
    .WithEnvironment("SIMULATOR_REDIS_KEY_SINK_COUNTER", "flinkdotnet:sample:processed_message_counter") // Redis sink counter key
    .WithEnvironment("SIMULATOR_REDIS_KEY_GLOBAL_SEQUENCE", "flinkdotnet:global_sequence_id") // Redis global sequence key
    .WithEnvironment("SIMULATOR_KAFKA_TOPIC", "flinkdotnet.sample.topic") // Default Kafka topic
    .WithEnvironment("ASPIRE_DASHBOARD_URL", "http://localhost:18888") // Default Aspire Dashboard URL
    .WithEnvironment("DOTNET_ENVIRONMENT", "Development");

await builder.Build().RunAsync();
