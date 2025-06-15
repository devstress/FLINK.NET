using FlinkDotNet.Core.Abstractions.Context;
using FlinkDotNet.Core.Abstractions.Sinks;
using FlinkDotNet.Core.Abstractions.Operators; // For IOperatorLifecycle
using Confluent.Kafka;
using Microsoft.Extensions.Configuration; // Required for reading connection string & topic
using FlinkDotNet.Common.Constants;

namespace FlinkJobSimulator
{
    public class KafkaSinkFunction<T> : ISinkFunction<T>, IOperatorLifecycle where T : class // Assuming T will be string for this sample
    {
        private IProducer<Null, T>? _producer;
        private readonly string _topic;
        private string _taskName = nameof(KafkaSinkFunction<T>);
        private long _processedCount = 0;
        private const long LogFrequency = 10000;

        // Static configuration for LocalStreamExecutor compatibility
        public static string? GlobalKafkaTopic { get; set; }
        public static Program.IKafkaConnectionProvider? GlobalKafkaConnectionProvider { get; set; }

        private static readonly IConfiguration Configuration = new ConfigurationBuilder()
            .AddEnvironmentVariables()
            .Build();

        // Constructor with dependencies (for manual instantiation)
        public KafkaSinkFunction(string topic) // Constructor requires topic
        {
            _topic = topic ?? throw new ArgumentNullException(nameof(topic));
            Console.WriteLine($"KafkaSinkFunction will use Kafka topic: '{_topic}'");
        }

        // Parameterless constructor (for LocalStreamExecutor reflection)
        public KafkaSinkFunction()
        {
            _topic = GlobalKafkaTopic ?? Configuration?["SIMULATOR_KAFKA_TOPIC"] ?? "flinkdotnet.default.topic";
            Console.WriteLine($"KafkaSinkFunction parameterless constructor: topic '{_topic}'");
        }

        public void Open(IRuntimeContext context)
        {
            _taskName = context.TaskName;
            Console.WriteLine($"[{_taskName}] Opening KafkaSinkFunction for topic '{_topic}'.");

            // Get bootstrap servers using Aspire service discovery first, then fallbacks
            string? bootstrapServers = null;
            
            // 1. Try to use the global Kafka connection provider (for Aspire service bindings)
            if (GlobalKafkaConnectionProvider != null)
            {
                try
                {
                    bootstrapServers = GlobalKafkaConnectionProvider.GetBootstrapServers();
                    Console.WriteLine($"[{_taskName}] Using Kafka bootstrap servers from Aspire service provider: {bootstrapServers}");
                }
                catch (Exception ex)
                {
                    Console.WriteLine($"[{_taskName}] WARNING: Failed to get Kafka from service provider: {ex.Message}");
                }
            }
            
            // 2. Fallback to connection string (Aspire service reference)
            if (string.IsNullOrEmpty(bootstrapServers))
            {
                bootstrapServers = Configuration?["ConnectionStrings__kafka"];
                if (!string.IsNullOrEmpty(bootstrapServers))
                {
                    Console.WriteLine($"[{_taskName}] Using Kafka bootstrap servers from Aspire connection string: {bootstrapServers}");
                }
            }
            
            // 3. Fallback to environment variables (external integration tests)
            if (string.IsNullOrEmpty(bootstrapServers))
            {
                bootstrapServers = Configuration?["DOTNET_KAFKA_BOOTSTRAP_SERVERS"];
                if (!string.IsNullOrEmpty(bootstrapServers))
                {
                    Console.WriteLine($"[{_taskName}] Using Kafka bootstrap servers from environment variable: {bootstrapServers}");
                }
            }
            
            // 4. Final fallback to service constants
            if (string.IsNullOrEmpty(bootstrapServers))
            {
                bootstrapServers = ServiceUris.KafkaBootstrapServers;
                Console.WriteLine($"[{_taskName}] Using default Kafka bootstrap servers: {bootstrapServers}");
            }

            // Fix IPv6 issue by forcing IPv4 localhost resolution
            var cleanBootstrapServers = bootstrapServers.Replace("localhost", "127.0.0.1");
            if (cleanBootstrapServers != bootstrapServers)
            {
                Console.WriteLine($"[{_taskName}] Fixed IPv6 issue: Using {cleanBootstrapServers} instead of {bootstrapServers}");
                bootstrapServers = cleanBootstrapServers;
            }

            var config = new ProducerConfig
            {
                BootstrapServers = bootstrapServers,
                SecurityProtocol = SecurityProtocol.Plaintext, // Explicitly set to plaintext for local testing
                SocketTimeoutMs = 10000 // 10 seconds timeout
                // Add other producer configurations if needed, e.g., Acks, Retries, etc.
                // For high throughput, consider:
                // LingerMs = 5, // Time to wait for more messages before sending a batch
                // BatchNumMessages = 10000, // Number of messages to batch
                // CompressionType = CompressionType.Snappy, // Or Lz4, Gzip
            };

            try
            {
                _producer = new ProducerBuilder<Null, T>(config).Build();
                Console.WriteLine($"[{_taskName}] Kafka producer created for bootstrap servers: {bootstrapServers}. Topic: {_topic}");
                
                // Try to create the topic if it doesn't exist (synchronously for Open method)
                EnsureTopicExistsAsync(bootstrapServers).GetAwaiter().GetResult();
            }
            catch (Exception ex)
            {
                Console.WriteLine($"[{_taskName}] ERROR: Could not create Kafka producer. Error: {ex.Message}");
                throw; // Rethrow to indicate failure to open
            }
        }

        public void Invoke(T record, ISinkContext context)
        {
            if (_producer == null)
            {
                return;
            }

            if (record is string recordString && recordString.StartsWith("BARRIER_"))
            {
                Console.WriteLine($"[{_taskName}] Received Barrier Marker in Kafka Sink: {recordString}");
                return;
            }

            ProduceWithRetry(record);
        }

        private void ProduceWithRetry(T record)
        {
            const int maxRetries = 3;
            for (int attempt = 1; attempt <= maxRetries; attempt++)
            {
                try
                {
                    if (_producer == null) throw new InvalidOperationException("Kafka producer not initialized");
                    var message = new Message<Null, T> { Value = record };
                    _producer.Produce(_topic, message, (deliveryReport) =>
                    {
                        if (deliveryReport.Error.Code != ErrorCode.NoError)
                        {
                            Console.WriteLine($"[{_taskName}] ERROR: Failed to deliver message to Kafka topic '{_topic}': {deliveryReport.Error.Reason}");
                        }
                    });

                    long currentCount = Interlocked.Increment(ref _processedCount);
                    if (currentCount % LogFrequency == 0)
                    {
                        Console.WriteLine($"[{_taskName}] Produced {currentCount} records to Kafka topic '{_topic}'.");
                    }
                    return; // Success, exit retry loop
                }
                catch (ProduceException<Null, T> e) when (attempt < maxRetries)
                {
                    Console.WriteLine($"[{_taskName}] WARNING: Retry {attempt}/{maxRetries} failed for Kafka topic '{_topic}': {e.Message} [Code: {e.Error.Code}]");
                    Thread.Sleep(100 * attempt);
                }
                catch (Exception ex) when (attempt < maxRetries)
                {
                    Console.WriteLine($"[{_taskName}] WARNING: Retry {attempt}/{maxRetries} failed for Kafka topic '{_topic}': {ex.GetType().Name} - {ex.Message}");
                    Thread.Sleep(100 * attempt);
                }
                catch (Exception ex)
                {
                    Console.WriteLine($"[{_taskName}] ERROR: All {maxRetries} attempts failed for Kafka topic '{_topic}': {ex.GetType().Name} - {ex.Message}");
                    return;
                }
            }
        }

        private async Task EnsureTopicExistsAsync(string bootstrapServers)
        {
            try
            {
                var adminConfig = new AdminClientConfig 
                { 
                    BootstrapServers = bootstrapServers, // Already cleaned to use IPv4 in calling method
                    SecurityProtocol = SecurityProtocol.Plaintext, // Explicitly set to plaintext for local testing
                    SocketTimeoutMs = 10000, // 10 seconds timeout
                    ApiVersionRequestTimeoutMs = 10000
                };
                using var admin = new AdminClientBuilder(adminConfig).Build();
                
                Console.WriteLine($"[{_taskName}] Connecting to Kafka admin at {bootstrapServers} to check topic '{_topic}'...");
                
                // Check if topic exists
                var metadata = admin.GetMetadata(TimeSpan.FromSeconds(15));
                var topicExists = metadata.Topics.Any(t => t.Topic == _topic);
                
                if (!topicExists)
                {
                    Console.WriteLine($"[{_taskName}] Topic '{_topic}' does not exist. Creating it...");
                    
                    var topicSpec = new Confluent.Kafka.Admin.TopicSpecification
                    {
                        Name = _topic,
                        NumPartitions = 1,
                        ReplicationFactor = 1
                    };

                    await admin.CreateTopicsAsync(new[] { topicSpec });
                    Console.WriteLine($"[{_taskName}] Topic '{_topic}' created successfully.");
                    
                    // Wait a bit for topic to be fully available
                    await Task.Delay(2000);
                }
                else
                {
                    Console.WriteLine($"[{_taskName}] Topic '{_topic}' already exists.");
                }
            }
            catch (Exception ex)
            {
                Console.WriteLine($"[{_taskName}] WARNING: Could not create/verify topic '{_topic}': {ex.GetType().Name} - {ex.Message}");
                Console.WriteLine($"[{_taskName}] Continuing anyway - topic may be auto-created on first message or may already exist.");
                // Don't throw - let the producer try to work anyway
            }
        }

        public void Close()
        {
            Console.WriteLine($"[{_taskName}] Closing KafkaSinkFunction. Total records attempted for Kafka topic '{_topic}': {_processedCount}.");
            try
            {
                if (_producer != null)
                {
                    Console.WriteLine($"[{_taskName}] Flushing Kafka producer before closing...");
                    _producer.Flush(TimeSpan.FromSeconds(30)); // Increased flush timeout for large message volumes
                    Console.WriteLine($"[{_taskName}] Kafka producer flush completed.");
                }
            }
            catch (Exception ex)
            {
                 Console.WriteLine($"[{_taskName}] ERROR: Exception during Kafka producer flush: {ex.Message}");
            }
            _producer?.Dispose();
            Console.WriteLine($"[{_taskName}] Kafka producer disposed.");
        }
    }
}
