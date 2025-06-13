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

        private static readonly IConfiguration Configuration = new ConfigurationBuilder()
            .AddEnvironmentVariables()
            .Build();

        public KafkaSinkFunction(string? topic = null) // Constructor allows overriding topic
        {
            _topic = topic ?? Configuration?["SIMULATOR_KAFKA_TOPIC"] ?? "flinkdotnet.default.topic";
            Console.WriteLine($"KafkaSinkFunction will use Kafka topic: '{_topic}'");
        }

        public void Open(IRuntimeContext context)
        {
            _taskName = context.TaskName;
            Console.WriteLine($"[{_taskName}] Opening KafkaSinkFunction for topic '{_topic}'.");

            // Priority order for Kafka bootstrap servers:
            // 1. DOTNET_KAFKA_BOOTSTRAP_SERVERS (set by port discovery script for stress tests)
            // 2. ConnectionStrings__kafka (Aspire service reference)
            // 3. ServiceUris.KafkaBootstrapServers (default fallback)
            
            string? bootstrapServers = Configuration?["DOTNET_KAFKA_BOOTSTRAP_SERVERS"];
            if (!string.IsNullOrEmpty(bootstrapServers))
            {
                Console.WriteLine($"[{_taskName}] Using Kafka bootstrap servers from DOTNET_KAFKA_BOOTSTRAP_SERVERS: {bootstrapServers}");
            }
            else
            {
                bootstrapServers = Configuration?["ConnectionStrings__kafka"];
                if (!string.IsNullOrEmpty(bootstrapServers))
                {
                    Console.WriteLine($"[{_taskName}] Using Kafka bootstrap servers from Aspire service reference: {bootstrapServers}");
                }
                else
                {
                    bootstrapServers = ServiceUris.KafkaBootstrapServers;
                    Console.WriteLine($"[{_taskName}] Using default Kafka bootstrap servers: {bootstrapServers}");
                }
            }

            var config = new ProducerConfig
            {
                BootstrapServers = bootstrapServers,
                SecurityProtocol = SecurityProtocol.Plaintext, // Explicitly set to plaintext for local testing
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
                // In a real scenario, sink would perform checkpointing actions here (e.g., flush, commit transaction).
                // For this PoC, we just log and don't send the barrier marker itself to Kafka as a data message.
                // If we wanted to see barriers in Kafka for debugging, we could send them to a separate topic or with a special key.
                return;
            }

            try
            {
                var message = new Message<Null, T> { Value = record };
                // Use synchronous produce to ensure message is sent before continuing
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
            }
            catch (ProduceException<Null, T> e)
            {
                Console.WriteLine($"[{_taskName}] ERROR: Failed to deliver message to Kafka topic '{_topic}': {e.Message} [Code: {e.Error.Code}]");
            }
            catch (Exception ex)
            {
                 Console.WriteLine($"[{_taskName}] ERROR: Unexpected error producing to Kafka topic '{_topic}': {ex.Message}");
            }
        }

        private async Task EnsureTopicExistsAsync(string bootstrapServers)
        {
            try
            {
                var adminConfig = new AdminClientConfig 
                { 
                    BootstrapServers = bootstrapServers,
                    SecurityProtocol = SecurityProtocol.Plaintext // Explicitly set to plaintext for local testing
                };
                using var admin = new AdminClientBuilder(adminConfig).Build();
                
                // Check if topic exists
                var metadata = admin.GetMetadata(TimeSpan.FromSeconds(10));
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
                }
                else
                {
                    Console.WriteLine($"[{_taskName}] Topic '{_topic}' already exists.");
                }
            }
            catch (Exception ex)
            {
                Console.WriteLine($"[{_taskName}] WARNING: Could not create/verify topic '{_topic}': {ex.Message}");
                Console.WriteLine($"[{_taskName}] Continuing anyway - topic may be auto-created on first message.");
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
