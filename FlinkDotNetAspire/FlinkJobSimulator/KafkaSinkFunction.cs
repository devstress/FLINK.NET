using FlinkDotNet.Core.Abstractions.Context;
using FlinkDotNet.Core.Abstractions.Sinks;
using FlinkDotNet.Core.Abstractions.Operators; // For IOperatorLifecycle
using FlinkDotNet.Core.Abstractions.Runtime; // For IRuntimeContext
using Confluent.Kafka;
using System;
using System.Threading; // For Interlocked
using System.Threading.Tasks; // For Task from ProduceAsync
using Microsoft.Extensions.Configuration; // Required for reading connection string & topic

namespace FlinkJobSimulator
{
    public class KafkaSinkFunction<T> : ISinkFunction<T>, IOperatorLifecycle where T : class // Assuming T will be string for this sample
    {
        private IProducer<Null, T>? _producer;
        private string _topic;
        private string _taskName = nameof(KafkaSinkFunction<T>);
        private long _processedCount = 0;
        private const long LogFrequency = 10000;

        private static IConfiguration? _configuration;

        static KafkaSinkFunction()
        {
            _configuration = new ConfigurationBuilder()
                .AddEnvironmentVariables()
                .Build();
        }

        public KafkaSinkFunction(string? topic = null) // Constructor allows overriding topic
        {
            _topic = topic ?? _configuration?["SIMULATOR_KAFKA_TOPIC"] ?? "flinkdotnet.default.topic";
            Console.WriteLine($"KafkaSinkFunction will use Kafka topic: '{_topic}'");
        }

        public void Open(IRuntimeContext context)
        {
            _taskName = context.TaskName;
            Console.WriteLine($"[{_taskName}] Opening KafkaSinkFunction for topic '{_topic}'.");

            string? bootstrapServers = _configuration?["ConnectionStrings__kafka"];
            if (string.IsNullOrEmpty(bootstrapServers))
            {
                Console.WriteLine($"[{_taskName}] ERROR: Kafka bootstrap servers 'ConnectionStrings__kafka' not found in environment variables.");
                // Attempt a local default if not found (useful for non-Aspire testing)
                bootstrapServers = "localhost:9092";
                Console.WriteLine($"[{_taskName}] Using default Kafka bootstrap servers: {bootstrapServers}");
            }
            else
            {
                Console.WriteLine($"[{_taskName}] Found Kafka bootstrap servers from environment: {bootstrapServers}");
            }

            var config = new ProducerConfig
            {
                BootstrapServers = bootstrapServers,
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
            }
            catch (Exception ex)
            {
                Console.WriteLine($"[{_taskName}] ERROR: Could not create Kafka producer. Error: {ex.Message}");
                throw; // Rethrow to indicate failure to open
            }
        }

        public async void Invoke(T record, ISinkContext context) // Stays async void
        {
            if (_producer == null)
            {
                return;
            }

            if (record is string recordString && recordString.StartsWith("PROTO_BARRIER_"))
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
                await _producer.ProduceAsync(_topic, message);

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

        public void Close()
        {
            Console.WriteLine($"[{_taskName}] Closing KafkaSinkFunction. Total records attempted for Kafka topic '{_topic}': {_processedCount}.");
            try
            {
                _producer?.Flush(TimeSpan.FromSeconds(10)); // Recommended to flush before closing
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
