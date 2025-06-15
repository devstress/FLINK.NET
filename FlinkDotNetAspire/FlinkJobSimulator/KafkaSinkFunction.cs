using FlinkDotNet.Core.Abstractions.Context;
using FlinkDotNet.Core.Abstractions.Sinks;
using FlinkDotNet.Core.Abstractions.Operators; // For IOperatorLifecycle
using Confluent.Kafka;
using Microsoft.Extensions.Configuration; // Required for reading connection string & topic

namespace FlinkJobSimulator
{
    public class KafkaSinkFunction<T> : ISinkFunction<T>, IOperatorLifecycle where T : class // Assuming T will be string for this sample
    {
        private IProducer<string, T>? _producer;
        private readonly string _topic;
        private string _taskName = nameof(KafkaSinkFunction<T>);
        private long _processedCount = 0;
        private const long LogFrequency = 10000;

        // Static configuration for LocalStreamExecutor compatibility
        public static string? GlobalKafkaTopic { get; set; }
        public static Program.IKafkaProducerService? GlobalKafkaProducerService { get; set; }

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

            // Use the Aspire-injected producer from DI container
            if (GlobalKafkaProducerService != null)
            {
                try
                {
                    var originalProducer = GlobalKafkaProducerService.GetProducer();
                    
                    // Cast to the appropriate type since Aspire gives us IProducer<string, string>
                    // but we need IProducer<string, T>
                    if (originalProducer is IProducer<string, T> typedProducer)
                    {
                        _producer = typedProducer;
                        Console.WriteLine($"[{_taskName}] Using Aspire-injected Kafka producer for topic '{_topic}'");
                    }
                    else
                    {
                        // For string types, we can safely cast
                        if (typeof(T) == typeof(string))
                        {
                            _producer = (IProducer<string, T>)originalProducer;
                            Console.WriteLine($"[{_taskName}] Using Aspire-injected Kafka producer (cast to string type) for topic '{_topic}'");
                        }
                        else
                        {
                            throw new InvalidOperationException($"Aspire producer type mismatch. Expected IProducer<string, {typeof(T).Name}>, got {originalProducer.GetType().Name}");
                        }
                    }
                }
                catch (Exception ex)
                {
                    Console.WriteLine($"[{_taskName}] ERROR: Failed to get Kafka producer from Aspire service: {ex.Message}");
                    throw; // Rethrow to indicate failure to open
                }
            }
            else
            {
                Console.WriteLine($"[{_taskName}] ERROR: GlobalKafkaProducerService not available. Aspire injection failed.");
                throw new InvalidOperationException("Kafka producer service not available. Ensure Aspire DI is properly configured.");
            }

            // Verify the producer is working
            if (_producer == null)
            {
                Console.WriteLine($"[{_taskName}] ERROR: Kafka producer is null after initialization");
                throw new InvalidOperationException("Kafka producer initialization failed");
            }
            
            Console.WriteLine($"[{_taskName}] Kafka producer successfully initialized for topic: {_topic}");
        }

        public void Invoke(T record, ISinkContext context)
        {
            if (_producer == null)
            {
                Console.WriteLine($"[{_taskName}] WARNING: Kafka producer is null, skipping message");
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
                    var message = new Message<string, T> { Value = record };
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
                        Console.WriteLine($"[{_taskName}] Produced {currentCount} records to Kafka topic '{_topic}' using Aspire producer.");
                    }
                    return; // Success, exit retry loop
                }
                catch (ProduceException<string, T> e) when (attempt < maxRetries)
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

        public void Close()
        {
            Console.WriteLine($"[{_taskName}] Closing KafkaSinkFunction. Total records processed for Kafka topic '{_topic}': {_processedCount}.");
            
            // Flush the producer but don't dispose it since it's managed by Aspire DI
            try
            {
                if (_producer != null)
                {
                    Console.WriteLine($"[{_taskName}] Flushing Aspire-managed Kafka producer before closing...");
                    _producer.Flush(TimeSpan.FromSeconds(10)); // Reduced flush timeout since producer will be reused
                    Console.WriteLine($"[{_taskName}] Kafka producer flush completed.");
                }
            }
            catch (Exception ex)
            {
                 Console.WriteLine($"[{_taskName}] ERROR: Exception during Kafka producer flush: {ex.Message}");
            }
            
            // Don't dispose the producer - it's managed by Aspire DI container
            Console.WriteLine($"[{_taskName}] Kafka sink closed (producer remains managed by Aspire).");
        }
    }
}
