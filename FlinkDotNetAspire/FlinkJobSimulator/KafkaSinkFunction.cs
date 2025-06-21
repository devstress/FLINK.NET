using FlinkDotNet.Core.Abstractions.Context;
using FlinkDotNet.Core.Abstractions.Sinks;
using FlinkDotNet.Core.Abstractions.Operators; // For IOperatorLifecycle
using Confluent.Kafka;
using Microsoft.Extensions.Configuration; // Required for reading connection string & topic
using FlinkDotNet.Common.Constants;
using System.Diagnostics; // For Process.Start

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

            var bootstrapServers = DiscoverKafkaBootstrapServers();
            var config = CreateProducerConfig(bootstrapServers);
            InitializeProducerWithRetry(config, bootstrapServers);
        }

        private string DiscoverKafkaBootstrapServers()
        {
            // Multi-strategy Kafka bootstrap server discovery for maximum reliability:
            // 1. DOTNET_KAFKA_BOOTSTRAP_SERVERS (for external integration tests - most reliable)
            // 2. ConnectionStrings__kafka (Aspire service reference)
            // 3. Port scanning for Docker containers (fallback)
            // 4. ServiceUris.KafkaBootstrapServers (default fallback)
            
            string? bootstrapServers = Configuration?["DOTNET_KAFKA_BOOTSTRAP_SERVERS"];
            Console.WriteLine($"[{_taskName}] 🔍 DEBUG: DOTNET_KAFKA_BOOTSTRAP_SERVERS = '{bootstrapServers}'");
            
            if (!string.IsNullOrEmpty(bootstrapServers))
            {
                Console.WriteLine($"[{_taskName}] Using Kafka bootstrap servers from external integration test: {bootstrapServers}");
                return FixIpv6Issues(bootstrapServers);
            }

            bootstrapServers = TryAspireServiceReference();
            if (!string.IsNullOrEmpty(bootstrapServers))
            {
                return FixIpv6Issues(bootstrapServers);
            }

            bootstrapServers = TryDockerDiscovery() ?? ServiceUris.KafkaBootstrapServers;
            Console.WriteLine($"[{_taskName}] Using fallback Kafka bootstrap servers: {bootstrapServers}");
            return FixIpv6Issues(bootstrapServers);
        }

        private string? TryAspireServiceReference()
        {
            var bootstrapServers = Configuration?["ConnectionStrings__kafka"];
            Console.WriteLine($"[{_taskName}] 🔍 DEBUG: ConnectionStrings__kafka = '{bootstrapServers}'");
            
            if (!string.IsNullOrEmpty(bootstrapServers))
            {
                Console.WriteLine($"[{_taskName}] Using Kafka bootstrap servers from Aspire service reference: {bootstrapServers}");
                
                // Validate Aspire connection with fallback to Docker discovery
                if (!IsKafkaPortAccessible(bootstrapServers))
                {
                    Console.WriteLine($"[{_taskName}] ⚠️ Aspire Kafka port not accessible, attempting Docker container discovery...");
                    var discoveredPort = DiscoverDockerKafkaPort();
                    if (!string.IsNullOrEmpty(discoveredPort))
                    {
                        Console.WriteLine($"[{_taskName}] ✅ Docker discovery successful: {discoveredPort}");
                        return discoveredPort;
                    }
                    Console.WriteLine($"[{_taskName}] ❌ Docker discovery failed, continuing with Aspire port...");
                }
                return bootstrapServers;
            }
            return null;
        }

        private string? TryDockerDiscovery()
        {
            var discoveredPort = DiscoverDockerKafkaPort();
            if (!string.IsNullOrEmpty(discoveredPort))
            {
                Console.WriteLine($"[{_taskName}] Using Kafka bootstrap servers from Docker discovery: {discoveredPort}");
                return discoveredPort;
            }
            return null;
        }

        private string FixIpv6Issues(string bootstrapServers)
        {
            var cleanBootstrapServers = bootstrapServers.Replace("localhost", "127.0.0.1");
            if (cleanBootstrapServers != bootstrapServers)
            {
                Console.WriteLine($"[{_taskName}] Fixed IPv6 issue: Using {cleanBootstrapServers} instead of {bootstrapServers}");
            }
            return cleanBootstrapServers;
        }

        private ProducerConfig CreateProducerConfig(string bootstrapServers)
        {
            return new ProducerConfig
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
        }

        private void InitializeProducerWithRetry(ProducerConfig config, string bootstrapServers)
        {
            const int maxRetries = 5;
            var currentRetry = 0;
            var baseDelay = TimeSpan.FromSeconds(2);
            
            while (currentRetry < maxRetries)
            {
                try
                {
                    Console.WriteLine($"[{_taskName}] Attempt {currentRetry + 1}/{maxRetries} - Creating Kafka producer for bootstrap servers: {bootstrapServers}");
                    _producer = new ProducerBuilder<Null, T>(config).Build();
                    Console.WriteLine($"[{_taskName}] ✅ Kafka producer created successfully for topic '{_topic}'");
                    
                    // Try to create the topic if it doesn't exist (synchronously for Open method)
                    EnsureTopicExistsAsync(bootstrapServers).GetAwaiter().GetResult();
                    Console.WriteLine($"[{_taskName}] ✅ Kafka topic '{_topic}' verified/created successfully");
                    return; // Success, exit retry loop
                }
                catch (Exception ex)
                {
                    currentRetry++;
                    HandleRetryException(ex, currentRetry, maxRetries);
                    
                    if (currentRetry >= maxRetries)
                    {
                        HandleMaxRetriesExceeded();
                        return;
                    }
                    
                    var delay = TimeSpan.FromMilliseconds(baseDelay.TotalMilliseconds * Math.Pow(2, currentRetry - 1));
                    Console.WriteLine($"[{_taskName}] ⏳ Waiting {delay.TotalSeconds:F1}s before retry {currentRetry + 1}...");
                    Thread.Sleep(delay);
                }
            }
        }

        private void HandleRetryException(Exception ex, int currentRetry, int maxRetries)
        {
            Console.WriteLine($"[{_taskName}] ❌ Kafka connection attempt {currentRetry}/{maxRetries} failed: {ex.Message}");
        }

        private void HandleMaxRetriesExceeded()
        {
            Console.WriteLine($"[{_taskName}] ⚠️ WARNING: All Kafka connection attempts failed. Switching to NO-OP mode for resilience.");
            Console.WriteLine($"[{_taskName}] ⚠️ FlinkJobSimulator will continue running but Kafka sink will be disabled.");
            Console.WriteLine($"[{_taskName}] ⚠️ This allows the job to complete and update Redis counters for monitoring.");
            
            // Set a flag to indicate Kafka is unavailable but don't throw exception
            _producer = null; // This will make Invoke() return early
        }

        public void Invoke(T record, ISinkContext context)
        {
            if (_producer == null)
            {
                // Kafka is unavailable - log but don't fail the job
                // This allows the job to continue and update Redis counters
                long currentCount = Interlocked.Read(ref _processedCount);
                if (currentCount < 10 || currentCount % LogFrequency == 0)
                {
                    Console.WriteLine($"⚠️ KAFKA SINK DISABLED: [{_taskName}] Kafka unavailable - skipping message #{currentCount + 1} for topic '{_topic}' (this is expected if Kafka failed to connect)");
                }
                
                // Still increment counter to track messages that would have been sent
                long newCount = Interlocked.Increment(ref _processedCount);
                if (newCount % LogFrequency == 0)
                {
                    Console.WriteLine($"⚠️ KAFKA SINK DISABLED: [{_taskName}] Would have produced {newCount} records to Kafka topic '{_topic}' if available.");
                }
                return;
            }

            // 🔄 ENHANCED KAFKA LOGGING: Log message reception
            long currentCount2 = Interlocked.Read(ref _processedCount);
            if (currentCount2 < 10 || currentCount2 % LogFrequency == 0)
            {
                Console.WriteLine($"🔄 KAFKA SINK INVOKE: [{_taskName}] Received message #{currentCount2 + 1} for topic '{_topic}': {record}");
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
                    TryProduceMessage(record, attempt);
                    return; // Success, exit retry loop
                }
                catch (ProduceException<Null, T> e) when (attempt < maxRetries)
                {
                    LogAndWaitForRetry(attempt, maxRetries, e.Message, e.Error.Code.ToString());
                }
                catch (Exception ex) when (attempt < maxRetries)
                {
                    LogAndWaitForRetry(attempt, maxRetries, ex.Message, ex.GetType().Name);
                }
                catch (Exception ex)
                {
                    Console.WriteLine($"💥 KAFKA FATAL: [{_taskName}] All {maxRetries} attempts failed for Kafka topic '{_topic}': {ex.GetType().Name} - {ex.Message}");
                    return;
                }
            }
        }

        private void TryProduceMessage(T record, int attempt)
        {
            if (_producer == null) throw new InvalidOperationException("Kafka producer not initialized");
            
            long currentCount = Interlocked.Read(ref _processedCount);
            LogProduceAttempt(currentCount, attempt);
            
            var message = new Message<Null, T> { Value = record };
            _producer.Produce(_topic, message, CreateDeliveryReportHandler(currentCount));

            long newCount = Interlocked.Increment(ref _processedCount);
            LogProduceSuccess(newCount);
        }

        private void LogProduceAttempt(long currentCount, int attempt)
        {
            if (currentCount < 10 || currentCount % LogFrequency == 0)
            {
                Console.WriteLine($"🔄 KAFKA COMM: [{_taskName}] Attempting to produce message #{currentCount + 1} to topic '{_topic}', attempt: {attempt}");
            }
        }

        private Action<DeliveryReport<Null, T>> CreateDeliveryReportHandler(long currentCount)
        {
            return deliveryReport =>
            {
                if (deliveryReport.Error.Code != ErrorCode.NoError)
                {
                    Console.WriteLine($"💥 KAFKA DELIVERY ERROR: [{_taskName}] Failed to deliver message to Kafka topic '{_topic}': {deliveryReport.Error.Reason}");
                }
                else if (currentCount < 10 || currentCount % LogFrequency == 0)
                {
                    Console.WriteLine($"✅ KAFKA DELIVERY SUCCESS: [{_taskName}] Message delivered to topic '{_topic}', partition: {deliveryReport.Partition}, offset: {deliveryReport.Offset}");
                }
            };
        }

        private void LogProduceSuccess(long newCount)
        {
            if (newCount % LogFrequency == 0)
            {
                Console.WriteLine($"[{_taskName}] Produced {newCount} records to Kafka topic '{_topic}'.");
            }
            
            if (newCount <= 10 || newCount % LogFrequency == 0)
            {
                Console.WriteLine($"✅ KAFKA COMM SUCCESS: [{_taskName}] Kafka produce call succeeded for message #{newCount} to topic '{_topic}' (awaiting delivery confirmation)");
            }
        }

        private void LogAndWaitForRetry(int attempt, int maxRetries, string message, string errorCode)
        {
            Console.WriteLine($"🚨 KAFKA FAILURE: [{_taskName}] Retry {attempt}/{maxRetries} failed for Kafka topic '{_topic}': {message} [Code: {errorCode}]");
            Thread.Sleep(100 * attempt);
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

        private bool IsKafkaPortAccessible(string bootstrapServers)
        {
            try
            {
                // Simple TCP connection test to see if port is accessible
                var parts = bootstrapServers.Split(':');
                if (parts.Length == 2 && int.TryParse(parts[1], out int port))
                {
                    using var tcpClient = new System.Net.Sockets.TcpClient();
                    var connectTask = tcpClient.ConnectAsync(parts[0], port);
                    return connectTask.Wait(TimeSpan.FromSeconds(3));
                }
                return false;
            }
            catch (Exception ex)
            {
                Console.WriteLine($"[{_taskName}] Port accessibility check failed: {ex.Message}");
                return false;
            }
        }

        private string? DiscoverDockerKafkaPort()
        {
            try
            {
                Console.WriteLine($"[{_taskName}] 🔍 Attempting Docker Kafka port discovery...");
                
                // Try to discover Kafka port from Docker containers
                var processInfo = new ProcessStartInfo
                {
                    FileName = "docker",
                    Arguments = "ps --format \"table {{.Names}}\\t{{.Ports}}\" --filter ancestor=bitnami/kafka",
                    RedirectStandardOutput = true,
                    UseShellExecute = false,
                    CreateNoWindow = true
                };

                using var process = Process.Start(processInfo);
                if (process != null)
                {
                    var output = process.StandardOutput.ReadToEnd();
                    process.WaitForExit();
                    
                    Console.WriteLine($"[{_taskName}] Docker output: {output}");
                    
                    // Parse output to find Kafka port mapping (look for pattern like "127.0.0.1:32789->9092/tcp")
                    var lines = output.Split('\n', StringSplitOptions.RemoveEmptyEntries);
                    var kafkaLine = lines.FirstOrDefault(line => line.Contains("->9092/tcp"));
                    if (kafkaLine != null)
                    {
                        var match = System.Text.RegularExpressions.Regex.Match(kafkaLine, @"127\.0\.0\.1:(\d+)->9092/tcp");
                        if (match.Success)
                        {
                            var port = match.Groups[1].Value;
                            var discoveredBootstrap = $"127.0.0.1:{port}";
                            Console.WriteLine($"[{_taskName}] ✅ Discovered Kafka port: {discoveredBootstrap}");
                            return discoveredBootstrap;
                        }
                    }
                }
                
                Console.WriteLine($"[{_taskName}] ❌ No Kafka containers found in Docker discovery");
                return null;
            }
            catch (Exception ex)
            {
                Console.WriteLine($"[{_taskName}] Docker discovery failed: {ex.Message}");
                return null;
            }
        }
    }
}
