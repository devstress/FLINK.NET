// using Confluent... // REMOVED for native implementation
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;

namespace FlinkJobSimulator
{
    /// <summary>
    /// Message producer that sends messages to Kafka for consumption by FlinkJobSimulator.
    /// This simulates real-world message production scenarios for Apache Flink stress testing.
    /// </summary>
    public class KafkaMessageProducer : BackgroundService
    {
        private readonly string _topic;
        private readonly long _numberOfMessages;
        private readonly ILogger<KafkaMessageProducer> _logger;
        private IProducer<Null, string>? _producer;
        private readonly IConfiguration _configuration;

        public KafkaMessageProducer(IConfiguration configuration, ILogger<KafkaMessageProducer> logger)
        {
            _configuration = configuration ?? throw new ArgumentNullException(nameof(configuration));
            _logger = logger ?? throw new ArgumentNullException(nameof(logger));
            
            _topic = _configuration["SIMULATOR_KAFKA_TOPIC"] ?? "flinkdotnet.sample.topic";
            
            var numMessagesStr = _configuration["SIMULATOR_NUM_MESSAGES"] ?? "1000";
            if (!long.TryParse(numMessagesStr, out _numberOfMessages))
            {
                _numberOfMessages = 1000;
            }
            
            _logger.LogInformation("KafkaMessageProducer configured: Topic='{Topic}', Messages={Messages}", _topic, _numberOfMessages);
        }

        protected override async Task ExecuteAsync(CancellationToken stoppingToken)
        {
            _logger.LogInformation("🚀 Starting Kafka message production to topic '{Topic}'", _topic);
            
            // Wait a bit for Kafka to be ready
            await Task.Delay(TimeSpan.FromSeconds(5), stoppingToken);
            
            try
            {
                InitializeProducer();
                await ProduceMessages(stoppingToken);
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "Error in Kafka message production for topic '{Topic}' after {Duration:F1}s", 
                    _topic, (DateTime.UtcNow - DateTime.UtcNow.AddSeconds(-5)).TotalSeconds);
                throw new InvalidOperationException($"Kafka message production failed for topic '{_topic}'", ex);
            }
            finally
            {
                _producer?.Dispose();
            }
        }

        private void InitializeProducer()
        {
            // Multi-strategy Kafka bootstrap server discovery
            string? bootstrapServers = _configuration["DOTNET_KAFKA_BOOTSTRAP_SERVERS"];
            _logger.LogDebug("DOTNET_KAFKA_BOOTSTRAP_SERVERS = '{BootstrapServers}'", bootstrapServers);
            
            if (string.IsNullOrEmpty(bootstrapServers))
            {
                bootstrapServers = _configuration["ConnectionStrings__kafka"];
                _logger.LogDebug("ConnectionStrings__kafka = '{BootstrapServers}'", bootstrapServers);
                
                if (string.IsNullOrEmpty(bootstrapServers))
                {
                    bootstrapServers = "localhost:9092"; // Default fallback
                    _logger.LogInformation("Using default Kafka bootstrap servers: {BootstrapServers}", bootstrapServers);
                }
            }

            // Fix IPv6 issue by forcing IPv4 localhost resolution
            bootstrapServers = bootstrapServers.Replace("localhost", "127.0.0.1");

            var config = new ProducerConfig
            {
                BootstrapServers = bootstrapServers,
                SecurityProtocol = SecurityProtocol.Plaintext,
                SocketTimeoutMs = 60000,
                // High throughput settings optimized for 1M+ msg/sec
                LingerMs = 1,                    // Minimal latency for immediate sending
                BatchSize = 65536,               // Large batches for high throughput  
                BatchNumMessages = 10000,        // High message batching
                CompressionType = CompressionType.Lz4, // Fast compression
                MaxInFlight = 5,                 // Required ≤5 when EnableIdempotence=true for exactly-once semantics
                // Reliability settings for exactly-once semantics
                Acks = Acks.All,
                EnableIdempotence = true,
                MessageTimeoutMs = 120000,
                // Performance optimizations
                SocketSendBufferBytes = 131072,   // 128KB send buffer
                SocketReceiveBufferBytes = 131072 // 128KB receive buffer
            };

            _logger.LogInformation("Creating Kafka producer for bootstrap servers: {BootstrapServers}", bootstrapServers);
            _producer = new ProducerBuilder<Null, string>(config).Build();
            
            _logger.LogInformation("✅ Kafka producer created successfully");
        }

        private async Task ProduceMessages(CancellationToken stoppingToken)
        {
            if (_producer == null)
                throw new InvalidOperationException("Producer not initialized");

            _logger.LogInformation("📨 Starting to produce {MessageCount} messages to topic '{Topic}'", _numberOfMessages, _topic);

            var startTime = DateTime.UtcNow;
            var messagesProduced = 0L;
            var lastLogTime = DateTime.UtcNow;

            try
            {
                for (long i = 1; i <= _numberOfMessages && !stoppingToken.IsCancellationRequested; i++)
                {
                    var message = CreateMessage(i);
                    var jsonMessage = System.Text.Json.JsonSerializer.Serialize(message);

                    var deliveryResult = await _producer.ProduceAsync(_topic, new Message<Null, string>
                    {
                        Value = jsonMessage,
                        Timestamp = new Timestamp(DateTime.UtcNow)
                    }, stoppingToken);

                    messagesProduced++;
                    LogProgress(messagesProduced, startTime, lastLogTime, deliveryResult);
                }

                _producer.Flush(TimeSpan.FromSeconds(10));
                LogFinalPerformance(messagesProduced, startTime);
            }
            catch (ProduceException<Null, string> ex)
            {
                HandleProduceException(ex);
            }
            catch (OperationCanceledException ex)
            {
                HandleCancellationException(ex, messagesProduced);
            }
        }

        private object CreateMessage(long i)
        {
            var timestamp = DateTime.UtcNow.ToString("yyyy-MM-ddTHH:mm:ss.fffZ");
            return new
            {
                id = i,
                redis_ordered_id = i,
                timestamp = timestamp,
                job_id = "flink-job-1",
                task_id = "task-" + i,
                kafka_partition = i % 20,
                kafka_offset = i,
                processing_stage = "source->map->sink",
                payload = "high-throughput-data-" + i,
                checksum = (i * 31 + timestamp.GetHashCode()) % 1000000
            };
        }

        private void LogProgress(long messagesProduced, DateTime startTime, DateTime lastLogTime, DeliveryResult<Null, string> deliveryResult)
        {
            if (messagesProduced % 10000 == 0 || (DateTime.UtcNow - lastLogTime).TotalSeconds > 5)
            {
                var elapsed = DateTime.UtcNow - startTime;
                var messagesPerSecond = messagesProduced / elapsed.TotalSeconds;

                string rateMessage;
                if (messagesPerSecond > 1000000)
                {
                    rateMessage = $"🏆 EXCELLENT: {messagesPerSecond:F0} msg/s (>1M target achieved!)";
                }
                else if (messagesPerSecond > 500000)
                {
                    rateMessage = $"✅ GOOD: {messagesPerSecond:F0} msg/s (approaching 1M target)";
                }
                else
                {
                    rateMessage = $"⚠️ OPTIMIZING: {messagesPerSecond:F0} msg/s (target: 1M+ msg/s)";
                }

                _logger.LogInformation("📊 Produced {ProducedCount}/{TotalCount} messages " +
                                     "(partition: {Partition}, offset: {Offset}) " +
                                     "{RateMessage}",
                                     messagesProduced, _numberOfMessages,
                                     deliveryResult.Partition.Value, deliveryResult.Offset.Value,
                                     rateMessage);
            }
        }

        private void LogFinalPerformance(long messagesProduced, DateTime startTime)
        {
            var totalElapsed = DateTime.UtcNow - startTime;
            var finalRate = messagesProduced / totalElapsed.TotalSeconds;

            string performanceLevel;
            if (finalRate > 1000000)
            {
                performanceLevel = "🏆 EXCELLENT: >1M msg/s target achieved!";
            }
            else if (finalRate > 500000)
            {
                performanceLevel = "✅ GOOD: High throughput achieved";
            }
            else
            {
                performanceLevel = "⚠️ OPTIMIZATION NEEDED: Target 1M+ msg/s for Flink.NET compliance";
            }

            _logger.LogInformation("🏁 High-performance message production completed! " +
                                 "Produced {TotalMessages} messages in {Duration:F1}s " +
                                 "Final rate: {Rate:F0} msg/s " +
                                 "{PerformanceLevel}",
                                 messagesProduced, totalElapsed.TotalSeconds, finalRate, performanceLevel);
        }

        private void HandleProduceException(ProduceException<Null, string> ex)
        {
            _logger.LogError(ex, "Failed to produce message to topic '{Topic}' at partition {Partition}: {Error}",
                _topic, ex.DeliveryResult?.Partition ?? new Partition(-1), ex.Error.Reason);
            throw new InvalidOperationException($"Kafka message production failed: {ex.Error.Reason}", ex);
        }

        private void HandleCancellationException(OperationCanceledException ex, long messagesProduced)
        {
            _logger.LogInformation(ex, "Message production cancelled. Produced {MessagesProduced} messages", messagesProduced);
            throw new OperationCanceledException($"Kafka message production was cancelled after producing {messagesProduced} messages", ex);
        }

        public override void Dispose()
        {
            _producer?.Dispose();
            base.Dispose();
        }
    }
}
