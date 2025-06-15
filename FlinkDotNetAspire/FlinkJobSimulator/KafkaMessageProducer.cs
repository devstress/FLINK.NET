using Confluent.Kafka;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using System.Text;

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
                await InitializeProducer();
                await ProduceMessages(stoppingToken);
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "Error in Kafka message production");
                throw;
            }
            finally
            {
                _producer?.Dispose();
            }
        }

        private async Task InitializeProducer()
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
                SocketTimeoutMs = 10000,
                // High throughput settings for stress testing
                LingerMs = 5,
                BatchNumMessages = 1000,
                CompressionType = CompressionType.Snappy,
                // Reliability settings
                Acks = Acks.All,
                MessageTimeoutMs = 30000
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
                    var message = $"Message-{i:D6}-{DateTime.UtcNow:yyyy-MM-dd-HH-mm-ss}";
                    
                    var deliveryResult = await _producer.ProduceAsync(_topic, new Message<Null, string>
                    {
                        Value = message
                    }, stoppingToken);
                    
                    messagesProduced++;
                    
                    // Log production progress
                    if (messagesProduced % 100 == 0 || (DateTime.UtcNow - lastLogTime).TotalSeconds > 10)
                    {
                        var elapsed = DateTime.UtcNow - startTime;
                        var messagesPerSecond = messagesProduced / elapsed.TotalSeconds;
                        
                        _logger.LogInformation("📊 Produced {ProducedCount}/{TotalCount} messages " +
                                             "(partition: {Partition}, offset: {Offset}) " +
                                             "Rate: {Rate:F1} msg/s",
                                             messagesProduced, _numberOfMessages,
                                             deliveryResult.Partition.Value, deliveryResult.Offset.Value,
                                             messagesPerSecond);
                        lastLogTime = DateTime.UtcNow;
                    }
                    
                    // Small delay between messages to simulate realistic production
                    if (i % 100 == 0)
                    {
                        await Task.Delay(50, stoppingToken); // 50ms pause every 100 messages
                    }
                }
                
                // Flush any remaining messages
                _producer.Flush(TimeSpan.FromSeconds(10));
                
                var totalElapsed = DateTime.UtcNow - startTime;
                var finalRate = messagesProduced / totalElapsed.TotalSeconds;
                
                _logger.LogInformation("🏁 Message production completed! " +
                                     "Produced {TotalMessages} messages in {Duration:F1}s " +
                                     "Final rate: {Rate:F1} msg/s",
                                     messagesProduced, totalElapsed.TotalSeconds, finalRate);
            }
            catch (ProduceException<Null, string> ex)
            {
                _logger.LogError(ex, "Failed to produce message: {Error}", ex.Error.Reason);
                throw;
            }
            catch (OperationCanceledException)
            {
                _logger.LogInformation("Message production cancelled. Produced {MessagesProduced} messages", messagesProduced);
                throw;
            }
        }

        public override void Dispose()
        {
            _producer?.Dispose();
            base.Dispose();
        }
    }
}