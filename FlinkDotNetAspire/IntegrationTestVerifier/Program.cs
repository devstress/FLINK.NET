#pragma warning disable S3776 // Cognitive Complexity of methods is too high
namespace IntegrationTestVerifier
{
    using System;
    using System.Collections.Generic;
    using System.Diagnostics;
    using System.Linq;
    using System.Threading;
    using System.Threading.Tasks;
    using Confluent.Kafka;
    using Microsoft.Extensions.Configuration;
    using StackExchange.Redis;

    public static class Program
    {

        public static async Task<int> Main(string[] args)
        {
            Console.WriteLine("Integration Test Verifier Started.");

            var configuration = new ConfigurationBuilder()
                .AddEnvironmentVariables()
                .Build();

            if (args.Contains("--health-check"))
            {
                Console.WriteLine("Running in --health-check mode.");
                return await RunHealthCheckAsync(configuration);
            }
            else
            {
                return await RunFullVerificationAsync(configuration);
            }
        }

        private static async Task<int> RunHealthCheckAsync(IConfigurationRoot config)
        {
            bool redisOk = false;
            bool kafkaOk = false;
            var redisConnectionString = config["DOTNET_REDIS_URL"];
            var kafkaBootstrapServers = config["DOTNET_KAFKA_BOOTSTRAP_SERVERS"];

            if (string.IsNullOrEmpty(redisConnectionString))
            {
                redisConnectionString = "localhost:6379";
                Console.WriteLine($"Redis connection string not found. Using default: {redisConnectionString}");
            }
            if (string.IsNullOrEmpty(kafkaBootstrapServers))
            {
                kafkaBootstrapServers = "localhost:9092";
                Console.WriteLine($"Kafka bootstrap servers not found. Using default: {kafkaBootstrapServers}");
            }

            // Redis Health Check
            Console.WriteLine($"Attempting Redis connection to: {redisConnectionString}");
            redisOk = await WaitForRedisAsync(redisConnectionString);
            Console.WriteLine(redisOk ? "Redis connection successful." : "Redis connection failed.");

            // Kafka Health Check
            Console.WriteLine($"Attempting Kafka connection to: {kafkaBootstrapServers}");
            kafkaOk = WaitForKafka(kafkaBootstrapServers);
            Console.WriteLine(kafkaOk ? "Kafka connection successful (metadata retrieved)." : "Kafka connection failed.");

            return (redisOk && kafkaOk) ? 0 : 1;
        }

        private static async Task<int> RunFullVerificationAsync(IConfigurationRoot config)
        {
            var redisConnectionStringFull = config["DOTNET_REDIS_URL"];
            var kafkaBootstrapServersFull = config["DOTNET_KAFKA_BOOTSTRAP_SERVERS"];
            var globalSequenceKey = config["SIMULATOR_REDIS_KEY_GLOBAL_SEQUENCE"] ?? "flinkdotnet:global_sequence_id";
            var sinkCounterKey = config["SIMULATOR_REDIS_KEY_SINK_COUNTER"] ?? "flinkdotnet:sample:processed_message_counter";
            var kafkaTopic = config["SIMULATOR_KAFKA_TOPIC"] ?? "flinkdotnet.sample.topic";

            if (!int.TryParse(config["SIMULATOR_NUM_MESSAGES"], out int expectedMessages))
            {
                Console.WriteLine("Warning: SIMULATOR_NUM_MESSAGES environment variable not set or not a valid integer.");
                expectedMessages = 100; // Defaulting
                Console.WriteLine($"Defaulting to {expectedMessages} expected messages for verification logic.");
            }

            Console.WriteLine($"Expected messages: {expectedMessages}");
            // ... (other Console.WriteLine for config values)

            if (string.IsNullOrEmpty(redisConnectionStringFull))
            {
                redisConnectionStringFull = "localhost:6379";
                Console.WriteLine($"Redis connection string not found. Using default: {redisConnectionStringFull}");
            }

            if (string.IsNullOrEmpty(kafkaBootstrapServersFull))
            {
                kafkaBootstrapServersFull = "localhost:9092";
                Console.WriteLine($"Kafka bootstrap servers not found. Using default: {kafkaBootstrapServersFull}");
            }

            var verificationStopwatch = Stopwatch.StartNew();

            bool allChecksPassed = true;
            allChecksPassed &= await VerifyRedisAsync(redisConnectionStringFull, expectedMessages, globalSequenceKey, sinkCounterKey, 1);
            allChecksPassed &= VerifyKafkaAsync(kafkaBootstrapServersFull, kafkaTopic, expectedMessages);

            verificationStopwatch.Stop();
            Console.WriteLine($"Verification time for {expectedMessages} messages: {verificationStopwatch.ElapsedMilliseconds} ms");
            if (verificationStopwatch.ElapsedMilliseconds > 1000)
            {
                Console.WriteLine("Processing exceeded 1 second.");
                allChecksPassed = false;
            }

            Console.WriteLine(allChecksPassed ? "\nIntegration tests PASSED." : "\nIntegration tests FAILED.");
            return allChecksPassed ? 0 : 1;
        }

        private static async Task<bool> VerifyRedisAsync(string connectionString, int expectedMessages, string globalSeqKey, string sinkCounterKey, int attemptNumber)
        {
            Console.WriteLine("Connecting to Redis...");
            ConnectionMultiplexer? redis = null;
            bool redisVerified = true;
            try
            {
                redis = await ConnectionMultiplexer.ConnectAsync(connectionString);
                if (!redis.IsConnected)
                {
                    throw new InvalidOperationException("Failed to connect to Redis.");
                }
                Console.WriteLine("Successfully connected to Redis.");
                IDatabase db = redis.GetDatabase();

                async Task<bool> CheckRedisKey(string keyName, string description) {
                    Console.WriteLine($"Reading Redis key: {keyName} ({description})");
                    RedisValue value = await db.StringGetAsync(keyName);
                    if (!value.HasValue) {
                        Console.WriteLine($"Error: Redis key '{keyName}' not found.");
                        return false;
                    }
                    if ((long)value != expectedMessages) {
                        Console.WriteLine($"Error: Redis key '{keyName}' has value {(long)value}, expected {expectedMessages}.");
                        return false;
                    }
                    Console.WriteLine($"Success: Redis key '{keyName}' is {value}.");
                    return true;
                }

                redisVerified &= await CheckRedisKey(globalSeqKey, "Global Sequence");
                redisVerified &= await CheckRedisKey(sinkCounterKey, "Sink Counter");
            }
            catch (Exception ex)
            {
                Console.WriteLine($"Redis check failed on attempt {attemptNumber}: {ex.Message}");
                redisVerified = false;
            }
            finally
            {
                if (redis != null) {
                    await redis.DisposeAsync();
                }
            }
            return redisVerified;
        }

        private static bool VerifyKafkaAsync(string bootstrapServers, string topic, int expectedMessages)
        {
            Console.WriteLine("\nConnecting to Kafka...");
            bool kafkaVerified = true;
            var consumerConfig = new ConsumerConfig
            {
                BootstrapServers = bootstrapServers,
                GroupId = $"flinkdotnet-integration-verifier-{Guid.NewGuid()}",
                AutoOffsetReset = AutoOffsetReset.Earliest,
                EnableAutoCommit = false
            };

            using (var consumer = new ConsumerBuilder<Ignore, string>(consumerConfig).Build())
            {
                consumer.Subscribe(topic);
                Console.WriteLine($"Subscribed to Kafka topic: {topic}");

                var messagesConsumed = new List<string>();
                var consumeTimeout = TimeSpan.FromSeconds(60);
                var stopwatch = Stopwatch.StartNew();

                try
                {
                    while (stopwatch.Elapsed < consumeTimeout && messagesConsumed.Count < expectedMessages)
                    {
                        var consumeResult = consumer.Consume(TimeSpan.FromSeconds(10));
                        if (consumeResult == null || consumeResult.IsPartitionEOF)
                        {
                            // Log and decide if it's an early exit or continue polling
                            if (messagesConsumed.Count < expectedMessages && stopwatch.Elapsed >= consumeTimeout) {
                                Console.WriteLine($"Timeout: Expected {expectedMessages}, got {messagesConsumed.Count}.");
                                kafkaVerified = false;
                                break;
                            }
                            if (messagesConsumed.Count >= expectedMessages)
                            {
                                break;
                            }
                            continue;
                        }
                        messagesConsumed.Add(consumeResult.Message.Value);
                    }
                }
                catch (ConsumeException e)
                {
                    Console.WriteLine($"Kafka consume error: {e.Error.Reason}");
                    kafkaVerified = false;
                }
                finally
                {
                    consumer.Close(); // Close before dispose for graceful shutdown
                }

                Console.WriteLine($"Finished consuming. Total messages received: {messagesConsumed.Count}");
                if (messagesConsumed.Count < expectedMessages)
                {
                    Console.WriteLine($"Error: Expected at least {expectedMessages} messages from Kafka topic '{topic}', but only received {messagesConsumed.Count}.");
                    kafkaVerified = false;
                }
                else
                {
                    Console.WriteLine($"Success: Received {messagesConsumed.Count} messages from Kafka topic '{topic}'.");
                    for(int i = 0; i < Math.Min(messagesConsumed.Count, 5); i++) {
                        // CA1310: Use StringComparison
                        if (!messagesConsumed[i].StartsWith("MessagePayload_Seq-", StringComparison.Ordinal)) {
                           Console.WriteLine($"Error: Kafka message '{messagesConsumed[i]}' (sample {i}) does not match expected format 'MessagePayload_Seq-'.");
                        }
                    }
                }
            }
            return kafkaVerified;
        }

        private static async Task<bool> WaitForRedisAsync(string connectionString, int retries = 5, int delaySeconds = 2)
        {
            for (int i = 0; i < retries; i++)
            {
                try
                {
                    using var redis = await ConnectionMultiplexer.ConnectAsync(connectionString);
                    if (redis.IsConnected)
                    {
                        return true;
                    }
                }
                catch
                {
                    // ignored
                }
                await Task.Delay(TimeSpan.FromSeconds(delaySeconds));
            }
            return false;
        }

        private static bool WaitForKafka(string bootstrapServers, int retries = 5, int delaySeconds = 2)
        {
            var adminConfig = new AdminClientConfig { BootstrapServers = bootstrapServers };
            for (int i = 0; i < retries; i++)
            {
                try
                {
                    using var admin = new AdminClientBuilder(adminConfig).Build();
                    var metadata = admin.GetMetadata(TimeSpan.FromSeconds(10));
                    if (metadata.Topics != null)
                    {
                        return true;
                    }
                }
                catch
                {
                    // ignored
                }
                Thread.Sleep(TimeSpan.FromSeconds(delaySeconds));
            }
            return false;
        }
    }
}
