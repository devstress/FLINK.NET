#pragma warning disable S3776 // Cognitive Complexity of methods is too high
using FlinkDotNet.Common.Constants;

namespace IntegrationTestVerifier
{
    using System;
    using System.Collections.Generic;
    using System.Diagnostics;
    using System.Linq;
    using System.Threading;
    using System.Threading.Tasks;
    using System.Text.RegularExpressions;
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
                redisConnectionString = ServiceUris.RedisConnectionString;
                Console.WriteLine($"Redis connection string not found. Using default: {redisConnectionString}");
            }
            if (string.IsNullOrEmpty(kafkaBootstrapServers))
            {
                kafkaBootstrapServers = ServiceUris.KafkaBootstrapServers;
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
                redisConnectionStringFull = ServiceUris.RedisConnectionString;
                Console.WriteLine($"Redis connection string not found. Using default: {redisConnectionStringFull}");
            }

            if (string.IsNullOrEmpty(kafkaBootstrapServersFull))
            {
                kafkaBootstrapServersFull = ServiceUris.KafkaBootstrapServers;
                Console.WriteLine($"Kafka bootstrap servers not found. Using default: {kafkaBootstrapServersFull}");
            }

            var verificationStopwatch = Stopwatch.StartNew();

            bool allChecksPassed = true;
            allChecksPassed &= await VerifyRedisAsync(redisConnectionStringFull, expectedMessages, globalSequenceKey, sinkCounterKey, 1);
            allChecksPassed &= VerifyKafkaAsync(kafkaBootstrapServersFull, kafkaTopic, expectedMessages);

            verificationStopwatch.Stop();
            Console.WriteLine($"Verification time for {expectedMessages} messages: {verificationStopwatch.ElapsedMilliseconds} ms");
            
            // Read max allowed time from environment variable, default to 10 seconds
            long maxAllowedTimeMs = 10000; // 10 seconds default
            if (long.TryParse(config["MAX_ALLOWED_TIME_MS"], out long configuredTimeMs))
            {
                maxAllowedTimeMs = configuredTimeMs;
            }
            if (verificationStopwatch.ElapsedMilliseconds > maxAllowedTimeMs)
            {
                Console.WriteLine($"(Failed) Processing time {verificationStopwatch.ElapsedMilliseconds}ms exceeded maximum allowed {maxAllowedTimeMs}ms.");
                allChecksPassed = false;
            }
            else
            {
                Console.WriteLine($"(Passed) Processing time {verificationStopwatch.ElapsedMilliseconds}ms is less than {maxAllowedTimeMs}ms.");
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
                EnableAutoCommit = false,
                SecurityProtocol = SecurityProtocol.Plaintext // Explicitly set to plaintext for local testing
            };

            using (var consumer = new ConsumerBuilder<Ignore, string>(consumerConfig).Build())
            {
                consumer.Subscribe(topic);
                Console.WriteLine($"Subscribed to Kafka topic: {topic}");

                var messagesConsumed = new List<string>();
                var consumeTimeout = TimeSpan.FromSeconds(10);
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
                    
                    // Check FIFO ordering and print top 10 and bottom 10 messages
                    bool fifoOrderingPassed = VerifyFIFOOrdering(messagesConsumed);
                    if (fifoOrderingPassed)
                    {
                        Console.WriteLine("(Passed) FIFO ordered. Please print top 10 and top last 10.");
                        PrintTopAndBottomMessages(messagesConsumed, 10);
                    }
                    else
                    {
                        Console.WriteLine("(Failed) FIFO ordering check failed.");  
                        kafkaVerified = false;
                    }
                }
            }
            return kafkaVerified;
        }

        private static bool VerifyFIFOOrdering(List<string> messages)
        {
            Console.WriteLine("Verifying FIFO ordering...");
            
            long previousRedisOrderedId = 0;
            bool hasValidPreviousMessage = false;
            
            for (int i = 0; i < messages.Count; i++)
            {
                try
                {
                    // Skip barrier messages 
                    if (messages[i].StartsWith("BARRIER_", StringComparison.Ordinal))
                    {
                        continue;
                    }
                    
                    // Parse JSON message to extract redis_ordered_id
                    var message = messages[i];
                    if (!message.StartsWith("{", StringComparison.Ordinal))
                    {
                        Console.WriteLine($"Error: Message at index {i} is not JSON format: {message}");
                        return false;
                    }
                    
                    // Simple JSON parsing to extract redis_ordered_id
                    var redisOrderedIdMatch = Regex.Match(message, @"""redis_ordered_id"":(\d+)");
                    if (!redisOrderedIdMatch.Success)
                    {
                        Console.WriteLine($"Error: Could not extract redis_ordered_id from message at index {i}: {message}");
                        return false;
                    }
                    
                    long currentRedisOrderedId = long.Parse(redisOrderedIdMatch.Groups[1].Value);
                    
                    if (hasValidPreviousMessage && currentRedisOrderedId <= previousRedisOrderedId)
                    {
                        Console.WriteLine($"Error: FIFO ordering violated at index {i}. Current redis_ordered_id: {currentRedisOrderedId}, Previous: {previousRedisOrderedId}");
                        return false;
                    }
                    
                    previousRedisOrderedId = currentRedisOrderedId;
                    hasValidPreviousMessage = true;
                }
                catch (Exception ex)
                {
                    Console.WriteLine($"Error parsing message at index {i}: {ex.Message}");
                    return false;
                }
            }
            
            Console.WriteLine("FIFO ordering verification passed!");
            return true;
        }

        private static void PrintTopAndBottomMessages(List<string> messages, int count)
        {
            // Get non-barrier messages
            var nonBarrierMessages = messages.Where(m => !m.StartsWith("BARRIER_", StringComparison.Ordinal)).ToList();
            
            Console.WriteLine($"\nTop {count} messages:");
            for (int i = 0; i < Math.Min(count, nonBarrierMessages.Count); i++)
            {
                Console.WriteLine($"  [{i+1}]: {nonBarrierMessages[i]}");
            }
            
            if (nonBarrierMessages.Count > count)
            {
                Console.WriteLine($"\nBottom {count} messages:");
                int startIndex = Math.Max(0, nonBarrierMessages.Count - count);
                for (int i = startIndex; i < nonBarrierMessages.Count; i++)
                {
                    Console.WriteLine($"  [{i+1}]: {nonBarrierMessages[i]}");
                }
            }
        }

        private static async Task<bool> WaitForRedisAsync(string connectionString, int maxAttempts = 2, int delaySeconds = 5)
        {
            for (int i = 0; i < maxAttempts; i++)
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

        private static bool WaitForKafka(string bootstrapServers, int maxAttempts = 2, int delaySeconds = 5)
        {
            var adminConfig = new AdminClientConfig 
            { 
                BootstrapServers = bootstrapServers,
                SecurityProtocol = SecurityProtocol.Plaintext // Explicitly set to plaintext for local testing
            };
            for (int i = 0; i < maxAttempts; i++)
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
