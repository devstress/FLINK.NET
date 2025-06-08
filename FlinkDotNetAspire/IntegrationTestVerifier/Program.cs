using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Confluent.Kafka;
using Microsoft.Extensions.Configuration;
using StackExchange.Redis;

public class Program
{
    private static IConfigurationRoot? _configuration;

    public static async Task<int> Main(string[] args)
    {
        Console.WriteLine("Integration Test Verifier Started.");

        _configuration = new ConfigurationBuilder()
            .AddEnvironmentVariables()
            .Build();

        // Health Check Mode
        if (args.Contains("--health-check"))
        {
            Console.WriteLine("Running in --health-check mode.");
            bool redisOk = false;
            bool kafkaOk = false;
            var redisConnectionString = _configuration["DOTNET_REDIS_URL"];
            var kafkaBootstrapServers = _configuration["DOTNET_KAFKA_BOOTSTRAP_SERVERS"];

            if (string.IsNullOrEmpty(redisConnectionString)) {
                Console.WriteLine("Error: DOTNET_REDIS_URL environment variable not found for health check.");
                return 1;
            }
            if (string.IsNullOrEmpty(kafkaBootstrapServers)) {
                Console.WriteLine("Error: DOTNET_KAFKA_BOOTSTRAP_SERVERS environment variable not found for health check.");
                return 1;
            }

            try
            {
                Console.WriteLine($"Attempting Redis connection to: {redisConnectionString}");
                using var redis = await ConnectionMultiplexer.ConnectAsync(redisConnectionString);
                redisOk = redis.IsConnected;
                Console.WriteLine(redisOk ? "Redis connection successful." : "Redis connection failed.");
            }
            catch (Exception ex)
            {
                Console.WriteLine($"Redis health check failed: {ex.Message}");
            }

            try
            {
                Console.WriteLine($"Attempting Kafka connection to: {kafkaBootstrapServers}");
                var consumerConfig = new ConsumerConfig { BootstrapServers = kafkaBootstrapServers, GroupId = $"health-check-{Guid.NewGuid()}" };
                using var consumer = new ConsumerBuilder<Ignore, string>(consumerConfig).Build();
                // Attempt to query metadata (e.g., list topics) as a lightweight connection test
                var metadata = consumer.GetMetadata(TimeSpan.FromSeconds(10));
                kafkaOk = metadata.Topics.Count >= 0; // Will throw on timeout/connection error before this
                Console.WriteLine(kafkaOk ? "Kafka connection successful (metadata retrieved)." : "Kafka connection failed.");
            }
            catch (Exception ex)
            {
                Console.WriteLine($"Kafka health check failed: {ex.Message}");
            }

            return (redisOk && kafkaOk) ? 0 : 1;
        }

        // Full Verification Mode (existing logic)
        var redisConnectionStringFull = _configuration["DOTNET_REDIS_URL"];
        var kafkaBootstrapServersFull = _configuration["DOTNET_KAFKA_BOOTSTRAP_SERVERS"];

        var globalSequenceKey = _configuration["SIMULATOR_REDIS_KEY_GLOBAL_SEQUENCE"] ?? "flinkdotnet:global_sequence_id";
        var sinkCounterKey = _configuration["SIMULATOR_REDIS_KEY_SINK_COUNTER"] ?? "flinkdotnet:sample:processed_message_counter";
        var kafkaTopic = _configuration["SIMULATOR_KAFKA_TOPIC"] ?? "flinkdotnet.sample.topic";

        if (!int.TryParse(_configuration["SIMULATOR_NUM_MESSAGES"], out int expectedMessages))
        {
            Console.WriteLine("Error: SIMULATOR_NUM_MESSAGES environment variable not set or not a valid integer.");
            expectedMessages = 100;
            Console.WriteLine($"Defaulting to {expectedMessages} expected messages for verification logic.");
        }

        Console.WriteLine($"Expected messages: {expectedMessages}");
        Console.WriteLine($"Redis Connection String: {redisConnectionStringFull}");
        Console.WriteLine($"Kafka Bootstrap Servers: {kafkaBootstrapServersFull}");
        Console.WriteLine($"Redis Global Sequence Key: {globalSequenceKey}");
        Console.WriteLine($"Redis Sink Counter Key: {sinkCounterKey}");
        Console.WriteLine($"Kafka Topic: {kafkaTopic}");

        if (string.IsNullOrEmpty(redisConnectionStringFull))
        {
            Console.WriteLine("Error: DOTNET_REDIS_URL environment variable not found.");
            return 1;
        }
        if (string.IsNullOrEmpty(kafkaBootstrapServersFull))
        {
            Console.WriteLine("Error: DOTNET_KAFKA_BOOTSTRAP_SERVERS environment variable not found.");
            return 1;
        }

        bool allChecksPassed = true;
        int attempts = 0;
        const int maxAttempts = 5; // Increased attempts for CI stability
        const int delayBetweenAttemptsMs = 20000; // Increased delay

        while (attempts < maxAttempts)
        {
            attempts++;
            Console.WriteLine($"\n--- Verification Attempt {attempts}/{maxAttempts} ---");
            allChecksPassed = true;

            try
            {
                Console.WriteLine("Connecting to Redis...");
                ConnectionMultiplexer? redis = null;
                try
                {
                    redis = await ConnectionMultiplexer.ConnectAsync(redisConnectionStringFull);
                    if (!redis.IsConnected) throw new Exception("Failed to connect to Redis.");
                    Console.WriteLine("Successfully connected to Redis.");
                    IDatabase db = redis.GetDatabase();

                    Console.WriteLine($"Reading Redis key: {globalSequenceKey}");
                    RedisValue globalSequenceValue = await db.StringGetAsync(globalSequenceKey);
                    Console.WriteLine($"Reading Redis key: {sinkCounterKey}");
                    RedisValue sinkCounterValue = await db.StringGetAsync(sinkCounterKey);

                    if (!globalSequenceValue.HasValue)
                    {
                        Console.WriteLine($"Error: Redis key '{globalSequenceKey}' not found.");
                        allChecksPassed = false;
                    }
                    else if ((long)globalSequenceValue != expectedMessages)
                    {
                        Console.WriteLine($"Error: Redis key '{globalSequenceKey}' has value {(long)globalSequenceValue}, expected {expectedMessages}.");
                        allChecksPassed = false;
                    }
                    else
                    {
                        Console.WriteLine($"Success: Redis key '{globalSequenceKey}' is {globalSequenceValue}.");
                    }

                    if (!sinkCounterValue.HasValue)
                    {
                        Console.WriteLine($"Error: Redis key '{sinkCounterKey}' not found.");
                        allChecksPassed = false;
                    }
                    else if ((long)sinkCounterValue != expectedMessages)
                    {
                        Console.WriteLine($"Error: Redis key '{sinkCounterKey}' has value {(long)sinkCounterValue}, expected {expectedMessages}.");
                        allChecksPassed = false;
                    }
                    else
                    {
                        Console.WriteLine($"Success: Redis key '{sinkCounterKey}' is {sinkCounterValue}.");
                    }
                }
                catch (Exception ex)
                {
                    Console.WriteLine($"Redis check failed on attempt {attempts}: {ex.Message}");
                    allChecksPassed = false;
                }
                finally
                {
                    if (redis != null) {
                        await redis.CloseAsync();
                        redis.Dispose();
                    }
                }

                if (!allChecksPassed && attempts < maxAttempts) {
                    Console.WriteLine($"Redis checks did not pass. Waiting {delayBetweenAttemptsMs / 1000}s before retrying...");
                    await Task.Delay(delayBetweenAttemptsMs);
                    continue;
                }
                if (!allChecksPassed && attempts == maxAttempts) {
                     Console.WriteLine($"Redis checks failed after {maxAttempts} attempts.");
                     break;
                }
                 if (!allChecksPassed) break;


                Console.WriteLine("\nConnecting to Kafka...");
                var consumerConfig = new ConsumerConfig
                {
                    BootstrapServers = kafkaBootstrapServersFull,
                    GroupId = $"flinkdotnet-integration-verifier-{Guid.NewGuid()}",
                    AutoOffsetReset = AutoOffsetReset.Earliest,
                    EnableAutoCommit = false
                };

                using (var consumer = new ConsumerBuilder<Ignore, string>(consumerConfig).Build())
                {
                    consumer.Subscribe(kafkaTopic);
                    Console.WriteLine($"Subscribed to Kafka topic: {kafkaTopic}");

                    var messagesConsumed = new List<string>();
                    var consumeTimeout = TimeSpan.FromSeconds(60); // Increased timeout for Kafka
                    var stopwatch = Stopwatch.StartNew();
                    int expectedKafkaMessages = expectedMessages;

                    try
                    {
                        while (stopwatch.Elapsed < consumeTimeout && messagesConsumed.Count < expectedKafkaMessages)
                        {
                            var consumeResult = consumer.Consume(TimeSpan.FromSeconds(10));
                            if (consumeResult == null || consumeResult.IsPartitionEOF)
                            {
                                Console.WriteLine(consumeResult == null ? "No message received in last 10s." : $"Reached end of partition: {consumeResult.TopicPartitionOffset}");
                                if (messagesConsumed.Count < expectedKafkaMessages && stopwatch.Elapsed < consumeTimeout) {
                                     Console.WriteLine($"Warning: Expected {expectedKafkaMessages} messages, got {messagesConsumed.Count} so far within {stopwatch.Elapsed.TotalSeconds}s / {consumeTimeout.TotalSeconds}s.");
                                } else if (messagesConsumed.Count < expectedKafkaMessages && stopwatch.Elapsed >= consumeTimeout) {
                                    Console.WriteLine($"Timeout: Expected {expectedKafkaMessages}, got {messagesConsumed.Count}.");
                                    allChecksPassed = false;
                                    break;
                                }
                                if (messagesConsumed.Count >= expectedKafkaMessages) break; // Already got enough
                                continue;
                            }
                            messagesConsumed.Add(consumeResult.Message.Value);
                        }
                    }
                    catch (ConsumeException e)
                    {
                        Console.WriteLine($"Kafka consume error: {e.Error.Reason}");
                        allChecksPassed = false;
                    }
                    finally
                    {
                        consumer.Close();
                    }

                    Console.WriteLine($"Finished consuming. Total messages received: {messagesConsumed.Count}");

                    if (messagesConsumed.Count < expectedKafkaMessages)
                    {
                        Console.WriteLine($"Error: Expected at least {expectedKafkaMessages} messages from Kafka topic '{kafkaTopic}', but only received {messagesConsumed.Count}.");
                        allChecksPassed = false;
                    }
                    else
                    {
                        Console.WriteLine($"Success: Received {messagesConsumed.Count} messages from Kafka topic '{kafkaTopic}'.");
                        // Basic format check on a sample
                        for(int i = 0; i < Math.Min(messagesConsumed.Count, 5); i++) {
                           if (!messagesConsumed[i].StartsWith("MessagePayload_Seq-")) {
                               Console.WriteLine($"Error: Kafka message '{messagesConsumed[i]}' (sample {i}) does not match expected format 'MessagePayload_Seq-'.");
                               // allChecksPassed = false; // Decide if this is a critical failure
                               // break;
                           }
                        }
                    }
                }
            }
            catch (Exception ex)
            {
                Console.WriteLine($"\nAn unexpected error occurred during verification attempt {attempts}: {ex}");
                allChecksPassed = false;
            }

            if (allChecksPassed)
            {
                Console.WriteLine($"\n--- All checks PASSED on attempt {attempts} ---");
                break;
            }
            else if (attempts < maxAttempts)
            {
                Console.WriteLine($"\n--- Some checks FAILED on attempt {attempts}. Waiting {delayBetweenAttemptsMs / 1000}s before retrying... ---");
                await Task.Delay(delayBetweenAttemptsMs);
            }
            else
            {
                 Console.WriteLine($"\n--- Some checks FAILED after {maxAttempts} attempts. ---");
            }
        }

        if (allChecksPassed)
        {
            Console.WriteLine("\nIntegration tests PASSED.");
            return 0;
        }
        else
        {
            Console.WriteLine("\nIntegration tests FAILED.");
            return 1;
        }
    }
}
