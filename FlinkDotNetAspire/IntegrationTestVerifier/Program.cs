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
            Console.WriteLine("=== FlinkDotNet Integration Test Verifier Started ===");
            Console.WriteLine($"Started at: {DateTime.UtcNow:yyyy-MM-dd HH:mm:ss} UTC");
            Console.WriteLine($"Arguments: {string.Join(" ", args)}");

            var configuration = new ConfigurationBuilder()
                .AddEnvironmentVariables()
                .Build();
            
            // Log all relevant environment variables for debugging
            Console.WriteLine("\n=== Environment Variables ===");
            var envVars = new[]
            {
                "DOTNET_REDIS_URL", "DOTNET_KAFKA_BOOTSTRAP_SERVERS", "SIMULATOR_NUM_MESSAGES",
                "SIMULATOR_REDIS_KEY_GLOBAL_SEQUENCE", "SIMULATOR_REDIS_KEY_SINK_COUNTER",
                "SIMULATOR_KAFKA_TOPIC", "MAX_ALLOWED_TIME_MS", "DOTNET_ENVIRONMENT"
            };
            
            foreach (var envVar in envVars)
            {
                var value = configuration[envVar];
                Console.WriteLine($"{envVar}: {(string.IsNullOrEmpty(value) ? "<not set>" : value)}");
            }

            if (args.Contains("--health-check"))
            {
                Console.WriteLine("\n=== Running in --health-check mode ===");
                return await RunHealthCheckAsync(configuration);
            }
            else
            {
                Console.WriteLine("\n=== Running full verification ===");
                return await RunFullVerificationAsync(configuration);
            }
        }

        private static async Task<int> RunHealthCheckAsync(IConfigurationRoot config)
        {
            Console.WriteLine("\n=== Health Check Starting ===");
            bool redisOk = false;
            bool kafkaOk = false;
            var redisConnectionString = config["DOTNET_REDIS_URL"];
            var kafkaBootstrapServers = config["DOTNET_KAFKA_BOOTSTRAP_SERVERS"];

            if (string.IsNullOrEmpty(redisConnectionString))
            {
                redisConnectionString = ServiceUris.RedisConnectionString;
                Console.WriteLine($"⚠ Redis connection string not found in env. Using default: {redisConnectionString}");
            }
            else
            {
                Console.WriteLine($"✓ Redis connection string found in env: {redisConnectionString}");
            }
            
            if (string.IsNullOrEmpty(kafkaBootstrapServers))
            {
                kafkaBootstrapServers = ServiceUris.KafkaBootstrapServers;
                Console.WriteLine($"⚠ Kafka bootstrap servers not found in env. Using default: {kafkaBootstrapServers}");
            }
            else
            {
                Console.WriteLine($"✓ Kafka bootstrap servers found in env: {kafkaBootstrapServers}");
            }

            // Redis Health Check
            Console.WriteLine($"\n--- Redis Health Check ---");
            Console.WriteLine($"Connecting to Redis: {redisConnectionString}");
            var redisStopwatch = System.Diagnostics.Stopwatch.StartNew();
            redisOk = await WaitForRedisAsync(redisConnectionString);
            redisStopwatch.Stop();
            Console.WriteLine($"Redis result: {(redisOk ? "✓ SUCCESS" : "✗ FAILED")} (took {redisStopwatch.ElapsedMilliseconds}ms)");

            // Kafka Health Check
            Console.WriteLine($"\n--- Kafka Health Check ---");
            Console.WriteLine($"Connecting to Kafka: {kafkaBootstrapServers}");
            var kafkaStopwatch = System.Diagnostics.Stopwatch.StartNew();
            kafkaOk = WaitForKafka(kafkaBootstrapServers);
            kafkaStopwatch.Stop();
            Console.WriteLine($"Kafka result: {(kafkaOk ? "✓ SUCCESS" : "✗ FAILED")} (took {kafkaStopwatch.ElapsedMilliseconds}ms)");

            var overall = redisOk && kafkaOk;
            Console.WriteLine($"\n=== Health Check Complete ===");
            Console.WriteLine($"Overall result: {(overall ? "✓ PASSED" : "✗ FAILED")}");
            Console.WriteLine($"Redis: {(redisOk ? "✓" : "✗")}, Kafka: {(kafkaOk ? "✓" : "✗")}");
            
            return overall ? 0 : 1;
        }

        private static async Task<int> RunFullVerificationAsync(IConfigurationRoot config)
        {
            Console.WriteLine("\n=== Full Verification Starting ===");
            var redisConnectionStringFull = config["DOTNET_REDIS_URL"];
            var kafkaBootstrapServersFull = config["DOTNET_KAFKA_BOOTSTRAP_SERVERS"];
            var globalSequenceKey = config["SIMULATOR_REDIS_KEY_GLOBAL_SEQUENCE"] ?? "flinkdotnet:global_sequence_id";
            var sinkCounterKey = config["SIMULATOR_REDIS_KEY_SINK_COUNTER"] ?? "flinkdotnet:sample:processed_message_counter";
            var kafkaTopic = config["SIMULATOR_KAFKA_TOPIC"] ?? "flinkdotnet.sample.topic";

            if (!int.TryParse(config["SIMULATOR_NUM_MESSAGES"], out int expectedMessages))
            {
                Console.WriteLine("⚠ Warning: SIMULATOR_NUM_MESSAGES environment variable not set or not a valid integer.");
                expectedMessages = 100; // Defaulting
                Console.WriteLine($"Defaulting to {expectedMessages} expected messages for verification logic.");
            }

            Console.WriteLine($"\n--- Verification Configuration ---");
            Console.WriteLine($"Expected messages: {expectedMessages:N0}");
            Console.WriteLine($"Global sequence key: {globalSequenceKey}");
            Console.WriteLine($"Sink counter key: {sinkCounterKey}");
            Console.WriteLine($"Kafka topic: {kafkaTopic}");

            if (string.IsNullOrEmpty(redisConnectionStringFull))
            {
                redisConnectionStringFull = ServiceUris.RedisConnectionString;
                Console.WriteLine($"⚠ Redis connection string not found. Using default: {redisConnectionStringFull}");
            }
            else
            {
                Console.WriteLine($"✓ Redis connection string: {redisConnectionStringFull}");
            }

            if (string.IsNullOrEmpty(kafkaBootstrapServersFull))
            {
                kafkaBootstrapServersFull = ServiceUris.KafkaBootstrapServers;
                Console.WriteLine($"⚠ Kafka bootstrap servers not found. Using default: {kafkaBootstrapServersFull}");
            }
            else
            {
                Console.WriteLine($"✓ Kafka bootstrap servers: {kafkaBootstrapServersFull}");
            }

            var verificationStopwatch = Stopwatch.StartNew();

            Console.WriteLine($"\n=== Starting Verification Process ===");
            bool allChecksPassed = true;
            
            Console.WriteLine("\n--- Redis Verification ---");
            allChecksPassed &= await VerifyRedisAsync(redisConnectionStringFull, expectedMessages, globalSequenceKey, sinkCounterKey, 1);
            
            Console.WriteLine("\n--- Kafka Verification ---");
            allChecksPassed &= VerifyKafkaAsync(kafkaBootstrapServersFull, kafkaTopic, expectedMessages);

            verificationStopwatch.Stop();
            Console.WriteLine($"\n--- Performance Check ---");
            Console.WriteLine($"Total verification time for {expectedMessages:N0} messages: {verificationStopwatch.ElapsedMilliseconds:N0} ms");
            
            // Read max allowed time from environment variable, default to 10 seconds
            long maxAllowedTimeMs = 10000; // 10 seconds default
            if (long.TryParse(config["MAX_ALLOWED_TIME_MS"], out long configuredTimeMs))
            {
                maxAllowedTimeMs = configuredTimeMs;
            }
            
            if (verificationStopwatch.ElapsedMilliseconds > maxAllowedTimeMs)
            {
                Console.WriteLine($"✗ PERFORMANCE FAIL: Processing time {verificationStopwatch.ElapsedMilliseconds:N0}ms exceeded maximum allowed {maxAllowedTimeMs:N0}ms.");
                allChecksPassed = false;
            }
            else
            {
                Console.WriteLine($"✓ PERFORMANCE PASS: Processing time {verificationStopwatch.ElapsedMilliseconds:N0}ms is within {maxAllowedTimeMs:N0}ms limit.");
            }

            Console.WriteLine($"\n=== Final Result ===");
            Console.WriteLine($"Integration tests {(allChecksPassed ? "✓ PASSED" : "✗ FAILED")}");
            Console.WriteLine($"Completed at: {DateTime.UtcNow:yyyy-MM-dd HH:mm:ss} UTC");
            
            return allChecksPassed ? 0 : 1;
        }

        private static async Task<bool> VerifyRedisAsync(string connectionString, int expectedMessages, string globalSeqKey, string sinkCounterKey, int attemptNumber)
        {
            Console.WriteLine($"Connecting to Redis ({connectionString})...");
            ConnectionMultiplexer? redis = null;
            bool redisVerified = true;
            try
            {
                redis = await ConnectionMultiplexer.ConnectAsync(connectionString);
                if (!redis.IsConnected)
                {
                    throw new InvalidOperationException("Failed to connect to Redis.");
                }
                Console.WriteLine("✓ Successfully connected to Redis.");
                IDatabase db = redis.GetDatabase();

                async Task<bool> CheckRedisKey(string keyName, string description) {
                    Console.WriteLine($"Checking Redis key: {keyName} ({description})");
                    RedisValue value = await db.StringGetAsync(keyName);
                    if (!value.HasValue) {
                        Console.WriteLine($"✗ ERROR: Redis key '{keyName}' not found.");
                        return false;
                    }
                    var actualValue = (long)value;
                    if (actualValue != expectedMessages) {
                        Console.WriteLine($"✗ ERROR: Redis key '{keyName}' has value {actualValue:N0}, expected {expectedMessages:N0}.");
                        Console.WriteLine($"  Difference: {Math.Abs(actualValue - expectedMessages):N0} messages");
                        return false;
                    }
                    Console.WriteLine($"✓ SUCCESS: Redis key '{keyName}' has correct value: {actualValue:N0}");
                    return true;
                }

                redisVerified &= await CheckRedisKey(globalSeqKey, "Global Sequence");
                redisVerified &= await CheckRedisKey(sinkCounterKey, "Sink Counter");
                
                Console.WriteLine($"Redis verification result: {(redisVerified ? "✓ PASSED" : "✗ FAILED")}");
            }
            catch (Exception ex)
            {
                Console.WriteLine($"✗ Redis check failed on attempt {attemptNumber}: {ex.Message}");
                Console.WriteLine($"  Exception type: {ex.GetType().Name}");
                if (ex.InnerException != null)
                {
                    Console.WriteLine($"  Inner exception: {ex.InnerException.Message}");
                }
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
            Console.WriteLine($"Connecting to Kafka ({bootstrapServers})...");
            
            var consumerConfig = CreateKafkaConsumerConfig(bootstrapServers);
            using var consumer = new ConsumerBuilder<Ignore, string>(consumerConfig).Build();
            
            consumer.Subscribe(topic);
            Console.WriteLine($"✓ Subscribed to Kafka topic: {topic}");

            var messagesConsumed = ConsumeKafkaMessages(consumer, expectedMessages);
            bool kafkaVerified = ValidateKafkaResults(messagesConsumed, expectedMessages, topic);
            
            Console.WriteLine($"Kafka verification result: {(kafkaVerified ? "✓ PASSED" : "✗ FAILED")}");
            return kafkaVerified;
        }

        private static ConsumerConfig CreateKafkaConsumerConfig(string bootstrapServers)
        {
            return new ConsumerConfig
            {
                BootstrapServers = bootstrapServers,
                GroupId = $"flinkdotnet-integration-verifier-{Guid.NewGuid()}",
                AutoOffsetReset = AutoOffsetReset.Earliest,
                EnableAutoCommit = false,
                SecurityProtocol = SecurityProtocol.Plaintext // Explicitly set to plaintext for local testing
            };
        }

        private static List<string> ConsumeKafkaMessages(IConsumer<Ignore, string> consumer, int expectedMessages)
        {
            var messagesConsumed = new List<string>();
            var consumeTimeout = TimeSpan.FromSeconds(30);
            var stopwatch = Stopwatch.StartNew();
            var lastLogTime = DateTime.UtcNow;

            try
            {
                Console.WriteLine($"Starting to consume messages (timeout: {consumeTimeout.TotalSeconds}s)...");
                while (stopwatch.Elapsed < consumeTimeout && messagesConsumed.Count < expectedMessages)
                {
                    var consumeResult = consumer.Consume(TimeSpan.FromSeconds(5));
                    if (consumeResult == null || consumeResult.IsPartitionEOF)
                    {
                        HandleNoMessage(messagesConsumed, expectedMessages, stopwatch, ref lastLogTime, consumeTimeout);
                        if (messagesConsumed.Count >= expectedMessages) break;
                        continue;
                    }
                    
                    messagesConsumed.Add(consumeResult.Message.Value);
                    LogConsumeProgress(messagesConsumed.Count, expectedMessages);
                }
            }
            catch (ConsumeException e)
            {
                Console.WriteLine($"✗ Kafka consume error: {e.Error.Reason}");
                Console.WriteLine($"  Error code: {e.Error.Code}");
            }
            finally
            {
                consumer.Close();
            }

            stopwatch.Stop();
            Console.WriteLine($"Kafka consumption completed in {stopwatch.Elapsed.TotalSeconds:F1}s");
            Console.WriteLine($"Total messages received: {messagesConsumed.Count:N0}");
            
            return messagesConsumed;
        }

        private static void HandleNoMessage(List<string> messagesConsumed, int expectedMessages, 
            Stopwatch stopwatch, ref DateTime lastLogTime, TimeSpan consumeTimeout)
        {
            var now = DateTime.UtcNow;
            if ((now - lastLogTime).TotalSeconds >= 5)
            {
                Console.WriteLine($"Waiting for messages... Current count: {messagesConsumed.Count:N0}/{expectedMessages:N0} ({messagesConsumed.Count * 100.0 / expectedMessages:F1}%). Elapsed: {stopwatch.Elapsed.TotalSeconds:F1}s");
                lastLogTime = now;
            }
            
            if (messagesConsumed.Count < expectedMessages && stopwatch.Elapsed >= consumeTimeout)
            {
                Console.WriteLine($"✗ TIMEOUT: Expected {expectedMessages:N0}, got {messagesConsumed.Count:N0} messages.");
            }
        }

        private static void LogConsumeProgress(int currentCount, int expectedMessages)
        {
            if (currentCount % Math.Max(1, expectedMessages / 10) == 0)
            {
                Console.WriteLine($"Progress: {currentCount:N0}/{expectedMessages:N0} messages ({currentCount * 100.0 / expectedMessages:F1}%)");
            }
        }

        private static bool ValidateKafkaResults(List<string> messagesConsumed, int expectedMessages, string topic)
        {
            bool kafkaVerified = true;
            
            if (messagesConsumed.Count < expectedMessages)
            {
                Console.WriteLine($"✗ ERROR: Expected at least {expectedMessages:N0} messages from Kafka topic '{topic}', but only received {messagesConsumed.Count:N0}.");
                Console.WriteLine($"  Shortfall: {expectedMessages - messagesConsumed.Count:N0} messages ({(expectedMessages - messagesConsumed.Count) * 100.0 / expectedMessages:F1}%)");
                kafkaVerified = false;
            }
            else
            {
                Console.WriteLine($"✓ SUCCESS: Received {messagesConsumed.Count:N0} messages from Kafka topic '{topic}'.");
                
                Console.WriteLine("Verifying message ordering...");
                bool fifoOrderingPassed = VerifyFIFOOrdering(messagesConsumed);
                if (fifoOrderingPassed)
                {
                    Console.WriteLine("✓ FIFO ordering verification PASSED");
                    PrintTopAndBottomMessages(messagesConsumed, 10);
                }
                else
                {
                    Console.WriteLine("✗ FIFO ordering verification FAILED");  
                    kafkaVerified = false;
                }
            }
            
            return kafkaVerified;
        }

        private static bool VerifyFIFOOrdering(List<string> messages)
        {
            Console.WriteLine($"Verifying FIFO ordering for {messages.Count:N0} messages...");
            
            long previousRedisOrderedId = 0;
            bool hasValidPreviousMessage = false;
            int nonBarrierCount = 0;
            int barrierCount = 0;
            
            for (int i = 0; i < messages.Count; i++)
            {
                try
                {
                    // Skip barrier messages 
                    if (messages[i].StartsWith("BARRIER_", StringComparison.Ordinal))
                    {
                        barrierCount++;
                        continue;
                    }
                    
                    nonBarrierCount++;
                    
                    // Parse JSON message to extract redis_ordered_id
                    var message = messages[i];
                    if (!message.StartsWith("{", StringComparison.Ordinal))
                    {
                        Console.WriteLine($"✗ ERROR: Message at index {i} is not JSON format: {message}");
                        return false;
                    }
                    
                    // Simple JSON parsing to extract redis_ordered_id
                    var redisOrderedIdMatch = Regex.Match(message, @"""redis_ordered_id"":(\d+)");
                    if (!redisOrderedIdMatch.Success)
                    {
                        Console.WriteLine($"✗ ERROR: Could not extract redis_ordered_id from message at index {i}: {message}");
                        return false;
                    }
                    
                    long currentRedisOrderedId = long.Parse(redisOrderedIdMatch.Groups[1].Value);
                    
                    if (hasValidPreviousMessage && currentRedisOrderedId <= previousRedisOrderedId)
                    {
                        Console.WriteLine($"✗ ERROR: FIFO ordering violated at message index {i}.");
                        Console.WriteLine($"  Current redis_ordered_id: {currentRedisOrderedId}, Previous: {previousRedisOrderedId}");
                        Console.WriteLine($"  Current message: {message}");
                        return false;
                    }
                    
                    previousRedisOrderedId = currentRedisOrderedId;
                    hasValidPreviousMessage = true;
                }
                catch (Exception ex)
                {
                    Console.WriteLine($"✗ ERROR: Error parsing message at index {i}: {ex.Message}");
                    Console.WriteLine($"  Message: {messages[i]}");
                    return false;
                }
            }
            
            Console.WriteLine($"✓ FIFO ordering verification passed!");
            Console.WriteLine($"  Total messages: {messages.Count:N0} (Data: {nonBarrierCount:N0}, Barriers: {barrierCount:N0})");
            Console.WriteLine($"  Sequence range: 1 to {previousRedisOrderedId:N0}");
            return true;
        }

        private static void PrintTopAndBottomMessages(List<string> messages, int count)
        {
            // Get non-barrier messages
            var nonBarrierMessages = messages.Where(m => !m.StartsWith("BARRIER_", StringComparison.Ordinal)).ToList();
            
            Console.WriteLine($"\n--- Sample Messages (showing first and last {count} of {nonBarrierMessages.Count:N0} data messages) ---");
            
            Console.WriteLine($"\nFirst {count} messages:");
            for (int i = 0; i < Math.Min(count, nonBarrierMessages.Count); i++)
            {
                Console.WriteLine($"  [{i+1}]: {nonBarrierMessages[i]}");
            }
            
            if (nonBarrierMessages.Count > count)
            {
                Console.WriteLine($"\nLast {count} messages:");
                int startIndex = Math.Max(0, nonBarrierMessages.Count - count);
                for (int i = startIndex; i < nonBarrierMessages.Count; i++)
                {
                    Console.WriteLine($"  [{i+1}]: {nonBarrierMessages[i]}");
                }
            }
            
            Console.WriteLine($"--- End Sample Messages ---");
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
