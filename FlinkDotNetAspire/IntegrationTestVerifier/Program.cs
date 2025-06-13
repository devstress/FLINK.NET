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
    using System.Net.Sockets;
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
            Console.WriteLine("\n🏥 === INFRASTRUCTURE HEALTH CHECK ===");
            Console.WriteLine("📋 Validating Redis and Kafka container accessibility");
            
            bool redisOk = false;
            bool kafkaOk = false;
            var redisConnectionString = config["DOTNET_REDIS_URL"];
            var kafkaBootstrapServers = config["DOTNET_KAFKA_BOOTSTRAP_SERVERS"];

            // Basic port connectivity check similar to workflow logic
            static bool CheckPort(string host, int port)
            {
                try
                {
                    using var client = new TcpClient();
                    var task = client.ConnectAsync(host, port);
                    return task.Wait(TimeSpan.FromSeconds(3)) && client.Connected;
                }
                catch
                {
                    return false;
                }
            }

            Console.WriteLine("\n🔍 DISCOVERY: Resolving service connection strings");
            if (string.IsNullOrEmpty(redisConnectionString))
            {
                redisConnectionString = ServiceUris.RedisConnectionString;
                Console.WriteLine($"   ⚠ Redis connection string not found in env. Using default: {redisConnectionString}");
            }
            else
            {
                Console.WriteLine($"   ✅ Redis connection string found: {redisConnectionString}");
            }
            
            if (string.IsNullOrEmpty(kafkaBootstrapServers))
            {
                kafkaBootstrapServers = ServiceUris.KafkaBootstrapServers;
                Console.WriteLine($"   ⚠ Kafka bootstrap servers not found in env. Using default: {kafkaBootstrapServers}");
            }
            else
            {
                Console.WriteLine($"   ✅ Kafka bootstrap servers found: {kafkaBootstrapServers}");
            }

            // Port reachability checks
            if (!string.IsNullOrEmpty(redisConnectionString) && redisConnectionString.Contains(':'))
            {
                var portPart = redisConnectionString.Split(':')[1];
                if (int.TryParse(portPart, out var port))
                {
                    Console.WriteLine($"\n   🔌 Testing Redis port reachability (localhost:{port})...");
                    Console.WriteLine($"      {(CheckPort("localhost", port) ? "✅ Port reachable" : "❌ Port unreachable")}");
                }
            }

            if (!string.IsNullOrEmpty(kafkaBootstrapServers) && kafkaBootstrapServers.Contains(':'))
            {
                var portPart = kafkaBootstrapServers.Split(':')[1];
                if (int.TryParse(portPart, out var port))
                {
                    Console.WriteLine($"   🔌 Testing Kafka port reachability (localhost:{port})...");
                    Console.WriteLine($"      {(CheckPort("localhost", port) ? "✅ Port reachable" : "❌ Port unreachable")}");
                }
            }

            // Redis Health Check
            Console.WriteLine($"\n🔴 HEALTH CHECK 1: Redis Service");
            Console.WriteLine($"   📌 GIVEN: Redis container should be accessible at {redisConnectionString}");
            Console.WriteLine($"   🎯 WHEN: Attempting connection and basic operations");
            var redisStopwatch = System.Diagnostics.Stopwatch.StartNew();
            redisOk = await WaitForRedisAsync(redisConnectionString);
            redisStopwatch.Stop();
            Console.WriteLine($"   {(redisOk ? "✅ THEN: Redis health check PASSED" : "❌ THEN: Redis health check FAILED")} (took {redisStopwatch.ElapsedMilliseconds}ms)");

            // Kafka Health Check
            Console.WriteLine($"\n🟡 HEALTH CHECK 2: Kafka Service");
            Console.WriteLine($"   📌 GIVEN: Kafka container should be accessible at {kafkaBootstrapServers}");
            Console.WriteLine($"   🎯 WHEN: Attempting connection and metadata retrieval");
            var kafkaStopwatch = System.Diagnostics.Stopwatch.StartNew();
            kafkaOk = WaitForKafka(kafkaBootstrapServers);
            kafkaStopwatch.Stop();
            Console.WriteLine($"   {(kafkaOk ? "✅ THEN: Kafka health check PASSED" : "❌ THEN: Kafka health check FAILED")} (took {kafkaStopwatch.ElapsedMilliseconds}ms)");

            var overall = redisOk && kafkaOk;
            Console.WriteLine($"\n🏁 === HEALTH CHECK SUMMARY ===");
            if (overall)
            {
                Console.WriteLine("🎉 INFRASTRUCTURE: ✅ **HEALTHY** - All services accessible");
                Console.WriteLine($"   ✓ Redis: Operational");
                Console.WriteLine($"   ✓ Kafka: Operational");
            }
            else
            {
                Console.WriteLine("💥 INFRASTRUCTURE: ❌ **UNHEALTHY** - Service failures detected");
                Console.WriteLine($"   {(redisOk ? "✓" : "❌")} Redis: {(redisOk ? "Operational" : "Failed")}");
                Console.WriteLine($"   {(kafkaOk ? "✓" : "❌")} Kafka: {(kafkaOk ? "Operational" : "Failed")}");
            }
            
            return overall ? 0 : 1;
        }

        private static void PrintBddScenarioDocumentation(string globalSequenceKey, int expectedMessages, string sinkCounterKey, string kafkaTopic)
        {
            Console.WriteLine("📖 GIVEN: Local Flink.NET Setup with Aspire orchestration");
            Console.WriteLine($"   ├─ Redis provides sequence ID generation (key: '{globalSequenceKey}')");
            Console.WriteLine($"   ├─ HighVolumeSourceFunction generates {expectedMessages:N0} ordered messages");
            Console.WriteLine($"   ├─ RedisIncrementSinkFunction counts messages (key: '{sinkCounterKey}')");
            Console.WriteLine($"   └─ KafkaSinkFunction writes messages to topic ('{kafkaTopic}')");
            Console.WriteLine("");
            
            Console.WriteLine("🎯 WHEN: FlinkJobSimulator executes the dual-sink job");
            Console.WriteLine("   ├─ Source: Redis INCR generates sequence IDs 1 to N");
            Console.WriteLine("   ├─ Map: SimpleToUpperMapOperator processes messages (P=1 for FIFO order)");
            Console.WriteLine("   ├─ Fork: Stream splits to Redis sink AND Kafka sink");
            Console.WriteLine("   └─ Execution: LocalStreamExecutor runs the job");
            Console.WriteLine("");
            
            Console.WriteLine("✅ THEN: Expected behavior according to documentation:");
            Console.WriteLine($"   ├─ Global sequence key should equal {expectedMessages:N0}");
            Console.WriteLine($"   ├─ Sink counter key should equal {expectedMessages:N0}");
            Console.WriteLine($"   ├─ Kafka topic contains {expectedMessages:N0} ordered messages");
            Console.WriteLine($"   └─ FIFO ordering maintained with Redis-generated sequence IDs");
        }

        private static bool ValidatePerformanceRequirements(Stopwatch verificationStopwatch, int expectedMessages, IConfigurationRoot config)
        {
            verificationStopwatch.Stop();
            Console.WriteLine($"\n🚀 SCENARIO 3: Performance Requirements");
            Console.WriteLine($"   📋 Testing: Processing time within acceptable limits");
            Console.WriteLine($"   📊 Actual verification time: {verificationStopwatch.ElapsedMilliseconds:N0}ms for {expectedMessages:N0} messages");
            
            // Read max allowed time from environment variable, default to 10 seconds
            long maxAllowedTimeMs = 10000; // 10 seconds default
            if (long.TryParse(config["MAX_ALLOWED_TIME_MS"], out long configuredTimeMs))
            {
                maxAllowedTimeMs = configuredTimeMs;
            }
            
            if (verificationStopwatch.ElapsedMilliseconds > maxAllowedTimeMs)
            {
                Console.WriteLine($"   ❌ THEN: Performance requirement FAILED");
                Console.WriteLine($"      📈 Expected: ≤ {maxAllowedTimeMs:N0}ms, Actual: {verificationStopwatch.ElapsedMilliseconds:N0}ms");
                return false;
            }
            else
            {
                Console.WriteLine($"   ✅ THEN: Performance requirement PASSED");
                Console.WriteLine($"      📈 Expected: ≤ {maxAllowedTimeMs:N0}ms, Actual: {verificationStopwatch.ElapsedMilliseconds:N0}ms");
                return true;
            }
        }

        private static void PrintFinalResult(bool allChecksPassed)
        {
            Console.WriteLine($"\n🏁 === FINAL VERIFICATION RESULT ===");
            if (allChecksPassed)
            {
                Console.WriteLine("🎉 STRESS TEST: ✅ **PASSED** - All scenarios validated successfully");
                Console.WriteLine("   ✓ Redis sequence generation and sink counting");
                Console.WriteLine("   ✓ Kafka message ordering and content");
                Console.WriteLine("   ✓ Performance within acceptable limits");
            }
            else
            {
                Console.WriteLine("💥 STRESS TEST: ❌ **FAILED** - One or more scenarios failed validation");
                Console.WriteLine("   ℹ️  Check individual scenario results above for details");
            }
        }

        private static async Task<int> RunFullVerificationAsync(IConfigurationRoot config)
        {
            Console.WriteLine("\n=== 🧪 FLINK.NET HIGH-THROUGHPUT STRESS TEST VERIFICATION ===");
            Console.WriteLine("📋 BDD Test Scenario: Local High Throughput Test with Redis Sequenced Messages to Kafka & Redis Sink");
            Console.WriteLine("");
            
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

            // Print test specification from documentation
            PrintBddScenarioDocumentation(globalSequenceKey, expectedMessages, sinkCounterKey, kafkaTopic);

            if (string.IsNullOrEmpty(redisConnectionStringFull))
            {
                redisConnectionStringFull = ServiceUris.RedisConnectionString;
                Console.WriteLine($"\n⚠ Redis connection string not found. Using default: {redisConnectionStringFull}");
            }
            else
            {
                Console.WriteLine($"\n✅ Redis connection discovered: {redisConnectionStringFull}");
            }

            if (string.IsNullOrEmpty(kafkaBootstrapServersFull))
            {
                kafkaBootstrapServersFull = ServiceUris.KafkaBootstrapServers;
                Console.WriteLine($"⚠ Kafka bootstrap servers not found. Using default: {kafkaBootstrapServersFull}");
            }
            else
            {
                Console.WriteLine($"✅ Kafka bootstrap servers discovered: {kafkaBootstrapServersFull}");
            }

            var verificationStopwatch = Stopwatch.StartNew();

            Console.WriteLine($"\n🔍 === VERIFICATION EXECUTION ===");
            bool allChecksPassed = true;
            
            Console.WriteLine("\n🔴 SCENARIO 1: Redis Sink Verification");
            Console.WriteLine("   📋 Testing: Source sequence generation and sink message counting");
            allChecksPassed &= await VerifyRedisAsync(redisConnectionStringFull, expectedMessages, globalSequenceKey, sinkCounterKey, 1);
            
            Console.WriteLine("\n🟡 SCENARIO 2: Kafka Sink Verification");
            Console.WriteLine("   📋 Testing: Message ordering and content in Kafka topic");
            allChecksPassed &= VerifyKafkaAsync(kafkaBootstrapServersFull, kafkaTopic, expectedMessages);

            allChecksPassed &= ValidatePerformanceRequirements(verificationStopwatch, expectedMessages, config);

            PrintFinalResult(allChecksPassed);
            Console.WriteLine($"📅 Completed at: {DateTime.UtcNow:yyyy-MM-dd HH:mm:ss} UTC");
            
            return allChecksPassed ? 0 : 1;
        }

        private static async Task<bool> VerifyRedisAsync(string connectionString, int expectedMessages, string globalSeqKey, string sinkCounterKey, int attemptNumber)
        {
            Console.WriteLine($"🔗 Connecting to Redis ({connectionString})...");
            ConnectionMultiplexer? redis = null;
            bool redisVerified = true;
            try
            {
                redis = await ConnectionMultiplexer.ConnectAsync(connectionString);
                if (!redis.IsConnected)
                {
                    throw new InvalidOperationException("Failed to connect to Redis.");
                }
                Console.WriteLine("   ✅ Successfully connected to Redis.");
                IDatabase db = redis.GetDatabase();

                async Task<bool> CheckRedisKey(string keyName, string description, string testStep) {
                    Console.WriteLine($"\n   🔍 {testStep}: Checking {description}");
                    Console.WriteLine($"      📌 GIVEN: Redis key '{keyName}' should exist");
                    Console.WriteLine($"      🎯 WHEN: FlinkJobSimulator completed execution");
                    
                    RedisValue value = await db.StringGetAsync(keyName);
                    if (!value.HasValue) {
                        Console.WriteLine($"      ❌ THEN: Key validation FAILED - Redis key '{keyName}' not found");
                        Console.WriteLine($"         💡 This indicates the {description.ToLower()} did not execute or failed to write");
                        return false;
                    }
                    
                    var actualValue = (long)value;
                    Console.WriteLine($"         📊 Key exists with value: {actualValue:N0}");
                    
                    if (actualValue != expectedMessages) {
                        Console.WriteLine($"      ❌ THEN: Value validation FAILED");
                        Console.WriteLine($"         📊 Expected: {expectedMessages:N0} messages");
                        Console.WriteLine($"         📊 Actual: {actualValue:N0} messages");
                        Console.WriteLine($"         📊 Difference: {Math.Abs(actualValue - expectedMessages):N0} messages ({Math.Abs(actualValue - expectedMessages) * 100.0 / expectedMessages:F1}% gap)");
                        
                        if (keyName == globalSeqKey)
                        {
                            Console.WriteLine($"         💡 This indicates HighVolumeSourceFunction stopped early or encountered errors");
                        }
                        else
                        {
                            Console.WriteLine($"         💡 This indicates RedisIncrementSinkFunction processed fewer messages than source generated");
                        }
                        return false;
                    }
                    Console.WriteLine($"      ✅ THEN: Value validation PASSED - Correct value: {actualValue:N0}");
                    return true;
                }

                Console.WriteLine($"\n   📋 Verifying Redis data according to stress test documentation:");
                redisVerified &= await CheckRedisKey(globalSeqKey, "Source Sequence Generation", "TEST 1.1");
                redisVerified &= await CheckRedisKey(sinkCounterKey, "Redis Sink Processing", "TEST 1.2");
                
                if (redisVerified)
                {
                    Console.WriteLine($"\n   🎉 Redis verification result: ✅ **PASSED**");
                    Console.WriteLine($"      ✓ Source generated {expectedMessages:N0} sequential IDs");
                    Console.WriteLine($"      ✓ Redis sink processed {expectedMessages:N0} messages");
                    Console.WriteLine($"      ✓ Perfect 1:1 message flow from source to Redis sink");
                }
                else
                {
                    Console.WriteLine($"\n   💥 Redis verification result: ❌ **FAILED**");
                    Console.WriteLine($"      ❌ Message count mismatch indicates processing pipeline issues");
                }
            }
            catch (Exception ex)
            {
                Console.WriteLine($"\n   💥 Redis verification result: ❌ **FAILED** (Connection Error)");
                Console.WriteLine($"      🔌 Connection attempt {attemptNumber} failed: {ex.Message}");
                Console.WriteLine($"      🔍 Exception type: {ex.GetType().Name}");
                if (ex.InnerException != null)
                {
                    Console.WriteLine($"      🔍 Inner exception: {ex.InnerException.Message}");
                }
                Console.WriteLine($"      💡 This indicates Redis container is not accessible or misconfigured");
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
            Console.WriteLine($"\n   🔗 Connecting to Kafka ({bootstrapServers})...");
            
            // Fix IPv6 issue by forcing IPv4 localhost resolution
            var cleanBootstrapServers = bootstrapServers.Replace("localhost", "127.0.0.1");
            if (cleanBootstrapServers != bootstrapServers)
            {
                Console.WriteLine($"   🔧 Fixed IPv6 issue: Using {cleanBootstrapServers} instead of {bootstrapServers}");
            }
            
            var consumerConfig = CreateKafkaConsumerConfig(cleanBootstrapServers);
            using var consumer = new ConsumerBuilder<Ignore, string>(consumerConfig).Build();
            
            Console.WriteLine($"   📋 GIVEN: Kafka topic '{topic}' should contain ordered messages");
            Console.WriteLine($"   🎯 WHEN: FlinkJobSimulator completed execution via KafkaSinkFunction");
            
            try
            {
                consumer.Subscribe(topic);
                Console.WriteLine($"   ✅ Successfully subscribed to Kafka topic: {topic}");
            }
            catch (Exception ex)
            {
                Console.WriteLine($"   ❌ THEN: Kafka subscription FAILED");
                Console.WriteLine($"      🔌 Could not subscribe to topic '{topic}': {ex.Message}");
                Console.WriteLine($"      💡 This indicates Kafka container is not accessible or topic doesn't exist");
                return false;
            }

            var messagesConsumed = ConsumeKafkaMessages(consumer, expectedMessages);
            bool kafkaVerified = ValidateKafkaResults(messagesConsumed, expectedMessages);
            
            if (kafkaVerified)
            {
                Console.WriteLine($"\n   🎉 Kafka verification result: ✅ **PASSED**");
                Console.WriteLine($"      ✓ Received {messagesConsumed.Count:N0} messages from topic '{topic}'");
                Console.WriteLine($"      ✓ FIFO ordering maintained with Redis sequence IDs");
                Console.WriteLine($"      ✓ Perfect 1:1 message flow from source to Kafka sink");
            }
            else
            {
                Console.WriteLine($"\n   💥 Kafka verification result: ❌ **FAILED**");
                Console.WriteLine($"      ❌ Message consumption or ordering validation failed");
            }
            
            return kafkaVerified;
        }

        private static ConsumerConfig CreateKafkaConsumerConfig(string bootstrapServers)
        {
            return new ConsumerConfig
            {
                BootstrapServers = bootstrapServers, // Already cleaned to use 127.0.0.1 in calling method
                GroupId = $"flinkdotnet-integration-verifier-{Guid.NewGuid()}",
                AutoOffsetReset = AutoOffsetReset.Earliest,
                EnableAutoCommit = false,
                SecurityProtocol = SecurityProtocol.Plaintext, // Explicitly set to plaintext for local testing
                SocketTimeoutMs = 10000, // 10 seconds timeout
                SessionTimeoutMs = 30000 // 30 seconds session timeout
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
                Console.WriteLine($"❌ Kafka consume error: {e.Error.Reason}");
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
                Console.WriteLine($"❌ TIMEOUT: Expected {expectedMessages:N0}, got {messagesConsumed.Count:N0} messages.");
            }
        }

        private static void LogConsumeProgress(int currentCount, int expectedMessages)
        {
            if (currentCount % Math.Max(1, expectedMessages / 10) == 0)
            {
                Console.WriteLine($"Progress: {currentCount:N0}/{expectedMessages:N0} messages ({currentCount * 100.0 / expectedMessages:F1}%)");
            }
        }

        private static bool ValidateKafkaResults(List<string> messagesConsumed, int expectedMessages)
        {
            bool kafkaVerified = true;
            
            Console.WriteLine($"\n      🔍 TEST 2.1: Message Volume Validation");
            Console.WriteLine($"         📌 GIVEN: Expected {expectedMessages:N0} messages in topic");
            Console.WriteLine($"         📊 ACTUAL: Received {messagesConsumed.Count:N0} messages");
            
            if (messagesConsumed.Count < expectedMessages)
            {
                var shortfall = expectedMessages - messagesConsumed.Count;
                var percentage = shortfall * 100.0 / expectedMessages;
                Console.WriteLine($"         ❌ THEN: Volume validation FAILED");
                Console.WriteLine($"            📊 Shortfall: {shortfall:N0} messages ({percentage:F1}% missing)");
                Console.WriteLine($"            💡 This indicates KafkaSinkFunction failed to produce all messages");
                kafkaVerified = false;
            }
            else
            {
                Console.WriteLine($"         ✅ THEN: Volume validation PASSED");
                Console.WriteLine($"            📊 Received sufficient messages: {messagesConsumed.Count:N0}");
                
                Console.WriteLine($"\n      🔍 TEST 2.2: FIFO Ordering Validation");
                Console.WriteLine($"         📌 GIVEN: Messages should be ordered by Redis sequence IDs");
                Console.WriteLine($"         🎯 WHEN: Verifying redis_ordered_id field progression");
                
                bool fifoOrderingPassed = VerifyFIFOOrdering(messagesConsumed);
                if (fifoOrderingPassed)
                {
                    Console.WriteLine($"         ✅ THEN: FIFO ordering validation PASSED");
                    Console.WriteLine($"            📊 All messages properly ordered by Redis sequence");
                    PrintTopAndBottomMessages(messagesConsumed, 3); // Reduced to 3 for less verbose output
                }
                else
                {
                    Console.WriteLine($"         ❌ THEN: FIFO ordering validation FAILED");
                    Console.WriteLine($"            💡 This indicates message order corruption in the pipeline");
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
                        Console.WriteLine($"❌ ERROR: Message at index {i} is not JSON format: {message}");
                        return false;
                    }
                    
                    // Simple JSON parsing to extract redis_ordered_id
                    var redisOrderedIdMatch = Regex.Match(message, @"""redis_ordered_id"":(\d+)");
                    if (!redisOrderedIdMatch.Success)
                    {
                        Console.WriteLine($"❌ ERROR: Could not extract redis_ordered_id from message at index {i}: {message}");
                        return false;
                    }
                    
                    long currentRedisOrderedId = long.Parse(redisOrderedIdMatch.Groups[1].Value);
                    
                    if (hasValidPreviousMessage && currentRedisOrderedId <= previousRedisOrderedId)
                    {
                        Console.WriteLine($"❌ ERROR: FIFO ordering violated at message index {i}.");
                        Console.WriteLine($"  Current redis_ordered_id: {currentRedisOrderedId}, Previous: {previousRedisOrderedId}");
                        Console.WriteLine($"  Current message: {message}");
                        return false;
                    }
                    
                    previousRedisOrderedId = currentRedisOrderedId;
                    hasValidPreviousMessage = true;
                }
                catch (Exception ex)
                {
                    Console.WriteLine($"❌ ERROR: Error parsing message at index {i}: {ex.Message}");
                    Console.WriteLine($"  Message: {messages[i]}");
                    return false;
                }
            }
            
            Console.WriteLine($"✅ FIFO ordering verification passed!");
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
            Console.WriteLine($"WaitForRedisAsync: connectionString='{connectionString}', maxAttempts={maxAttempts}, delaySeconds={delaySeconds}");
            
            for (int i = 0; i < maxAttempts; i++)
            {
                try
                {
                    Console.WriteLine($"Redis attempt {i + 1}/{maxAttempts}: Connecting to {connectionString}");
                    var stopwatch = System.Diagnostics.Stopwatch.StartNew();
                    
                    // Increase connection timeout for CI environments
                    var options = ConfigurationOptions.Parse(connectionString);
                    options.ConnectTimeout = 15000; // 15 seconds instead of default 5 seconds
                    options.SyncTimeout = 15000;    // 15 seconds for operations
                    options.AbortOnConnectFail = false; // Don't abort on first connection failure
                    
                    using var redis = await ConnectionMultiplexer.ConnectAsync(options);
                    stopwatch.Stop();
                    
                    if (redis.IsConnected)
                    {
                        Console.WriteLine($"✅ Redis connection successful in {stopwatch.ElapsedMilliseconds}ms");
                        
                        // Test basic operation
                        var db = redis.GetDatabase();
                        await db.PingAsync();
                        Console.WriteLine("✅ Redis ping successful");
                        return true;
                    }
                    else
                    {
                        Console.WriteLine($"❌ Redis connection established but not connected (took {stopwatch.ElapsedMilliseconds}ms)");
                    }
                }
                catch (Exception ex)
                {
                    Console.WriteLine($"❌ Redis connection failed: {ex.GetType().Name}: {ex.Message}");
                    if (ex.InnerException != null)
                    {
                        Console.WriteLine($"   Inner exception: {ex.InnerException.GetType().Name}: {ex.InnerException.Message}");
                    }
                }
                
                if (i < maxAttempts - 1)
                {
                    Console.WriteLine($"Waiting {delaySeconds} seconds before next Redis attempt...");
                    await Task.Delay(TimeSpan.FromSeconds(delaySeconds));
                }
            }
            
            Console.WriteLine($"❌ Redis connection failed after {maxAttempts} attempts");
            return false;
        }

        private static bool WaitForKafka(string bootstrapServers, int maxAttempts = 2, int delaySeconds = 5)
        {
            Console.WriteLine($"      🔍 Testing Kafka connectivity: bootstrapServers='{bootstrapServers}', maxAttempts={maxAttempts}, delaySeconds={delaySeconds}");
            
            // Fix IPv6 issue by forcing IPv4 localhost resolution
            var cleanBootstrapServers = bootstrapServers.Replace("localhost", "127.0.0.1");
            if (cleanBootstrapServers != bootstrapServers)
            {
                Console.WriteLine($"      🔧 Fixed IPv6 issue: Using {cleanBootstrapServers} instead of {bootstrapServers}");
            }
            
            var adminConfig = new AdminClientConfig 
            { 
                BootstrapServers = cleanBootstrapServers,
                SecurityProtocol = SecurityProtocol.Plaintext, // Explicitly set to plaintext for local testing
                SocketTimeoutMs = 10000, // 10 seconds timeout
                ApiVersionRequestTimeoutMs = 10000 // 10 seconds for API version requests
            };
            
            for (int i = 0; i < maxAttempts; i++)
            {
                try
                {
                    Console.WriteLine($"      ⏳ Kafka attempt {i + 1}/{maxAttempts}: Connecting to {cleanBootstrapServers}");
                    var stopwatch = System.Diagnostics.Stopwatch.StartNew();
                    
                    using var admin = new AdminClientBuilder(adminConfig).Build();
                    var metadata = admin.GetMetadata(TimeSpan.FromSeconds(10));
                    stopwatch.Stop();
                    
                    if (metadata.Topics != null)
                    {
                        Console.WriteLine($"      ✅ Kafka connection successful in {stopwatch.ElapsedMilliseconds}ms");
                        Console.WriteLine($"      📊 Found {metadata.Topics.Count} topics, {metadata.Brokers.Count} brokers");
                        return true;
                    }
                    else
                    {
                        Console.WriteLine($"      ❌ Kafka metadata retrieved but no topics found (took {stopwatch.ElapsedMilliseconds}ms)");
                    }
                }
                catch (Exception ex)
                {
                    Console.WriteLine($"      ❌ Kafka connection failed: {ex.GetType().Name}: {ex.Message}");
                    if (ex.InnerException != null)
                    {
                        Console.WriteLine($"         Inner exception: {ex.InnerException.GetType().Name}: {ex.InnerException.Message}");
                    }
                }
                
                if (i < maxAttempts - 1)
                {
                    Console.WriteLine($"      ⏳ Waiting {delaySeconds} seconds before next Kafka attempt...");
                    Thread.Sleep(TimeSpan.FromSeconds(delaySeconds));
                }
            }
            
            Console.WriteLine($"      ❌ Kafka connection failed after {maxAttempts} attempts");
            return false;
        }
    }
}
