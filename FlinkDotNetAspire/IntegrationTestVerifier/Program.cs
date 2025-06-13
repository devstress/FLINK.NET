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
    using System.IO;
    using Confluent.Kafka;
    using Microsoft.Extensions.Configuration;
    using StackExchange.Redis;

    /// <summary>
    /// System resource monitoring and mathematical analysis for BDD stress testing
    /// </summary>
    public sealed class SystemResourceMonitor : IDisposable
    {
        private readonly Timer _monitoringTimer;
        private readonly List<ResourceSnapshot> _snapshots = new();
        private readonly object _lock = new();
        private readonly Process _currentProcess;
        private bool _disposed;

        public SystemResourceMonitor()
        {
            _currentProcess = Process.GetCurrentProcess();
            _monitoringTimer = new Timer(TakeSnapshot, null, TimeSpan.Zero, TimeSpan.FromMilliseconds(100));
        }

        private void TakeSnapshot(object? state)
        {
            try
            {
                lock (_lock)
                {
                    if (_disposed) return;
                    
                    var snapshot = new ResourceSnapshot
                    {
                        Timestamp = DateTime.UtcNow,
                        TotalRamMB = GetTotalSystemRamMB(),
                        AvailableRamMB = GetAvailableSystemRamMB(),
                        ProcessRamMB = GetProcessRamMB(),
                        CpuCores = Environment.ProcessorCount,
                        ProcessCpuUsagePercent = GetProcessCpuUsage()
                    };
                    
                    _snapshots.Add(snapshot);
                    
                    // Keep only last 1000 snapshots (100 seconds at 100ms intervals)
                    if (_snapshots.Count > 1000)
                    {
                        _snapshots.RemoveAt(0);
                    }
                }
            }
            catch
            {
                // Ignore monitoring errors
            }
        }

        public ResourceAnalysis GetResourceAnalysis(int totalMessages, int taskManagers)
        {
            lock (_lock)
            {
                if (_snapshots.Count == 0)
                    return new ResourceAnalysis();

                var latest = _snapshots[_snapshots.Count - 1];
                
                return new ResourceAnalysis
                {
                    SystemSpec = new SystemSpecification
                    {
                        TotalRamMB = latest.TotalRamMB,
                        AvailableRamMB = latest.AvailableRamMB,
                        CpuCores = latest.CpuCores,
                        TaskManagerInstances = taskManagers
                    },
                    CurrentUsage = new ResourceUsage
                    {
                        ProcessRamMB = latest.ProcessRamMB,
                        RamUtilizationPercent = (double)latest.ProcessRamMB / latest.TotalRamMB * 100,
                        CpuUsagePercent = latest.ProcessCpuUsagePercent
                    },
                    PredictedRequirements = CalculatePredictions(totalMessages, taskManagers, latest),
                    PerformanceMetrics = CalculatePerformanceMetrics()
                };
            }
        }

        private PredictedRequirements CalculatePredictions(int totalMessages, int taskManagers, ResourceSnapshot current)
        {
            // Mathematical analysis based on Flink.NET architecture
            const double MessageSizeBytes = 128; // Estimated average message size
            const double OverheadMultiplier = 3.5; // Memory overhead for serialization, queuing, state
            const double RedisConnectionBytes = 1024 * 1024; // 1MB per Redis connection
            const double KafkaConnectionBytes = 2 * 1024 * 1024; // 2MB per Kafka connection
            
            // Memory calculations
            var messagesInMemoryAtOnce = Math.Min(totalMessages, taskManagers * 1000); // Buffer limit per task
            var dataMemoryMB = (messagesInMemoryAtOnce * MessageSizeBytes * OverheadMultiplier) / (1024 * 1024);
            var connectionMemoryMB = ((RedisConnectionBytes + KafkaConnectionBytes) * taskManagers) / (1024 * 1024);
            var taskManagerOverheadMB = taskManagers * 10; // 10MB per TaskManager instance
            var totalRequiredMemoryMB = dataMemoryMB + connectionMemoryMB + taskManagerOverheadMB + 512; // 512MB base

            // CPU calculations
            var messagesPerCore = (double)totalMessages / current.CpuCores;
            var estimatedProcessingTimeMs = messagesPerCore * 0.001; // 1 microsecond per message per core
            var parallelEfficiency = Math.Min(1.0, (double)taskManagers / current.CpuCores);
            var adjustedProcessingTimeMs = estimatedProcessingTimeMs / parallelEfficiency;

            // Throughput calculations
            var theoreticalThroughputMsgPerSec = current.CpuCores * 1000000; // 1M messages/sec per core theoretical
            var practicalThroughputMsgPerSec = theoreticalThroughputMsgPerSec * 0.3; // 30% efficiency for I/O overhead
            var estimatedCompletionTimeMs = (double)totalMessages / practicalThroughputMsgPerSec * 1000;

            return new PredictedRequirements
            {
                RequiredMemoryMB = totalRequiredMemoryMB,
                MemorySafetyMarginPercent = ((double)current.AvailableRamMB - totalRequiredMemoryMB) / current.AvailableRamMB * 100,
                EstimatedProcessingTimeMs = adjustedProcessingTimeMs,
                OptimalTaskManagerCount = Math.Max(1, Math.Min(taskManagers, current.CpuCores * 2)),
                PredictedThroughputMsgPerSec = practicalThroughputMsgPerSec,
                EstimatedCompletionTimeMs = estimatedCompletionTimeMs,
                MemoryPerMessage = dataMemoryMB / messagesInMemoryAtOnce,
                CpuTimePerMessage = adjustedProcessingTimeMs / totalMessages
            };
        }

        private PerformanceMetrics CalculatePerformanceMetrics()
        {
            if (_snapshots.Count < 2)
                return new PerformanceMetrics();

            var recent = _snapshots.TakeLast(Math.Min(50, _snapshots.Count)).ToList();
            
            return new PerformanceMetrics
            {
                PeakMemoryMB = recent.Max(s => s.ProcessRamMB),
                AverageMemoryMB = recent.Average(s => s.ProcessRamMB),
                PeakCpuPercent = recent.Max(s => s.ProcessCpuUsagePercent),
                AverageCpuPercent = recent.Average(s => s.ProcessCpuUsagePercent),
                MonitoringDurationSec = (recent[recent.Count - 1].Timestamp - recent[0].Timestamp).TotalSeconds
            };
        }

        private static long GetTotalSystemRamMB()
        {
            try
            {
                var memInfo = File.ReadAllLines("/proc/meminfo");
                var totalLine = memInfo.FirstOrDefault(line => line.StartsWith("MemTotal:"));
                if (totalLine != null)
                {
                    var parts = totalLine.Split(new[] { ' ', '\t' }, StringSplitOptions.RemoveEmptyEntries);
                    if (parts.Length >= 2 && long.TryParse(parts[1], out var totalKB))
                    {
                        return totalKB / 1024; // Convert KB to MB
                    }
                }
                return 16384; // Default 16GB
            }
            catch
            {
                return 16384;
            }
        }

        private static long GetAvailableSystemRamMB()
        {
            try
            {
                var memInfo = File.ReadAllLines("/proc/meminfo");
                var availableLine = memInfo.FirstOrDefault(line => line.StartsWith("MemAvailable:"));
                if (availableLine != null)
                {
                    var parts = availableLine.Split(new[] { ' ', '\t' }, StringSplitOptions.RemoveEmptyEntries);
                    if (parts.Length >= 2 && long.TryParse(parts[1], out var availableKB))
                    {
                        return availableKB / 1024; // Convert KB to MB
                    }
                }
                return 14336; // Default ~14GB available
            }
            catch
            {
                return 14336;
            }
        }

        private long GetProcessRamMB()
        {
            try
            {
                _currentProcess.Refresh();
                return _currentProcess.WorkingSet64 / (1024 * 1024);
            }
            catch
            {
                return 0;
            }
        }

        private double GetProcessCpuUsage()
        {
            try
            {
                _currentProcess.Refresh();
                return _currentProcess.TotalProcessorTime.TotalMilliseconds / Environment.TickCount * 100;
            }
            catch
            {
                return 0.0;
            }
        }

        public void Dispose()
        {
            if (!_disposed)
            {
                _disposed = true;
                _monitoringTimer?.Dispose();
                _currentProcess?.Dispose();
                lock (_lock)
                {
                    _snapshots.Clear();
                }
            }
        }
    }

    public class ResourceSnapshot
    {
        public DateTime Timestamp { get; set; }
        public long TotalRamMB { get; set; }
        public long AvailableRamMB { get; set; }
        public long ProcessRamMB { get; set; }
        public int CpuCores { get; set; }
        public double ProcessCpuUsagePercent { get; set; }
    }

    public class ResourceAnalysis
    {
        public SystemSpecification SystemSpec { get; set; } = new();
        public ResourceUsage CurrentUsage { get; set; } = new();
        public PredictedRequirements PredictedRequirements { get; set; } = new();
        public PerformanceMetrics PerformanceMetrics { get; set; } = new();
    }

    public class SystemSpecification
    {
        public long TotalRamMB { get; set; }
        public long AvailableRamMB { get; set; }
        public int CpuCores { get; set; }
        public int TaskManagerInstances { get; set; }
    }

    public class ResourceUsage
    {
        public long ProcessRamMB { get; set; }
        public double RamUtilizationPercent { get; set; }
        public double CpuUsagePercent { get; set; }
    }

    public class PredictedRequirements
    {
        public double RequiredMemoryMB { get; set; }
        public double MemorySafetyMarginPercent { get; set; }
        public double EstimatedProcessingTimeMs { get; set; }
        public int OptimalTaskManagerCount { get; set; }
        public double PredictedThroughputMsgPerSec { get; set; }
        public double EstimatedCompletionTimeMs { get; set; }
        public double MemoryPerMessage { get; set; }
        public double CpuTimePerMessage { get; set; }
    }

    public class PerformanceMetrics
    {
        public long PeakMemoryMB { get; set; }
        public double AverageMemoryMB { get; set; }
        public double PeakCpuPercent { get; set; }
        public double AverageCpuPercent { get; set; }
        public double MonitoringDurationSec { get; set; }
    }

    public class RedisPerformanceMetrics
    {
        public double ReadSpeedOpsPerSec { get; set; }
        public double WriteSpeedOpsPerSec { get; set; }
        public double ReadLatencyMs { get; set; }
        public double WriteLatencyMs { get; set; }
        public int TestOpsCount { get; set; }
        public double TotalTestDurationMs { get; set; }
    }

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

        private static void PrintBddScenarioDocumentation(string globalSequenceKey, int expectedMessages, string sinkCounterKey, string kafkaTopic, ResourceAnalysis analysis)
        {
            Console.WriteLine("📖 GIVEN: Local Flink.NET Setup with Aspire orchestration");
            Console.WriteLine($"   ├─ Redis provides sequence ID generation (key: '{globalSequenceKey}')");
            Console.WriteLine($"   ├─ HighVolumeSourceFunction generates {expectedMessages:N0} ordered messages");
            Console.WriteLine($"   ├─ RedisIncrementSinkFunction counts messages (key: '{sinkCounterKey}')");
            Console.WriteLine($"   └─ KafkaSinkFunction writes messages to topic ('{kafkaTopic}')");
            Console.WriteLine("");
            
            Console.WriteLine("🔧 SYSTEM SPECIFICATIONS & MATHEMATICAL ANALYSIS:");
            Console.WriteLine($"   ├─ 🖥️  Hardware: {analysis.SystemSpec.CpuCores} CPU cores, {analysis.SystemSpec.TotalRamMB:N0}MB total RAM");
            Console.WriteLine($"   ├─ 💾 Available: {analysis.SystemSpec.AvailableRamMB:N0}MB RAM ({(double)analysis.SystemSpec.AvailableRamMB/analysis.SystemSpec.TotalRamMB*100:F1}% of total)");
            Console.WriteLine($"   ├─ ⚡ Parallel: {analysis.SystemSpec.TaskManagerInstances} TaskManager instances");
            Console.WriteLine($"   └─ 🎯 Target: {expectedMessages:N0} messages @ ~{analysis.PredictedRequirements.MemoryPerMessage*1024:F2}KB per message");
            Console.WriteLine("");
            
            Console.WriteLine("📊 MATHEMATICAL PREDICTIONS:");
            Console.WriteLine($"   ├─ 🧮 Memory Required: {analysis.PredictedRequirements.RequiredMemoryMB:F1}MB");
            Console.WriteLine($"   ├─ 🛡️  Safety Margin: {analysis.PredictedRequirements.MemorySafetyMarginPercent:F1}% memory headroom");
            Console.WriteLine($"   ├─ ⏱️  CPU Time/Message: {analysis.PredictedRequirements.CpuTimePerMessage*1000000:F2} microseconds");
            Console.WriteLine($"   ├─ 🚀 Predicted Throughput: {analysis.PredictedRequirements.PredictedThroughputMsgPerSec:N0} messages/second");
            Console.WriteLine($"   ├─ ⏰ Estimated Completion: {analysis.PredictedRequirements.EstimatedCompletionTimeMs:F0}ms");
            Console.WriteLine($"   └─ ✅ Optimal TaskManagers: {analysis.PredictedRequirements.OptimalTaskManagerCount} (current: {analysis.SystemSpec.TaskManagerInstances})");
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
            Console.WriteLine($"   ├─ FIFO ordering maintained with Redis-generated sequence IDs");
            Console.WriteLine($"   ├─ Memory usage stays below {analysis.PredictedRequirements.RequiredMemoryMB:F0}MB threshold");
            Console.WriteLine($"   └─ Processing completes within predicted {analysis.PredictedRequirements.EstimatedCompletionTimeMs:F0}ms timeframe");
        }

        private static bool ValidatePerformanceRequirements(Stopwatch verificationStopwatch, int expectedMessages, IConfigurationRoot config, ResourceAnalysis analysis)
        {
            verificationStopwatch.Stop();
            Console.WriteLine($"\n🚀 SCENARIO 3: Performance & Resource Validation");
            Console.WriteLine($"   📋 Testing: Processing time and resource utilization within acceptable limits");
            
            PrintTimingAnalysis(verificationStopwatch, expectedMessages, analysis);
            
            long maxAllowedTimeMs = GetMaxAllowedTimeMs(config);
            bool timingPassed = ValidateAndPrintCriticalAssertion(verificationStopwatch, expectedMessages, maxAllowedTimeMs);
            
            PrintRedisPerformanceAnalysis();
            
            bool memoryPassed = ValidateAndPrintMemoryAnalysis(analysis);
            bool cpuPassed = ValidateAndPrintCpuAnalysis(analysis);
            bool throughputPassed = ValidateAndPrintThroughputAnalysis(verificationStopwatch, expectedMessages, analysis);
            
            bool allPassed = timingPassed && memoryPassed && cpuPassed && throughputPassed;
            PrintAssessmentResults(timingPassed, memoryPassed, cpuPassed, throughputPassed, allPassed, maxAllowedTimeMs);
            
            return allPassed;
        }

        private static void PrintTimingAnalysis(Stopwatch verificationStopwatch, int expectedMessages, ResourceAnalysis analysis)
        {
            Console.WriteLine($"\n⏰ TIMING ANALYSIS:");
            Console.WriteLine($"   📊 Actual verification time: {verificationStopwatch.ElapsedMilliseconds:N0}ms for {expectedMessages:N0} messages");
            Console.WriteLine($"   🎯 Predicted completion time: {analysis.PredictedRequirements.EstimatedCompletionTimeMs:F0}ms");
            Console.WriteLine($"   📈 Prediction accuracy: {(analysis.PredictedRequirements.EstimatedCompletionTimeMs / verificationStopwatch.ElapsedMilliseconds * 100):F1}% of actual");
            
            var processingTimePerMessageMs = verificationStopwatch.ElapsedMilliseconds / (double)expectedMessages;
            var messagesPerMs = expectedMessages / (double)verificationStopwatch.ElapsedMilliseconds;
            Console.WriteLine($"   🚀 Processing time per message: {processingTimePerMessageMs:F4}ms/msg");
            Console.WriteLine($"   🚀 Processing rate: {messagesPerMs:F2} msg/ms ({messagesPerMs * 1000:F0} msg/sec)");
            
            Console.WriteLine($"\n📊 TOTAL PROCESSING TIME BREAKDOWN:");
            Console.WriteLine($"   ⏱️  Total verification duration: {verificationStopwatch.ElapsedMilliseconds:N0}ms");
            Console.WriteLine($"   ⏱️  Average per message: {processingTimePerMessageMs:F4}ms");
            Console.WriteLine($"   ⏱️  Monitoring duration: {analysis.PerformanceMetrics.MonitoringDurationSec:F1}s");
        }

        private static long GetMaxAllowedTimeMs(IConfigurationRoot config)
        {
            long maxAllowedTimeMs = 1000; // 1 second default
            if (long.TryParse(config["MAX_ALLOWED_TIME_MS"], out long configuredTimeMs))
            {
                maxAllowedTimeMs = configuredTimeMs;
            }
            return maxAllowedTimeMs;
        }

        private static bool ValidateAndPrintCriticalAssertion(Stopwatch verificationStopwatch, int expectedMessages, long maxAllowedTimeMs)
        {
            bool timingPassed = verificationStopwatch.ElapsedMilliseconds <= maxAllowedTimeMs;
            
            Console.WriteLine($"\n🎯 CRITICAL PERFORMANCE ASSERTION:");
            Console.WriteLine($"   📋 REQUIREMENT: Process {expectedMessages:N0} messages in less than {maxAllowedTimeMs:N0}ms (1 second)");
            Console.WriteLine($"   📊 ACTUAL TIME: {verificationStopwatch.ElapsedMilliseconds:N0}ms");
            Console.WriteLine($"   📈 PERFORMANCE: {(timingPassed ? "✅ ASSERTION PASSED" : "❌ ASSERTION FAILED")}");
            
            if (!timingPassed)
            {
                var exceededBy = verificationStopwatch.ElapsedMilliseconds - maxAllowedTimeMs;
                var exceededPercent = (double)exceededBy / maxAllowedTimeMs * 100;
                Console.WriteLine($"   ⚠️  EXCEEDED BY: {exceededBy:N0}ms ({exceededPercent:F1}% over limit)");
            }
            else
            {
                var underBy = maxAllowedTimeMs - verificationStopwatch.ElapsedMilliseconds;
                var underPercent = (double)underBy / maxAllowedTimeMs * 100;
                Console.WriteLine($"   ✅ UNDER LIMIT BY: {underBy:N0}ms ({underPercent:F1}% under limit)");
            }
            
            return timingPassed;
        }

        private static void PrintRedisPerformanceAnalysis()
        {
            if (s_lastRedisPerformance != null)
            {
                Console.WriteLine($"\n🔴 REDIS PERFORMANCE ANALYSIS:");
                Console.WriteLine($"   📊 Read speed from Redis: {s_lastRedisPerformance.ReadSpeedOpsPerSec:N0} ops/sec");
                Console.WriteLine($"   📊 Write speed to Redis: {s_lastRedisPerformance.WriteSpeedOpsPerSec:N0} ops/sec");
                Console.WriteLine($"   📊 Read latency: {s_lastRedisPerformance.ReadLatencyMs:F2}ms avg");
                Console.WriteLine($"   📊 Write latency: {s_lastRedisPerformance.WriteLatencyMs:F2}ms avg");
                Console.WriteLine($"   📊 Performance test duration: {s_lastRedisPerformance.TotalTestDurationMs:F0}ms ({s_lastRedisPerformance.TestOpsCount} ops)");
            }
        }

        private static bool ValidateAndPrintMemoryAnalysis(ResourceAnalysis analysis)
        {
            Console.WriteLine($"\n💾 MEMORY ANALYSIS:");
            Console.WriteLine($"   📊 Peak process memory: {analysis.PerformanceMetrics.PeakMemoryMB:N0}MB");
            Console.WriteLine($"   📊 Average process memory: {analysis.PerformanceMetrics.AverageMemoryMB:F1}MB");
            Console.WriteLine($"   🎯 Predicted requirement: {analysis.PredictedRequirements.RequiredMemoryMB:F1}MB");
            Console.WriteLine($"   🛡️  Safety margin: {analysis.PredictedRequirements.MemorySafetyMarginPercent:F1}% ({(analysis.SystemSpec.AvailableRamMB - analysis.PredictedRequirements.RequiredMemoryMB):F0}MB headroom)");
            Console.WriteLine($"   📈 Memory efficiency: {(analysis.PerformanceMetrics.PeakMemoryMB / analysis.PredictedRequirements.RequiredMemoryMB * 100):F1}% of predicted");
            
            return analysis.PredictedRequirements.MemorySafetyMarginPercent > 10; // Require 10% safety margin
        }

        private static bool ValidateAndPrintCpuAnalysis(ResourceAnalysis analysis)
        {
            Console.WriteLine($"\n⚡ CPU ANALYSIS:");
            Console.WriteLine($"   📊 Peak CPU usage: {analysis.PerformanceMetrics.PeakCpuPercent:F1}%");
            Console.WriteLine($"   📊 Average CPU usage: {analysis.PerformanceMetrics.AverageCpuPercent:F1}%");
            Console.WriteLine($"   🎯 Available cores: {analysis.SystemSpec.CpuCores} ({analysis.SystemSpec.TaskManagerInstances} TaskManagers)");
            Console.WriteLine($"   📈 CPU efficiency: {(analysis.PerformanceMetrics.AverageCpuPercent / (analysis.SystemSpec.CpuCores * 25)):F1}% (target: <25% per core)");
            
            return analysis.PerformanceMetrics.PeakCpuPercent < (analysis.SystemSpec.CpuCores * 80); // Don't exceed 80% per core
        }

        private static bool ValidateAndPrintThroughputAnalysis(Stopwatch verificationStopwatch, int expectedMessages, ResourceAnalysis analysis)
        {
            Console.WriteLine($"\n🚀 THROUGHPUT ANALYSIS:");
            var actualThroughput = expectedMessages / (verificationStopwatch.ElapsedMilliseconds / 1000.0);
            Console.WriteLine($"   📊 Actual throughput: {actualThroughput:N0} messages/second");
            Console.WriteLine($"   🎯 Predicted throughput: {analysis.PredictedRequirements.PredictedThroughputMsgPerSec:N0} messages/second");
            Console.WriteLine($"   📈 Throughput achievement: {(actualThroughput / analysis.PredictedRequirements.PredictedThroughputMsgPerSec * 100):F1}% of predicted");
            
            return actualThroughput >= (analysis.PredictedRequirements.PredictedThroughputMsgPerSec * 0.5); // Achieve at least 50% of predicted
        }

        private static void PrintAssessmentResults(bool timingPassed, bool memoryPassed, bool cpuPassed, bool throughputPassed, bool allPassed, long maxAllowedTimeMs)
        {
            Console.WriteLine($"\n   🎯 ASSESSMENT RESULTS:");
            Console.WriteLine($"      ⏰ Timing: {(timingPassed ? "✅ PASS" : "❌ FAIL")} (≤{maxAllowedTimeMs:N0}ms requirement)");
            Console.WriteLine($"      💾 Memory: {(memoryPassed ? "✅ PASS" : "❌ FAIL")} (≥10% safety margin requirement)");
            Console.WriteLine($"      ⚡ CPU: {(cpuPassed ? "✅ PASS" : "❌ FAIL")} (<80% per core requirement)");
            Console.WriteLine($"      🚀 Throughput: {(throughputPassed ? "✅ PASS" : "❌ FAIL")} (≥50% of predicted requirement)");
            
            if (allPassed)
            {
                Console.WriteLine($"   ✅ THEN: Performance & resource requirements PASSED");
                Console.WriteLine($"      📈 System utilization within optimal bounds");
                Console.WriteLine($"      🎯 Mathematical predictions validated");
            }
            else
            {
                Console.WriteLine($"   ❌ THEN: Performance & resource requirements FAILED");
                Console.WriteLine($"      📈 System performance or resource usage exceeded thresholds");
                if (!timingPassed)
                    Console.WriteLine($"         ⏰ Timing exceeded {maxAllowedTimeMs:N0}ms limit");
                if (!memoryPassed)
                    Console.WriteLine($"         💾 Memory safety margin below 10% threshold");
                if (!cpuPassed)
                    Console.WriteLine($"         ⚡ CPU usage exceeded 80% per core");
                if (!throughputPassed)
                    Console.WriteLine($"         🚀 Throughput below 50% of predicted performance");
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
            
            // Initialize resource monitoring
            using var resourceMonitor = new SystemResourceMonitor();
            
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

            // Wait for initial resource baseline
            await Task.Delay(1000);
            
            // Get initial resource analysis (assuming 20 TaskManager instances as per recent changes)
            var analysis = resourceMonitor.GetResourceAnalysis(expectedMessages, 20);

            // Print test specification from documentation with resource analysis
            PrintBddScenarioDocumentation(globalSequenceKey, expectedMessages, sinkCounterKey, kafkaTopic, analysis);

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

            // Get final resource analysis after test execution
            analysis = resourceMonitor.GetResourceAnalysis(expectedMessages, 20);
            allChecksPassed &= ValidatePerformanceRequirements(verificationStopwatch, expectedMessages, config, analysis);

            PrintFinalResult(allChecksPassed);
            Console.WriteLine($"📅 Completed at: {DateTime.UtcNow:yyyy-MM-dd HH:mm:ss} UTC");
            
            return allChecksPassed ? 0 : 1;
        }

        private static async Task<bool> VerifyRedisAsync(string connectionString, int expectedMessages, string globalSeqKey, string sinkCounterKey, int attemptNumber)
        {
            Console.WriteLine($"🔗 Connecting to Redis ({connectionString})...");
            ConnectionMultiplexer? redis = null;
            try
            {
                redis = await ConnectionMultiplexer.ConnectAsync(connectionString);
                if (!redis.IsConnected)
                {
                    throw new InvalidOperationException("Failed to connect to Redis.");
                }
                Console.WriteLine("   ✅ Successfully connected to Redis.");
                IDatabase db = redis.GetDatabase();

                await CheckJobExecutionError(db);
                bool redisVerified = await PerformRedisValidation(db, expectedMessages, globalSeqKey, sinkCounterKey);
                LogRedisVerificationResults(redisVerified, expectedMessages);
                
                return redisVerified;
            }
            catch (Exception ex)
            {
                LogRedisConnectionError(ex, attemptNumber);
                return false;
            }
            finally
            {
                if (redis != null) {
                    await redis.DisposeAsync();
                }
            }
        }

        private static async Task CheckJobExecutionError(IDatabase db)
        {
            var jobErrorKey = "flinkdotnet:job_execution_error";
            RedisValue jobError = await db.StringGetAsync(jobErrorKey);
            if (jobError.HasValue)
            {
                Console.WriteLine($"\n   🚨 JOB EXECUTION ERROR DETECTED:");
                Console.WriteLine($"      Error: {jobError}");
                Console.WriteLine($"      This explains why sinks are not processing messages.");
                
                // Enhanced diagnostics for different error types
                var errorString = jobError.ToString();
                if (errorString.Contains("Redis", StringComparison.OrdinalIgnoreCase))
                {
                    Console.WriteLine($"      💡 Redis-related error detected - check Redis connectivity and performance");
                }
                else if (errorString.Contains("Timeout", StringComparison.OrdinalIgnoreCase))
                {
                    Console.WriteLine($"      💡 Timeout error detected - job may need more time or resources");
                }
                else if (errorString.Contains("LocalStreamExecutor", StringComparison.OrdinalIgnoreCase))
                {
                    Console.WriteLine($"      💡 LocalStreamExecutor error - check operator chain execution");
                }
                
                Console.WriteLine($"      Clearing error indicator for next test...");
                await db.KeyDeleteAsync(jobErrorKey);
            }
        }

        private static async Task<bool> PerformRedisValidation(IDatabase db, int expectedMessages, string globalSeqKey, string sinkCounterKey)
        {
            Console.WriteLine($"\n   📋 Verifying Redis data according to stress test documentation:");
            
            // Measure Redis performance first
            var redisPerf = await MeasureRedisPerformance(db);
            Console.WriteLine($"\n   ⚡ Redis Performance Measurements:");
            Console.WriteLine($"      📊 Read speed: {redisPerf.ReadSpeedOpsPerSec:N0} ops/sec (avg latency: {redisPerf.ReadLatencyMs:F2}ms)");
            Console.WriteLine($"      📊 Write speed: {redisPerf.WriteSpeedOpsPerSec:N0} ops/sec (avg latency: {redisPerf.WriteLatencyMs:F2}ms)");
            Console.WriteLine($"      📊 Test operations: {redisPerf.TestOpsCount:N0} in {redisPerf.TotalTestDurationMs:F0}ms");

            // Store for later use in performance validation
            s_lastRedisPerformance = redisPerf;
            
            bool redisVerified = true;
            redisVerified &= await CheckRedisKey(db, globalSeqKey, "Source Sequence Generation", "TEST 1.1", expectedMessages);
            redisVerified &= await CheckRedisKey(db, sinkCounterKey, "Redis Sink Processing", "TEST 1.2", expectedMessages);
            return redisVerified;
        }

        private static RedisPerformanceMetrics? s_lastRedisPerformance;

        private static async Task<RedisPerformanceMetrics> MeasureRedisPerformance(IDatabase db)
        {
            const int testOpsCount = 100; // Small test to avoid interference
            const string testKeyPrefix = "perf_test_";
            var testKeys = new List<string>();
            
            var stopwatch = Stopwatch.StartNew();
            
            // Write performance test
            var writeStopwatch = Stopwatch.StartNew();
            for (int i = 0; i < testOpsCount; i++)
            {
                var key = $"{testKeyPrefix}{i}";
                testKeys.Add(key);
                await db.StringSetAsync(key, $"test_value_{i}");
            }
            writeStopwatch.Stop();
            
            // Read performance test
            var readStopwatch = Stopwatch.StartNew();
            for (int i = 0; i < testOpsCount; i++)
            {
                await db.StringGetAsync(testKeys[i]);
            }
            readStopwatch.Stop();
            
            stopwatch.Stop();
            
            // Cleanup test keys
            try
            {
                var redisKeys = testKeys.Select(k => (RedisKey)k).ToArray();
                await db.KeyDeleteAsync(redisKeys);
            }
            catch
            {
                // Ignore cleanup errors
            }
            
            return new RedisPerformanceMetrics
            {
                ReadSpeedOpsPerSec = testOpsCount / (readStopwatch.ElapsedMilliseconds / 1000.0),
                WriteSpeedOpsPerSec = testOpsCount / (writeStopwatch.ElapsedMilliseconds / 1000.0),
                ReadLatencyMs = readStopwatch.ElapsedMilliseconds / (double)testOpsCount,
                WriteLatencyMs = writeStopwatch.ElapsedMilliseconds / (double)testOpsCount,
                TestOpsCount = testOpsCount,
                TotalTestDurationMs = stopwatch.ElapsedMilliseconds
            };
        }

        private static async Task<bool> CheckRedisKey(IDatabase db, string keyName, string description, string testStep, int expectedMessages)
        {
            Console.WriteLine($"\n   🔍 {testStep}: Checking {description}");
            Console.WriteLine($"      📌 GIVEN: Redis key '{keyName}' should exist");
            Console.WriteLine($"      🎯 WHEN: FlinkJobSimulator completed execution");
            
            RedisValue value = await db.StringGetAsync(keyName);
            if (!value.HasValue) {
                Console.WriteLine($"      ❌ THEN: Key validation FAILED - Redis key '{keyName}' not found");
                Console.WriteLine($"         💡 This indicates the {description.ToLower()} did not execute or failed to write");
                
                // Enhanced diagnostics for missing keys
                await ProvideEnhancedDiagnostics(db, description);
                return false;
            }
            
            var actualValue = (long)value;
            Console.WriteLine($"         📊 Key exists with value: {actualValue:N0}");
            
            if (actualValue != expectedMessages) {
                LogValueValidationFailure(actualValue, expectedMessages, keyName);
                
                // Enhanced diagnostics for value mismatches
                await ProvideValueMismatchDiagnostics(db, actualValue, expectedMessages, description);
                return false;
            }
            Console.WriteLine($"      ✅ THEN: Value validation PASSED - Correct value: {actualValue:N0}");
            return true;
        }

        private static async Task ProvideEnhancedDiagnostics(IDatabase db, string description)
        {
            Console.WriteLine($"\n      🔍 ENHANCED DIAGNOSTICS for missing {description}:");
            
            // Check if there are any job execution errors
            var jobErrorKey = "flinkdotnet:job_execution_error";
            RedisValue jobError = await db.StringGetAsync(jobErrorKey);
            if (jobError.HasValue)
            {
                Console.WriteLine($"         🚨 JOB EXECUTION ERROR FOUND: {jobError}");
                Console.WriteLine($"         ⚠️  This explains why {description.ToLower()} failed");
            }
            
            // Check for partial execution (look for related keys)
            try
            {
                var server = db.Multiplexer.GetServer(db.Multiplexer.GetEndPoints()[0]);
                var allKeys = server.Keys(pattern: "flinkdotnet*").Select(k => k.ToString()).ToList();
                
                Console.WriteLine($"         📋 Found {allKeys.Count} Redis keys with 'flinkdotnet' prefix:");
                foreach (var key in allKeys.Take(10)) // Show first 10 keys
                {
                    var val = await db.StringGetAsync(key);
                    Console.WriteLine($"           - {key}: {val}");
                }
                if (allKeys.Count > 10)
                {
                    Console.WriteLine($"           ... and {allKeys.Count - 10} more keys");
                }
                
                // 🔍 ROOT CAUSE ANALYSIS
                Console.WriteLine($"\n         🔍 ROOT CAUSE ANALYSIS:");
                if (allKeys.Count == 0)
                {
                    Console.WriteLine($"           🚨 CRITICAL: No FlinkDotNet keys found - Job never started or Redis connection failed");
                    Console.WriteLine($"           💡 SUGGESTION: Check AppHost startup logs, verify Redis container is running");
                }
                else if (allKeys.Any(k => k.Contains("global_sequence_id")))
                {
                    var seqKey = allKeys.First(k => k.Contains("global_sequence_id"));
                    var seqVal = await db.StringGetAsync(seqKey);
                    Console.WriteLine($"           📊 Source function generated {seqVal} messages but stopped early");
                    Console.WriteLine($"           💡 SUGGESTION: Check for LocalStreamExecutor timeout, source function errors, or resource exhaustion");
                }
                else
                {
                    Console.WriteLine($"           🚨 CRITICAL: Source function never initialized - sequence generation failed");
                    Console.WriteLine($"           💡 SUGGESTION: Check source function startup, Redis connectivity in source");
                }
                
                // Check for sink processing indicators
                if (description.Contains("Sink", StringComparison.OrdinalIgnoreCase) && 
                    !allKeys.Any(k => k.Contains("processed_message_counter")))
                {
                    Console.WriteLine($"           🚨 SINK ISSUE: No sink counter key found - RedisIncrementSinkFunction never executed");
                    Console.WriteLine($"           💡 SUGGESTION: Check sink function registration, Redis sink connectivity");
                }
            }
            catch (Exception ex)
            {
                Console.WriteLine($"         ⚠️  Could not enumerate keys: {ex.Message}");
                Console.WriteLine($"         💡 This may indicate Redis connectivity issues during diagnostics");
            }
        }

        private static async Task ProvideValueMismatchDiagnostics(IDatabase db, long actualValue, int expectedMessages, string description)
        {
            Console.WriteLine($"\n      🔍 VALUE MISMATCH DIAGNOSTICS:");
            Console.WriteLine($"         📊 Gap Analysis: {expectedMessages - actualValue:N0} messages missing ({(double)(expectedMessages - actualValue) / expectedMessages * 100:F1}% failure rate)");
            
            if (description.Contains("Source", StringComparison.OrdinalIgnoreCase))
            {
                Console.WriteLine($"         💡 Source Function Insights:");
                Console.WriteLine($"            - Source stopped at {actualValue:N0}/{expectedMessages:N0} messages");
                Console.WriteLine($"            - This suggests LocalStreamExecutor timeout or error in source execution");
                Console.WriteLine($"            - Check AppHost logs for source function error messages");
                
                // 🔍 SOURCE-SPECIFIC DIAGNOSTICS
                Console.WriteLine($"\n         🔍 SOURCE-SPECIFIC DIAGNOSTICS:");
                if (actualValue == 0)
                {
                    Console.WriteLine($"            🚨 CRITICAL: Source never generated any messages");
                    Console.WriteLine($"            💡 LIKELY CAUSES: Redis connection failure, source function not registered, job execution error");
                }
                else if (actualValue < expectedMessages * 0.1)
                {
                    Console.WriteLine($"            ⚠️  Source failed very early (<10% completion)");
                    Console.WriteLine($"            💡 LIKELY CAUSES: Source initialization error, immediate timeout, resource exhaustion");
                }
                else if (actualValue < expectedMessages * 0.5)
                {
                    Console.WriteLine($"            ⚠️  Source failed mid-execution (<50% completion)");
                    Console.WriteLine($"            💡 LIKELY CAUSES: Redis connection timeout, memory issues, LocalStreamExecutor timeout");
                }
                else
                {
                    Console.WriteLine($"            ✅ Source made good progress (>{actualValue * 100.0 / expectedMessages:F1}% completion)");
                    Console.WriteLine($"            💡 LIKELY CAUSES: Controlled shutdown, late-stage timeout, resource constraints");
                }
            }
            else if (description.Contains("Sink", StringComparison.OrdinalIgnoreCase))
            {
                Console.WriteLine($"         💡 Sink Function Insights:");
                Console.WriteLine($"            - Sink processed {actualValue:N0}/{expectedMessages:N0} messages");
                Console.WriteLine($"            - Check if source generated full volume or if sink processing failed");
                
                // Check source volume
                var sourceKey = "flinkdotnet:global_sequence_id";
                var sourceValue = await db.StringGetAsync(sourceKey);
                if (sourceValue.HasValue)
                {
                    var sourceCount = (long)sourceValue;
                    Console.WriteLine($"            - Source generated {sourceValue} messages vs sink processed {actualValue}");
                    if (sourceCount > actualValue)
                    {
                        var dataLoss = sourceCount - actualValue;
                        var dataLossPercent = (double)dataLoss / sourceCount * 100;
                        Console.WriteLine($"            - ⚠️  Data loss: Sink missed {dataLoss} messages ({dataLossPercent:F1}% loss rate)");
                        
                        // 🔍 SINK-SPECIFIC DIAGNOSTICS
                        Console.WriteLine($"\n         🔍 SINK-SPECIFIC DIAGNOSTICS:");
                        if (actualValue == 0)
                        {
                            Console.WriteLine($"            🚨 CRITICAL: Sink never processed any messages despite source generating {sourceCount}");
                            Console.WriteLine($"            💡 LIKELY CAUSES: Sink function not registered, sink Redis connection failure, sink execution error");
                        }
                        else if (dataLossPercent > 50)
                        {
                            Console.WriteLine($"            ⚠️  High data loss rate (>{dataLossPercent:F1}%)");
                            Console.WriteLine($"            💡 LIKELY CAUSES: Sink connection instability, processing exceptions, sink timeout");
                        }
                        else
                        {
                            Console.WriteLine($"            ⚠️  Moderate data loss ({dataLossPercent:F1}%)");
                            Console.WriteLine($"            💡 LIKELY CAUSES: Processing backpressure, occasional failures, late shutdown");
                        }
                    }
                    else if (sourceCount == actualValue)
                    {
                        Console.WriteLine($"            ✅ Perfect source-to-sink ratio - data flow is working correctly");
                        Console.WriteLine($"            💡 Issue is likely in source generation capacity, not sink processing");
                    }
                }
                else
                {
                    Console.WriteLine($"            🚨 CRITICAL: Cannot compare with source - source key not found");
                    Console.WriteLine($"            💡 Both source and sink may have failed completely");
                }
            }
        }

        private static void LogValueValidationFailure(long actualValue, int expectedMessages, string keyName)
        {
            Console.WriteLine($"      ❌ THEN: Value validation FAILED");
            Console.WriteLine($"         📊 Expected: {expectedMessages:N0} messages");
            Console.WriteLine($"         📊 Actual: {actualValue:N0} messages");
            Console.WriteLine($"         📊 Difference: {Math.Abs(actualValue - expectedMessages):N0} messages ({Math.Abs(actualValue - expectedMessages) * 100.0 / expectedMessages:F1}% gap)");
            
            if (keyName.Contains("global_sequence"))
            {
                Console.WriteLine($"         💡 This indicates HighVolumeSourceFunction stopped early or encountered errors");
            }
            else
            {
                Console.WriteLine($"         💡 This indicates RedisIncrementSinkFunction processed fewer messages than source generated");
            }
        }

        private static void LogRedisVerificationResults(bool redisVerified, int expectedMessages)
        {
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

        private static void LogRedisConnectionError(Exception ex, int attemptNumber)
        {
            Console.WriteLine($"\n   💥 Redis verification result: ❌ **FAILED** (Connection Error)");
            Console.WriteLine($"      🔌 Connection attempt {attemptNumber} failed: {ex.Message}");
            Console.WriteLine($"      🔍 Exception type: {ex.GetType().Name}");
            if (ex.InnerException != null)
            {
                Console.WriteLine($"      🔍 Inner exception: {ex.InnerException.Message}");
            }
            Console.WriteLine($"      💡 This indicates Redis container is not accessible or misconfigured");
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
                
                // 🔍 KAFKA-SPECIFIC DIAGNOSTICS
                Console.WriteLine($"\n      🔍 KAFKA SUBSCRIPTION DIAGNOSTICS:");
                Console.WriteLine($"         📊 Bootstrap servers: {cleanBootstrapServers}");
                Console.WriteLine($"         📊 Topic name: {topic}");
                Console.WriteLine($"         📊 Exception type: {ex.GetType().Name}");
                
                if (ex.Message.Contains("UnknownTopicOrPart"))
                {
                    Console.WriteLine($"         🚨 ROOT CAUSE: Topic '{topic}' does not exist on Kafka broker");
                    Console.WriteLine($"         💡 LIKELY CAUSES:");
                    Console.WriteLine($"            - KafkaSinkFunction failed to create topic during job execution");
                    Console.WriteLine($"            - Kafka auto-topic creation is disabled");
                    Console.WriteLine($"            - Topic name mismatch between producer and consumer");
                    Console.WriteLine($"         💡 SUGGESTIONS:");
                    Console.WriteLine($"            - Check AppHost logs for KafkaSinkFunction errors");
                    Console.WriteLine($"            - Verify Kafka container is running and topic creation succeeded");
                    Console.WriteLine($"            - Check if job execution completed successfully");
                }
                else if (ex.Message.Contains("timeout") || ex.Message.Contains("connect"))
                {
                    Console.WriteLine($"         🚨 ROOT CAUSE: Cannot connect to Kafka broker at {cleanBootstrapServers}");
                    Console.WriteLine($"         💡 SUGGESTIONS:");
                    Console.WriteLine($"            - Verify Kafka container is running on the expected port");
                    Console.WriteLine($"            - Check docker port mapping for Kafka service");
                    Console.WriteLine($"            - Ensure no firewall blocking localhost connections");
                }
                else
                {
                    Console.WriteLine($"         🚨 ROOT CAUSE: Unexpected Kafka subscription error");
                    Console.WriteLine($"         💡 SUGGESTION: Check Kafka broker logs and consumer configuration");
                }
                
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
                
                // 🔍 CONSUMPTION ERROR DIAGNOSTICS
                Console.WriteLine($"\n🔍 KAFKA CONSUMPTION ERROR DIAGNOSTICS:");
                if (e.Error.Code == ErrorCode.UnknownTopicOrPart)
                {
                    Console.WriteLine($"   🚨 ROOT CAUSE: Topic does not exist or partition not available");
                    Console.WriteLine($"   💡 LIKELY CAUSES:");
                    Console.WriteLine($"      - KafkaSinkFunction failed to create topic during job execution");
                    Console.WriteLine($"      - Topic was created but not yet available for consumption");
                    Console.WriteLine($"      - Producer hasn't written any messages to topic yet");
                }
                else if (e.Error.Code == ErrorCode.BrokerNotAvailable)
                {
                    Console.WriteLine($"   🚨 ROOT CAUSE: Kafka broker is not available or unreachable");
                    Console.WriteLine($"   💡 SUGGESTION: Check Kafka container status and network connectivity");
                }
                else
                {
                    Console.WriteLine($"   🚨 ROOT CAUSE: Unexpected Kafka consumption error ({e.Error.Code})");
                    Console.WriteLine($"   💡 SUGGESTION: Check Kafka broker logs and consumer permissions");
                }
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
                
                // 🔍 TIMEOUT DIAGNOSTICS
                Console.WriteLine($"\n🔍 KAFKA TIMEOUT DIAGNOSTICS:");
                if (messagesConsumed.Count == 0)
                {
                    Console.WriteLine($"   🚨 ROOT CAUSE: No messages received at all within {consumeTimeout.TotalSeconds}s timeout");
                    Console.WriteLine($"   💡 LIKELY CAUSES:");
                    Console.WriteLine($"      - KafkaSinkFunction never produced messages to topic");
                    Console.WriteLine($"      - Topic exists but is empty (producer failed)");
                    Console.WriteLine($"      - Wrong topic name between producer and consumer");
                    Console.WriteLine($"      - Kafka consumer offset configuration issue");
                }
                else
                {
                    var receivedPercent = messagesConsumed.Count * 100.0 / expectedMessages;
                    Console.WriteLine($"   ⚠️  PARTIAL SUCCESS: Received {messagesConsumed.Count:N0}/{expectedMessages:N0} messages ({receivedPercent:F1}%)");
                    Console.WriteLine($"   💡 LIKELY CAUSES:");
                    if (receivedPercent < 10)
                    {
                        Console.WriteLine($"      - Producer failed early in message generation");
                        Console.WriteLine($"      - Source function stopped generating messages");
                    }
                    else if (receivedPercent < 50)
                    {
                        Console.WriteLine($"      - Producer stopped mid-execution (timeout, error, resource limit)");
                    }
                    else
                    {
                        Console.WriteLine($"      - Producer nearly completed but stopped short");
                        Console.WriteLine($"      - May just need longer consumption timeout");
                    }
                }
                Console.WriteLine($"   💡 SUGGESTIONS:");
                Console.WriteLine($"      - Check FlinkJobSimulator execution logs for errors");
                Console.WriteLine($"      - Verify Redis sequence generation completed");
                Console.WriteLine($"      - Check AppHost for KafkaSinkFunction error messages");
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
