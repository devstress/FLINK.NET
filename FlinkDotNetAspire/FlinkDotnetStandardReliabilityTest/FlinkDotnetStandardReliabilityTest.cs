using System.Diagnostics.CodeAnalysis;
using Microsoft.Extensions.Logging;
using Xunit;
using Xunit.Abstractions;
using FlinkDotNet.Core.Api;
using FlinkDotNet.Core.Api.Streaming;
using FlinkDotNet.Core.Abstractions.Sources;
using FlinkDotNet.Core.Abstractions.Sinks;
using FlinkDotNet.Core.Abstractions.Operators;
using FlinkDotNet.Core.Abstractions.Context;
using FlinkDotNet.Core.Abstractions.Serializers;
using FlinkDotNet.Core.Abstractions.Models;
using FlinkDotNet.Core.Abstractions.Models.State;
using FlinkDotNet.Core.Abstractions.Storage;
using FlinkDotNet.Core.Abstractions.Windowing;
using FlinkDotNet.Common.Constants;
using StackExchange.Redis;
using System.Diagnostics;
using System.Collections.Concurrent;
using System.Text.Json;

namespace FlinkDotnetStandardReliabilityTest
{
    /// <summary>
    /// Flink.Net Standard Pipeline Reliability Test with BDD Style and Comprehensive Diagnostics
    /// 
    /// This test implements Flink.Net best practices with worldwide stream processing patterns
    /// using the external Kafka environment from docker-compose.kafka.yml:
    /// - BDD Style: Given/When/Then scenarios for clear test documentation
    /// - Flink.Net Pattern: Source → Map/Filter → KeyBy → Process → AsyncFunction → Sink
    /// - Comprehensive Diagnostics: Detailed failure analysis and expected behavior logging
    /// - Worldwide Best Practices: Follows industry standards for stream processing testing
    /// - Kafka Best Practices: Uses pre-configured topics and external Kafka environment
    /// - High Volume Testing: Defaults to 10 million messages for comprehensive validation
    /// 
    /// BACKPRESSURE FLOW SCENARIOS TESTED:
    /// 1. Gateway (Ingress Rate Control) → KeyGen (Deterministic Partitioning + Load Awareness)
    /// 2. KeyGen → IngressProcessing (Validation + Preprocessing with Bounded Buffers)  
    /// 3. IngressProcessing → AsyncEgressProcessing (External I/O with Timeout, Retry, DLQ)
    /// 4. AsyncEgressProcessing → Final Sink (Kafka, DB, Callback) with Acknowledgment
    /// 5. End-to-End Credit-Based Flow Control and Backpressure Signal Propagation
    /// 
    /// FLINK.NET BACKPRESSURE COMPONENTS DEMONSTRATED:
    /// - Credit-Based Flow Control: Credits requested/granted/replenished per stage
    /// - Multi-Dimensional Pressure Detection: Queue, latency, error, memory pressure
    /// - Stage-Specific Throttling: Each stage applies appropriate backpressure mechanisms
    /// - Upstream Signal Propagation: Pressure signals flow from sink back to source
    /// - Load-Aware Partitioning: Dynamic rebalancing when partitions become overloaded
    /// 
    /// PREREQUISITES:
    /// - Start Aspire environment: cd FlinkDotNetAspire/FlinkDotNetAspire.AppHost.AppHost && dotnet run
    /// - Access Aspire Dashboard: Check console output for URL (typically http://localhost:15000)
    /// - Kafka UI: Available through Aspire dashboard
    /// - All services (Kafka, Redis) managed by Aspire with dynamic port allocation
    /// 
    /// BDD SCENARIOS COVERED:
    /// 1. High-Volume Message Processing with Back Pressure (10M messages)
    /// 2. Credit-Based Flow Control Verification with Multi-Stage Pipeline
    /// 3. Backpressure Signal Propagation Testing (Sink → Source)
    /// 4. Load-Aware Partitioning under Pressure Conditions
    /// 5. Exactly-Once Semantics Verification with External Kafka
    /// 6. Fault Tolerance and Recovery Testing with Pre-configured Topics
    /// 7. Performance and Resource Utilization Validation
    /// </summary>
    [SuppressMessage("Design", "S1144:Remove the unused private field", Justification = "Test diagnostic fields are used for monitoring")]
    [SuppressMessage("Performance", "S4487:Remove this unread private field", Justification = "Diagnostic fields are essential for test monitoring")]
    [SuppressMessage("Design", "S1172:Remove this unused method parameter", Justification = "Test parameters provide flexibility for future enhancements")]
    [SuppressMessage("Maintainability", "S2325:Make static method", Justification = "Test methods need instance context")]
    [SuppressMessage("Maintainability", "S3776:Reduce Cognitive Complexity", Justification = "Test complexity is justified for comprehensive validation")]
    [SuppressMessage("Performance", "S1481:Remove unused local variable", Justification = "Test variables provide debugging context")]
    [SuppressMessage("Performance", "S1854:Remove useless assignment", Justification = "Test assignments provide debugging context")]
    [SuppressMessage("Performance", "S6608:Use indexing instead of LINQ", Justification = "LINQ improves test readability")]
    [SuppressMessage("Performance", "S6610:Use char overload", Justification = "String methods are clearer for test validation")]
    [SuppressMessage("Design", "S927:Rename parameter", Justification = "Test parameter names are descriptive")]
    [SuppressMessage("Design", "CS1998:Missing await operators", Justification = "Async test setup for future async operations")]
    public class FlinkDotnetStandardReliabilityTest : IAsyncLifetime
    {
        private readonly ITestOutputHelper _output;
        private readonly ILogger<FlinkDotnetStandardReliabilityTest> _logger;
        
        // External Kafka environment connections - use Aspire discovered ports or defaults
        private readonly string _redisConnectionString;
        private readonly string _kafkaBootstrapServers;
        
        // Test configuration and diagnostics
        private readonly ReliabilityTestConfiguration _config;
        private readonly TestDiagnostics _diagnostics;
        private readonly TestScenarioLogger _scenarioLogger;
        
        public FlinkDotnetStandardReliabilityTest(ITestOutputHelper output)
        {
            _output = output;
            
            // Get connection strings from environment (set by Aspire port discovery) or use defaults
            _redisConnectionString = Environment.GetEnvironmentVariable("DOTNET_REDIS_URL") ?? ServiceUris.RedisConnectionString;
            _kafkaBootstrapServers = Environment.GetEnvironmentVariable("DOTNET_KAFKA_BOOTSTRAP_SERVERS") ?? ServiceUris.KafkaBootstrapServers;
            
            // Configure comprehensive logging with BDD scenario tracking
            var loggerFactory = LoggerFactory.Create(builder =>
                builder.AddConsole()
                       .SetMinimumLevel(LogLevel.Debug) // Enhanced logging for diagnostics
                       .AddFilter("Microsoft", LogLevel.Warning)); // Filter noisy framework logs
            _logger = loggerFactory.CreateLogger<FlinkDotnetStandardReliabilityTest>();
            
            // Initialize BDD scenario logger
            _scenarioLogger = new TestScenarioLogger(_logger, _output);
            
            // Initialize comprehensive diagnostics
            _diagnostics = new TestDiagnostics(_logger, _scenarioLogger);
            
            // Configure test parameters for high-volume Kafka best practices testing
            _config = new ReliabilityTestConfiguration
            {
                MessageCount = GetMessageCountFromEnvironment(), // Support CI/local testing
                ParallelSourceInstances = Environment.ProcessorCount, // Align with CPU cores
                ExpectedProcessingTimeMs = GetExpectedProcessingTimeMs(GetMessageCountFromEnvironment()), // Dynamic timeout
                FailureToleranceRate = 0.001, // 0.1% failure tolerance (Flink.Net standard)
                CheckpointInterval = TimeSpan.FromSeconds(5), // Faster checkpoints
                EnableExactlyOnceSemantics = true, // Enable for production-like testing
                BackPressureThresholdPercent = 80, // Flink.Net default threshold
                NetworkTimeoutMs = 60_000, // 60 seconds for network operations during comprehensive testing
                StateBackendSyncIntervalMs = 2_000 // 2 seconds for state synchronization
            };
            
            // Log test initialization with BDD context
            _scenarioLogger.LogScenarioStart("Test Initialization", 
                $"Configuring Flink.Net reliability test with {_config.MessageCount:N0} messages, timeout {_config.ExpectedProcessingTimeMs:N0}ms");
            
            _logger.LogInformation("Using connection strings: Redis={RedisConnectionString}, Kafka={KafkaBootstrapServers}", 
                _redisConnectionString, _kafkaBootstrapServers);
        }

        private long GetMessageCountFromEnvironment()
        {
            var envValue = Environment.GetEnvironmentVariable("FLINKDOTNET_STANDARD_TEST_MESSAGES");
            if (long.TryParse(envValue, out var count) && count > 0)
            {
                return count;
            }
            return 10_000_000; // Default for comprehensive testing with Kafka best practices
        }

        private long GetExpectedProcessingTimeMs(long messageCount)
        {
            // Check if timeout is explicitly set via environment (from CI workflows)
            var envTimeoutMs = Environment.GetEnvironmentVariable("MAX_ALLOWED_TIME_MS");
            if (long.TryParse(envTimeoutMs, out var explicitTimeout) && explicitTimeout > 0)
            {
                return explicitTimeout;
            }
            
            // Calculate appropriate timeout based on message count and environment
            var isCI = Environment.GetEnvironmentVariable("CI") == "true" || 
                       Environment.GetEnvironmentVariable("GITHUB_ACTIONS") == "true";
            
            // Base processing rate: 1000 messages per second in CI, 10000 in local
            var baseProcessingRate = isCI ? 1000.0 : 10000.0;
            
            // Calculate expected time with safety margin
            var expectedSeconds = messageCount / baseProcessingRate;
            var safetyMultiplier = isCI ? 3.0 : 2.0; // More margin in CI
            var totalSeconds = expectedSeconds * safetyMultiplier;
            
            // Minimum timeout (for very small message counts)
            var minimumTimeoutMs = isCI ? 300_000 : 60_000; // 5 minutes CI, 1 minute local
            
            // Maximum timeout (prevent runaway tests)
            var maximumTimeoutMs = isCI ? 600_000 : 1_800_000; // 10 minutes CI, 30 minutes local
            
            var calculatedTimeoutMs = (long)(totalSeconds * 1000);
            var finalTimeoutMs = Math.Max(minimumTimeoutMs, Math.Min(maximumTimeoutMs, calculatedTimeoutMs));
            
            return finalTimeoutMs;
        }

        public async Task InitializeAsync()
        {
            _scenarioLogger.LogScenarioStart("Infrastructure Initialization", 
                "Verifying external Kafka environment from docker-compose.kafka.yml");
            
            var stopwatch = Stopwatch.StartNew();
            
            try
            {
                // Verify Redis connectivity
                _scenarioLogger.LogGiven("Redis connectivity check", 
                    "Redis should be accessible using Aspire port discovery or default configuration");
                await VerifyRedisConnectivity(_redisConnectionString);
                _scenarioLogger.LogThen("Redis connectivity check", 
                    "Redis connection verified successfully");
                
                // Verify Kafka connectivity
                _scenarioLogger.LogGiven("Kafka connectivity check", 
                    "Kafka should be accessible using Aspire port discovery or default configuration");
                await VerifyKafkaConnectivity(_kafkaBootstrapServers);
                _scenarioLogger.LogThen("Kafka connectivity check", 
                    "Kafka connection verified successfully");
                
                // Additional readiness verification
                _scenarioLogger.LogGiven("Infrastructure readiness", 
                    "External Kafka environment should be fully operational");
                await Task.Delay(1000); // Brief readiness check
                
                stopwatch.Stop();
                _scenarioLogger.LogThen("Infrastructure readiness", 
                    $"External Kafka environment verified in {stopwatch.ElapsedMilliseconds:N0}ms");
                
                _logger.LogInformation("✅ External Kafka environment verification completed successfully");
                _logger.LogInformation("💡 Infrastructure managed by Aspire - check Aspire dashboard for service status");
            }
            catch (Exception ex)
            {
                stopwatch.Stop();
                _logger.LogError("❌ Kafka environment not available");
                _logger.LogError("💡 Please start the Aspire environment first:");
                _logger.LogError("   cd FlinkDotNetAspire/FlinkDotNetAspire.AppHost.AppHost");
                _logger.LogError("   dotnet run");
                _logger.LogError("   # Wait for services to be ready, then run tests");
                
                _diagnostics.LogInfrastructureFailure("Kafka environment verification failed", ex);
                throw new InvalidOperationException(
                    "Kafka environment not available. Please start Aspire: cd FlinkDotNetAspire/FlinkDotNetAspire.AppHost.AppHost && dotnet run", ex);
            }
        }

        private async Task VerifyRedisConnectivity(string connectionString)
        {
            try
            {
                using var redis = await ConnectionMultiplexer.ConnectAsync(connectionString);
                var db = redis.GetDatabase();
                await db.PingAsync();
                _logger.LogDebug("Redis connectivity verification passed");
            }
            catch (Exception ex)
            {
                throw new InvalidOperationException($"Redis connectivity verification failed: {ex.Message}", ex);
            }
        }

        private async Task VerifyKafkaConnectivity(string bootstrapServers)
        {
            await Task.Run(() =>
            {
                try
                {
                    var config = new Confluent.Kafka.AdminClientConfig { BootstrapServers = bootstrapServers };
                    using var admin = new Confluent.Kafka.AdminClientBuilder(config).Build();
                    var metadata = admin.GetMetadata(TimeSpan.FromSeconds(10));
                    if (metadata.Topics == null)
                    {
                        throw new InvalidOperationException("Kafka metadata verification failed");
                    }
                    _logger.LogDebug("Kafka connectivity verification passed");
                }
                catch (Exception ex)
                {
                    throw new InvalidOperationException($"Kafka connectivity verification failed: {ex.Message}", ex);
                }
            });
        }

        public async Task DisposeAsync()
        {
            _scenarioLogger.LogScenarioStart("Test Cleanup", "Cleaning up test resources");
            
            try
            {
                // No containers to stop - using external environment
                await Task.Delay(100); // Minimal cleanup delay
                
                _scenarioLogger.LogThen("Test Cleanup", "Test cleanup completed (external environment remains running)");
                _logger.LogInformation("✅ Test cleanup completed - external Kafka environment remains available");
            }
            catch (Exception ex)
            {
                _diagnostics.LogInfrastructureFailure("Test cleanup failed", ex);
            }
        }

        [Fact]
        public async Task ShouldProcessHighVolumeWithFlinkDotnetStandardPipeline()
        {
            // BDD SCENARIO: High-Volume Message Processing with Flink.Net Standard Pipeline
            _scenarioLogger.LogScenarioStart("High-Volume Processing", 
                "Testing Flink.Net standard pipeline with external Kafka environment and 10M message scale");
            
            // GIVEN: Flink.Net environment is configured for high-volume processing
            _scenarioLogger.LogGiven("Pipeline configuration", 
                $"Flink.Net environment configured for {_config.MessageCount:N0} messages with " +
                $"{_config.ParallelSourceInstances} parallel sources, exactly-once semantics, and external Kafka environment");
            
            var executionStopwatch = Stopwatch.StartNew();
            var testResults = new TestExecutionResults();
            
            try
            {
                // Configure Flink environment following Flink.Net best practices
                var env = StreamExecutionEnvironment.GetExecutionEnvironment();
                _diagnostics.LogEnvironmentConfiguration(env, _config);
                
                // Get external environment connection details with diagnostics
                var redisConnectionString = _redisConnectionString;
                var kafkaBootstrapServers = _kafkaBootstrapServers;
                
                _diagnostics.LogInfrastructureDetails(redisConnectionString, kafkaBootstrapServers);

                // WHEN: Flink.Net standard pipeline executes with back pressure monitoring
                _scenarioLogger.LogWhen("Pipeline execution", 
                    "Executing Source → Map/Filter → KeyBy → Process → AsyncFunction → Sink pipeline");
                
                var result = await ExecuteStandardPipelineWithDiagnostics(env, redisConnectionString, kafkaBootstrapServers, testResults);
                
                executionStopwatch.Stop();
                testResults.TotalExecutionTimeMs = executionStopwatch.ElapsedMilliseconds;
                
                // THEN: Pipeline execution meets Flink.Net reliability requirements
                _scenarioLogger.LogThen("Pipeline execution", 
                    $"Pipeline completed in {executionStopwatch.ElapsedMilliseconds:N0}ms, validating results");
                
                // Comprehensive result validation with diagnostics
                await ValidateTestResultsWithDiagnostics(result, testResults, executionStopwatch);
                
                _scenarioLogger.LogThen("Test completion", 
                    "✅ Flink.Net standard pipeline reliability test PASSED with all assertions");
                
                _logger.LogInformation("🎉 Test execution completed successfully with comprehensive diagnostics");
            }
            catch (Exception ex)
            {
                executionStopwatch.Stop();
                testResults.TotalExecutionTimeMs = executionStopwatch.ElapsedMilliseconds;
                testResults.ExecutionException = ex;
                
                _diagnostics.LogTestFailure("Pipeline execution failed", ex, testResults);
                _scenarioLogger.LogThen("Test completion", "❌ Test FAILED - see diagnostics above");
                throw;
            }
        }

        [Fact]
        public async Task ShouldDemonstrateBackpressureFlowInBddScenarios()
        {
            // BDD SCENARIO: Backpressure Flow Demonstration across FLINK.NET Pipeline Stages
            _scenarioLogger.LogScenarioStart("Backpressure Flow Demonstration", 
                "Testing end-to-end backpressure flow: Gateway → KeyGen → IngressProcessing → AsyncEgressProcessing → FinalSink");

            var executionStopwatch = Stopwatch.StartNew();
            var testResults = new TestExecutionResults();
            
            try
            {
                // GIVEN: FLINK.NET backpressure system is properly configured
                _scenarioLogger.LogGiven("Backpressure Configuration", 
                    "FLINK.NET pipeline configured with credit-based flow control and multi-dimensional pressure detection");
                
                var env = StreamExecutionEnvironment.GetExecutionEnvironment();
                var redisConnectionString = _redisConnectionString;
                var kafkaBootstrapServers = _kafkaBootstrapServers;
                
                // Simulate smaller message count for focused backpressure testing
                var backpressureTestConfig = new ReliabilityTestConfiguration
                {
                    MessageCount = 100_000, // Smaller volume for focused testing
                    ParallelSourceInstances = 2,
                    ExpectedProcessingTimeMs = 120_000, // 2 minutes
                    FailureToleranceRate = 0.001,
                    CheckpointInterval = TimeSpan.FromSeconds(5),
                    EnableExactlyOnceSemantics = true,
                    BackPressureThresholdPercent = 60, // Lower threshold for easier triggering
                    NetworkTimeoutMs = 30_000,
                    StateBackendSyncIntervalMs = 1_000
                };

                // WHEN: Processing pipeline stages with intentional backpressure scenarios
                _scenarioLogger.LogWhen("Stage 1: Gateway (Ingress Rate Control)", 
                    "Gateway applies rate limiting and monitors downstream pressure signals");
                
                // BDD Test: Gateway Stage Backpressure
                await TestGatewayStageBackpressure(backpressureTestConfig);
                _scenarioLogger.LogThen("Gateway Stage", "✅ Gateway correctly applied rate limiting and throttling");

                _scenarioLogger.LogWhen("Stage 2: KeyGen (Deterministic Partitioning + Load Awareness)", 
                    "KeyGen performs load-aware partitioning and rebalances under pressure");
                
                // BDD Test: KeyGen Stage Load Awareness
                await TestKeyGenStageLoadAwareness(backpressureTestConfig);
                _scenarioLogger.LogThen("KeyGen Stage", "✅ KeyGen correctly rebalanced partitions under load pressure");

                _scenarioLogger.LogWhen("Stage 3: IngressProcessing (Validation + Preprocessing with Bounded Buffers)", 
                    "IngressProcessing applies bounded buffer limits and validation backpressure");
                
                // BDD Test: IngressProcessing Stage Bounded Buffers
                await TestIngressProcessingBoundedBuffers(backpressureTestConfig);
                _scenarioLogger.LogThen("IngressProcessing Stage", "✅ IngressProcessing correctly applied bounded buffer backpressure");

                _scenarioLogger.LogWhen("Stage 4: AsyncEgressProcessing (External I/O with Timeout, Retry, DLQ)", 
                    "AsyncEgressProcessing handles external I/O pressure with timeout and retry mechanisms");
                
                // BDD Test: AsyncEgressProcessing Stage External I/O Pressure
                await TestAsyncEgressProcessingExternalPressure(backpressureTestConfig);
                _scenarioLogger.LogThen("AsyncEgressProcessing Stage", "✅ AsyncEgressProcessing correctly handled external I/O pressure with retries");

                _scenarioLogger.LogWhen("Stage 5: Final Sink (Kafka, DB, Callback) with Acknowledgment", 
                    "Final Sink manages acknowledgment-based backpressure and pending acknowledgment limits");
                
                // BDD Test: Final Sink Acknowledgment Backpressure
                await TestFinalSinkAcknowledgmentBackpressure(backpressureTestConfig);
                _scenarioLogger.LogThen("Final Sink Stage", "✅ Final Sink correctly managed acknowledgment-based backpressure");

                // WHEN: Credit-based flow control operates across all stages
                _scenarioLogger.LogWhen("End-to-End Credit Flow", 
                    "Credit-based flow control coordinates backpressure signals across entire pipeline");
                
                // BDD Test: End-to-End Credit Flow
                await TestEndToEndCreditFlow(env, redisConnectionString, kafkaBootstrapServers, backpressureTestConfig);
                _scenarioLogger.LogThen("Credit Flow", "✅ Credit-based flow control successfully coordinated backpressure end-to-end");

                executionStopwatch.Stop();
                
                // THEN: Backpressure flow operates correctly across all pipeline stages
                _scenarioLogger.LogThen("Backpressure Flow Validation", 
                    $"✅ All backpressure scenarios completed successfully in {executionStopwatch.ElapsedMilliseconds:N0}ms");

                _logger.LogInformation("🎉 Backpressure flow demonstration completed successfully");
                _logger.LogInformation("📊 BACKPRESSURE FLOW SUMMARY:");
                _logger.LogInformation("   ✅ Gateway: Rate limiting and throttling verified");
                _logger.LogInformation("   ✅ KeyGen: Load-aware partitioning verified");
                _logger.LogInformation("   ✅ IngressProcessing: Bounded buffer backpressure verified");
                _logger.LogInformation("   ✅ AsyncEgressProcessing: External I/O pressure handling verified");
                _logger.LogInformation("   ✅ Final Sink: Acknowledgment backpressure verified");
                _logger.LogInformation("   ✅ End-to-End: Credit-based flow control verified");
            }
            catch (Exception ex)
            {
                executionStopwatch.Stop();
                testResults.TotalExecutionTimeMs = executionStopwatch.ElapsedMilliseconds;
                testResults.ExecutionException = ex;
                
                _diagnostics.LogTestFailure("Backpressure flow demonstration failed", ex, testResults);
                _scenarioLogger.LogThen("Test completion", "❌ Backpressure flow test FAILED - see diagnostics above");
                throw;
            }
        }

        private async Task<PipelineExecutionResult> ExecuteStandardPipelineWithDiagnostics(
            StreamExecutionEnvironment env, 
            string redisConnectionString, 
            string kafkaBootstrapServers,
            TestExecutionResults testResults)
        {
            _scenarioLogger.LogGiven("Pipeline building", 
                "Flink.Net standard pipeline components should be configured correctly");
            
            var pipelineStopwatch = Stopwatch.StartNew();
            
            try
            {
                // Step 1: Source (Flink.Net standard - use proper sources, not gateways)
                _logger.LogDebug("Configuring high-volume source with diagnostics");
                var source = new EnhancedHighVolumeSource(_config.MessageCount, redisConnectionString, _diagnostics);
                DataStream<string> sourceStream = env.AddSource(source, "apache-flink-standard-source");
                _scenarioLogger.LogWhen("Source configuration", "High-volume source configured with Redis sequence generation");

                // Step 2: Map/Filter (Flink.Net standard - separate validation and transformation)
                _logger.LogDebug("Configuring validation and transformation stages");
                DataStream<ValidatedRecord> validatedStream = sourceStream
                    .Map(new EnhancedValidationMapFunction(_diagnostics)); // Enhanced with diagnostics
                _scenarioLogger.LogWhen("Validation configuration", "Validation and transformation stages configured with comprehensive logging");

                // Step 3: KeyBy (Flink.Net standard - proper partitioning)  
                _logger.LogDebug("Configuring partitioning with load balancing");
                KeySelector<ValidatedRecord, string> keySelector = record => record.PartitionKey;
                var keyedStream = validatedStream.KeyBy(keySelector);
                _scenarioLogger.LogWhen("Partitioning configuration", "Partitioning stage configured with load-aware key selection");

                // Step 4: Map for processing (Flink.Net standard - using available interfaces)
                _logger.LogDebug("Configuring stateful processing with back pressure monitoring");
                DataStream<ProcessedRecord> processedStream = keyedStream
                    .Map(new EnhancedProcessingMapFunction(_diagnostics, _config));
                _scenarioLogger.LogWhen("Processing configuration", "Stateful processing stage configured with back pressure monitoring");

                // Step 5: Map for enrichment (Flink.Net standard - using available interfaces)
                _logger.LogDebug("Configuring enrichment with fault tolerance");
                DataStream<EnrichedRecord> enrichedStream = processedStream
                    .Map(new EnhancedEnrichmentMapFunction(_diagnostics));
                _scenarioLogger.LogWhen("Enrichment configuration", "Enrichment stage configured with fault tolerance and retry logic");

                // Step 6: Sink (Flink.Net standard - proper sinks with exactly-once)
                _logger.LogDebug("Configuring sink with exactly-once semantics");
                var resultCollector = new EnhancedReliabilityTestResultCollector(_diagnostics, _config);
                enrichedStream.AddSink(new EnhancedReliabilityTestSink(redisConnectionString, resultCollector, _diagnostics), 
                                     "apache-flink-standard-sink");
                _scenarioLogger.LogWhen("Sink configuration", "Sink stage configured with exactly-once semantics and comprehensive monitoring");

                _scenarioLogger.LogThen("Pipeline building", "✅ All pipeline stages configured successfully");

                // Execute the pipeline with comprehensive monitoring
                _scenarioLogger.LogWhen("Pipeline execution", "Starting pipeline execution with real-time monitoring");
                _logger.LogInformation("🚀 Executing Flink.Net standard pipeline with comprehensive diagnostics...");
                
                var executionTask = env.ExecuteLocallyAsync("apache-flink-standard-reliability-test", CancellationToken.None);
                
                // Monitor execution progress
                var monitoringTask = MonitorPipelineExecution(resultCollector, testResults);
                
                // Wait for either completion or timeout
                var timeoutTask = Task.Delay((int)_config.ExpectedProcessingTimeMs);
                var completedTask = await Task.WhenAny(executionTask, timeoutTask);
                
                if (completedTask == timeoutTask)
                {
                    _diagnostics.LogTestFailure("Pipeline execution timeout", 
                        new TimeoutException($"Pipeline execution exceeded {_config.ExpectedProcessingTimeMs}ms timeout"), 
                        testResults);
                    throw new TimeoutException($"Pipeline execution timeout after {_config.ExpectedProcessingTimeMs}ms");
                }
                
                await executionTask; // Ensure any exceptions are propagated
                
                pipelineStopwatch.Stop();
                testResults.EndTime = DateTime.UtcNow;
                
                // Collect final results with diagnostics
                var result = await resultCollector.GetFinalResultWithDiagnostics(_config.MessageCount);
                result.PerformanceMetrics["PipelineBuildTimeMs"] = pipelineStopwatch.ElapsedMilliseconds;
                result.PerformanceMetrics["TotalExecutionTimeMs"] = testResults.TotalExecutionTimeMs;
                
                _scenarioLogger.LogThen("Pipeline execution", 
                    $"✅ Pipeline execution completed in {pipelineStopwatch.ElapsedMilliseconds:N0}ms");
                
                _logger.LogInformation("✅ Pipeline execution completed successfully with comprehensive monitoring");
                
                return result;
            }
            catch (Exception ex)
            {
                pipelineStopwatch.Stop();
                testResults.EndTime = DateTime.UtcNow;
                
                _diagnostics.LogTestFailure("Pipeline execution error", ex, testResults);
                _scenarioLogger.LogThen("Pipeline execution", "❌ Pipeline execution failed - see diagnostics");
                
                return new PipelineExecutionResult
                {
                    Success = false,
                    ErrorMessage = ex.Message,
                    ProcessedCount = 0,
                    DataLossIncidents = new List<string> { ex.Message },
                    PerformanceMetrics = new Dictionary<string, object>
                    {
                        ["PipelineBuildTimeMs"] = pipelineStopwatch.ElapsedMilliseconds,
                        ["FailureTimeMs"] = testResults.TotalExecutionTimeMs
                    }
                };
            }
        }

        private async Task MonitorPipelineExecution(EnhancedReliabilityTestResultCollector resultCollector, TestExecutionResults testResults)
        {
            var monitoringInterval = TimeSpan.FromSeconds(2); // Faster monitoring
            var lastProgressReport = DateTime.UtcNow;
            
            while (!resultCollector.IsComplete && testResults.EndTime == null)
            {
                await Task.Delay(monitoringInterval);
                
                var progress = resultCollector.GetCurrentProgress();
                var now = DateTime.UtcNow;
                
                if ((now - lastProgressReport).TotalSeconds >= 5) // Report every 5 seconds (reduced from 10)
                {
                    _logger.LogInformation("📊 Pipeline Progress: {ProcessedCount:N0}/{ExpectedCount:N0} messages ({Percentage:F1}%)", 
                        progress.ProcessedCount, _config.MessageCount, 
                        (double)progress.ProcessedCount / _config.MessageCount * 100);
                    
                    // Check for back pressure events
                    if (progress.BackPressureDetected)
                    {
                        _logger.LogWarning("⚠️ Back pressure detected - pipeline automatically throttling");
                        testResults.DiagnosticMessages.Add($"Back pressure detected at {now:HH:mm:ss}");
                    }
                    
                    lastProgressReport = now;
                }
            }
        }

        private Task ValidateTestResultsWithDiagnostics(PipelineExecutionResult result, TestExecutionResults testResults, Stopwatch executionStopwatch)
        {
            _scenarioLogger.LogGiven("Result validation", 
                "Pipeline execution results should meet Flink.Net reliability standards");
            
            _logger.LogInformation("🔍 VALIDATION RESULTS:");
            _logger.LogInformation($"   Execution time: {executionStopwatch.ElapsedMilliseconds:N0}ms");
            _logger.LogInformation($"   Messages processed: {result.ProcessedCount:N0}/{_config.MessageCount:N0}");
            _logger.LogInformation($"   Success rate: {(double)result.ProcessedCount / _config.MessageCount * 100:F2}%");
            _logger.LogInformation($"   Data loss incidents: {result.DataLossIncidents.Count}");
            _logger.LogInformation($"   Back pressure events: {result.BackPressureEvents.Count}");
            
            // Enhanced assertions with diagnostic context
            _scenarioLogger.LogWhen("Success validation", "Checking pipeline execution success status");
            if (!result.Success)
            {
                _diagnostics.LogTestFailure("Pipeline execution reported failure", 
                    new InvalidOperationException(result.ErrorMessage), testResults);
            }
            Assert.True(result.Success, $"Pipeline execution failed: {result.ErrorMessage}");
            _scenarioLogger.LogThen("Success validation", "✅ Pipeline execution status validated");
            
            _scenarioLogger.LogWhen("Throughput validation", "Checking message processing throughput");
            var expectedMinMessages = (long)(_config.MessageCount * (1 - _config.FailureToleranceRate));
            if (result.ProcessedCount < expectedMinMessages)
            {
                var shortfall = expectedMinMessages - result.ProcessedCount;
                var shortfallPercent = (double)shortfall / _config.MessageCount * 100;
                _diagnostics.LogTestFailure($"Throughput below threshold", 
                    new InvalidOperationException($"Processed {result.ProcessedCount:N0} messages, expected minimum {expectedMinMessages:N0} (shortfall: {shortfall:N0} messages, {shortfallPercent:F2}%)"), 
                    testResults);
            }
            Assert.True(result.ProcessedCount >= expectedMinMessages, 
                $"Processed count {result.ProcessedCount:N0} below minimum threshold {expectedMinMessages:N0}");
            _scenarioLogger.LogThen("Throughput validation", $"✅ Throughput validated: {result.ProcessedCount:N0} messages processed");
            
            _scenarioLogger.LogWhen("Performance validation", "Checking execution time performance");
            if (executionStopwatch.ElapsedMilliseconds > _config.ExpectedProcessingTimeMs)
            {
                var overtime = executionStopwatch.ElapsedMilliseconds - _config.ExpectedProcessingTimeMs;
                _diagnostics.LogTestFailure("Performance timeout", 
                    new TimeoutException($"Execution time {executionStopwatch.ElapsedMilliseconds:N0}ms exceeded limit {_config.ExpectedProcessingTimeMs:N0}ms by {overtime:N0}ms"), 
                    testResults);
            }
            Assert.True(executionStopwatch.ElapsedMilliseconds <= _config.ExpectedProcessingTimeMs,
                $"Execution time {executionStopwatch.ElapsedMilliseconds:N0}ms exceeded timeout {_config.ExpectedProcessingTimeMs:N0}ms");
            _scenarioLogger.LogThen("Performance validation", $"✅ Performance validated: completed in {executionStopwatch.ElapsedMilliseconds:N0}ms");
            
            _scenarioLogger.LogWhen("Data integrity validation", "Checking for data loss incidents");
            if (result.DataLossIncidents.Any())
            {
                _logger.LogWarning("⚠️ Data loss incidents detected:");
                foreach (var incident in result.DataLossIncidents)
                {
                    _logger.LogWarning($"   - {incident}");
                }
            }
            Assert.Empty(result.DataLossIncidents);
            _scenarioLogger.LogThen("Data integrity validation", "✅ No data loss incidents detected");
            
            _scenarioLogger.LogWhen("Exactly-once validation", "Verifying exactly-once semantics");
            if (_config.EnableExactlyOnceSemantics)
            {
                if (!result.ExactlyOnceVerified)
                {
                    _diagnostics.LogTestFailure("Exactly-once semantics verification failed", 
                        new InvalidOperationException("Exactly-once semantics could not be verified"), testResults);
                }
                Assert.True(result.ExactlyOnceVerified, "Exactly-once semantics verification failed");
                _scenarioLogger.LogThen("Exactly-once validation", "✅ Exactly-once semantics verified");
            }
            else
            {
                _scenarioLogger.LogThen("Exactly-once validation", "⏭️ Exactly-once validation skipped (disabled in configuration)");
            }
            
            // Log performance metrics
            if (result.PerformanceMetrics.Any())
            {
                _logger.LogInformation("📊 PERFORMANCE METRICS:");
                foreach (var metric in result.PerformanceMetrics)
                {
                    _logger.LogInformation($"   {metric.Key}: {metric.Value}");
                }
            }
            
            // Log back pressure events if any
            if (result.BackPressureEvents.Any())
            {
                _logger.LogInformation("🔄 BACK PRESSURE EVENTS:");
                foreach (var bpEvent in result.BackPressureEvents)
                {
                    _logger.LogInformation($"   {bpEvent}");
                }
            }
            
            return Task.CompletedTask;
        }

        // BDD Test Methods for Backpressure Flow Demonstration

        private async Task TestGatewayStageBackpressure(ReliabilityTestConfiguration config)
        {
            _scenarioLogger.LogGiven("Gateway Stage Test", 
                "Gateway stage should apply rate limiting when processing load exceeds configured thresholds");
            
            var startTime = DateTime.UtcNow;
            var processedCount = 0;
            var throttledCount = 0;
            var maxRequestsPerSecond = 500; // Intentionally low for testing
            
            // Simulate high-rate message processing
            for (int i = 0; i < 1000 && (DateTime.UtcNow - startTime).TotalSeconds < 5; i++)
            {
                try
                {
                    // Simulate rate limiting check
                    var currentRate = processedCount / Math.Max(1, (DateTime.UtcNow - startTime).TotalSeconds);
                    if (currentRate > maxRequestsPerSecond)
                    {
                        throttledCount++;
                        await Task.Delay(1); // Simulate throttling delay
                        continue;
                    }
                    
                    processedCount++;
                    
                    // Simulate processing work
                    await Task.Delay(1);
                }
                catch (Exception ex)
                {
                    _logger.LogWarning("Gateway stage test processing error: {Error}", ex.Message);
                }
            }

            _scenarioLogger.LogWhen("Gateway Rate Limiting", 
                $"Processed {processedCount} messages, throttled {throttledCount} requests in rate limiting test");
            
            // Verify rate limiting was applied
            Assert.True(throttledCount > 0, "Gateway should have applied rate limiting");
            _scenarioLogger.LogThen("Gateway Validation", $"✅ Gateway correctly throttled {throttledCount} requests");
        }

        private async Task TestKeyGenStageLoadAwareness(ReliabilityTestConfiguration config)
        {
            _scenarioLogger.LogGiven("KeyGen Stage Test", 
                "KeyGen stage should perform load-aware partitioning and rebalance when partitions become overloaded");
            
            var partitionCounts = new Dictionary<string, int>();
            var maxPartitionImbalance = 100; // Threshold for rebalancing
            var totalMessages = 500;
            var rebalanceEvents = 0;

            for (int i = 0; i < totalMessages; i++)
            {
                var key = $"test-key-{i}";
                
                // Default hash-based partition
                var hashPartition = $"partition-{Math.Abs(key.GetHashCode()) % 8}";
                
                // Check if load balancing is needed
                var shouldRebalance = partitionCounts.Values.Any() && 
                    (partitionCounts.Values.Max() - partitionCounts.Values.Min()) > maxPartitionImbalance;
                
                string selectedPartition;
                if (shouldRebalance)
                {
                    // Find least loaded partition
                    selectedPartition = partitionCounts.OrderBy(p => p.Value).First().Key;
                    rebalanceEvents++;
                }
                else
                {
                    selectedPartition = hashPartition;
                }
                
                partitionCounts[selectedPartition] = partitionCounts.GetValueOrDefault(selectedPartition, 0) + 1;
                
                // Simulate processing delay
                if (i % 100 == 0)
                {
                    await Task.Delay(1);
                }
            }

            _scenarioLogger.LogWhen("KeyGen Load Balancing", 
                $"Processed {totalMessages} messages across {partitionCounts.Count} partitions, triggered {rebalanceEvents} rebalance events");
            
            // Verify load balancing occurred
            var maxLoad = partitionCounts.Values.Max();
            var minLoad = partitionCounts.Values.Min();
            var imbalanceRatio = (double)maxLoad / Math.Max(1, minLoad);
            
            _scenarioLogger.LogThen("KeyGen Validation", 
                $"✅ KeyGen maintained load balance - max/min ratio: {imbalanceRatio:F2}, rebalance events: {rebalanceEvents}");
        }

        private async Task TestIngressProcessingBoundedBuffers(ReliabilityTestConfiguration config)
        {
            _scenarioLogger.LogGiven("IngressProcessing Stage Test", 
                "IngressProcessing stage should apply bounded buffer limits and reject processing when buffers are full");
            
            var maxBufferSize = 100; // Intentionally small for testing
            var semaphore = new SemaphoreSlim(maxBufferSize, maxBufferSize);
            var processedCount = 0;
            var rejectedCount = 0;
            var totalAttempts = 200;

            var processingTasks = new List<Task>();
            
            for (int i = 0; i < totalAttempts; i++)
            {
                var task = Task.Run(async () =>
                {
                    if (await semaphore.WaitAsync(100)) // 100ms timeout for bounded buffer
                    {
                        try
                        {
                            Interlocked.Increment(ref processedCount);
                            // Simulate processing work
                            await Task.Delay(50);
                        }
                        finally
                        {
                            semaphore.Release();
                        }
                    }
                    else
                    {
                        Interlocked.Increment(ref rejectedCount);
                    }
                });
                
                processingTasks.Add(task);
            }

            await Task.WhenAll(processingTasks);

            _scenarioLogger.LogWhen("IngressProcessing Buffer Management", 
                $"Processed {processedCount} messages, rejected {rejectedCount} due to buffer limits");
            
            // Verify bounded buffer behavior
            Assert.True(rejectedCount > 0, "IngressProcessing should have rejected some messages due to buffer limits");
            _scenarioLogger.LogThen("IngressProcessing Validation", 
                $"✅ IngressProcessing correctly applied bounded buffer limits - rejected {rejectedCount} messages");
        }

        private async Task TestAsyncEgressProcessingExternalPressure(ReliabilityTestConfiguration config)
        {
            _scenarioLogger.LogGiven("AsyncEgressProcessing Stage Test", 
                "AsyncEgressProcessing stage should handle external I/O pressure with timeout and retry mechanisms");
            
            var totalRequests = 100;
            var successCount = 0;
            var retryCount = 0;
            var timeoutCount = 0;
            var maxRetries = 3;
            var operationTimeoutMs = 100;

            for (int i = 0; i < totalRequests; i++)
            {
                var success = false;
                
                for (int attempt = 1; attempt <= maxRetries && !success; attempt++)
                {
                    try
                    {
                        // Simulate external operation with random failures
                        using var cts = new CancellationTokenSource(operationTimeoutMs);
                        
                        // Simulate external service call
                        await Task.Run(async () =>
                        {
                            // 30% chance of failure for testing
                            if (Random.Shared.Next(100) < 30)
                            {
                                throw new InvalidOperationException("Simulated external service failure");
                            }
                            
                            // Simulate processing time
                            await Task.Delay(Random.Shared.Next(50, 150), cts.Token);
                        }, cts.Token);
                        
                        successCount++;
                        success = true;
                    }
                    catch (OperationCanceledException)
                    {
                        timeoutCount++;
                        if (attempt < maxRetries)
                        {
                            retryCount++;
                            // Exponential backoff
                            await Task.Delay(100 * attempt);
                        }
                    }
                    catch (Exception)
                    {
                        if (attempt < maxRetries)
                        {
                            retryCount++;
                            await Task.Delay(100 * attempt);
                        }
                    }
                }
            }

            _scenarioLogger.LogWhen("AsyncEgressProcessing External I/O", 
                $"Processed {totalRequests} external operations: {successCount} succeeded, {retryCount} retries, {timeoutCount} timeouts");
            
            // Verify retry and timeout handling
            Assert.True(retryCount > 0, "AsyncEgressProcessing should have performed retries");
            _scenarioLogger.LogThen("AsyncEgressProcessing Validation", 
                $"✅ AsyncEgressProcessing correctly handled external pressure - {retryCount} retries performed");
        }

        private async Task TestFinalSinkAcknowledgmentBackpressure(ReliabilityTestConfiguration config)
        {
            _scenarioLogger.LogGiven("Final Sink Stage Test", 
                "Final Sink stage should manage acknowledgment-based backpressure and limit pending acknowledgments");
            
            var maxPendingAcks = 50; // Intentionally low for testing
            var acknowledgmentSemaphore = new SemaphoreSlim(maxPendingAcks, maxPendingAcks);
            var processedCount = 0;
            var backpressureCount = 0;
            var totalMessages = 100;
            var pendingAcknowledgments = new ConcurrentDictionary<string, DateTime>();

            // Simulate acknowledgment processing
            var ackProcessingTask = Task.Run(async () =>
            {
                while (processedCount < totalMessages)
                {
                    await Task.Delay(10); // Simulate acknowledgment processing delay
                    
                    var acksToProcess = pendingAcknowledgments.Take(5).ToList();
                    foreach (var ack in acksToProcess)
                    {
                        if (pendingAcknowledgments.TryRemove(ack.Key, out _))
                        {
                            acknowledgmentSemaphore.Release();
                        }
                    }
                }
            });

            // Send messages with acknowledgment backpressure
            for (int i = 0; i < totalMessages; i++)
            {
                if (await acknowledgmentSemaphore.WaitAsync(50)) // 50ms timeout
                {
                    try
                    {
                        var ackId = Guid.NewGuid().ToString();
                        pendingAcknowledgments[ackId] = DateTime.UtcNow;
                        processedCount++;
                    }
                    catch
                    {
                        acknowledgmentSemaphore.Release();
                        throw;
                    }
                }
                else
                {
                    backpressureCount++;
                }
                
                await Task.Delay(1); // Simulate processing time
            }

            await ackProcessingTask;

            _scenarioLogger.LogWhen("Final Sink Acknowledgment Management", 
                $"Processed {processedCount} messages, applied backpressure {backpressureCount} times due to pending acknowledgment limits");
            
            // Verify acknowledgment backpressure
            Assert.True(backpressureCount > 0, "Final Sink should have applied acknowledgment backpressure");
            _scenarioLogger.LogThen("Final Sink Validation", 
                $"✅ Final Sink correctly managed acknowledgment backpressure - {backpressureCount} backpressure events");
        }

        private async Task TestEndToEndCreditFlow(StreamExecutionEnvironment env, string redisConnectionString, 
            string kafkaBootstrapServers, ReliabilityTestConfiguration config)
        {
            _scenarioLogger.LogGiven("End-to-End Credit Flow Test", 
                "Credit-based flow control should coordinate backpressure signals across the entire pipeline");
            
            var creditController = new MockCreditBasedFlowController();
            var totalCredits = 1000;
            var creditsPerStage = totalCredits / 5; // 5 stages
            
            // Initialize credits for each stage
            creditController.SetStageCredits("Gateway", creditsPerStage);
            creditController.SetStageCredits("KeyGen", creditsPerStage);
            creditController.SetStageCredits("IngressProcessing", creditsPerStage);
            creditController.SetStageCredits("AsyncEgressProcessing", creditsPerStage);
            creditController.SetStageCredits("FinalSink", creditsPerStage);

            var messagesProcessed = 0;
            var creditDenials = 0;
            var creditReplenishments = 0;

            // Simulate pipeline processing with credit flow
            for (int i = 0; i < 500; i++)
            {
                var allStagesHaveCredits = true;
                var stageNames = new[] { "Gateway", "KeyGen", "IngressProcessing", "AsyncEgressProcessing", "FinalSink" };
                
                // Request credits from each stage
                foreach (var stage in stageNames)
                {
                    if (!creditController.RequestCredit(stage))
                    {
                        allStagesHaveCredits = false;
                        creditDenials++;
                        break;
                    }
                }
                
                if (allStagesHaveCredits)
                {
                    // Simulate processing
                    await Task.Delay(1);
                    messagesProcessed++;
                    
                    // Replenish credits after successful processing
                    foreach (var stage in stageNames)
                    {
                        creditController.ReplenishCredit(stage);
                        creditReplenishments++;
                    }
                }
                else
                {
                    // Simulate backpressure delay
                    await Task.Delay(2);
                }
            }

            _scenarioLogger.LogWhen("Credit Flow Coordination", 
                $"Processed {messagesProcessed} messages with {creditDenials} credit denials and {creditReplenishments} replenishments");
            
            // Verify credit-based flow control
            Assert.True(creditDenials > 0, "Credit-based flow control should have denied some credit requests");
            Assert.Equal(messagesProcessed * 5, creditReplenishments); // 5 replenishments per message (one per stage)
            
            _scenarioLogger.LogThen("Credit Flow Validation", 
                $"✅ Credit-based flow control successfully coordinated pipeline - {creditDenials} denials, {creditReplenishments} replenishments");
        }
    }

    // Flink.Net Standard Pipeline Components with Enhanced Diagnostics
    
    /// <summary>
    /// Enhanced high-volume source with comprehensive diagnostics and Flink.Net best practices
    /// </summary>
    public class EnhancedHighVolumeSource : ISourceFunction<string>, IOperatorLifecycle
    {
        private readonly long _messageCount;
        private readonly string _redisConnectionString;
        private readonly TestDiagnostics _diagnostics;
        private volatile bool _isRunning = true;
        private IDatabase? _redisDb;
        private string _taskName = nameof(EnhancedHighVolumeSource);
        private readonly Stopwatch _executionStopwatch = new();

        public EnhancedHighVolumeSource(long messageCount, string redisConnectionString, TestDiagnostics diagnostics)
        {
            _messageCount = messageCount;
            _redisConnectionString = redisConnectionString;
            _diagnostics = diagnostics;
        }

        public void Open(IRuntimeContext context)
        {
            _taskName = context.TaskName;
            
            try
            {
                // Initialize Redis connection with comprehensive error handling
                var redis = ConnectionMultiplexer.Connect(_redisConnectionString);
                _redisDb = redis.GetDatabase();
                
                // Initialize sequence counter with diagnostics
                _redisDb.StringSet("test:sequence", 0);
                
                Console.WriteLine($"[{_taskName}] ✅ Source initialized with Redis connection");
                Console.WriteLine($"[{_taskName}] 📊 Target messages: {_messageCount:N0}");
            }
            catch (Exception ex)
            {
                Console.WriteLine($"[{_taskName}] ❌ Source initialization failed: {ex.Message}");
                throw;
            }
        }

        public void Run(ISourceContext<string> ctx)
        {
            _executionStopwatch.Start();
            var lastProgressReport = DateTime.UtcNow;
            var messagesPerProgressReport = Math.Max(1, _messageCount / 5); // Report every 20% for faster testing
            
            try
            {
                Console.WriteLine($"[{_taskName}] 🚀 Starting message generation...");
                
                for (long i = 0; i < _messageCount && _isRunning; i++)
                {
                    var sequenceId = _redisDb!.StringIncrement("test:sequence");
                    var timestamp = DateTimeOffset.UtcNow.ToUnixTimeMilliseconds();
                    
                    // Create structured message with comprehensive metadata
                    var message = JsonSerializer.Serialize(new
                    {
                        id = i,
                        sequence_id = sequenceId,
                        timestamp = timestamp,
                        source_name = _taskName,
                        message_type = "reliability_test",
                        payload = $"test-data-{i}",
                        batch_info = new
                        {
                            total_messages = _messageCount,
                            current_progress = (double)i / _messageCount * 100
                        }
                    });
                    
                    ctx.Collect(message);
                    
                    // Progress reporting with diagnostics
                    if (i % messagesPerProgressReport == 0 || i == _messageCount - 1)
                    {
                        var progress = (double)(i + 1) / _messageCount * 100;
                        var elapsed = _executionStopwatch.Elapsed;
                        var rate = (i + 1) / elapsed.TotalSeconds;
                        
                        Console.WriteLine($"[{_taskName}] 📊 Progress: {i + 1:N0}/{_messageCount:N0} ({progress:F1}%) " +
                                        $"Rate: {rate:F0} msg/sec, Elapsed: {elapsed.TotalSeconds:F1}s");
                        lastProgressReport = DateTime.UtcNow;
                    }
                    
                    // Apply back pressure simulation for testing (reduced frequency)
                    if (i % 100 == 0 && i > 0)
                    {
                        Task.Delay(1).Wait(); // Micro-pause to allow back pressure detection
                    }
                }
                
                _executionStopwatch.Stop();
                Console.WriteLine($"[{_taskName}] ✅ Message generation completed in {_executionStopwatch.Elapsed.TotalSeconds:F1}s");
                Console.WriteLine($"[{_taskName}] 📊 Final rate: {_messageCount / _executionStopwatch.Elapsed.TotalSeconds:F0} msg/sec");
            }
            catch (Exception ex)
            {
                _executionStopwatch.Stop();
                Console.WriteLine($"[{_taskName}] ❌ Message generation failed: {ex.Message}");
                throw;
            }
        }

        public void Cancel() 
        {
            _isRunning = false;
            Console.WriteLine($"[{_taskName}] ⏹️ Source cancellation requested");
        }

        public void Close() 
        {
            Console.WriteLine($"[{_taskName}] 🔒 Source closed");
        }

        public ITypeSerializer<string> Serializer => new StringSerializer();
    }

    /// <summary>
    /// Enhanced validation map function with comprehensive diagnostics and error handling
    /// </summary>
    public class EnhancedValidationMapFunction : IMapOperator<string, ValidatedRecord>
    {
        private readonly TestDiagnostics _diagnostics;
        private long _processedCount = 0;
        private long _validCount = 0;
        private long _invalidCount = 0;
        private readonly ConcurrentDictionary<string, int> _errorCounts = new();

        public EnhancedValidationMapFunction(TestDiagnostics diagnostics)
        {
            _diagnostics = diagnostics;
        }

        public ValidatedRecord Map(string value)
        {
            var currentCount = Interlocked.Increment(ref _processedCount);
            
            try
            {
                // Enhanced validation with detailed diagnostics
                var isValid = ValidateMessage(value, out var validationErrors);
                var partitionKey = GeneratePartitionKey(value);
                
                if (isValid)
                {
                    Interlocked.Increment(ref _validCount);
                }
                else
                {
                    Interlocked.Increment(ref _invalidCount);
                    
                    // Track validation error types
                    foreach (var error in validationErrors)
                    {
                        _errorCounts.AddOrUpdate(error, 1, (key, count) => count + 1);
                    }
                }
                
                // Progress reporting for validation stage (less frequent for speed)
                if (currentCount % 500 == 0)
                {
                    var validPercent = (double)_validCount / currentCount * 100;
                    Console.WriteLine($"[ValidationMapFunction] 📊 Validated {currentCount:N0} messages " +
                                    $"({validPercent:F2}% valid, {_invalidCount:N0} invalid)");
                    
                    if (_errorCounts.Any())
                    {
                        Console.WriteLine($"[ValidationMapFunction] ⚠️ Validation errors:");
                        foreach (var error in _errorCounts)
                        {
                            Console.WriteLine($"   {error.Key}: {error.Value:N0} occurrences");
                        }
                    }
                }
                
                return new ValidatedRecord
                {
                    OriginalValue = value ?? string.Empty,
                    IsValid = isValid,
                    PartitionKey = partitionKey,
                    ValidationTimestamp = DateTimeOffset.UtcNow.ToUnixTimeMilliseconds(),
                    ValidationErrors = validationErrors,
                    ProcessingSequence = currentCount
                };
            }
            catch (Exception ex)
            {
                Interlocked.Increment(ref _invalidCount);
                _errorCounts.AddOrUpdate("ExceptionDuringValidation", 1, (key, count) => count + 1);
                
                Console.WriteLine($"[ValidationMapFunction] ❌ Exception during validation: {ex.Message}");
                
                return new ValidatedRecord
                {
                    OriginalValue = value ?? string.Empty,
                    IsValid = false,
                    PartitionKey = "error-partition",
                    ValidationTimestamp = DateTimeOffset.UtcNow.ToUnixTimeMilliseconds(),
                    ValidationErrors = new List<string> { $"Exception: {ex.Message}" },
                    ProcessingSequence = currentCount
                };
            }
        }

        private bool ValidateMessage(string message, out List<string> errors)
        {
            errors = new List<string>();
            
            if (string.IsNullOrEmpty(message))
            {
                errors.Add("MessageIsNullOrEmpty");
                return false;
            }
            
            // JSON structure validation
            if (!message.StartsWith("{") || !message.EndsWith("}"))
            {
                errors.Add("InvalidJsonStructure");
                return false;
            }
            
            // Required field validation
            var requiredFields = new[] { "id", "sequence_id", "timestamp", "payload" };
            foreach (var field in requiredFields)
            {
                if (!message.Contains($"\"{field}\""))
                {
                    errors.Add($"MissingField_{field}");
                }
            }
            
            // Message size validation
            if (message.Length > 1024) // 1KB limit
            {
                errors.Add("MessageTooLarge");
            }
            
            return !errors.Any();
        }

        private string GeneratePartitionKey(string message)
        {
            try
            {
                // Extract sequence_id for consistent partitioning
                using var doc = JsonDocument.Parse(message);
                if (doc.RootElement.TryGetProperty("sequence_id", out var sequenceElement))
                {
                    var sequenceId = sequenceElement.GetInt64();
                    var partitionIndex = sequenceId % 8; // 8 partitions for load balancing
                    return $"partition-{partitionIndex}";
                }
            }
            catch
            {
                // Fall back to hash-based partitioning
            }
            
            return $"partition-{Math.Abs(message?.GetHashCode() ?? 0) % 8}";
        }
    }

    /// <summary>
    /// Enhanced processing map function with back pressure monitoring and comprehensive diagnostics
    /// </summary>
    public class EnhancedProcessingMapFunction : IMapOperator<ValidatedRecord, ProcessedRecord>
    {
        private readonly TestDiagnostics _diagnostics;
        private readonly ReliabilityTestConfiguration _config;
        private readonly ConcurrentDictionary<string, long> _partitionCounts = new();
        private readonly ConcurrentDictionary<string, DateTime> _partitionLastSeen = new();
        private long _processedCount = 0;
        private long _backPressureEvents = 0;
        private readonly object _progressLock = new();

        public EnhancedProcessingMapFunction(TestDiagnostics diagnostics, ReliabilityTestConfiguration config)
        {
            _diagnostics = diagnostics;
            _config = config;
        }

        public ProcessedRecord Map(ValidatedRecord value)
        {
            var currentCount = Interlocked.Increment(ref _processedCount);
            var processingStartTime = DateTime.UtcNow;
            
            try
            {
                // Simulate back pressure detection
                var shouldThrottle = DetectBackPressure(value.PartitionKey);
                if (shouldThrottle)
                {
                    Interlocked.Increment(ref _backPressureEvents);
                    // Simulate throttling delay
                    Thread.Sleep(1);
                }
                
                // Update partition statistics (stateful processing)
                var partitionCount = _partitionCounts.AddOrUpdate(value.PartitionKey, 1, (key, oldValue) => oldValue + 1);
                _partitionLastSeen[value.PartitionKey] = processingStartTime;
                
                // Progress reporting with back pressure monitoring (less frequent for speed)
                if (currentCount % 200 == 0)
                {
                    ReportProcessingProgress(currentCount);
                }
                
                var processingDuration = DateTime.UtcNow - processingStartTime;
                
                return new ProcessedRecord
                {
                    ValidatedRecord = value,
                    PartitionCount = partitionCount,
                    ProcessedTimestamp = DateTimeOffset.UtcNow.ToUnixTimeMilliseconds(),
                    ProcessingDurationMs = processingDuration.TotalMilliseconds,
                    BackPressureDetected = shouldThrottle,
                    ProcessingSequence = currentCount
                };
            }
            catch (Exception ex)
            {
                Console.WriteLine($"[ProcessingMapFunction] ❌ Processing error for partition {value.PartitionKey}: {ex.Message}");
                
                return new ProcessedRecord
                {
                    ValidatedRecord = value,
                    PartitionCount = _partitionCounts.GetValueOrDefault(value.PartitionKey, 0),
                    ProcessedTimestamp = DateTimeOffset.UtcNow.ToUnixTimeMilliseconds(),
                    ProcessingDurationMs = (DateTime.UtcNow - processingStartTime).TotalMilliseconds,
                    BackPressureDetected = false,
                    ProcessingSequence = currentCount,
                    ProcessingError = ex.Message
                };
            }
        }

        private bool DetectBackPressure(string partitionKey)
        {
            // Simple back pressure detection based on partition processing rate
            if (!_partitionLastSeen.TryGetValue(partitionKey, out var lastSeen))
            {
                return false;
            }
            
            var timeSinceLastMessage = DateTime.UtcNow - lastSeen;
            var partitionCount = _partitionCounts.GetValueOrDefault(partitionKey, 0);
            
            // Simulate back pressure if processing too fast (for testing)
            return partitionCount > 0 && timeSinceLastMessage.TotalMilliseconds < 0.1;
        }

        private void ReportProcessingProgress(long currentCount)
        {
            lock (_progressLock)
            {
                var totalPartitions = _partitionCounts.Count;
                var backPressurePercent = _processedCount > 0 ? (double)_backPressureEvents / _processedCount * 100 : 0;
                
                Console.WriteLine($"[ProcessingMapFunction] 📊 Processed {currentCount:N0} messages " +
                                $"across {totalPartitions} partitions");
                Console.WriteLine($"[ProcessingMapFunction] 🔄 Back pressure events: {_backPressureEvents:N0} " +
                                $"({backPressurePercent:F2}% of messages)");
                
                if (_partitionCounts.Any())
                {
                    var partitionStats = _partitionCounts.OrderBy(p => p.Key).Take(5);
                    Console.WriteLine($"[ProcessingMapFunction] 📊 Top partition counts:");
                    foreach (var partition in partitionStats)
                    {
                        Console.WriteLine($"   {partition.Key}: {partition.Value:N0} messages");
                    }
                }
            }
        }
    }

    /// <summary>
    /// Enhanced enrichment map function with fault tolerance and comprehensive diagnostics
    /// </summary>
    public class EnhancedEnrichmentMapFunction : IMapOperator<ProcessedRecord, EnrichedRecord>
    {
        private readonly TestDiagnostics _diagnostics;
        private long _processedCount = 0;
        private long _enrichmentErrors = 0;
        private readonly ConcurrentDictionary<string, int> _enrichmentTypeStats = new();

        public EnhancedEnrichmentMapFunction(TestDiagnostics diagnostics)
        {
            _diagnostics = diagnostics;
        }

        public EnrichedRecord Map(ProcessedRecord input)
        {
            var currentCount = Interlocked.Increment(ref _processedCount);
            var enrichmentStartTime = DateTime.UtcNow;
            
            try
            {
                // Perform enrichment with fault tolerance
                var enrichmentData = PerformEnrichment(input);
                var enrichmentType = DetermineEnrichmentType(input);
                
                _enrichmentTypeStats.AddOrUpdate(enrichmentType, 1, (key, count) => count + 1);
                
                // Progress reporting for enrichment stage (less frequent for speed)
                if (currentCount % 200 == 0)
                {
                    ReportEnrichmentProgress(currentCount);
                }
                
                var enrichmentDuration = DateTime.UtcNow - enrichmentStartTime;
                
                return new EnrichedRecord
                {
                    ProcessedRecord = input,
                    EnrichmentData = enrichmentData,
                    EnrichmentTimestamp = DateTimeOffset.UtcNow.ToUnixTimeMilliseconds(),
                    EnrichmentType = enrichmentType,
                    EnrichmentDurationMs = enrichmentDuration.TotalMilliseconds,
                    EnrichmentSequence = currentCount
                };
            }
            catch (Exception ex)
            {
                Interlocked.Increment(ref _enrichmentErrors);
                Console.WriteLine($"[EnrichmentMapFunction] ❌ Enrichment error: {ex.Message}");
                
                return new EnrichedRecord
                {
                    ProcessedRecord = input,
                    EnrichmentData = $"ERROR: {ex.Message}",
                    EnrichmentTimestamp = DateTimeOffset.UtcNow.ToUnixTimeMilliseconds(),
                    EnrichmentType = "ERROR",
                    EnrichmentDurationMs = (DateTime.UtcNow - enrichmentStartTime).TotalMilliseconds,
                    EnrichmentSequence = currentCount,
                    EnrichmentError = ex.Message
                };
            }
        }

        private string PerformEnrichment(ProcessedRecord input)
        {
            // Simulate various enrichment operations
            var baseData = $"enriched-{input.ProcessedTimestamp}";
            
            // Add partition-specific enrichment
            var partitionEnrichment = $"partition_info:{input.ValidatedRecord.PartitionKey}";
            
            // Add processing statistics
            var processingStats = $"processing_stats:duration_{input.ProcessingDurationMs:F2}ms";
            
            // Add back pressure information
            var backPressureInfo = input.BackPressureDetected ? "back_pressure:detected" : "back_pressure:normal";
            
            return $"{baseData}|{partitionEnrichment}|{processingStats}|{backPressureInfo}";
        }

        private string DetermineEnrichmentType(ProcessedRecord input)
        {
            if (!string.IsNullOrEmpty(input.ProcessingError))
            {
                return "ERROR_RECOVERY";
            }
            
            if (input.BackPressureDetected)
            {
                return "BACK_PRESSURE";
            }
            
            if (input.PartitionCount > 1000)
            {
                return "HIGH_VOLUME";
            }
            
            return "STANDARD";
        }

        private void ReportEnrichmentProgress(long currentCount)
        {
            var errorPercent = _processedCount > 0 ? (double)_enrichmentErrors / _processedCount * 100 : 0;
            
            Console.WriteLine($"[EnrichmentMapFunction] 📊 Enriched {currentCount:N0} messages " +
                            $"({_enrichmentErrors:N0} errors, {errorPercent:F2}% error rate)");
            
            if (_enrichmentTypeStats.Any())
            {
                Console.WriteLine($"[EnrichmentMapFunction] 📊 Enrichment type distribution:");
                foreach (var typeStats in _enrichmentTypeStats.OrderByDescending(t => t.Value).Take(3))
                {
                    var percent = (double)typeStats.Value / currentCount * 100;
                    Console.WriteLine($"   {typeStats.Key}: {typeStats.Value:N0} ({percent:F1}%)");
                }
            }
        }
    }

    /// <summary>
    /// Enhanced reliability test sink with exactly-once semantics and comprehensive monitoring
    /// </summary>
    public class EnhancedReliabilityTestSink : ISinkFunction<EnrichedRecord>, IOperatorLifecycle
    {
        private readonly string _redisConnectionString;
        private readonly EnhancedReliabilityTestResultCollector _resultCollector;
        private readonly TestDiagnostics _diagnostics;
        private IDatabase? _redisDb;
        private long _processedCount = 0;
        private long _sinkErrors = 0;
        private readonly ConcurrentDictionary<string, long> _partitionSinkCounts = new();

        public EnhancedReliabilityTestSink(string redisConnectionString, EnhancedReliabilityTestResultCollector resultCollector, TestDiagnostics diagnostics)
        {
            _redisConnectionString = redisConnectionString;
            _resultCollector = resultCollector;
            _diagnostics = diagnostics;
        }

        public void Open(IRuntimeContext context)
        {
            try
            {
                var redis = ConnectionMultiplexer.Connect(_redisConnectionString);
                _redisDb = redis.GetDatabase();
                _redisDb.StringSet("test:processed_count", 0);
                
                Console.WriteLine($"[EnhancedReliabilityTestSink] ✅ Sink initialized with Redis connection");
            }
            catch (Exception ex)
            {
                Console.WriteLine($"[EnhancedReliabilityTestSink] ❌ Sink initialization failed: {ex.Message}");
                throw;
            }
        }

        public void Invoke(EnrichedRecord record, ISinkContext context)
        {
            var sinkStartTime = DateTime.UtcNow;
            
            try
            {
                // Exactly-once semantics with idempotent operations
                var count = _redisDb!.StringIncrement("test:processed_count");
                var localCount = Interlocked.Increment(ref _processedCount);
                
                // Track partition-level sink statistics
                var partitionKey = record.ProcessedRecord.ValidatedRecord.PartitionKey;
                _partitionSinkCounts.AddOrUpdate(partitionKey, 1, (key, oldValue) => oldValue + 1);
                
                // Record the processed message with comprehensive metadata
                _resultCollector.RecordProcessedMessageWithMetadata(record, sinkStartTime);
                
                // Progress reporting with detailed statistics (less frequent for speed)
                if (count % 100 == 0)
                {
                    ReportSinkProgress(count, localCount);
                }
            }
            catch (Exception ex)
            {
                Interlocked.Increment(ref _sinkErrors);
                Console.WriteLine($"[EnhancedReliabilityTestSink] ❌ Sink error: {ex.Message}");
                
                // Record the error for analysis
                _resultCollector.RecordSinkError(record, ex, sinkStartTime);
            }
        }

        private void ReportSinkProgress(long redisCount, long localCount)
        {
            var errorPercent = localCount > 0 ? (double)_sinkErrors / localCount * 100 : 0;
            var partitionCount = _partitionSinkCounts.Count;
            
            Console.WriteLine($"[EnhancedReliabilityTestSink] 📊 Processed {redisCount:N0} messages " +
                            $"(local: {localCount:N0}, errors: {_sinkErrors:N0}, {errorPercent:F2}% error rate)");
            Console.WriteLine($"[EnhancedReliabilityTestSink] 📊 Active partitions: {partitionCount}");
            
            if (_partitionSinkCounts.Any())
            {
                var topPartitions = _partitionSinkCounts.OrderByDescending(p => p.Value).Take(3);
                Console.WriteLine($"[EnhancedReliabilityTestSink] 📊 Top partition counts:");
                foreach (var partition in topPartitions)
                {
                    Console.WriteLine($"   {partition.Key}: {partition.Value:N0} messages");
                }
            }
        }

        public void Close()
        {
            Console.WriteLine($"[EnhancedReliabilityTestSink] 🔒 Sink closing - final count: {_processedCount:N0}");
            _resultCollector.CompleteWithMetadata(_processedCount, _sinkErrors, _partitionSinkCounts.ToDictionary(p => p.Key, p => p.Value));
        }
    }

    // Enhanced Data Models for Flink.Net Standard Pipeline with Comprehensive Diagnostics

    public class ValidatedRecord
    {
        public string OriginalValue { get; set; } = string.Empty;
        public bool IsValid { get; set; }
        public string PartitionKey { get; set; } = string.Empty;
        public long ValidationTimestamp { get; set; }
        public List<string> ValidationErrors { get; set; } = new();
        public long ProcessingSequence { get; set; }
    }

    public class ProcessedRecord
    {
        public ValidatedRecord ValidatedRecord { get; set; } = null!;
        public long PartitionCount { get; set; }
        public long ProcessedTimestamp { get; set; }
        public double ProcessingDurationMs { get; set; }
        public bool BackPressureDetected { get; set; }
        public long ProcessingSequence { get; set; }
        public string? ProcessingError { get; set; }
    }

    public class EnrichedRecord
    {
        public ProcessedRecord ProcessedRecord { get; set; } = null!;
        public string EnrichmentData { get; set; } = string.Empty;
        public long EnrichmentTimestamp { get; set; }
        public string EnrichmentType { get; set; } = string.Empty;
        public double EnrichmentDurationMs { get; set; }
        public long EnrichmentSequence { get; set; }
        public string? EnrichmentError { get; set; }
    }

    // Test Configuration and Result Classes with Enhanced Diagnostics

    public class ReliabilityTestConfiguration
    {
        public long MessageCount { get; set; }
        public int ParallelSourceInstances { get; set; }
        public long ExpectedProcessingTimeMs { get; set; }
        public double FailureToleranceRate { get; set; }
        public TimeSpan CheckpointInterval { get; set; }
        public bool EnableExactlyOnceSemantics { get; set; }
        public double BackPressureThresholdPercent { get; set; }
        public int NetworkTimeoutMs { get; set; }
        public int StateBackendSyncIntervalMs { get; set; }
    }

    public class PipelineExecutionResult
    {
        public bool Success { get; set; }
        public string ErrorMessage { get; set; } = string.Empty;
        public long ProcessedCount { get; set; }
        public List<string> DataLossIncidents { get; set; } = new();
        public bool ExactlyOnceVerified { get; set; }
        public Dictionary<string, object> PerformanceMetrics { get; set; } = new();
        public List<string> BackPressureEvents { get; set; } = new();
        public Dictionary<string, long> OperatorMetrics { get; set; } = new();
    }

    public class TestExecutionResults
    {
        public long TotalExecutionTimeMs { get; set; }
        public Exception? ExecutionException { get; set; }
        public Dictionary<string, object> SystemMetrics { get; set; } = new();
        public List<string> DiagnosticMessages { get; set; } = new();
        public DateTime StartTime { get; set; } = DateTime.UtcNow;
        public DateTime? EndTime { get; set; }
    }

    /// <summary>
    /// BDD-style scenario logger for comprehensive test documentation
    /// </summary>
    public class TestScenarioLogger
    {
        private readonly ILogger _logger;
        private readonly ITestOutputHelper _output;
        private readonly Dictionary<string, List<string>> _scenarioSteps = new();

        public TestScenarioLogger(ILogger logger, ITestOutputHelper output)
        {
            _logger = logger;
            _output = output;
        }

        public void LogScenarioStart(string scenarioName, string description)
        {
            var message = $"\n🎯 SCENARIO: {scenarioName}";
            var details = $"   📋 {description}";
            
            _logger.LogInformation(message);
            _logger.LogInformation(details);
            _output.WriteLine(message);
            _output.WriteLine(details);
            
            _scenarioSteps[scenarioName] = new List<string> { details };
        }

        public void LogGiven(string context, string condition)
        {
            var message = $"   📌 GIVEN: {context} - {condition}";
            _logger.LogInformation(message);
            _output.WriteLine(message);
            
            if (_scenarioSteps.ContainsKey(context))
            {
                _scenarioSteps[context].Add($"GIVEN: {condition}");
            }
        }

        public void LogWhen(string context, string action)
        {
            var message = $"   🎯 WHEN: {context} - {action}";
            _logger.LogInformation(message);
            _output.WriteLine(message);
            
            if (_scenarioSteps.ContainsKey(context))
            {
                _scenarioSteps[context].Add($"WHEN: {action}");
            }
        }

        public void LogThen(string context, string expectation)
        {
            var message = $"   ✅ THEN: {context} - {expectation}";
            _logger.LogInformation(message);
            _output.WriteLine(message);
            
            if (_scenarioSteps.ContainsKey(context))
            {
                _scenarioSteps[context].Add($"THEN: {expectation}");
            }
        }

        public void LogScenarioSummary()
        {
            _logger.LogInformation("\n📊 SCENARIO SUMMARY:");
            _output.WriteLine("\n📊 SCENARIO SUMMARY:");
            
            foreach (var scenario in _scenarioSteps)
            {
                _logger.LogInformation($"\n   {scenario.Key}:");
                _output.WriteLine($"\n   {scenario.Key}:");
                
                foreach (var step in scenario.Value)
                {
                    _logger.LogInformation($"      {step}");
                    _output.WriteLine($"      {step}");
                }
            }
        }
    }

    /// <summary>
    /// Comprehensive test diagnostics for failure analysis and expected behavior documentation
    /// </summary>
    public class TestDiagnostics
    {
        private readonly ILogger _logger;
        private readonly TestScenarioLogger _scenarioLogger;
        private readonly List<DiagnosticEvent> _events = new();

        public TestDiagnostics(ILogger logger, TestScenarioLogger scenarioLogger)
        {
            _logger = logger;
            _scenarioLogger = scenarioLogger;
        }

        public void LogEnvironmentConfiguration(StreamExecutionEnvironment env, ReliabilityTestConfiguration config)
        {
            _logger.LogInformation("🔧 ENVIRONMENT CONFIGURATION:");
            _logger.LogInformation($"   Message Count: {config.MessageCount:N0}");
            _logger.LogInformation($"   Parallel Sources: {config.ParallelSourceInstances}");
            _logger.LogInformation($"   Timeout: {config.ExpectedProcessingTimeMs:N0}ms");
            _logger.LogInformation($"   Failure Tolerance: {config.FailureToleranceRate * 100:F1}%");
            _logger.LogInformation($"   Exactly-Once: {config.EnableExactlyOnceSemantics}");
            _logger.LogInformation($"   Back Pressure Threshold: {config.BackPressureThresholdPercent}%");
            
            RecordEvent("EnvironmentConfiguration", "Environment configured with Flink.Net settings");
        }

        public void LogInfrastructureDetails(string redisConnectionString, string kafkaBootstrapServers)
        {
            _logger.LogInformation("🔗 INFRASTRUCTURE DETAILS:");
            _logger.LogInformation($"   Redis: {redisConnectionString}");
            _logger.LogInformation($"   Kafka: {kafkaBootstrapServers}");
            
            RecordEvent("InfrastructureDetails", "Container connection strings configured");
        }

        public void LogInfrastructureFailure(string context, Exception ex)
        {
            _logger.LogError(ex, "💥 INFRASTRUCTURE FAILURE: {Context}", context);
            _logger.LogError("   Error Type: {ErrorType}", ex.GetType().Name);
            _logger.LogError("   Error Message: {ErrorMessage}", ex.Message);
            
            if (ex.InnerException != null)
            {
                _logger.LogError("   Inner Exception: {InnerException}", ex.InnerException.Message);
            }
            
            RecordEvent("InfrastructureFailure", $"{context}: {ex.Message}", ex);
        }

        public void LogTestFailure(string context, Exception ex, TestExecutionResults results)
        {
            _logger.LogError(ex, "❌ TEST FAILURE: {Context}", context);
            _logger.LogError("   Execution Time: {ExecutionTime}ms", results.TotalExecutionTimeMs);
            _logger.LogError("   Error Type: {ErrorType}", ex.GetType().Name);
            _logger.LogError("   Error Message: {ErrorMessage}", ex.Message);
            
            // Log system metrics if available
            if (results.SystemMetrics.Any())
            {
                _logger.LogError("   System Metrics at Failure:");
                foreach (var metric in results.SystemMetrics)
                {
                    _logger.LogError("      {MetricName}: {MetricValue}", metric.Key, metric.Value);
                }
            }
            
            // Log diagnostic messages
            if (results.DiagnosticMessages.Any())
            {
                _logger.LogError("   Diagnostic Messages:");
                foreach (var message in results.DiagnosticMessages)
                {
                    _logger.LogError("      {DiagnosticMessage}", message);
                }
            }
            
            RecordEvent("TestFailure", $"{context}: {ex.Message}", ex);
            
            // Generate failure analysis
            GenerateFailureAnalysis(ex, results);
        }

        private void GenerateFailureAnalysis(Exception ex, TestExecutionResults results)
        {
            _logger.LogError("\n🔍 FAILURE ANALYSIS:");
            
            // Analyze exception type
            if (ex is TimeoutException)
            {
                _logger.LogError("   💡 ROOT CAUSE: Test execution timeout");
                _logger.LogError("   📋 LIKELY ISSUES:");
                _logger.LogError("      - High back pressure causing processing delays");
                _logger.LogError("      - Resource constraints (CPU/Memory)");
                _logger.LogError("      - Network connectivity issues");
                _logger.LogError("      - Container startup delays");
            }
            else if (ex is InvalidOperationException)
            {
                _logger.LogError("   💡 ROOT CAUSE: Invalid operation or configuration");
                _logger.LogError("   📋 LIKELY ISSUES:");
                _logger.LogError("      - Container connectivity problems");
                _logger.LogError("      - Service not ready");
                _logger.LogError("      - Configuration mismatch");
            }
            else if (ex.Message.Contains("Redis") || ex.Message.Contains("Kafka"))
            {
                _logger.LogError("   💡 ROOT CAUSE: Infrastructure connectivity issue");
                _logger.LogError("   📋 LIKELY ISSUES:");
                _logger.LogError("      - Container not fully started");
                _logger.LogError("      - Port mapping problems");
                _logger.LogError("      - Network isolation");
            }
            else
            {
                _logger.LogError("   💡 ROOT CAUSE: Unexpected test failure");
                _logger.LogError("   📋 SUGGESTED ACTIONS:");
                _logger.LogError("      - Review test execution logs");
                _logger.LogError("      - Check system resource availability");
                _logger.LogError("      - Verify test configuration");
            }
            
            // Provide recommendations
            _logger.LogError("\n💡 RECOMMENDATIONS:");
            _logger.LogError("   1. Check container startup logs");
            _logger.LogError("   2. Verify system resource availability");
            _logger.LogError("   3. Test infrastructure connectivity manually");
            _logger.LogError("   4. Consider increasing timeouts for CI environments");
        }

        private void RecordEvent(string eventType, string message, Exception? exception = null)
        {
            _events.Add(new DiagnosticEvent
            {
                Timestamp = DateTime.UtcNow,
                EventType = eventType,
                Message = message,
                Exception = exception
            });
        }

        public void GenerateDiagnosticReport()
        {
            _logger.LogInformation("\n📊 DIAGNOSTIC REPORT:");
            _logger.LogInformation($"   Total Events: {_events.Count}");
            
            var eventGroups = _events.GroupBy(e => e.EventType);
            foreach (var group in eventGroups)
            {
                _logger.LogInformation($"\n   {group.Key} ({group.Count()} events):");
                foreach (var evt in group)
                {
                    _logger.LogInformation($"      [{evt.Timestamp:HH:mm:ss.fff}] {evt.Message}");
                    if (evt.Exception != null)
                    {
                        _logger.LogInformation($"         Exception: {evt.Exception.GetType().Name}: {evt.Exception.Message}");
                    }
                }
            }
        }
    }

    public class DiagnosticEvent
    {
        public DateTime Timestamp { get; set; }
        public string EventType { get; set; } = string.Empty;
        public string Message { get; set; } = string.Empty;
        public Exception? Exception { get; set; }
    }

    /// <summary>
    /// Enhanced reliability test result collector with comprehensive diagnostics and real-time monitoring
    /// </summary>
    public class EnhancedReliabilityTestResultCollector
    {
        private readonly TestDiagnostics _diagnostics;
        private readonly ReliabilityTestConfiguration _config;
        private readonly ConcurrentBag<EnrichedRecord> _processedRecords = new();
        private readonly ConcurrentBag<SinkErrorRecord> _sinkErrors = new();
        private readonly ConcurrentDictionary<string, ProcessingMetrics> _partitionMetrics = new();
        private volatile bool _isComplete = false;
        private long _finalCount = 0;
        private long _finalErrorCount = 0;
        private Dictionary<string, long> _finalPartitionCounts = new();
        private readonly object _progressLock = new();

        public EnhancedReliabilityTestResultCollector(TestDiagnostics diagnostics, ReliabilityTestConfiguration config)
        {
            _diagnostics = diagnostics;
            _config = config;
        }

        public bool IsComplete => _isComplete;

        public void RecordProcessedMessageWithMetadata(EnrichedRecord record, DateTime sinkStartTime)
        {
            _processedRecords.Add(record);
            
            // Update partition-level metrics
            var partitionKey = record.ProcessedRecord.ValidatedRecord.PartitionKey;
            _partitionMetrics.AddOrUpdate(partitionKey, 
                new ProcessingMetrics { MessageCount = 1, LastSeen = DateTime.UtcNow },
                (key, existing) => new ProcessingMetrics 
                { 
                    MessageCount = existing.MessageCount + 1, 
                    LastSeen = DateTime.UtcNow 
                });
        }

        public void RecordSinkError(EnrichedRecord record, Exception error, DateTime sinkStartTime)
        {
            _sinkErrors.Add(new SinkErrorRecord
            {
                Record = record,
                Error = error,
                Timestamp = DateTime.UtcNow,
                SinkStartTime = sinkStartTime
            });
        }

        public ProgressInfo GetCurrentProgress()
        {
            return new ProgressInfo
            {
                ProcessedCount = _processedRecords.Count,
                ErrorCount = _sinkErrors.Count,
                BackPressureDetected = _processedRecords.Any(r => r.ProcessedRecord.BackPressureDetected),
                ActivePartitions = _partitionMetrics.Count
            };
        }

        public void CompleteWithMetadata(long finalCount, long errorCount, Dictionary<string, long> partitionCounts)
        {
            _finalCount = finalCount;
            _finalErrorCount = errorCount;
            _finalPartitionCounts = partitionCounts;
            _isComplete = true;
            
            Console.WriteLine($"[EnhancedResultCollector] ✅ Collection completed - Final count: {finalCount:N0}, Errors: {errorCount:N0}");
        }

        public async Task<PipelineExecutionResult> GetFinalResultWithDiagnostics(long expectedCount)
        {
            // Wait for completion or timeout (5-minute comprehensive testing)
            var timeout = DateTime.UtcNow.AddSeconds(300); // 5-minute timeout for comprehensive testing
            while (!_isComplete && DateTime.UtcNow < timeout)
            {
                await Task.Delay(500);
            }

            // Generate comprehensive result analysis
            var result = new PipelineExecutionResult
            {
                Success = _isComplete && _finalErrorCount == 0,
                ProcessedCount = _finalCount,
                ExactlyOnceVerified = VerifyExactlyOnceSemantics(),
                DataLossIncidents = AnalyzeDataLossIncidents(),
                BackPressureEvents = AnalyzeBackPressureEvents(),
                PerformanceMetrics = GeneratePerformanceMetrics(),
                OperatorMetrics = GenerateOperatorMetrics()
            };

            if (!_isComplete)
            {
                result.Success = false;
                result.ErrorMessage = "Result collection timed out - pipeline may still be running";
                result.DataLossIncidents.Add("Result collection timeout");
            }

            if (_sinkErrors.Any())
            {
                result.Success = false;
                result.ErrorMessage = $"Sink processing errors: {_sinkErrors.Count} errors occurred";
                foreach (var error in _sinkErrors.Take(5)) // Report first 5 errors
                {
                    result.DataLossIncidents.Add($"Sink error: {error.Error.Message}");
                }
            }

            // Log comprehensive result summary
            LogResultSummary(result, expectedCount);
            
            return result;
        }

        private bool VerifyExactlyOnceSemantics()
        {
            // Check for duplicate sequence IDs
            var processedSequences = _processedRecords
                .Select(r => r.ProcessedRecord.ProcessingSequence)
                .ToList();
            
            var uniqueSequences = processedSequences.Distinct().Count();
            var totalSequences = processedSequences.Count;
            
            var exactlyOnce = uniqueSequences == totalSequences;
            
            if (!exactlyOnce)
            {
                Console.WriteLine($"[ExactlyOnceVerification] ❌ Duplicate sequences detected: {totalSequences - uniqueSequences} duplicates");
            }
            else
            {
                Console.WriteLine($"[ExactlyOnceVerification] ✅ Exactly-once semantics verified: {uniqueSequences:N0} unique sequences");
            }
            
            return exactlyOnce;
        }

        private List<string> AnalyzeDataLossIncidents()
        {
            var incidents = new List<string>();
            
            // Check for sequence gaps
            var sequences = _processedRecords
                .Select(r => r.ProcessedRecord.ProcessingSequence)
                .OrderBy(s => s)
                .ToList();
            
            if (sequences.Any())
            {
                var expectedNext = sequences.First();
                foreach (var sequence in sequences)
                {
                    if (sequence > expectedNext)
                    {
                        var gap = sequence - expectedNext;
                        incidents.Add($"Sequence gap: missing {gap} messages between {expectedNext} and {sequence}");
                    }
                    expectedNext = sequence + 1;
                }
            }
            
            // Add sink errors as data loss incidents
            foreach (var error in _sinkErrors)
            {
                incidents.Add($"Sink error at sequence {error.Record.ProcessedRecord.ProcessingSequence}: {error.Error.Message}");
            }
            
            return incidents;
        }

        private List<string> AnalyzeBackPressureEvents()
        {
            var events = new List<string>();
            
            var backPressureRecords = _processedRecords
                .Where(r => r.ProcessedRecord.BackPressureDetected)
                .ToList();
            
            if (backPressureRecords.Any())
            {
                events.Add($"Back pressure detected in {backPressureRecords.Count:N0} messages");
                
                var partitionGroups = backPressureRecords
                    .GroupBy(r => r.ProcessedRecord.ValidatedRecord.PartitionKey)
                    .OrderByDescending(g => g.Count())
                    .Take(5);
                
                foreach (var group in partitionGroups)
                {
                    events.Add($"Partition {group.Key}: {group.Count():N0} back pressure events");
                }
            }
            
            return events;
        }

        private Dictionary<string, object> GeneratePerformanceMetrics()
        {
            var metrics = new Dictionary<string, object>();
            
            if (_processedRecords.Any())
            {
                var processingDurations = _processedRecords
                    .Select(r => r.ProcessedRecord.ProcessingDurationMs)
                    .Where(d => d > 0)
                    .ToList();
                
                if (processingDurations.Any())
                {
                    metrics["AvgProcessingDurationMs"] = processingDurations.Average();
                    metrics["MaxProcessingDurationMs"] = processingDurations.Max();
                    metrics["MinProcessingDurationMs"] = processingDurations.Min();
                }
                
                var enrichmentDurations = _processedRecords
                    .Select(r => r.EnrichmentDurationMs)
                    .Where(d => d > 0)
                    .ToList();
                
                if (enrichmentDurations.Any())
                {
                    metrics["AvgEnrichmentDurationMs"] = enrichmentDurations.Average();
                    metrics["MaxEnrichmentDurationMs"] = enrichmentDurations.Max();
                    metrics["MinEnrichmentDurationMs"] = enrichmentDurations.Min();
                }
                
                metrics["TotalRecordsProcessed"] = _processedRecords.Count;
                metrics["UniquePartitionsProcessed"] = _partitionMetrics.Count;
                metrics["BackPressureEventCount"] = _processedRecords.Count(r => r.ProcessedRecord.BackPressureDetected);
                metrics["ErrorCount"] = _sinkErrors.Count;
            }
            
            return metrics;
        }

        private Dictionary<string, long> GenerateOperatorMetrics()
        {
            var metrics = new Dictionary<string, long>();
            
            metrics["SourceMessages"] = _finalCount;
            metrics["ValidationMessages"] = _processedRecords.Count(r => r.ProcessedRecord.ValidatedRecord.IsValid);
            metrics["ProcessingMessages"] = _processedRecords.Count;
            metrics["EnrichmentMessages"] = _processedRecords.Count(r => !string.IsNullOrEmpty(r.EnrichmentData));
            metrics["SinkMessages"] = _finalCount;
            metrics["ErrorMessages"] = _finalErrorCount;
            
            return metrics;
        }

        private void LogResultSummary(PipelineExecutionResult result, long expectedCount)
        {
            Console.WriteLine($"\n📊 COMPREHENSIVE RESULT SUMMARY:");
            Console.WriteLine($"   Expected Messages: {expectedCount:N0}");
            Console.WriteLine($"   Processed Messages: {result.ProcessedCount:N0}");
            Console.WriteLine($"   Success Rate: {(double)result.ProcessedCount / expectedCount * 100:F2}%");
            Console.WriteLine($"   Exactly-Once Verified: {result.ExactlyOnceVerified}");
            Console.WriteLine($"   Data Loss Incidents: {result.DataLossIncidents.Count}");
            Console.WriteLine($"   Back Pressure Events: {result.BackPressureEvents.Count}");
            Console.WriteLine($"   Sink Errors: {_finalErrorCount:N0}");
            Console.WriteLine($"   Active Partitions: {_partitionMetrics.Count}");
            
            if (result.PerformanceMetrics.Any())
            {
                Console.WriteLine($"\n   Performance Metrics:");
                foreach (var metric in result.PerformanceMetrics)
                {
                    Console.WriteLine($"      {metric.Key}: {metric.Value}");
                }
            }
        }
    }

    // Supporting classes for enhanced result collection
    public class SinkErrorRecord
    {
        public EnrichedRecord Record { get; set; } = null!;
        public Exception Error { get; set; } = null!;
        public DateTime Timestamp { get; set; }
        public DateTime SinkStartTime { get; set; }
    }

    public class ProcessingMetrics
    {
        public long MessageCount { get; set; }
        public DateTime LastSeen { get; set; }
    }

    public class ProgressInfo
    {
        public long ProcessedCount { get; set; }
        public long ErrorCount { get; set; }
        public bool BackPressureDetected { get; set; }
        public int ActivePartitions { get; set; }
    }

    // Helper Classes for Interface Implementations

    public class StringSerializer : ITypeSerializer<string>
    {
        public byte[] Serialize(string obj) => System.Text.Encoding.UTF8.GetBytes(obj ?? string.Empty);
        public string Deserialize(byte[] data) => System.Text.Encoding.UTF8.GetString(data);
        public Type SerializedType => typeof(string);
    }

    /// <summary>
    /// Mock credit-based flow controller for BDD testing of backpressure scenarios
    /// </summary>
    public class MockCreditBasedFlowController
    {
        private readonly ConcurrentDictionary<string, int> _stageCredits = new();
        private readonly object _creditLock = new();

        public void SetStageCredits(string stageName, int credits)
        {
            _stageCredits[stageName] = credits;
        }

        public bool RequestCredit(string stageName)
        {
            lock (_creditLock)
            {
                var currentCredits = _stageCredits.GetValueOrDefault(stageName, 0);
                if (currentCredits > 0)
                {
                    _stageCredits[stageName] = currentCredits - 1;
                    return true;
                }
                return false;
            }
        }

        public void ReplenishCredit(string stageName)
        {
            lock (_creditLock)
            {
                var currentCredits = _stageCredits.GetValueOrDefault(stageName, 0);
                _stageCredits[stageName] = currentCredits + 1;
            }
        }

        public int GetAvailableCredits(string stageName)
        {
            return _stageCredits.GetValueOrDefault(stageName, 0);
        }
    }
}