using FlinkDotNet.Proto.Internal;
using Grpc.Net.Client;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.Logging;
using Google.Protobuf.WellKnownTypes;
using System.Text.Json;

namespace FlinkJobSimulator
{
    /// <summary>
    /// Service responsible for submitting jobs to JobManager following Apache Flink 2.0 architecture.
    /// Instead of acting as a direct consumer, FlinkJobSimulator creates and submits JobGraphs to JobManager,
    /// which then deploys tasks to the 20 registered TaskManagers for proper load distribution.
    /// </summary>
    public class JobSubmissionService
    {
        private readonly IConfiguration _configuration;
        private readonly ILogger<JobSubmissionService> _logger;
        private readonly string _jobManagerAddress;

        public JobSubmissionService(IConfiguration configuration, ILogger<JobSubmissionService> logger)
        {
            _configuration = configuration ?? throw new ArgumentNullException(nameof(configuration));
            _logger = logger ?? throw new ArgumentNullException(nameof(logger));
            
            // Resolve JobManager address for gRPC communication
            _jobManagerAddress = ResolveJobManagerAddress();
            _logger.LogInformation("JobSubmissionService configured to use JobManager at: {JobManagerAddress}", _jobManagerAddress);
        }

        /// <summary>
        /// Create and submit a Kafka-to-Redis streaming job to JobManager.
        /// This follows Apache Flink 2.0 pattern where JobManager coordinates task deployment across TaskManagers.
        /// </summary>
        public async Task<bool> SubmitKafkaToRedisStreamingJobAsync()
        {
            _logger.LogInformation("🚀 Creating JobGraph for Kafka-to-Redis streaming job following Apache Flink 2.0 architecture");

            var jobGraph = CreateKafkaToRedisJobGraph();

            var request = new SubmitJobRequest
            {
                JobGraph = jobGraph
            };

            const int maxAttempts = 5;
            for (int attempt = 1; attempt <= maxAttempts; attempt++)
            {
                try
                {
                    _logger.LogInformation("📤 Attempt {Attempt}/{Max} submitting JobGraph to JobManager at {JobManagerAddress}", attempt, maxAttempts, _jobManagerAddress);

                    var channelOptions = new GrpcChannelOptions();
                    
                    // Configure insecure transport if needed (for CI/development environments)
                    var allowUnsecured = Environment.GetEnvironmentVariable("ASPIRE_ALLOW_UNSECURED_TRANSPORT");
                    if (string.Equals(allowUnsecured, "true", StringComparison.OrdinalIgnoreCase))
                    {
                        _logger.LogInformation("🔓 Using insecure gRPC transport for CI/development environment");
                        channelOptions.Credentials = Grpc.Core.ChannelCredentials.Insecure;
                    }

                    using var channel = GrpcChannel.ForAddress(_jobManagerAddress, channelOptions);
                    var client = new JobManagerInternalService.JobManagerInternalServiceClient(channel);

                    var response = await client.SubmitJobAsync(request);

                    if (response.Success)
                    {
                        _logger.LogInformation("✅ Job submitted successfully! JobId: {JobId}, Message: {Message}", response.JobId, response.Message);
                        await Program.WriteRunningStateLogAsync();
                        return true;
                    }

                    _logger.LogWarning("❌ Job submission attempt {Attempt} failed: {Message}", attempt, response.Message);
                }
                catch (Exception ex)
                {
                    _logger.LogWarning(ex, "⚠️ Job submission attempt {Attempt}/{Max} failed", attempt, maxAttempts);
                }

                if (attempt < maxAttempts)
                {
                    await Task.Delay(2000);
                }
            }

            _logger.LogError("❌ All attempts to submit job to JobManager at {JobManagerAddress} failed", _jobManagerAddress);
            return false;
        }

        /// <summary>
        /// Create a JobGraph for Kafka-to-Redis streaming processing.
        /// This defines the computation topology that JobManager will deploy across TaskManagers.
        /// </summary>
        private JobGraph CreateKafkaToRedisJobGraph()
        {
            var kafkaTopic = _configuration["SIMULATOR_KAFKA_TOPIC"] ?? "flinkdotnet.sample.topic";
            var redisSinkCounterKey = _configuration["SIMULATOR_REDIS_KEY_SINK_COUNTER"] ?? "flinkdotnet:sample:processed_message_counter";
            var globalSequenceKey = _configuration["SIMULATOR_REDIS_KEY_GLOBAL_SEQUENCE"] ?? "flinkdotnet:global_sequence_id";
            var numMessages = _configuration["SIMULATOR_NUM_MESSAGES"] ?? "1000000";
            
            // Create source vertex (Kafka consumer)
            var sourceVertex = new JobVertex
            {
                Id = "kafka-source-vertex",
                Name = "Kafka Source",
                VertexType = FlinkDotNet.Proto.Internal.VertexType.Source,
                Parallelism = 20, // Distribute across all 20 TaskManagers
                InputTypeName = "System.String",
                OutputTypeName = "System.String",
                InputSerializerTypeName = "FlinkDotNet.Core.Serializers.StringSerializer",
                OutputSerializerTypeName = "FlinkDotNet.Core.Serializers.StringSerializer",
                OperatorDefinition = new OperatorDefinition
                {
                    FullyQualifiedName = "FlinkJobSimulator.FlinkKafkaSourceFunction",
                    Configuration = Struct.Parser.ParseJson(JsonSerializer.Serialize(new Dictionary<string, object>
                    {
                        ["topic"] = kafkaTopic,
                        ["consumerGroupId"] = "flinkdotnet-stress-test-consumer-group",
                        ["checkpointingEnabled"] = true,
                        ["exactlyOnceMode"] = true
                    }))
                }
            };

            // Create sink vertex (Redis counter updates)
            var sinkVertex = new JobVertex
            {
                Id = "redis-sink-vertex", 
                Name = "Redis Sink",
                VertexType = FlinkDotNet.Proto.Internal.VertexType.Sink,
                Parallelism = 20, // Distribute across all 20 TaskManagers for high throughput
                InputTypeName = "System.String",
                OutputTypeName = "System.String",
                InputSerializerTypeName = "FlinkDotNet.Core.Serializers.StringSerializer", 
                OutputSerializerTypeName = "FlinkDotNet.Core.Serializers.StringSerializer",
                OperatorDefinition = new OperatorDefinition
                {
                    FullyQualifiedName = "FlinkJobSimulator.RedisIncrementSinkFunction",
                    Configuration = Struct.Parser.ParseJson(JsonSerializer.Serialize(new Dictionary<string, object>
                    {
                        ["redisSinkCounterKey"] = redisSinkCounterKey,
                        ["globalSequenceKey"] = globalSequenceKey,
                        ["expectedMessages"] = int.Parse(numMessages),
                        ["highThroughputMode"] = true
                    }))
                }
            };

            // Add edge IDs to vertices
            sourceVertex.OutputEdgeIds.Add("source-to-sink-edge");
            sinkVertex.InputEdgeIds.Add("source-to-sink-edge");

            // Create edge connecting source to sink
            var sourceToSinkEdge = new JobEdge
            {
                Id = "source-to-sink-edge",
                SourceVertexId = "kafka-source-vertex",
                TargetVertexId = "redis-sink-vertex", 
                ShuffleMode = FlinkDotNet.Proto.Internal.ShuffleMode.Hash, // Hash partitioning for load balancing
                DataTypeName = "System.String",
                SerializerTypeName = "FlinkDotNet.Core.Serializers.StringSerializer"
            };

            // Create the complete JobGraph
            var jobGraph = new JobGraph
            {
                JobName = "Kafka-to-Redis Streaming Job",
                Status = "SUBMITTED",
                SubmissionTime = Timestamp.FromDateTime(DateTime.UtcNow)
            };

            jobGraph.Vertices.Add(sourceVertex);
            jobGraph.Vertices.Add(sinkVertex);
            jobGraph.Edges.Add(sourceToSinkEdge);

            // Add serializer registrations
            jobGraph.SerializerTypeRegistrations["System.String"] = "FlinkDotNet.Core.Serializers.StringSerializer";

            _logger.LogInformation("✅ JobGraph created with {VertexCount} vertices and {EdgeCount} edges, total parallelism: {TotalParallelism}",
                jobGraph.Vertices.Count, jobGraph.Edges.Count, jobGraph.Vertices.Sum(v => v.Parallelism));

            return jobGraph;
        }

        /// <summary>
        /// Resolve JobManager gRPC address using Aspire service discovery
        /// </summary>
        private string ResolveJobManagerAddress()
        {
            // Try Aspire service reference first (when running in Aspire)
            // When AppHost uses .WithReference(jobManager), Aspire creates connection strings like "ConnectionStrings__jobmanager"
            var aspireJobManagerUrl = _configuration.GetConnectionString("jobmanager");
            if (!string.IsNullOrEmpty(aspireJobManagerUrl))
            {
                _logger.LogInformation("🔍 Using Aspire service discovery for JobManager: {AspireUrl}", aspireJobManagerUrl);
                return aspireJobManagerUrl;
            }

            // Try alternative Aspire configuration patterns
            // Aspire may also create service entries like "services__jobmanager__http__0" or "services__jobmanager__grpc__0"
            var aspireServiceUrl = _configuration["services:jobmanager:grpc:0"];
            if (!string.IsNullOrEmpty(aspireServiceUrl))
            {
                _logger.LogInformation("🔍 Using Aspire service configuration for JobManager gRPC: {AspireUrl}", aspireServiceUrl);
                return aspireServiceUrl;
            }

            // Try Aspire HTTP endpoint configuration and use it for gRPC (since .NET projects often share HTTP/gRPC on same port)
            var aspireHttpUrl = _configuration["services:jobmanager:http:0"];
            if (!string.IsNullOrEmpty(aspireHttpUrl))
            {
                // For .NET gRPC services, HTTP and gRPC typically use the same port
                _logger.LogInformation("🔍 Using Aspire HTTP endpoint for JobManager gRPC (same port): {GrpcUrl}", aspireHttpUrl);
                return aspireHttpUrl;
            }

            // Try direct connection string patterns that Aspire might use
            foreach (var (key, value) in _configuration.AsEnumerable())
            {
                if (key != null && value != null && 
                    key.Contains("jobmanager", StringComparison.OrdinalIgnoreCase) &&
                    (value.StartsWith("http://", StringComparison.OrdinalIgnoreCase) || 
                     value.StartsWith("https://", StringComparison.OrdinalIgnoreCase)))
                {
                    _logger.LogInformation("🔍 Found JobManager URL via configuration key {Key}: {Url}", key, value);
                    return value;
                }
            }

            // Log all available configuration keys for debugging
            _logger.LogWarning("🔍 JobManager address not found via Aspire service discovery. Available configuration keys:");
            foreach (var item in _configuration.AsEnumerable())
            {
                if (item.Key != null && (item.Key.Contains("jobmanager", StringComparison.OrdinalIgnoreCase) || 
                    item.Key.Contains("ConnectionStrings", StringComparison.OrdinalIgnoreCase) ||
                    item.Key.Contains("services", StringComparison.OrdinalIgnoreCase)))
                {
                    _logger.LogWarning("  {Key} = {Value}", item.Key, item.Value ?? "null");
                }
            }

            // Check if we have Aspire unsecured transport enabled
            var allowUnsecured = Environment.GetEnvironmentVariable("ASPIRE_ALLOW_UNSECURED_TRANSPORT");
            var useInsecure = string.Equals(allowUnsecured, "true", StringComparison.OrdinalIgnoreCase);

            // For Aspire mode, try to discover the JobManager service at the standard fixed ports
            // Since JobManager uses combined HTTP1/HTTP2 endpoint in Aspire mode, use the HTTP port for gRPC
            var protocol = useInsecure ? "http" : "https";
            var aspireJobManagerPort = "8080"; // JobManager HTTP port that also serves gRPC in Aspire mode
            var aspireUrl = $"{protocol}://localhost:{aspireJobManagerPort}";
            
            _logger.LogWarning("🔍 Using Aspire fallback JobManager address: {AspireUrl} (combined HTTP1/HTTP2 endpoint)", aspireUrl);
            return aspireUrl;
        }
    }
}