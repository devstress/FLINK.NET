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

                    using var channel = GrpcChannel.ForAddress(_jobManagerAddress);
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
        /// Resolve JobManager gRPC address using Aspire service discovery or environment variables
        /// </summary>
        private string ResolveJobManagerAddress()
        {
            // Try Aspire service reference first (when running in Aspire)
            // Aspire creates connection strings in format: "services__jobmanager__grpc__0"
            var aspireJobManagerUrl = _configuration.GetConnectionString("jobmanager");
            if (!string.IsNullOrEmpty(aspireJobManagerUrl))
            {
                _logger.LogInformation("🔍 Using Aspire service discovery for JobManager: {AspireUrl}", aspireJobManagerUrl);
                return aspireJobManagerUrl;
            }

            // Try alternative Aspire configuration keys
            var aspireServiceUrl = _configuration["services:jobmanager:grpc:0"];
            if (!string.IsNullOrEmpty(aspireServiceUrl))
            {
                _logger.LogInformation("🔍 Using Aspire service configuration for JobManager: {AspireUrl}", aspireServiceUrl);
                return aspireServiceUrl;
            }

            // Try environment variable for full URL
            var envJobManagerUrl = Environment.GetEnvironmentVariable("JOBMANAGER_GRPC_ADDRESS")
                                   ?? Environment.GetEnvironmentVariable("DOTNET_JOBMANAGER_GRPC_ADDRESS");
            if (!string.IsNullOrEmpty(envJobManagerUrl))
            {
                _logger.LogInformation("🔍 Using environment variable for JobManager: {EnvUrl}", envJobManagerUrl);
                return envJobManagerUrl;
            }

            // Try environment variable for port only
            var envPortString = Environment.GetEnvironmentVariable("DOTNET_JOBMANAGER_GRPC_PORT");
            if (!string.IsNullOrEmpty(envPortString) && int.TryParse(envPortString, out var envPort))
            {
                var insecure = string.Equals(Environment.GetEnvironmentVariable("ASPIRE_ALLOW_UNSECURED_TRANSPORT"), "true", StringComparison.OrdinalIgnoreCase);
                var proto = insecure ? "http" : "https";
                var envUrl = $"{proto}://localhost:{envPort}";
                _logger.LogInformation("🔍 Using DOTNET_JOBMANAGER_GRPC_PORT to build JobManager URL: {EnvUrl}", envUrl);
                return envUrl;
            }

            // Check if we have Aspire unsecured transport enabled
            var allowUnsecured = Environment.GetEnvironmentVariable("ASPIRE_ALLOW_UNSECURED_TRANSPORT");
            var useInsecure = string.Equals(allowUnsecured, "true", StringComparison.OrdinalIgnoreCase);

            // Default fallback for local development
            var protocol = useInsecure ? "http" : "https";
            var defaultPort = useInsecure ? "50051" : "8081"; // Use gRPC default port 50051 for HTTP
            var defaultUrl = $"{protocol}://localhost:{defaultPort}";
            _logger.LogInformation("🔍 Using default JobManager address: {DefaultUrl} (unsecured: {UseInsecure})", defaultUrl, useInsecure);
            return defaultUrl;
        }
    }
}