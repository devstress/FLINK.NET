using System.Diagnostics.CodeAnalysis;

namespace FlinkDotNet.Common.Constants;

/// <summary>
/// Default port configurations for FlinkDotNet services
/// These values are used as fallbacks when environment variables are not set
/// </summary>
public static class ServicePorts
{
    /// <summary>
    /// Default port for JobManager gRPC service (fallback when JOBMANAGER_GRPC_PORT env var not set)
    /// </summary>
    public static int JobManagerGrpc => int.TryParse(Environment.GetEnvironmentVariable(EnvironmentVariables.JobManagerGrpcPort), out var port) ? port : 50051;

    /// <summary>
    /// Default port for JobManager HTTP API (fallback when JOBMANAGER_HTTP_PORT env var not set)
    /// </summary>
    public static int JobManagerHttp => int.TryParse(Environment.GetEnvironmentVariable(EnvironmentVariables.JobManagerHttpPort), out var port) ? port : 8088;

    /// <summary>
    /// Default port for Kafka service (dynamically assigned by Aspire)
    /// </summary>
    public static int Kafka => int.TryParse(Environment.GetEnvironmentVariable(EnvironmentVariables.KafkaPort), out var port) ? port : 9092;

    /// <summary>
    /// Default port for Redis service (dynamically assigned by Aspire)
    /// </summary>
    public static int Redis => int.TryParse(Environment.GetEnvironmentVariable(EnvironmentVariables.RedisPort), out var port) ? port : 6379;

    /// <summary>
    /// Default port for TaskManager gRPC service (fallback when TASKMANAGER_GRPC_PORT env var not set)
    /// </summary>
    public static int TaskManagerGrpc => int.TryParse(Environment.GetEnvironmentVariable(EnvironmentVariables.TaskManagerGrpcPort), out var port) ? port : 40051;


}

/// <summary>
/// Default host configurations for FlinkDotNet services
/// </summary>
public static class ServiceHosts
{
    /// <summary>
    /// Default localhost address
    /// </summary>
    public const string Localhost = "localhost";

    /// <summary>
    /// Default local IP address
    /// </summary>
    public const string LocalIP = "127.0.0.1";
}

/// <summary>
/// Service URI configurations combining hosts and ports
/// </summary>
public static class ServiceUris
{
    /// <summary>
    /// Default JobManager gRPC address (secure HTTPS)
    /// </summary>
    public static string JobManagerGrpc => $"https://{ServiceHosts.Localhost}:{ServicePorts.JobManagerGrpc}";

    /// <summary>
    /// Default JobManager HTTP API address (secure HTTPS)
    /// </summary>
    public static string JobManagerHttp => $"https://{ServiceHosts.Localhost}:{ServicePorts.JobManagerHttp}";

    /// <summary>
    /// Default Kafka bootstrap servers
    /// </summary>
    public static string KafkaBootstrapServers => $"{ServiceHosts.Localhost}:{ServicePorts.Kafka}";

    /// <summary>
    /// Default Redis connection string
    /// </summary>
    public static string RedisConnectionString => $"{ServiceHosts.Localhost}:{ServicePorts.Redis}";

    /// <summary>
    /// Default TaskManager gRPC address (secure HTTPS)
    /// </summary>
    /// <returns>TaskManager gRPC URI</returns>
    public static string TaskManagerGrpc() => $"https://{ServiceHosts.Localhost}:{ServicePorts.TaskManagerGrpc}";

    /// <summary>
    /// TaskManager gRPC address with specified port (secure HTTPS)
    /// </summary>
    /// <param name="port">Custom port number</param>
    /// <returns>TaskManager gRPC URI</returns>
    public static string TaskManagerGrpc(int port) => $"https://{ServiceHosts.Localhost}:{port}";

    /// <summary>
    /// Insecure service URIs for local development only
    /// </summary>
    /// <summary>
    /// Service URIs for insecure HTTP connections.
    /// WARNING: These should only be used for local development and testing.
    /// For production environments, use the Secure class with HTTPS endpoints.
    /// </summary>
    public static class Insecure
    {
        /// <summary>
        /// JobManager gRPC address (insecure HTTP - use only for local development)
        /// For production, use Secure.JobManagerGrpcHttps instead.
        /// </summary>
        [SuppressMessage("Security", "S5332:Using http protocol is insecure. Use https instead.", 
            Justification = "Intentionally insecure HTTP for local development - production code should use Secure.JobManagerGrpcHttps")]
        public static string JobManagerGrpcHttp => $"http://{ServiceHosts.Localhost}:{ServicePorts.JobManagerGrpc}";

        /// <summary>
        /// JobManager HTTP API address (insecure HTTP - use only for local development)
        /// For production, use Secure.JobManagerHttpsApi instead.
        /// </summary>
        [SuppressMessage("Security", "S5332:Using http protocol is insecure. Use https instead.", 
            Justification = "Intentionally insecure HTTP for local development - production code should use Secure.JobManagerHttpsApi")]
        public static string JobManagerHttpApi => $"http://{ServiceHosts.Localhost}:{ServicePorts.JobManagerHttp}";

        /// <summary>
        /// TaskManager gRPC address (insecure HTTP - use only for local development)
        /// For production, use Secure.TaskManagerGrpcHttps instead.
        /// </summary>
        /// <returns>TaskManager gRPC URI</returns>
        [SuppressMessage("Security", "S5332:Using http protocol is insecure. Use https instead.", 
            Justification = "Intentionally insecure HTTP for local development - production code should use Secure.TaskManagerGrpcHttps")]
        public static string TaskManagerGrpcHttp() => $"http://{ServiceHosts.Localhost}:{ServicePorts.TaskManagerGrpc}";

        /// <summary>
        /// TaskManager gRPC address with specified port (insecure HTTP - use only for local development)
        /// For production, use Secure.TaskManagerGrpcHttps instead.
        /// </summary>
        /// <param name="port">Custom port number</param>
        /// <returns>TaskManager gRPC URI</returns>
        [SuppressMessage("Security", "S5332:Using http protocol is insecure. Use https instead.", 
            Justification = "Intentionally insecure HTTP for local development - production code should use Secure.TaskManagerGrpcHttps")]
        public static string TaskManagerGrpcHttp(int port) => $"http://{ServiceHosts.Localhost}:{port}";
    }

    /// <summary>
    /// Service URIs for secure HTTPS connections.
    /// Use these for production environments.
    /// </summary>
    public static class Secure
    {
        /// <summary>
        /// JobManager gRPC address (secure HTTPS - recommended for production)
        /// </summary>
        public static string JobManagerGrpcHttps => $"https://{ServiceHosts.Localhost}:{ServicePorts.JobManagerGrpc}";

        /// <summary>
        /// JobManager HTTP API address (secure HTTPS - recommended for production)
        /// </summary>
        public static string JobManagerHttpsApi => $"https://{ServiceHosts.Localhost}:{ServicePorts.JobManagerHttp}";

        /// <summary>
        /// TaskManager gRPC address (secure HTTPS - recommended for production)
        /// </summary>
        /// <returns>TaskManager gRPC URI</returns>
        public static string TaskManagerGrpcHttps() => $"https://{ServiceHosts.Localhost}:{ServicePorts.TaskManagerGrpc}";

        /// <summary>
        /// TaskManager gRPC address with specified port (secure HTTPS - recommended for production)
        /// </summary>
        /// <param name="port">Custom port number</param>
        /// <returns>TaskManager gRPC URI</returns>
        public static string TaskManagerGrpcHttps(int port) => $"https://{ServiceHosts.Localhost}:{port}";
    }


}

/// <summary>
/// Environment variable names used across the application
/// </summary>
public static class EnvironmentVariables
{
    /// <summary>
    /// Environment variable for JobManager gRPC port
    /// </summary>
    public const string JobManagerGrpcPort = "JOBMANAGER_GRPC_PORT";

    /// <summary>
    /// Environment variable for JobManager HTTP port
    /// </summary>
    public const string JobManagerHttpPort = "JOBMANAGER_HTTP_PORT";

    /// <summary>
    /// Environment variable for TaskManager gRPC port
    /// </summary>
    public const string TaskManagerGrpcPort = "TASKMANAGER_GRPC_PORT";

    /// <summary>
    /// Environment variable for Kafka port (dynamically assigned by Aspire)
    /// </summary>
    public const string KafkaPort = "KAFKA_PORT";

    /// <summary>
    /// Environment variable for Redis port (dynamically assigned by Aspire)
    /// </summary>
    public const string RedisPort = "REDIS_PORT";

    /// <summary>
    /// Environment variable for Kafka bootstrap servers
    /// </summary>
    public const string KafkaBootstrapServers = "KAFKA_BOOTSTRAP_SERVERS";

    /// <summary>
    /// Environment variable for Redis connection string
    /// </summary>
    public const string RedisConnectionString = "REDIS_CONNECTION_STRING";
}