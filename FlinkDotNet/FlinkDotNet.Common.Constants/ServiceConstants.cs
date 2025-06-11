namespace FlinkDotNet.Common.Constants;

/// <summary>
/// Default port configurations for FlinkDotNet services
/// </summary>
public static class ServicePorts
{
    /// <summary>
    /// Default port for JobManager gRPC service
    /// </summary>
    public const int JobManagerGrpc = 50051;

    /// <summary>
    /// Default port for JobManager HTTP API (calculated as gRPC port + offset)
    /// </summary>
    public const int JobManagerHttp = 8088;

    /// <summary>
    /// Default port for Kafka service
    /// </summary>
    public const int Kafka = 9092;

    /// <summary>
    /// Default port for Redis service
    /// </summary>
    public const int Redis = 6379;

    /// <summary>
    /// Default port for TaskManager gRPC service
    /// </summary>
    public const int TaskManagerGrpc = 40051;

    /// <summary>
    /// Base port for TaskManager instances in Aspire deployment (51070)
    /// </summary>
    public const int TaskManagerAspireBasePort = 51070;
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
    /// Default JobManager gRPC address
    /// </summary>
    public static string JobManagerGrpc => $"http://{ServiceHosts.Localhost}:{ServicePorts.JobManagerGrpc}";

    /// <summary>
    /// Default JobManager HTTP API address
    /// </summary>
    public static string JobManagerHttp => $"http://{ServiceHosts.Localhost}:{ServicePorts.JobManagerHttp}";

    /// <summary>
    /// Default Kafka bootstrap servers
    /// </summary>
    public static string KafkaBootstrapServers => $"{ServiceHosts.Localhost}:{ServicePorts.Kafka}";

    /// <summary>
    /// Default Redis connection string
    /// </summary>
    public static string RedisConnectionString => $"{ServiceHosts.Localhost}:{ServicePorts.Redis}";

    /// <summary>
    /// Default TaskManager gRPC address with specified port
    /// </summary>
    /// <param name="port">Custom port number</param>
    /// <returns>TaskManager gRPC URI</returns>
    public static string TaskManagerGrpc(int port = ServicePorts.TaskManagerGrpc) => $"http://{ServiceHosts.Localhost}:{port}";

    /// <summary>
    /// Gets TaskManager port for Aspire deployment based on instance number
    /// </summary>
    /// <param name="instanceNumber">TaskManager instance number (1-based)</param>
    /// <returns>Port number for the TaskManager instance</returns>
    public static int GetTaskManagerAspirePort(int instanceNumber) => ServicePorts.TaskManagerAspireBasePort + instanceNumber;
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
    /// Environment variable for Kafka bootstrap servers
    /// </summary>
    public const string KafkaBootstrapServers = "KAFKA_BOOTSTRAP_SERVERS";

    /// <summary>
    /// Environment variable for Redis connection string
    /// </summary>
    public const string RedisConnectionString = "REDIS_CONNECTION_STRING";
}