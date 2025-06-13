using Xunit;

namespace FlinkDotNet.Common.Constants.Tests;

/// <summary>
/// Unit tests for the common constants library to ensure hardcoded values are properly centralized
/// </summary>
public class ServiceConstantsTests
{
    [Fact]
    public void ServicePorts_Should_Have_Expected_Defaults()
    {
        // Verify that the expected default ports are defined
        Assert.Equal(50051, ServicePorts.JobManagerGrpc);
        Assert.Equal(8088, ServicePorts.JobManagerHttp);
        Assert.Equal(9092, ServicePorts.Kafka);
        Assert.Equal(6379, ServicePorts.Redis);
        Assert.Equal(40051, ServicePorts.TaskManagerGrpc);
    }

    [Fact]
    public void ServiceHosts_Should_Have_Expected_Defaults()
    {
        // Verify that the expected default hosts are defined
        Assert.Equal("localhost", ServiceHosts.Localhost);
        Assert.Equal("127.0.0.1", ServiceHosts.LocalIP);
    }

    [Fact]
    public void ServiceUris_Should_Generate_Correct_Secure_URIs()
    {
        // Verify that the secure URI generation works correctly (default)
        Assert.Equal("https://localhost:50051", ServiceUris.JobManagerGrpc);
        Assert.Equal("https://localhost:8088", ServiceUris.JobManagerHttp);
        Assert.Equal("localhost:9092", ServiceUris.KafkaBootstrapServers);
        Assert.Equal("localhost:6379", ServiceUris.RedisConnectionString);
        Assert.Equal("https://localhost:40051", ServiceUris.TaskManagerGrpc());
    }

    [Fact]
    public void ServiceUris_Should_Generate_Correct_Insecure_URIs_For_Development()
    {
        // Verify that the insecure URI generation works correctly for local development
        Assert.Equal("http://localhost:50051", ServiceUris.Insecure.JobManagerGrpcHttp);
        Assert.Equal("http://localhost:8088", ServiceUris.Insecure.JobManagerHttpApi);
        Assert.Equal("http://localhost:40051", ServiceUris.Insecure.TaskManagerGrpcHttp());
    }

    [Fact]
    public void ServiceUris_TaskManagerGrpc_Should_Support_Custom_Port()
    {
        // Verify that custom ports work for TaskManager (secure version)
        Assert.Equal("https://localhost:51001", ServiceUris.TaskManagerGrpc(51001));
        Assert.Equal("https://localhost:51002", ServiceUris.TaskManagerGrpc(51002));
    }

    [Fact]
    public void ServiceUris_Insecure_TaskManagerGrpc_Should_Support_Custom_Port()
    {
        // Verify that custom ports work for TaskManager (insecure version for development)
        Assert.Equal("http://localhost:51001", ServiceUris.Insecure.TaskManagerGrpcHttp(51001));
        Assert.Equal("http://localhost:51002", ServiceUris.Insecure.TaskManagerGrpcHttp(51002));
    }

    [Fact]
    public void EnvironmentVariables_Should_Have_Expected_Names()
    {
        // Verify that environment variable names are consistent
        Assert.Equal("JOBMANAGER_GRPC_PORT", EnvironmentVariables.JobManagerGrpcPort);
        Assert.Equal("KAFKA_BOOTSTRAP_SERVERS", EnvironmentVariables.KafkaBootstrapServers);
        Assert.Equal("REDIS_CONNECTION_STRING", EnvironmentVariables.RedisConnectionString);
    }
}