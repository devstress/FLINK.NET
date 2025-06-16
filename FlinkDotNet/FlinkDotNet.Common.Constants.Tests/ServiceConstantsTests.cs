using Xunit;

namespace FlinkDotNet.Common.Constants.Tests;

/// <summary>
/// Unit tests for the common constants library to ensure hardcoded values are properly centralized
/// </summary>
public class ServiceConstantsTests
{
    [Fact]
    public void ServicePorts_Should_Use_Environment_Variables_When_Available()
    {
        // Set test environment variables
        Environment.SetEnvironmentVariable("JOBMANAGER_GRPC_PORT", "60051");
        Environment.SetEnvironmentVariable("JOBMANAGER_HTTP_PORT", "9088");
        Environment.SetEnvironmentVariable("TASKMANAGER_GRPC_PORT", "70051");
        
        try
        {
            // Verify that the ports read from environment variables
            Assert.Equal(60051, ServicePorts.JobManagerGrpc);
            Assert.Equal(9088, ServicePorts.JobManagerHttp);
            Assert.Equal(70051, ServicePorts.TaskManagerGrpc);
        }
        finally
        {
            // Clean up environment variables
            Environment.SetEnvironmentVariable("JOBMANAGER_GRPC_PORT", null);
            Environment.SetEnvironmentVariable("JOBMANAGER_HTTP_PORT", null);
            Environment.SetEnvironmentVariable("TASKMANAGER_GRPC_PORT", null);
        }
    }

    [Fact]
    public void ServicePorts_Should_Use_Aspire_Environment_Variables_When_Available()
    {
        // Set Aspire environment variables (these take precedence)
        Environment.SetEnvironmentVariable("DOTNET_KAFKA_PORT", "19092");
        Environment.SetEnvironmentVariable("DOTNET_REDIS_PORT", "16379");
        
        try
        {
            // Verify that the ports read from Aspire environment variables
            Assert.Equal(19092, ServicePorts.Kafka);
            Assert.Equal(16379, ServicePorts.Redis);
        }
        finally
        {
            // Clean up environment variables
            Environment.SetEnvironmentVariable("DOTNET_KAFKA_PORT", null);
            Environment.SetEnvironmentVariable("DOTNET_REDIS_PORT", null);
        }
    }

    [Fact]
    public void ServiceUris_Should_Use_Aspire_Connection_Strings_When_Available()
    {
        // Set Aspire connection string environment variables
        Environment.SetEnvironmentVariable("DOTNET_KAFKA_BOOTSTRAP_SERVERS", "localhost:19092");
        Environment.SetEnvironmentVariable("DOTNET_REDIS_URL", "localhost:16379");
        
        try
        {
            // Verify that the URIs use Aspire environment variables
            Assert.Equal("localhost:19092", ServiceUris.KafkaBootstrapServers);
            Assert.Equal("localhost:16379", ServiceUris.RedisConnectionString);
        }
        finally
        {
            // Clean up environment variables
            Environment.SetEnvironmentVariable("DOTNET_KAFKA_BOOTSTRAP_SERVERS", null);
            Environment.SetEnvironmentVariable("DOTNET_REDIS_URL", null);
        }
    }

    [Fact]
    public void ServicePorts_Should_Have_Expected_Defaults_When_No_Environment_Variables()
    {
        // Ensure no environment variables are set for these tests
        Environment.SetEnvironmentVariable("JOBMANAGER_GRPC_PORT", null);
        Environment.SetEnvironmentVariable("JOBMANAGER_HTTP_PORT", null);
        Environment.SetEnvironmentVariable("TASKMANAGER_GRPC_PORT", null);
        Environment.SetEnvironmentVariable("KAFKA_PORT", null);
        Environment.SetEnvironmentVariable("REDIS_PORT", null);
        Environment.SetEnvironmentVariable("DOTNET_KAFKA_PORT", null);
        Environment.SetEnvironmentVariable("DOTNET_REDIS_PORT", null);
        
        // Verify that the expected default ports are used as fallbacks
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
        // Clear any environment variables to ensure we test default behavior
        Environment.SetEnvironmentVariable("DOTNET_KAFKA_BOOTSTRAP_SERVERS", null);
        Environment.SetEnvironmentVariable("DOTNET_REDIS_URL", null);
        Environment.SetEnvironmentVariable("DOTNET_KAFKA_PORT", null);
        Environment.SetEnvironmentVariable("DOTNET_REDIS_PORT", null);
        
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
        
        // Verify Aspire environment variable names
        Assert.Equal("DOTNET_KAFKA_PORT", EnvironmentVariables.DotNetKafkaPort);
        Assert.Equal("DOTNET_REDIS_PORT", EnvironmentVariables.DotNetRedisPort);
        Assert.Equal("DOTNET_KAFKA_BOOTSTRAP_SERVERS", EnvironmentVariables.DotNetKafkaBootstrapServers);
        Assert.Equal("DOTNET_REDIS_URL", EnvironmentVariables.DotNetRedisUrl);
    }
}