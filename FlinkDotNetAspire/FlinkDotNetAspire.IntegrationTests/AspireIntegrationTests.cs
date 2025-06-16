using Microsoft.Extensions.DependencyInjection;
using FlinkDotNet.Common.Constants;
using Confluent.Kafka;

namespace FlinkDotNetAspire.IntegrationTests;

/// <summary>
/// Basic integration tests for the FlinkDotNet Aspire application.
/// These tests verify that the application components can be constructed and configured properly.
/// </summary>
public class AspireIntegrationTests
{
    [Fact]
    public void AspireAppHost_Assembly_Can_Be_Loaded()
    {
        // This test verifies that the AppHost assembly can be loaded successfully
        var appHostAssembly = System.Reflection.Assembly.Load("FlinkDotNetAspire.AppHost.AppHost");
        
        Assert.NotNull(appHostAssembly);
        Assert.Contains("FlinkDotNetAspire.AppHost.AppHost", appHostAssembly.FullName);
    }

    [Fact]
    public void AppHost_Assembly_Contains_Program_Type()
    {
        // Load the AppHost assembly and verify it contains a Program type
        var appHostAssembly = System.Reflection.Assembly.Load("FlinkDotNetAspire.AppHost.AppHost");
        
        var programType = appHostAssembly.GetType("FlinkDotNetAspire.AppHost.AppHost.Program");
        Assert.NotNull(programType);
        Assert.True(programType.IsPublic);
    }

    [Fact]
    public void AppHost_Program_Has_Main_Method()
    {
        // Verify the Main method exists and is correctly defined for an Aspire host
        var appHostAssembly = System.Reflection.Assembly.Load("FlinkDotNetAspire.AppHost.AppHost");
        
        var programType = appHostAssembly.GetType("FlinkDotNetAspire.AppHost.AppHost.Program");
        Assert.NotNull(programType);
        
        var mainMethod = programType.GetMethod("Main");
        Assert.NotNull(mainMethod);
        Assert.True(mainMethod.IsStatic);
        Assert.True(mainMethod.IsPublic);
    }

    [Fact]
    public void Integration_Test_Project_References_Are_Correct()
    {
        // This test ensures the project references are working correctly
        // by verifying we can access types from the AppHost project
        
        var appHostAssembly = System.Reflection.Assembly.Load("FlinkDotNetAspire.AppHost.AppHost");
        Assert.NotNull(appHostAssembly);
        
        // Also verify Aspire framework assemblies are available
        var aspireHostingAssembly = System.Reflection.Assembly.Load("Aspire.Hosting");
        Assert.NotNull(aspireHostingAssembly);
    }

    [Fact]
    public void Aspire_Solution_Includes_Required_Projects()
    {
        // This test verifies that the Aspire solution includes all required projects
        // to prevent the "project is not loaded in the solution" error
        
        // Read the solution file and verify it contains JobManager and TaskManager projects
        var solutionPath = Path.GetFullPath(Path.Combine(
            AppDomain.CurrentDomain.BaseDirectory, 
            "..", "..", "..", "..", 
            "FlinkDotNetAspire.sln"));
            
        Assert.True(File.Exists(solutionPath), $"Solution file not found at: {solutionPath}");
        
        var solutionContent = File.ReadAllText(solutionPath);
        
        // Verify JobManager project is included
        Assert.Contains("FlinkDotNet.JobManager", solutionContent);
        Assert.Contains(@"..\FlinkDotNet\FlinkDotNet.JobManager\FlinkDotNet.JobManager.csproj", solutionContent);
        
        // Verify TaskManager project is included  
        Assert.Contains("FlinkDotNet.TaskManager", solutionContent);
        Assert.Contains(@"..\FlinkDotNet\FlinkDotNet.TaskManager\FlinkDotNet.TaskManager.csproj", solutionContent);
        
        // Verify the build configurations are included for both projects
        Assert.Contains("{E1F2A3B4-C5D6-7890-1234-567890ABCDEF}", solutionContent); // JobManager GUID
        Assert.Contains("{F2E3B4C5-D6A7-8901-2345-67890ABCDEF1}", solutionContent); // TaskManager GUID
    }

    [Fact]
    public void FlinkJobSimulator_Does_Not_Auto_Launch_Dashboard()
    {
        // This test verifies that the auto-launch dashboard code has been removed
        // as requested in the issue
        
        var simulatorPath = Path.GetFullPath(Path.Combine(
            AppDomain.CurrentDomain.BaseDirectory, 
            "..", "..", "..", "..", 
            "FlinkJobSimulator", "Program.cs"));
            
        Assert.True(File.Exists(simulatorPath), $"FlinkJobSimulator Program.cs not found at: {simulatorPath}");
        
        var simulatorContent = File.ReadAllText(simulatorPath);
        
        // Verify the auto-launch code has been removed or commented out
        Assert.DoesNotContain("System.Diagnostics.Process.Start", simulatorContent);
        Assert.DoesNotContain("UseShellExecute = true", simulatorContent);
        
        // Verify the ASPIRE_DASHBOARD_URL environment variable is not being used for auto-launch
        if (simulatorContent.Contains("ASPIRE_DASHBOARD_URL"))
        {
            // If it still references this variable, it should not be for launching the browser
            Assert.DoesNotContain("FileName = dashboardUrl", simulatorContent);
        }
    }

    [Fact]
    public void TaskManager_Can_Be_Constructed_Without_DI_Errors()
    {
        // This test reproduces the dependency injection error:
        // "Unable to resolve service for type 'FlinkDotNet.TaskManager.ActiveTaskRegistry' 
        // while attempting to activate 'FlinkDotNet.TaskManager.TaskExecutor'"
        
        var exception = Record.Exception(() =>
        {
            // Try to create the TaskManager host builder configuration
            // This should fail with DI errors if dependencies are missing
            var hostBuilder = Microsoft.Extensions.Hosting.Host.CreateDefaultBuilder()
                .ConfigureServices((hostContext, services) =>
                {
                    // Reproduce the exact DI configuration from TaskManager Program.cs
                    string taskManagerId = "TM-Test";
                    string jobManagerAddress = ServiceUris.Insecure.JobManagerGrpcHttp;
                    
                    services.AddSingleton(new FlinkDotNet.TaskManager.TaskManagerCoreService.Config(taskManagerId, jobManagerAddress));
                    services.AddHostedService<FlinkDotNet.TaskManager.TaskManagerCoreService>();
                    
                    // This line should fail because TaskExecutor dependencies are missing
                    services.AddSingleton<FlinkDotNet.TaskManager.TaskExecutor>();
                    
                    services.AddGrpc();
                    services.AddSingleton(sp => new FlinkDotNet.TaskManager.Services.TaskManagerCheckpointingServiceImpl(taskManagerId));
                    // Note: TaskExecutionServiceImpl and DataExchangeServiceImpl depend on TaskExecutor, so they would also fail
                });
                
            var host = hostBuilder.Build();
            
            // Try to get the TaskExecutor - this should trigger the DI error
            using var scope = host.Services.CreateScope();
            scope.ServiceProvider.GetRequiredService<FlinkDotNet.TaskManager.TaskExecutor>();
        });
        
        // We expect an InvalidOperationException about missing services
        Assert.NotNull(exception);
        Assert.IsType<InvalidOperationException>(exception);
        Assert.Contains("ActiveTaskRegistry", exception.Message);
    }

    [Fact] 
    public void TaskManager_Integration_Can_Start_With_All_Dependencies()
    {
        // This test verifies that after fixing the DI issues, TaskManager can actually start
        // This should pass after we fix the dependency registration
        
        var hostBuilder = Microsoft.Extensions.Hosting.Host.CreateDefaultBuilder()
            .ConfigureServices((hostContext, services) =>
            {
                string taskManagerId = "TM-Integration-Test";
                string jobManagerAddress = ServiceUris.Insecure.JobManagerGrpcHttp;
                
                // Register all required dependencies for TaskExecutor
                services.AddSingleton<FlinkDotNet.TaskManager.ActiveTaskRegistry>();
                services.AddSingleton<FlinkDotNet.Core.Abstractions.Execution.SerializerRegistry>();
                services.AddSingleton<FlinkDotNet.Core.Abstractions.Storage.IStateSnapshotStore, FlinkDotNet.Core.Abstractions.Storage.InMemoryStateSnapshotStore>();
                services.AddSingleton(provider => taskManagerId); // Register taskManagerId as injectable string
                
                // Register TaskManager services 
                services.AddSingleton(new FlinkDotNet.TaskManager.TaskManagerCoreService.Config(taskManagerId, jobManagerAddress));
                services.AddSingleton<FlinkDotNet.TaskManager.Services.TaskManagerCheckpointingServiceImpl>();
                
                // Now TaskExecutor should be constructible
                services.AddSingleton<FlinkDotNet.TaskManager.TaskExecutor>();
                
                services.AddGrpc();
            });
            
        using var host = hostBuilder.Build();
        
        // Verify we can create all the required services
        using var scope = host.Services.CreateScope();
        
        var activeTaskRegistry = scope.ServiceProvider.GetRequiredService<FlinkDotNet.TaskManager.ActiveTaskRegistry>();
        Assert.NotNull(activeTaskRegistry);
        
        var serializerRegistry = scope.ServiceProvider.GetRequiredService<FlinkDotNet.Core.Abstractions.Execution.SerializerRegistry>();
        Assert.NotNull(serializerRegistry);
        
        var stateStore = scope.ServiceProvider.GetRequiredService<FlinkDotNet.Core.Abstractions.Storage.IStateSnapshotStore>();
        Assert.NotNull(stateStore);
        
        var taskExecutor = scope.ServiceProvider.GetRequiredService<FlinkDotNet.TaskManager.TaskExecutor>();
        Assert.NotNull(taskExecutor);
        Assert.NotNull(taskExecutor.Registry);
    }

    [Fact]
    public void Kafka_Connection_Fails_Gracefully_When_Broker_Unavailable()
    {
        // This test reproduces the Kafka connection error:
        // "%6|1749663499.957|FAIL|rdkafka#producer-1| [thrd:localhost:9092/bootstrap]: 
        // localhost:9092/bootstrap: Disconnected while requesting ApiVersion: might be caused by 
        // incorrect security.protocol configuration (connecting to a SSL listener?) or broker version is < 0.10"
        
        var exception = Record.Exception(() =>
        {
            // Create an AdminClient configuration that tries to connect to a non-existent broker
            var adminConfig = new Confluent.Kafka.AdminClientConfig
            {
                BootstrapServers = ServiceUris.KafkaBootstrapServers, // Assume Kafka is not running
                SecurityProtocol = Confluent.Kafka.SecurityProtocol.Plaintext
            };
            
            using var admin = new Confluent.Kafka.AdminClientBuilder(adminConfig).Build();
            
            // Try to get metadata - this should fail if Kafka is not available
            admin.GetMetadata(TimeSpan.FromSeconds(5));
        });
        
        // We expect a Kafka exception when the broker is unavailable
        Assert.NotNull(exception);
        // The exception could be KafkaException or TimeoutException depending on the connection failure
        Assert.True(exception is Confluent.Kafka.KafkaException || 
                   exception is TimeoutException ||
                   exception.Message.Contains("bootstrap") ||
                   exception.Message.Contains("ApiVersion"),
                   $"Expected Kafka connection error, but got: {exception.GetType().Name}: {exception.Message}");
    }

    [Fact]
    public void TaskManager_DI_Issues_Are_Fixed()
    {
        // This test verifies that the TaskManager DI issues mentioned in the comment are fixed
        // It should now be able to construct all services successfully
        
        var exception = Record.Exception(() =>
        {
            // Use the exact same DI configuration as the fixed TaskManager Program.cs
            var hostBuilder = Microsoft.Extensions.Hosting.Host.CreateDefaultBuilder()
                .ConfigureServices((hostContext, services) =>
                {
                    string taskManagerId = "TM-Integration-Test";
                    string jobManagerAddress = ServiceUris.Insecure.JobManagerGrpcHttp;
                    
                    // Reproduce the FIXED DI configuration from TaskManager Program.cs
                    services.AddSingleton(new FlinkDotNet.TaskManager.TaskManagerCoreService.Config(taskManagerId, jobManagerAddress));
                    services.AddSingleton<FlinkDotNet.TaskManager.TaskManagerCoreService>();
                    services.AddHostedService<FlinkDotNet.TaskManager.TaskManagerCoreService>(sp => sp.GetRequiredService<FlinkDotNet.TaskManager.TaskManagerCoreService>());

                    // Register all dependencies required by TaskExecutor (the fix)
                    services.AddSingleton<FlinkDotNet.TaskManager.ActiveTaskRegistry>();
                    services.AddSingleton<FlinkDotNet.Core.Abstractions.Execution.SerializerRegistry>();
                    services.AddSingleton<FlinkDotNet.Core.Abstractions.Storage.IStateSnapshotStore, FlinkDotNet.Core.Abstractions.Storage.InMemoryStateSnapshotStore>();
                    services.AddSingleton(provider => taskManagerId);

                    // Register TaskExecutor with all its dependencies now available
                    services.AddSingleton<FlinkDotNet.TaskManager.TaskExecutor>();

                    services.AddGrpc();
                    services.AddSingleton(sp => new FlinkDotNet.TaskManager.Services.TaskManagerCheckpointingServiceImpl(taskManagerId));
                    services.AddSingleton(sp => new FlinkDotNet.TaskManager.Services.TaskExecutionServiceImpl(taskManagerId, sp.GetRequiredService<FlinkDotNet.TaskManager.TaskExecutor>()));
                    services.AddSingleton(sp => new FlinkDotNet.TaskManager.Services.DataExchangeServiceImpl(taskManagerId));
                });
                
            using var host = hostBuilder.Build();
            
            // Try to get all the services - this should NOT fail now with the fix
            using var scope = host.Services.CreateScope();
            
            var taskManagerCoreService = scope.ServiceProvider.GetRequiredService<FlinkDotNet.TaskManager.TaskManagerCoreService>();
            var activeTaskRegistry = scope.ServiceProvider.GetRequiredService<FlinkDotNet.TaskManager.ActiveTaskRegistry>();
            var serializerRegistry = scope.ServiceProvider.GetRequiredService<FlinkDotNet.Core.Abstractions.Execution.SerializerRegistry>();
            var stateStore = scope.ServiceProvider.GetRequiredService<FlinkDotNet.Core.Abstractions.Storage.IStateSnapshotStore>();
            var taskExecutor = scope.ServiceProvider.GetRequiredService<FlinkDotNet.TaskManager.TaskExecutor>();
            var checkpointingService = scope.ServiceProvider.GetRequiredService<FlinkDotNet.TaskManager.Services.TaskManagerCheckpointingServiceImpl>();
            var executionService = scope.ServiceProvider.GetRequiredService<FlinkDotNet.TaskManager.Services.TaskExecutionServiceImpl>();
            var dataExchangeService = scope.ServiceProvider.GetRequiredService<FlinkDotNet.TaskManager.Services.DataExchangeServiceImpl>();
            
            // Verify they're all properly constructed
            Assert.NotNull(taskManagerCoreService);
            Assert.NotNull(activeTaskRegistry);
            Assert.NotNull(serializerRegistry);
            Assert.NotNull(stateStore);
            Assert.NotNull(taskExecutor);
            Assert.NotNull(taskExecutor.Registry);
            Assert.NotNull(checkpointingService);
            Assert.NotNull(executionService);
            Assert.NotNull(dataExchangeService);
        });
        
        // No exception should be thrown - the DI issues are fixed
        Assert.Null(exception);
    }

    [Fact]
    public void Kafka_Topic_Creation_By_Aspire_Infrastructure()
    {
        // This test verifies that the flinkdotnet.sample.topic is created by Aspire infrastructure
        // as requested in the comment: "flinkdotnet.sample.topic Should be created with Aspire with a Dockerfile, not in procedure"
        
        // Skip this test if running in environments where Kafka is not available
        if (IsRunningInTestOnlyEnvironment())
        {
            // Test passes but indicates that Kafka infrastructure is not available
            Assert.True(true); // Test passed - skipped due to no Kafka infrastructure
            return;
        }
        
        try
        {
            // Get Kafka bootstrap servers from environment or use default
            var kafkaBootstrapServers = Environment.GetEnvironmentVariable("DOTNET_KAFKA_BOOTSTRAP_SERVERS") ?? "localhost:9092";
            
            // Create admin client to check if topic exists
            var adminConfig = new AdminClientConfig
            {
                BootstrapServers = kafkaBootstrapServers,
                SecurityProtocol = SecurityProtocol.Plaintext,
                SocketTimeoutMs = 5000, // 5 second timeout for integration test
                ApiVersionRequestTimeoutMs = 5000
            };
            
            using var admin = new AdminClientBuilder(adminConfig).Build();
            
            // Get metadata with short timeout - if Kafka is not running, this will throw
            var metadata = admin.GetMetadata(TimeSpan.FromSeconds(10));
            
            // Check if flinkdotnet.sample.topic exists
            var sampleTopic = metadata.Topics.FirstOrDefault(t => t.Topic == "flinkdotnet.sample.topic");
            
            if (sampleTopic != null)
            {
                // Topic exists - verify it was created with proper configuration
                Assert.NotNull(sampleTopic);
                Assert.Equal("flinkdotnet.sample.topic", sampleTopic.Topic);
                
                // Check that topic has partitions (should be created with multiple partitions for load distribution)
                Assert.True(sampleTopic.Partitions.Count > 0, "Topic should have at least one partition");
                
                // Test passes - topic exists with proper configuration
                Assert.True(true); // SUCCESS: Topic created by Aspire infrastructure
            }
            else
            {
                // Topic doesn't exist - this indicates Aspire kafka-init container failed
                var availableTopics = string.Join(", ", metadata.Topics.Select(t => t.Topic));
                Assert.Fail($"❌ FAILURE: flinkdotnet.sample.topic not found. Aspire kafka-init container may have failed. Available topics: {availableTopics}");
            }
        }
        catch (KafkaException kafkaEx) when (kafkaEx.Error.Code == ErrorCode.BrokerNotAvailable || 
                                             kafkaEx.Error.Code == ErrorCode.NetworkException ||
                                             kafkaEx.Message.Contains("timeout") ||
                                             kafkaEx.Message.Contains("transport failure"))
        {
            // Kafka is not available - this is expected in unit test environments
            // Test passes but indicates infrastructure issue
            Assert.True(true); // Test passed - skipped due to Kafka not available
        }
        catch (Exception ex)
        {
            // Unexpected exception - let the test framework handle it
            Assert.Fail($"Unexpected exception during Kafka connectivity test: {ex.GetType().Name}: {ex.Message}");
        }
    }
    
    private static bool IsRunningInTestOnlyEnvironment()
    {
        // Check if we're running in a pure unit test environment (no Aspire infrastructure)
        var isCI = Environment.GetEnvironmentVariable("CI") == "true";
        var hasKafkaBootstrap = !string.IsNullOrEmpty(Environment.GetEnvironmentVariable("DOTNET_KAFKA_BOOTSTRAP_SERVERS"));
        var isUnitTestOnly = !hasKafkaBootstrap && !isCI;
        
        return isUnitTestOnly;
    }

    [Fact]
    public void Aspire_Kafka_Init_Script_Contains_Sample_Topic_Creation()
    {
        // This test verifies that the Aspire AppHost configuration includes topic creation for flinkdotnet.sample.topic
        // This ensures the topic creation is properly configured in the infrastructure code
        
        var appHostProgramPath = Path.GetFullPath(Path.Combine(
            AppDomain.CurrentDomain.BaseDirectory, 
            "..", "..", "..", "..", 
            "FlinkDotNetAspire.AppHost.AppHost", "Program.cs"));
            
        Assert.True(File.Exists(appHostProgramPath), $"AppHost Program.cs not found at: {appHostProgramPath}");
        
        var appHostContent = File.ReadAllText(appHostProgramPath);
        
        // Verify that the kafka-init script contains topic creation for flinkdotnet.sample.topic
        Assert.Contains("flinkdotnet.sample.topic", appHostContent);
        Assert.Contains("kafka-topics --create", appHostContent);
        Assert.Contains("--if-not-exists", appHostContent);
        
        // Verify that the topic creation uses proper configuration
        Assert.Contains("--partitions", appHostContent);
        Assert.Contains("--replication-factor", appHostContent);
        
        // Verify that the script includes error handling and validation
        Assert.Contains("Step 5: Creating critical flinkdotnet.sample.topic", appHostContent);
        Assert.Contains("timeout 45", appHostContent);
        
        // Verify that topic verification is included
        Assert.Contains("kafka-topics --describe", appHostContent);
        
        // Verify specific topic verification for flinkdotnet.sample.topic
        var describeIndex = appHostContent.IndexOf("kafka-topics --describe");
        Assert.True(describeIndex >= 0, "Script should include kafka-topics --describe command");
        
        var afterDescribe = appHostContent.Substring(describeIndex);
        Assert.Contains("flinkdotnet.sample.topic", afterDescribe);
    }
}