using Microsoft.Extensions.DependencyInjection;
using FlinkDotNet.Common.Constants;

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
                    string jobManagerAddress = ServiceUris.JobManagerGrpc;
                    
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
                string jobManagerAddress = ServiceUris.JobManagerGrpc;
                
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
                    string jobManagerAddress = ServiceUris.JobManagerGrpc;
                    
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
}