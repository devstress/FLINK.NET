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
        var appHostAssembly = System.Reflection.Assembly.LoadFrom(
            Path.Combine(AppDomain.CurrentDomain.BaseDirectory, "FlinkDotNetAspire.AppHost.AppHost.dll"));
        
        Assert.NotNull(appHostAssembly);
        Assert.Contains("FlinkDotNetAspire.AppHost.AppHost", appHostAssembly.FullName);
    }

    [Fact]
    public void AppHost_Assembly_Contains_Program_Type()
    {
        // Load the AppHost assembly and verify it contains a Program type
        var appHostAssembly = System.Reflection.Assembly.LoadFrom(
            Path.Combine(AppDomain.CurrentDomain.BaseDirectory, "FlinkDotNetAspire.AppHost.AppHost.dll"));
        
        var programType = appHostAssembly.GetType("Program");
        Assert.NotNull(programType);
        Assert.True(programType.IsPublic);
    }

    [Fact]
    public void AppHost_Program_Has_Main_Method()
    {
        // Verify the Main method exists and is correctly defined for an Aspire host
        var appHostAssembly = System.Reflection.Assembly.LoadFrom(
            Path.Combine(AppDomain.CurrentDomain.BaseDirectory, "FlinkDotNetAspire.AppHost.AppHost.dll"));
        
        var programType = appHostAssembly.GetType("Program");
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
        
        var appHostAssembly = System.Reflection.Assembly.LoadFrom(
            Path.Combine(AppDomain.CurrentDomain.BaseDirectory, "FlinkDotNetAspire.AppHost.AppHost.dll"));
        Assert.NotNull(appHostAssembly);
        
        // Also verify Aspire framework assemblies are available
        var aspireHostingAssembly = System.Reflection.Assembly.LoadFrom(
            Path.Combine(AppDomain.CurrentDomain.BaseDirectory, "Aspire.Hosting.dll"));
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
}