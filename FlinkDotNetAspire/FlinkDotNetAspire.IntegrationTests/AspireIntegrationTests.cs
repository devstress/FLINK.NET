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
}