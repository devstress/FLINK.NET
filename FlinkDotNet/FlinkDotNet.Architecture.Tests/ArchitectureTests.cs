using Microsoft.VisualStudio.TestTools.UnitTesting;
using NetArchTest.Rules;
using System.Reflection;

namespace FlinkDotNet.Architecture.Tests
{
    [TestClass]
    public class ArchitectureTests
    {
        // Define assembly names or easily identifiable namespace roots for layers
        // Adjust these based on the actual main namespaces of your projects if they differ from assembly names.
        private const string CoreAbstractionsNamespace = "FlinkDotNet.Core.Abstractions";
        private const string CoreApiNamespace = "FlinkDotNet.Core.Api";
        private const string CoreNamespace = "FlinkDotNet.Core"; // General Core, might contain concrete implementations for Core logic

        private const string ConnectorsSinksConsoleNamespace = "FlinkDotNet.Connectors.Sinks.Console";
        private const string ConnectorsSourcesFileNamespace = "FlinkDotNet.Connectors.Sources.File";
        private const string StorageFileSystemNamespace = "FlinkDotNet.Storage.FileSystem";
        // Add other infrastructure/connector namespaces as needed

        private const string JobManagerNamespace = "FlinkDotNet.JobManager";
        private const string TaskManagerNamespace = "FlinkDotNet.TaskManager";

        // Helper to load all relevant FlinkDotNet assemblies referenced by this test project
        private static readonly Types TypesInSolution = Types.InAssemblies(GetFlinkDotNetAssemblies());

        private static IEnumerable<Assembly> GetFlinkDotNetAssemblies()
        {
            // This dynamically loads assemblies referenced by the current test project
            // that start with "FlinkDotNet", excluding this test assembly itself.
            return AppDomain.CurrentDomain.GetAssemblies()
                .Where(a => a.GetName().Name?.StartsWith("FlinkDotNet") == true &&
                             a.GetName().Name != typeof(ArchitectureTests).Assembly.GetName().Name);
        }

        [TestMethod]
        public void CoreAbstractions_ShouldNotDependOn_Implementations()
        {
            var result = TypesInSolution
                .That()
                .ResideInNamespace(CoreAbstractionsNamespace)
                .ShouldNot()
                .HaveDependencyOn(ConnectorsSinksConsoleNamespace)
                .And().HaveDependencyOn(ConnectorsSourcesFileNamespace)
                .And().HaveDependencyOn(StorageFileSystemNamespace)
                .And().HaveDependencyOn(JobManagerNamespace) // Core Abstractions shouldn't know about JobManager specifics
                .And().HaveDependencyOn(TaskManagerNamespace) // Or TaskManager specifics
                .GetResult();

            Assert.IsTrue(result.IsSuccessful, "Core.Abstractions should not depend on concrete implementation projects. " + GetFailingTypes(result));
        }

        [TestMethod]
        public void CoreApi_ShouldNotDependOn_Implementations()
        {
            // Core.Api might depend on Core.Abstractions, but not on concrete connectors or storage.
            var result = TypesInSolution
                .That()
                .ResideInNamespace(CoreApiNamespace)
                .ShouldNot()
                .HaveDependencyOn(ConnectorsSinksConsoleNamespace)
                .And().HaveDependencyOn(ConnectorsSourcesFileNamespace)
                .And().HaveDependencyOn(StorageFileSystemNamespace)
                .And().HaveDependencyOn(JobManagerNamespace) // Core API shouldn't know about JobManager specifics beyond what abstractions provide
                .And().HaveDependencyOn(TaskManagerNamespace) // Or TaskManager specifics
                .GetResult();

            Assert.IsTrue(result.IsSuccessful, "Core.Api should not depend on concrete implementation projects. " + GetFailingTypes(result));
        }

        [TestMethod]
        public void CoreLogic_ShouldNotDependOn_SpecificConnectorsOrStorage()
        {
            // Core logic (non-Abstractions, non-Api) should ideally not depend directly on specific connectors or storage.
            // It might depend on abstractions defined in Core.Abstractions.
            var result = TypesInSolution
                .That()
                .ResideInNamespace(CoreNamespace) // Assuming FlinkDotNet.Core contains core logic
                .And().AreNotInterfaces() // Exclude interfaces which are fine
                .And().DoNotResideInNamespace(CoreAbstractionsNamespace) // Already covered
                .And().DoNotResideInNamespace(CoreApiNamespace) // Already covered
                .ShouldNot()
                .HaveDependencyOn(ConnectorsSinksConsoleNamespace)
                .And().HaveDependencyOn(ConnectorsSourcesFileNamespace)
                .And().HaveDependencyOn(StorageFileSystemNamespace)
                .GetResult();

            Assert.IsTrue(result.IsSuccessful, "Core logic should not directly depend on specific connector or storage projects. " + GetFailingTypes(result));
        }


        [TestMethod]
        public void Connectors_ShouldNotDependOn_OtherConnectors()
        {
            // Example: Console Sink should not depend on File Source
            var resultConsoleSink = TypesInSolution
                .That()
                .ResideInNamespace(ConnectorsSinksConsoleNamespace)
                .ShouldNot()
                .HaveDependencyOn(ConnectorsSourcesFileNamespace)
                .GetResult();

            var resultFileSource = TypesInSolution
                .That()
                .ResideInNamespace(ConnectorsSourcesFileNamespace)
                .ShouldNot()
                .HaveDependencyOn(ConnectorsSinksConsoleNamespace)
                .GetResult();

            Assert.IsTrue(resultConsoleSink.IsSuccessful, "Console Sink Connector should not depend on File Source Connector. " + GetFailingTypes(resultConsoleSink));
            Assert.IsTrue(resultFileSource.IsSuccessful, "File Source Connector should not depend on Console Sink Connector. " + GetFailingTypes(resultFileSource));
        }

        [TestMethod]
        public void JobManager_ShouldNotDependOn_TaskManager()
        {
            // Typically JobManager orchestrates TaskManagers, direct code dependency might be an issue
            // depending on the architecture (e.g., if communication is via RPC/interfaces).
            // This is a common rule to check.
            var result = TypesInSolution
                .That()
                .ResideInNamespace(JobManagerNamespace)
                .ShouldNot()
                .HaveDependencyOn(TaskManagerNamespace)
                .GetResult();

            Assert.IsTrue(result.IsSuccessful, "JobManager should generally not have a direct code dependency on TaskManager. " + GetFailingTypes(result));
        }

        [TestMethod]
        public void TaskManager_ShouldNotDependOn_JobManager()
        {
            // TaskManager might know about JobManager interfaces (for registration/RPC) but not concrete JobManager logic.
            // This rule might need refinement based on actual inter-process communication abstractions.
            var result = TypesInSolution
                .That()
                .ResideInNamespace(TaskManagerNamespace)
                .ShouldNot()
                .HaveDependencyOn(JobManagerNamespace)
                .GetResult();

            Assert.IsTrue(result.IsSuccessful, "TaskManager should generally not have a direct code dependency on JobManager. " + GetFailingTypes(result));
        }

        [TestMethod]
        public void InterfacesInCoreAbstractions_ShouldHave_IPrefix()
        {
            var result = TypesInSolution
                .That()
                .ResideInNamespace(CoreAbstractionsNamespace)
                .And()
                .AreInterfaces()
                .Should()
                .HaveNameStartingWith("I")
                .GetResult();

            Assert.IsTrue(result.IsSuccessful, "Interfaces in Core.Abstractions namespace should start with 'I'. " + GetFailingTypes(result));
        }

        // Helper method to format failing types for assertion messages
        private static string GetFailingTypes(NetArchTest.Rules.TestResult result)
        {
            if (result.IsSuccessful) return string.Empty;
            return "Failing types: " + string.Join(", ", result.FailingTypes?.Select(t => t.FullName) ?? Enumerable.Empty<string>());
        }
    }
}
