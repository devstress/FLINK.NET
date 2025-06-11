#!/usr/bin/env python3
"""
MCP Server for Flink.NET Testing Guidance
Provides testing patterns, recommendations, and test execution guidance
"""

import os
import json
import asyncio
from pathlib import Path
from typing import Dict, List, Any
import re

class FlinkDotNetTestingServer:
    def __init__(self):
        self.project_root = Path(os.getenv("PROJECT_ROOT", "."))
        self.test_commands = os.getenv("TEST_COMMANDS", "").split(";")
        self.test_patterns = {}
        self.test_structure = {}
    
    async def initialize(self):
        """Initialize the testing guidance server"""
        print("Initializing Flink.NET Testing Guidance MCP Server")
        await self.analyze_test_structure()
        await self.identify_test_patterns()
    
    async def analyze_test_structure(self):
        """Analyze the test project structure"""
        structure = {
            "test_projects": [],
            "test_categories": {
                "unit_tests": [],
                "integration_tests": [],
                "architecture_tests": []
            },
            "test_scripts": []
        }
        
        # Find test projects
        for test_proj in self.project_root.rglob("*.Tests.csproj"):
            project_info = {
                "name": test_proj.stem,
                "path": str(test_proj),
                "type": self.classify_test_type(test_proj.stem),
                "test_files": list(test_proj.parent.rglob("*.cs"))
            }
            structure["test_projects"].append(project_info)
            
            # Categorize tests
            if "Integration" in test_proj.stem:
                structure["test_categories"]["integration_tests"].append(project_info)
            elif "Architecture" in test_proj.stem:
                structure["test_categories"]["architecture_tests"].append(project_info)
            else:
                structure["test_categories"]["unit_tests"].append(project_info)
        
        # Find test scripts
        scripts_dir = self.project_root / "scripts"
        if scripts_dir.exists():
            for script in scripts_dir.glob("*test*"):
                structure["test_scripts"].append({
                    "name": script.name,
                    "path": str(script),
                    "type": "integration" if "integration" in script.name.lower() else "utility"
                })
        
        self.test_structure = structure
    
    def classify_test_type(self, project_name: str) -> str:
        """Classify test type based on project name"""
        if "Integration" in project_name:
            return "integration"
        elif "Architecture" in project_name:
            return "architecture"  
        elif "Performance" in project_name:
            return "performance"
        else:
            return "unit"
    
    async def identify_test_patterns(self):
        """Identify common testing patterns in the codebase"""
        patterns = {
            "test_frameworks": set(),
            "mocking_patterns": [],
            "assertion_patterns": [],
            "test_data_patterns": [],
            "async_test_patterns": [],
            "common_test_utilities": []
        }
        
        for test_project in self.test_structure["test_projects"]:
            for test_file in test_project["test_files"]:
                try:
                    content = Path(test_file).read_text(encoding='utf-8')
                    
                    # Identify test frameworks
                    if "[Fact]" in content or "using Xunit" in content:
                        patterns["test_frameworks"].add("xUnit")
                    if "[Test]" in content or "using NUnit" in content:
                        patterns["test_frameworks"].add("NUnit")
                    if "[TestMethod]" in content or "using Microsoft.VisualStudio.TestTools" in content:
                        patterns["test_frameworks"].add("MSTest")
                    
                    # Look for mocking patterns
                    if "Mock<" in content or "using Moq" in content:
                        patterns["mocking_patterns"].append({
                            "file": str(test_file),
                            "framework": "Moq",
                            "usage": self.extract_mock_usage(content)
                        })
                    
                    # Look for assertion patterns
                    assertion_keywords = ["Assert.", "Should.", "Expect."]
                    for keyword in assertion_keywords:
                        if keyword in content:
                            patterns["assertion_patterns"].append({
                                "file": str(test_file),
                                "style": keyword.rstrip('.'),
                                "examples": self.extract_assertion_examples(content, keyword)
                            })
                    
                    # Look for async test patterns
                    if "async Task" in content and ("[Fact]" in content or "[Test]" in content):
                        patterns["async_test_patterns"].append({
                            "file": str(test_file),
                            "pattern": "async_test_method"
                        })
                    
                    # Look for test data patterns
                    if "[Theory]" in content or "[TestCase]" in content:
                        patterns["test_data_patterns"].append({
                            "file": str(test_file),
                            "type": "parameterized_tests"
                        })
                        
                except Exception as e:
                    continue  # Skip files that can't be read
        
        # Convert sets to lists for JSON serialization
        patterns["test_frameworks"] = list(patterns["test_frameworks"])
        self.test_patterns = patterns
    
    def extract_mock_usage(self, content: str) -> List[str]:
        """Extract mock usage examples from test content"""
        mock_lines = []
        lines = content.split('\n')
        for line in lines:
            if "Mock<" in line or ".Setup(" in line or ".Verify(" in line:
                mock_lines.append(line.strip())
        return mock_lines[:5]  # Limit to 5 examples
    
    def extract_assertion_examples(self, content: str, keyword: str) -> List[str]:
        """Extract assertion examples from test content"""
        assertion_lines = []
        lines = content.split('\n')
        for line in lines:
            if keyword in line:
                assertion_lines.append(line.strip())
        return assertion_lines[:5]  # Limit to 5 examples
    
    async def get_testing_guidance(self, test_type: str = "unit") -> Dict[str, Any]:
        """Get testing guidance for specific test type"""
        guidance = {
            "recommended_framework": "xUnit",
            "common_patterns": [],
            "best_practices": [],
            "example_test_structure": "",
            "required_packages": []
        }
        
        if test_type == "unit":
            guidance.update({
                "common_patterns": [
                    "Arrange-Act-Assert pattern",
                    "Use Moq for mocking dependencies",
                    "Test public methods and behaviors",
                    "One assertion per test method"
                ],
                "best_practices": [
                    "Keep tests isolated and independent",
                    "Use descriptive test method names",
                    "Mock external dependencies",
                    "Test both success and failure scenarios"
                ],
                "example_test_structure": """
[Fact]
public void MethodName_Condition_ExpectedResult()
{
    // Arrange
    var mockDependency = new Mock<IDependency>();
    var sut = new SystemUnderTest(mockDependency.Object);
    
    // Act
    var result = sut.MethodToTest();
    
    // Assert
    Assert.NotNull(result);
    mockDependency.Verify(x => x.Method(), Times.Once);
}
                """,
                "required_packages": ["xunit", "xunit.runner.visualstudio", "Moq"]
            })
        
        elif test_type == "integration":
            guidance.update({
                "common_patterns": [
                    "Use .NET Aspire for orchestration",
                    "Test with real Redis and Kafka instances",
                    "Verify end-to-end scenarios",
                    "Use test containers when possible"
                ],
                "best_practices": [
                    "Ensure Docker is available",
                    "Clean up resources after tests",
                    "Use realistic test data",
                    "Test actual service communication"
                ],
                "example_test_structure": """
[Fact]
public async Task EndToEndJobExecution_Success()
{
    // Arrange - Start Aspire services
    using var aspireHost = CreateAspireHost();
    await aspireHost.StartAsync();
    
    // Act - Submit and execute job
    var result = await SubmitTestJob();
    
    // Assert - Verify job completion
    Assert.True(result.IsSuccess);
    Assert.Contains("completed", result.Status);
}
                """,
                "required_packages": ["Microsoft.AspNetCore.Mvc.Testing", "Docker.DotNet"]
            })
        
        return guidance
    
    async def analyze_test_coverage(self) -> Dict[str, Any]:
        """Analyze test coverage across the project"""
        coverage = {
            "projects_with_tests": len(self.test_structure["test_projects"]),
            "test_categories": {
                "unit": len(self.test_structure["test_categories"]["unit_tests"]),
                "integration": len(self.test_structure["test_categories"]["integration_tests"]), 
                "architecture": len(self.test_structure["test_categories"]["architecture_tests"])
            },
            "test_patterns_found": {
                "frameworks": self.test_patterns.get("test_frameworks", []),
                "mocking": len(self.test_patterns.get("mocking_patterns", [])),
                "async_tests": len(self.test_patterns.get("async_test_patterns", []))
            },
            "recommendations": []
        }
        
        # Add recommendations based on analysis
        if coverage["test_categories"]["integration"] == 0:
            coverage["recommendations"].append("Consider adding integration tests for end-to-end scenarios")
        
        if "xUnit" not in coverage["test_patterns_found"]["frameworks"]:
            coverage["recommendations"].append("Consider standardizing on xUnit framework")
        
        if coverage["test_patterns_found"]["mocking"] == 0:
            coverage["recommendations"].append("Consider using Moq for dependency mocking")
        
        return coverage
    
    async def get_test_execution_guide(self) -> Dict[str, Any]:
        """Get guide for test execution"""
        return {
            "unit_tests": {
                "command": "dotnet test FlinkDotNet/FlinkDotNet.sln -v minimal",
                "description": "Run all unit tests in the main solution",
                "requirements": [".NET 8.0 SDK"]
            },
            "integration_tests": {
                "linux_script": "bash scripts/run-integration-tests-in-linux.sh",
                "windows_script": "pwsh scripts/run-integration-tests-in-windows-os.ps1",
                "description": "Run integration tests with Aspire orchestration",
                "requirements": [".NET 8.0 SDK", "Docker Desktop", ".NET Aspire workload"]
            },
            "prerequisites": [
                "Install .NET 8.0 SDK",
                "Install Docker Desktop for integration tests",
                "Ensure all NuGet packages are restored"
            ],
            "troubleshooting": [
                "If tests fail, check Docker is running",
                "Ensure Redis and Kafka ports are available",
                "Check test output for detailed error messages"
            ]
        }
    
    async def handle_request(self, method: str, params: Dict[str, Any]) -> Dict[str, Any]:
        """Handle MCP requests"""
        if method == "get_testing_guidance":
            test_type = params.get("test_type", "unit")
            return await self.get_testing_guidance(test_type)
        elif method == "analyze_test_coverage":
            return await self.analyze_test_coverage()
        elif method == "get_test_execution_guide":
            return await self.get_test_execution_guide()
        elif method == "get_test_structure":
            return self.test_structure
        else:
            return {"error": f"Unknown method: {method}"}

async def main():
    """Main entry point for the MCP server"""
    server = FlinkDotNetTestingServer()
    await server.initialize()
    
    print("Flink.NET Testing Guidance MCP Server ready")
    print(f"Analyzed {len(server.test_structure['test_projects'])} test projects")
    print(f"Found frameworks: {server.test_patterns.get('test_frameworks', [])}")
    
    # Keep server running
    while True:
        await asyncio.sleep(1)

if __name__ == "__main__":
    asyncio.run(main())