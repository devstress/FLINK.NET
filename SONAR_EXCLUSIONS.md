# SonarCloud Configuration

This repository contains multiple solutions with different purposes, requiring different SonarCloud quality gate configurations.

## SonarCloud Exclusions

The following projects are excluded from SonarCloud coverage and reliability requirements as they are not production library code:

### FlinkDotNetAspire Projects
**Path**: `FlinkDotNetAspire/`  
**Exclusions**: Directory.Build.props + individual project files  

All projects in this solution are simulation, tooling, and integration test projects:
- **FlinkJobSimulator**: Simulation code for testing the FlinkDotNet ecosystem
- **IntegrationTestVerifier**: Integration testing tooling 
- **FlinkDotNetAspire.AppHost.AppHost**: Aspire host configuration
- **FlinkDotNetAspire.AppHost.ServiceDefaults**: Aspire service defaults/tooling

### FlinkDotNet.WebUI Projects
**Path**: `FlinkDotNet.WebUI/`  
**Exclusions**: Directory.Build.props + FlinkDotNet.WebUI.csproj  

This is a Blazor WebAssembly UI project:
- **FlinkDotNet.WebUI**: Web-based UI for FlinkDotNet (no unit tests, UI-focused)

### FlinkDotNet Test Projects
**Path**: `FlinkDotNet/`  
**Exclusions**: Individual project files  

Test projects that test other code but don't need coverage themselves:
- **FlinkDotNet.Architecture.Tests**: Architecture and dependency rule validation tests

## Properties Used

- `SonarQubeExclude=true`: Excludes the entire project from SonarCloud analysis
- `ExcludeFromCodeCoverage=true`: Excludes from code coverage requirements  
- `SonarQubeTestProject=true`: Marks as test/tooling project

## Core Library Projects Under Quality Gates

The following projects maintain full SonarCloud quality gates (80% coverage, A reliability rating):

### FlinkDotNet Core Libraries
- **FlinkDotNet.Core**: Main FlinkDotNet implementation
- **FlinkDotNet.Core.Abstractions**: Core abstractions and interfaces  
- **FlinkDotNet.Core.Api**: Public API layer
- **FlinkDotNet.JobManager**: Job management functionality
- **FlinkDotNet.TaskManager**: Task execution functionality
- **FlinkDotNet.Storage.FileSystem**: File system storage implementation
- **FlinkDotNet.Connectors.Sinks.Console**: Console sink connector
- **FlinkDotNet.Connectors.Sources.File**: File source connector

### FlinkDotNet Test Libraries with Coverage
- **FlinkDotNet.JobManager.Tests**: Unit tests for JobManager (120 tests)

## Quality Gate Strategy

SonarCloud quality gates are enforced on all FlinkDotNet core library projects. Each project must maintain:
- 80% or higher code coverage through comprehensive unit tests
- A reliability rating through proper error handling and robust code
- Clean code principles and maintainability standards

This ensures quality standards are maintained across the entire FlinkDotNet ecosystem.

## Verification

All exclusions have been verified:
- ✅ All 3 solutions build successfully
- ✅ All unit tests pass
- ✅ SonarCloud exclusions properly configured on simulation and UI projects
- ✅ Core library projects maintain quality gate requirements through comprehensive unit testing