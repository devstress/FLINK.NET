# Flink.NET

**Flink.NET** is an ambitious open-source project aiming to create a powerful, scalable, and fault-tolerant stream processing engine, built entirely within the .NET ecosystem. Inspired by [FlinkDotnet](https://flink.apache.org/), it seeks to provide .NET developers with a native solution for sophisticated real-time data processing.

The core objective is to implement fundamental stream processing concepts like stateful processing, exactly-once semantics, high throughput, low latency, and rich APIs, all within the .NET framework.

## Table of Contents
- [Why Flink.NET?](#why-flinknet)
- [Documentation Structure](#documentation-structure)
- [Project Status](#project-status)
- [Flink.Net Compatibility](#flink-20-compatibility)
- [Samples and Tutorials](#samples-and-tutorials)
- [AI-Assisted Development](#ai-assisted-development)
- [Getting Involved & Contribution](#getting-involved--contribution)
- [Code analysis status](#code-analysis-status)
- [License](#license)

## Why Flink.NET?

Flink.NET aims to provide a native .NET stream processing framework, leveraging the .NET ecosystem for familiar tools and languages. It targets optimized cloud-native performance and fosters open-source contribution. For .NET teams, it can simplify deployment and operations compared to polyglot environments, offering full control and customization.

## Documentation Structure

Detailed documentation for Flink.NET is maintained in the `docs/wiki/` directory.

*   **To get started with Flink.NET and learn its basics, please begin with the [Getting Started Guide](./docs/wiki/Getting-Started.md).**
*   For a complete overview of all documentation pages and their purposes, refer to the [Wiki Structure Outline](./docs/wiki/Wiki-Structure-Outline.md).

This structure ensures that the main README remains concise, providing a high-level overview and pointers to more detailed information.

## Project Status

Flink.NET is currently in an **alpha/foundational development stage**. Core components are being actively developed.
For detailed information on the current development status, implemented features, and future roadmap, please see [Project Status and Roadmap](./docs/wiki/Project-Status-And-Roadmap.md).

## Flink.Net Compatibility

An overview of how Flink.NET aligns with the capabilities introduced in Flink.Net can be found in the [Project Status and Roadmap](./docs/wiki/Project-Status-And-Roadmap.md#flink-20-compatibility) document. The project is still experimental, and full feature parity has not been achieved yet.

## Samples and Tutorials

Explore practical examples to understand Flink.NET's capabilities:

*   **[Local High Throughput Test](./docs/wiki/Sample-Local-High-Throughput-Test.md)**: Demonstrates setting up a local environment and running a high-throughput test.

## Building and Development Lifecycle

### Quick Build
For a simple build of all solutions, use the build scripts in the root directory:

**Windows:**
```cmd
build-all.cmd
```

**Linux/macOS:**
```bash
./build-all.sh
```

These scripts restore dependencies and build all major solutions in sequence:
- FlinkDotNet/FlinkDotNet.sln
- FlinkDotNetAspire/FlinkDotNetAspire.sln  
- FlinkDotNet.WebUI/FlinkDotNet.WebUI.sln

### Full Development Lifecycle
For comprehensive development validation that mirrors GitHub Actions workflows locally, use:

**Windows:**
```cmd
run-full-development-lifecycle.cmd
```

**Linux/macOS:**
```bash
./run-full-development-lifecycle.sh
```

These scripts run all GitHub workflows in parallel:
- **Unit Tests**: .NET unit tests with coverage collection
- **SonarCloud Analysis**: Code analysis and build validation
- **Stress Tests**: Aspire stress tests with Redis/Kafka containers
- **Integration Tests**: Aspire integration tests

The full development lifecycle scripts automatically install missing prerequisites on Windows systems.

## Testing

Flink.NET includes two types of automated tests designed for different purposes:

### Integration Tests (Lightweight Structure Validation)

**Purpose**: Fast, lightweight tests that validate AppHost structure, accessibility, and basic configuration without requiring full orchestration.

**Running Integration Tests**:
```bash
# Run lightweight integration tests
dotnet test FlinkDotNetAspire/FlinkDotNetAspire.IntegrationTests/FlinkDotNetAspire.IntegrationTests.csproj --configuration Release
```

These tests verify:
- AppHost assembly loading
- Program class accessibility  
- Main method existence and signature
- Project references and Aspire framework integration

**CI Workflow**: Integration tests run automatically via `.github/workflows/integration-tests.yml` on every pull request and main branch push.

### Stress Tests (High-Throughput Performance Validation)

**Purpose**: Full end-to-end tests that start the complete Aspire orchestration (Redis, Kafka, JobManager, TaskManager) and process large message volumes (1M+ messages) to validate performance and exactly-once processing.

**Running Stress Tests on Windows**:

A PowerShell script is provided for executing the stress tests locally. It ensures the .NET 8 SDK version 8 or later is installed and verifies that Docker Desktop is available (Aspire uses Docker for Redis and Kafka). The script starts the Aspire AppHost, waits 30 seconds for initialization, then performs health checks with a maximum of 2 attempts spaced 5 seconds apart before running the verification tests. Run the script from an elevated PowerShell prompt. The working directory will automatically switch to the script's location:

```powershell
pwsh scripts/run-integration-tests-in-windows-os.ps1
```

**Running Stress Tests on Linux**:

Linux users can run the stress tests using the accompanying shell script:

```bash
bash scripts/run-integration-tests-in-linux.sh
```

Pass an optional argument to control the number of simulated messages. The script verifies that the .NET 8 SDK and Docker are available, launches the Aspire AppHost, waits 30 seconds for initialization, performs health checks with a maximum of 2 attempts spaced 5 seconds apart, and then runs the verification tests.

**CI Workflow**: Stress tests run via `.github/workflows/stress-tests.yml` and process 1 million messages to validate high-throughput performance.

### Configuring JobManager Ports

The Aspire AppHost exposes the JobManager's REST and gRPC services on ports `8088` and `50051` by default. To override these values set the environment variables `JOBMANAGER_HTTP_PORT` and `JOBMANAGER_GRPC_PORT`. The TaskManager gRPC port can be configured with `TASKMANAGER_GRPC_PORT`.

## AI-Assisted Development
The development of Flink.NET has been significantly accelerated and enhanced with the assistance of GitHub Copilot Agent, ChatGPT's Codex Agent and Google's Jules Agent, showcasing a modern approach to software engineering.

### Enhanced AI Development Environment

Flink.NET includes comprehensive AI assistance infrastructure to help developers work more efficiently:

#### GitHub Copilot Integration
- **Custom Instructions**: See `.copilot/instructions.md` for Copilot-specific guidance
- **Project Context**: AI agents have access to comprehensive project knowledge
- **Development Patterns**: Built-in understanding of Flink.NET coding conventions

#### Model Context Protocol (MCP) Configuration
Configure GitHub Copilot with enhanced project context:

1. Add the MCP configuration from `.copilot/mcp-config.json` to your GitHub repository settings
2. Navigate to repository Settings → Copilot → Model Context Protocol
3. This enables GitHub Copilot to access project documentation and context directly

The MCP configuration provides:
- **Documentation Access**: Direct access to wiki content and project documentation
- **Project Understanding**: Enhanced context about Flink.NET architecture and patterns
- **Development Guidance**: Better assistance with implementation patterns and testing

#### Using GitHub Copilot Agent

Contributors can leverage GitHub Copilot Agent for development assistance:

1. **Create an Issue**: Open a new issue describing the feature or bug fix needed
2. **Assign to Copilot**: Wait for a contributor to assign the issue to `Copilot`
3. **Check Progress**: You will see 👀 reaction from Copilot and progress reports from the Issue & linked Pull Request
4. **Review Results**: The Copilot Agent will create pull requests that contributors can review and merge

#### AI Development Resources
- **AGENTS.md**: Comprehensive development guide for AI agents
- **.copilot/ai-context.md**: Complete project context and implementation details
- **.copilot/mcp-config.json**: MCP server configuration for enhanced AI assistance

## Getting Involved & Contribution

We welcome contributions! If you're a senior engineer interested in becoming an admin with merge rights, please contact the maintainers with your LinkedIn profile.

## Code analysis status
https://sonarcloud.io/summary/overall?id=devstress_FLINK.NET&branch=main

## License

This project is licensed under the Apache License 2.0. See the [LICENSE](LICENSE) file for details.
