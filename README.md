# Flink.NET

**Flink.NET** is an ambitious open-source project aiming to create a powerful, scalable, and fault-tolerant stream processing engine, built entirely within the .NET ecosystem. Inspired by [Apache Flink](https://flink.apache.org/), it seeks to provide .NET developers with a native solution for sophisticated real-time data processing.

The core objective is to implement fundamental stream processing concepts like stateful processing, exactly-once semantics, high throughput, low latency, and rich APIs, all within the .NET framework.

## Table of Contents
- [Why Flink.NET?](#why-flinknet)
- [Documentation Structure](#documentation-structure)
- [Project Status](#project-status)
- [Flink 2.0 Compatibility](#flink-20-compatibility)
- [Samples and Tutorials](#samples-and-tutorials)
- [AI-Assisted Development](#ai-assisted-development)
- [Getting Involved & Contribution](#getting-involved--contribution)
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

## Flink 2.0 Compatibility

An overview of how Flink.NET aligns with the capabilities introduced in Apache Flink 2.0 can be found in the [Project Status and Roadmap](./docs/wiki/Project-Status-And-Roadmap.md#flink-20-compatibility) document. The project is still experimental, and full feature parity has not been achieved yet.

## Samples and Tutorials

Explore practical examples to understand Flink.NET's capabilities:

*   **[Local High Throughput Test](./docs/wiki/Sample-Local-High-Throughput-Test.md)**: Demonstrates setting up a local environment and running a high-throughput test.

## Running Integration Tests on Windows

A PowerShell script is provided for executing the integration tests locally. It ensures the .NET 8 SDK version 8 or later is installed and verifies that Docker Desktop is available (Aspire uses Docker for Redis and Kafka). The script starts the Aspire AppHost and then runs the verification tests. Run the script from an elevated PowerShell prompt. The working directory will automatically switch to the script's location:

```powershell
pwsh scripts/run-integration-tests-in-windows-os.ps1
```



## Running Integration Tests on Linux

Linux users can run the integration tests using the accompanying shell script:

```bash
bash scripts/run-integration-tests-in-linux.sh
```

Pass an optional argument to control the number of simulated messages. The script verifies that the .NET 8 SDK and Docker are available, launches the Aspire AppHost, waits for a quick health check, and then runs the verification tests.

### Configuring JobManager Ports

The Aspire AppHost exposes the JobManager's REST and gRPC services on ports `8088` and `50051` by default. To override these values set the environment variables `JOBMANAGER_HTTP_PORT` and `JOBMANAGER_GRPC_PORT`. The TaskManager gRPC port can be configured with `TASKMANAGER_GRPC_PORT`.

## AI-Assisted Development

Flink.NET leverages modern AI-powered development tools to accelerate and enhance the development process. The following AI agents are supported:

### Supported AI Agents

- **GitHub Copilot Agent** ⭐ (Recommended)
- **ChatGPT's Codex AI**
- **Google's Jules AI**

### Using AI Agents

#### GitHub Copilot Agent (Recommended)

**Why Copilot Agent?**
GitHub Copilot Agent is the recommended AI assistant for Flink.NET development due to its deep integration with GitHub workflows, excellent .NET/C# support, and ability to understand the project context directly within your IDE.

**Assignment Process:**
1. **Repository Setup**: Ensure you have GitHub Copilot enabled in your GitHub account
2. **IDE Integration**: Install the GitHub Copilot extension in your preferred IDE (Visual Studio, VS Code, etc.)
3. **Project Context**: Open the Flink.NET repository in your IDE - Copilot will automatically understand the project structure and context
4. **Enable Copilot Chat**: Use `Ctrl+Shift+I` (or `Cmd+Shift+I` on Mac) to open Copilot Chat for contextual assistance

**Automation Process:**
- **Code Completion**: Copilot provides real-time code suggestions as you type
- **Documentation Generation**: Ask Copilot to generate XML documentation for your methods and classes
- **Test Generation**: Request unit tests for specific methods or classes
- **Code Review**: Use Copilot to review code changes and suggest improvements
- **Refactoring**: Get suggestions for code optimization and refactoring

**Example Usage:**
```csharp
// Type a comment describing what you want to implement
// Copilot will suggest the implementation
// Example: "Create a method to validate job graph vertices"
```

#### ChatGPT's Codex AI

ChatGPT can be used for:
- High-level architectural discussions
- Complex algorithm explanations
- Code review and optimization suggestions
- Documentation writing

**Usage**: Copy code snippets or describe your problem in ChatGPT for detailed explanations and solutions.

#### Google's Jules AI

Jules AI excels at:
- Code analysis and debugging
- Performance optimization suggestions
- Best practices recommendations

**Usage**: Integrate Jules AI through supported IDEs or use it for code analysis workflows.

### Best Practices for AI-Assisted Development

1. **Always Review AI Suggestions**: While AI tools are powerful, always review and test generated code
2. **Provide Context**: Give clear, specific prompts to get better results
3. **Iterate**: Use AI suggestions as starting points and refine based on project requirements
4. **Test Thoroughly**: Run tests after implementing AI-generated code to ensure correctness

## Getting Involved & Contribution

We welcome contributions! If you're a senior engineer interested in becoming an admin with merge rights, please contact the maintainers with your LinkedIn profile.

## License

This project is licensed under the Apache License 2.0. See the [LICENSE](LICENSE) file for details.
