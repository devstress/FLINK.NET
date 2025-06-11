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
The development of Flink.NET has been significantly accelerated and enhanced with the assistance of GitHub Copilot Agent, ChatGPT's Codex Agent and Google's Jules Agent, showcasing a modern approach to software engineering.

### Using GitHub Copilot Agent

Contributors can leverage GitHub Copilot Agent for development assistance:

1. **Create an Issue**: Open a new issue describing the feature or bug fix needed
2. **Assign to Copilot**: Mention `@copilot` in the issue to assign it to the Copilot Agent
3. **Check Progress**: Monitor the issue for updates and progress reports from the Copilot Agent
4. **Review Results**: The Copilot Agent will create pull requests that you can review and merge

## Getting Involved & Contribution

We welcome contributions! If you're a senior engineer interested in becoming an admin with merge rights, please contact the maintainers with your LinkedIn profile.

## License

This project is licensed under the Apache License 2.0. See the [LICENSE](LICENSE) file for details.
