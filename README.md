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

A PowerShell script is provided for executing the integration tests locally. It automatically installs the .NET 8 SDK and Docker Desktop using `winget` if they are missing. The tests now run inside a prebuilt Docker image (`flink-dotnet-windows`) that can also be used by the CI workflow. Run the script from an elevated PowerShell prompt:

```powershell
./scripts/run-integration-tests.ps1
```

The script pulls the prebuilt Docker image, starts a container running the Aspire AppHost (including Redis and Kafka), performs health checks, and then executes the verification tests.

By default, the image is retrieved from `ghcr.io/devstress/flink-dotnet-windows:latest`. Set the environment variable `FLINK_IMAGE_REPOSITORY` to override the repository if needed.

### Integration Test Image on GHCR

The Windows Docker image used for integration tests is published publicly to GitHub Container Registry (GHCR) at `ghcr.io/devstress/flink-dotnet-windows:latest`. Instructions on publishing or updating the image via GitHub Actions are available in [GHCR Public Image and GitHub Actions](./docs/wiki/GHCR-Tokens.md).

## AI-Assisted Development
The development of Flink.NET has been significantly accelerated and enhanced with the assistance of ChatGPT's Codex AI and Google's Jules AI, showcasing a modern approach to software engineering.

## Getting Involved & Contribution

We welcome contributions! If you're a senior engineer interested in becoming an admin with merge rights, please contact the maintainers with your LinkedIn profile.

## License

This project is licensed under the Apache License 2.0. See the [LICENSE](LICENSE) file for details.
