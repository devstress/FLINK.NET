# Welcome to Flink.NET

## Overview of Flink.NET

Flink.NET is a .NET library that allows developers to write and execute FlinkDotnet applications using C#. It aims to provide a familiar and intuitive experience for .NET developers while leveraging the power and scalability of FlinkDotnet.

*(See main [Readme.md](../../Readme.md) for more details)*

## Key Features & Goals

*   **Native .NET Development:** Write Flink applications entirely in C# using familiar .NET patterns and libraries.
*   **FlinkDotnet Compatibility:** Leverage the core strengths of FlinkDotnet, including its robust stream processing engine, fault tolerance, and state management capabilities.
*   **Extensibility:** Designed to be extensible, allowing for the addition of custom operators, connectors, and state backends.
*   **Simplified Deployment:** (Future Goal) Streamlined deployment options, potentially integrating with .NET Aspire and Kubernetes.

*(See main [Readme.md](../../Readme.md) for more details)*

## Relationship to FlinkDotnet

Flink.NET is built upon the foundations of FlinkDotnet. It acts as a .NET layer that interacts with Flink's core components.

*   **Philosophy of Alignment:** Flink.NET strives to align with FlinkDotnet's core concepts and architecture. Many Flink.NET components have direct counterparts in FlinkDotnet, and the documentation will often refer to the official FlinkDotnet documentation for deeper understanding.
*   **Key Differences:**
    *   **.NET Ecosystem:** Flink.NET is tailored for the .NET ecosystem, utilizing C# as the primary language and integrating with .NET libraries and tools.
    *   **Implementation Choices:** Specific implementation choices may differ from FlinkDotnet's Java implementation to better suit the .NET environment.

**FlinkDotnet References:**

*   [FlinkDotnet Home](https://flink.apache.org/)
*   [What is FlinkDotnet?](https://nightlies.apache.org/flink/flink-docs-stable/docs/concepts/overview/)

## Getting Started

Ready to dive in? Our [[Getting Started|Getting-Started]] guide will walk you through setting up your environment and writing your first Flink.NET application.

**FlinkDotnet References:**

*   [Flink Getting Started](https://nightlies.apache.org/flink/flink-docs-stable/docs/try-flink/local_installation/)
*   [Fundamental Concepts](https://nightlies.apache.org/flink/flink-docs-stable/docs/concepts/flink_architecture/)

## Use Cases

Flink.NET can be used for a variety of stream processing applications, including:

*   Real-time data analytics
*   Event-driven microservices
*   Complex event processing
*   Data ingestion and transformation pipelines
*   Anomaly detection

## Flink.Net Best Practices

For production-ready applications, follow our comprehensive best practices guides:

* **[[Flink.Net Best Practices: Stream Processing Patterns|Flink.Net-Best-Practices-Stream-Processing-Patterns]]** - Complete guide to Flink.Net standard pipeline patterns
* **[[Flink.Net Back Pressure|FLINK_NET_BACK_PRESSURE]]** - Credit-based flow control and back pressure handling
* **[[RocksDB State Backend|Core-Concepts-RocksDB-State-Backend]]** - Enterprise-grade state management

### Recommended Pipeline Pattern

Follow the Flink.Net standard pattern for optimal performance:

```
Source -> Map/Filter -> KeyBy -> Process/Window -> AsyncFunction -> Sink
```

This pattern provides:
- Superior performance and scalability
- Built-in fault tolerance and exactly-once semantics  
- Rich monitoring and observability
- Industry-standard patterns and maintainability

*(This section will be expanded with more specific examples in the future.)*

## Community & Contribution

Flink.NET is an open-source project, and we welcome contributions from the community!

*   **Getting Involved:** Join our community channels (links to be added) to ask questions, share ideas, and connect with other users.
*   **Contribution Guidelines:** Please see the main [Readme.md](../../Readme.md#getting-involved--contribution) for details on how to contribute to the project.
