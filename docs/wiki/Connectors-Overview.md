# Connectors in Flink.NET: Overview

[Back to Main Outline](./Wiki-Structure-Outline.md)

### Table of Contents
- [Role of Connectors](#role-of-connectors)
- [Available Connectors & Status](#available-connectors--status)
  - [Basic/Testing Connectors:](#basictesting-connectors)
  - [Planned Connectors:](#planned-connectors)
- [Developing Custom Connectors](#developing-custom-connectors)

---

Connectors are essential components in Flink.NET that allow your streaming applications to interact with various external systems for reading data (sources) and writing data (sinks).

## Role of Connectors

*   **Source Connectors (`ISourceFunction`)**: These are responsible for ingesting data from external systems into your Flink.NET application. Examples of data sources include message queues (like Kafka, RabbitMQ), file systems, databases, or custom data generators.
    *   Users implement the `FlinkDotNet.Core.Abstractions.Sources.ISourceFunction<T>` interface to create custom sources.
    *   The source function's `Run()` method is called to start emitting data, and `Cancel()` is called to stop it.
    *   Data is emitted via an `ISourceContext<T>`.

*   **Sink Connectors (`ISinkFunction`)**: These are responsible for taking processed data from your Flink.NET application and writing it to external systems. Examples include databases, file systems, message queues, or dashboards.
    *   Users implement the `FlinkDotNet.Core.Abstractions.Sinks.ISinkFunction<T>` interface.
    *   The sink function typically has an `Open()` method for setup, an `Invoke()` method called for each record, and a `Close()` method for cleanup.

## Available Connectors & Status

Flink.NET aims to provide a suite of connectors for common data systems. The availability and maturity of these connectors will evolve over time.

### Basic/Testing Connectors:

*   **`ConsoleSinkFunction<TIn>`**:
    *   Located in `FlinkDotNet.Connectors.Sinks.Console`.
    *   A simple sink that prints records to the console using `ToString()`. Useful for debugging and basic examples.
    *   Status: **Available** (basic implementation).

*   **`FileSourceFunction<TOut>`**:
    *   Located in `FlinkDotNet.Connectors.Sources.File`.
    *   A basic source that reads lines from a text file and deserializes them using a provided `ITypeSerializer`.
    *   Status: **Available** (basic implementation, primarily for testing, not for production use with fault tolerance or advanced file system interactions like directory monitoring).

### Planned Connectors:

The following connectors are planned for future development. Their specific features and timelines are TBD.

*   **Kafka Source & Sink**: For robust integration with Apache Kafka.
    *   See `[Source Connectors](./Connectors-Source.md)` (placeholder)
    *   See `[Sink Connectors](./Connectors-Sink.md)` (placeholder)
*   **File System Connectors (Advanced)**: More advanced file sources and sinks with features like directory monitoring, partitioned writing, and integration with distributed file systems.
    *   See `[Source Connectors](./Connectors-Source.md)` (placeholder)
    *   See `[Sink Connectors](./Connectors-Sink.md)` (placeholder)
*   **Transactional Sinks**: Sinks that can participate in Flink.NET's checkpointing mechanism to provide exactly-once semantics when writing to external systems that support transactions.
    *   See `[Sink Connectors](./Connectors-Sink.md)` (placeholder)

## Developing Custom Connectors

If Flink.NET does not yet provide a connector for your specific external system, you can develop your own by implementing the `ISourceFunction<T>` or `ISinkFunction<T>` interfaces. Consider the following when developing custom connectors:

*   **Serialization**: Ensure data types being read or written can be properly serialized/deserialized.
*   **Fault Tolerance**: For sources that need to replay data upon recovery, consider how to manage offsets or sequence numbers. For sinks aiming for exactly-once, integrate with Flink.NET's checkpointing mechanism (e.g., by implementing two-phase commit protocols if the external system supports it). This is an advanced topic.
*   **Configuration**: Provide clear ways to configure your connector (e.g., connection strings, topics, file paths).
*   **Lifecycle**: Properly implement `Open()` and `Close()` methods for resource management if your connector interacts with external clients or connections.

As the Flink.NET ecosystem grows, more pre-built connectors will become available. Check the project's documentation and repository for the latest updates on connector availability.

---
**Navigation**
*   *(No previous/next links for now, as other specific connector pages are placeholders from the main outline)*
