# Flink.NET GitHub Wiki Structure Proposal

### Table of Contents
- [1. Introduction](#1-introduction)
- [2. Core Concepts (Flink.NET Implementation)](#2-core-concepts-flinknet-implementation)
- [3. Developing Flink.NET Applications](#3-developing-flinknet-applications)
- [4. Connectors](#4-connectors)
- [5. JobManager API Reference](#5-jobmanager-api-reference)
- [6. Deployment](#6-deployment)
- [7. Advanced Topics](#7-advanced-topics)

This document outlines the proposed structure for the Flink.NET GitHub Wiki. The goal is to provide comprehensive documentation for users and developers, with clear references to Apache Flink''s concepts where applicable.

## 1. Introduction
    *   **Welcome to Flink.NET**
        *   Overview of Flink.NET (See main [Readme.md](../Readme.md))
        *   Key Features & Goals (See main [Readme.md](../Readme.md))
        *   Relationship to Apache Flink
            *   *Flink.NET Content:* Philosophy of alignment, key differences (e.g., .NET ecosystem, specific implementation choices).
            *   *Apache Flink Ref:* [Apache Flink Home](https://flink.apache.org/), [What is Apache Flink?](https://nightlies.apache.org/flink/flink-docs-stable/docs/concepts/overview/)
    *   **Getting Started**
        *   *Flink.NET Content:* [Setting up and Writing a Simple Application](./Getting-Started.md)
        *   *Apache Flink Ref:* [Flink Getting Started](https://nightlies.apache.org/flink/flink-docs-stable/docs/try-flink/local_installation/), [Fundamental Concepts](https://nightlies.apache.org/flink/flink-docs-stable/docs/concepts/flink_architecture/)
    *   **Use Cases**
        *   *Flink.NET Content:* Examples of potential applications for Flink.NET.
    *   **Community & Contribution**
        *   *Flink.NET Content:* How to get involved, contribution guidelines (See main [Readme.md](../Readme.md#getting-involved--contribution)).

## 2. Core Concepts (Flink.NET Implementation)
    *   **Architecture Overview** (See main [Readme.md](../Readme.md#system-design-overview))
        *   [JobManager](./Core-Concepts-JobManager.md)
            *   *Flink.NET Content:* Role in Flink.NET, HA setup (planned).
            *   *Apache Flink Ref:* [JobManager](https://nightlies.apache.org/flink/flink-docs-stable/docs/concepts/flink_architecture/#jobmanager), [High Availability (HA)](https://nightlies.apache.org/flink/flink-docs-stable/docs/deployment/ha/)
        *   [TaskManager](./Core-Concepts-TaskManager.md)
            *   *Flink.NET Content:* Role in Flink.NET, execution of tasks, pod structure.
            *   *Apache Flink Ref:* [TaskManager](https://nightlies.apache.org/flink/flink-docs-stable/docs/concepts/flink_architecture/#taskmanager), [Task Execution](https://nightlies.apache.org/flink/flink-docs-stable/docs/internals/task_execution/)
        *   Dataflow & JobGraph
            *   *Flink.NET Content:* How logical plans are translated and executed.
            *   *Apache Flink Ref:* [Dataflow Programming Model](https://nightlies.apache.org/flink/flink-docs-stable/docs/concepts/programming_model/), [Job Execution](https://nightlies.apache.org/flink/flink-docs-stable/docs/internals/job_scheduling/)
    *   **Stream Processing Model**
        *   Streams, Events, Transformations (briefly)
        *   *Apache Flink Ref:* [DataStream API Programming Guide](https://nightlies.apache.org/flink/flink-docs-stable/docs/dev/datastream/overview/)
    *   [State Management Overview](./Core-Concepts-State-Management-Overview.md)
        *   *Flink.NET Content:* Overview of state in Flink.NET (`IValueState`, `IListState`, `IMapState`), keyed state, state descriptors. Brief mention of planned state backends.
        *   *Apache Flink Ref:* [Working with State](https://nightlies.apache.org/flink/flink-docs-stable/docs/dev/datastream/fault-tolerance/state/)
    *   [Checkpointing & Fault Tolerance](./Core-Concepts-Checkpointing-Overview.md)
        *   *Flink.NET Content:* Overview of Flink.NET''s checkpointing mechanism, exactly-once semantics. Role of `CheckpointMetadata` and `OperatorStateMetadata`.
        *   *Apache Flink Ref:* [Checkpointing](https://nightlies.apache.org/flink/flink-docs-stable/docs/dev/datastream/fault-tolerance/checkpointing/), [Fault Tolerance Guarantees](https://nightlies.apache.org/flink/flink-docs-stable/docs/ops/state/fault_tolerance/)
    *   **Exactly-Once Semantics** (dedicated section, cross-referencing)
        *   *Flink.NET Content:* How Flink.NET aims to achieve this.
        *   *Apache Flink Ref:* [Fault Tolerance Guarantees](https://nightlies.apache.org/flink/flink-docs-stable/docs/ops/state/fault_tolerance/)
    *   **Memory Management**
        *   `[Overview](./Core-Concepts-Memory-Overview.md)`
        *   `[JobManager Memory](./Core-Concepts-Memory-JobManager.md)`
        *   `[TaskManager Memory](./Core-Concepts-Memory-TaskManager.md)`
        *   `[Network Memory Tuning](./Core-Concepts-Memory-Network.md)`
        *   `[Memory Tuning](./Core-Concepts-Memory-Tuning.md)`
        *   `[Memory Troubleshooting](./Core-Concepts-Memory-Troubleshooting.md)`
    *   **Serialization**
        *   `[Serialization Overview](./Core-Concepts-Serialization.md)`
        *   `[Serialization Strategy](./Core-Concepts-Serialization-Strategy.md)`

## 3. Developing Flink.NET Applications
    *   **Project Setup**
        *   *Flink.NET Content:* Required NuGet packages (`FlinkDotNet.Core.Abstractions`, etc.).
    *   `[Defining Data Types](./Developing-Data-Types.md)`
        *   *Flink.NET Content:* POCOs, serialization considerations (to be detailed later).
    *   `[Working with Operators](./Developing-Operators.md)`
        *   `IMapOperator` & `IRichMapOperator`
        *   `IFilterOperator` & `IRichFilterOperator`
        *   `IFlatMapOperator` & `IRichFlatMapOperator` (and `ICollector`)
        *   `IReduceOperator` & `IRichReduceOperator`
        *   `IAggregateOperator` & `IRichAggregateOperator`
        *   `IJoinFunction` & `IRichJoinFunction`
        *   `IWindowOperator` (placeholder)
        *   *For each:*
            *   *Flink.NET Content:* C# interface details, simple usage examples.
            *   *Apache Flink Ref:* Link to the corresponding Flink `Function`.
    *   `[Using IRuntimeContext](./Developing-RuntimeContext.md)`
        *   *Flink.NET Content:* Accessing job/task info, state, etc.
    *   `[Working with State](./Developing-State.md)`
        *   Using `StateDescriptor`s.
        *   Examples for each state type.
        *   State TTL (planned).
        *   *Apache Flink Ref:* [Working with State](https://nightlies.apache.org/flink/flink-docs-stable/docs/dev/datastream/fault-tolerance/state/).
    *   `[Windowing API (Future)](./Developing-Windowing-Api.md)`
        *   *Flink.NET Content:* Concepts and examples.
        *   *Apache Flink Ref:* [Windowing](https://nightlies.apache.org/flink/flink-docs-stable/docs/dev/datastream/operators/windows/).

## 4. Connectors
    *   `[Overview](./Connectors-Overview.md)`
    *   `[Source Connectors (Kafka, File - planned)](./Connectors-Source.md)`
    *   `[Sink Connectors (Console, File, Transactional - planned)](./Connectors-Sink.md)`
    *   *Apache Flink Ref:* [Connectors](https://nightlies.apache.org/flink/flink-docs-stable/docs/connectors/datastream/overview/).

## 5. JobManager API Reference
    *   `[REST API](./JobManager-Rest-Api.md)`
    *   `[Internal gRPC API](./JobManager-Grpc-Api.md)`

## 6. Deployment
    *   `[Kubernetes Deployment](./Deployment-Kubernetes.md)`
    *   `[Local Development and Testing](./Deployment-Local.md)` (potentially with Aspire)
    *   *Apache Flink Ref:* [Deployment](https://nightlies.apache.org/flink/flink-docs-stable/docs/deployment/overview/).

## 7. Advanced Topics
    *   `[Metrics and Monitoring](./Advanced-Metrics-Monitoring.md)`
    *   `[Security](./Advanced-Security.md)`
    *   **Performance Tuning** (Note: a dedicated [Memory Tuning](./Core-Concepts-Memory-Tuning.md) page now exists under Core Concepts. This could be expanded or link to it.)
