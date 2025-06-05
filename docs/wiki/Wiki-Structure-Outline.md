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
        *   *Flink.NET Content:* Setting up a development environment, writing a first simple Flink.NET application (conceptual, as the system evolves).
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

## 3. Developing Flink.NET Applications
    *   **Project Setup**
        *   *Flink.NET Content:* Required NuGet packages (`FlinkDotNet.Core.Abstractions`, etc.).
    *   **Defining Data Types**
        *   *Flink.NET Content:* POCOs, serialization considerations (to be detailed later).
    *   **Working with Operators (User-Defined Functions)**
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
    *   **Using `IRuntimeContext`**
        *   *Flink.NET Content:* Accessing job/task info, state, etc.
    *   **Working with State (Detailed)**
        *   Using `StateDescriptor`s.
        *   Examples for each state type.
        *   State TTL (planned).
        *   *Apache Flink Ref:* [Working with State](https://nightlies.apache.org/flink/flink-docs-stable/docs/dev/datastream/fault-tolerance/state/).
    *   **Windowing API (Future)**
        *   *Flink.NET Content:* Concepts and examples.
        *   *Apache Flink Ref:* [Windowing](https://nightlies.apache.org/flink/flink-docs-stable/docs/dev/datastream/operators/windows/).

## 4. Connectors
    *   **Overview**
    *   **Source Connectors** (Kafka, File - planned)
    *   **Sink Connectors** (Console, File, Transactional - planned)
    *   *Apache Flink Ref:* [Connectors](https://nightlies.apache.org/flink/flink-docs-stable/docs/connectors/datastream/overview/).

## 5. JobManager API Reference
    *   **REST API**
    *   **Internal gRPC API**

## 6. Deployment
    *   **Kubernetes Deployment**
    *   **Local Development/Testing** (potentially with Aspire)
    *   *Apache Flink Ref:* [Deployment](https://nightlies.apache.org/flink/flink-docs-stable/docs/deployment/overview/).

## 7. Advanced Topics (Placeholders)
    *   **Serialization**
    *   **Metrics & Monitoring**
    *   **Performance Tuning**
    *   **Security**
