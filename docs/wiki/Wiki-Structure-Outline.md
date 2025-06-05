# Flink.NET GitHub Wiki Structure Proposal

This document outlines the proposed structure for the Flink.NET GitHub Wiki. The goal is to provide comprehensive documentation for users and developers, with clear references to Apache Flink''s concepts where applicable.

## 1. Introduction
    *   **Welcome to Flink.NET**
        *   Overview of Flink.NET (links to Readme sections)
        *   Key Features & Goals (links to Readme sections)
        *   Relationship to Apache Flink
            *   *Flink.NET Content:* Philosophy of alignment, key differences (e.g., .NET ecosystem, specific implementation choices).
            *   *Apache Flink Ref:* Link to Apache Flink''s main page and "What is Flink?"
    *   **Getting Started**
        *   *Flink.NET Content:* Setting up a development environment, writing a first simple Flink.NET application (conceptual, as the system evolves).
        *   *Apache Flink Ref:* Link to Flink''s "Getting Started" and "Concepts" for basic stream processing ideas.
    *   **Use Cases**
        *   *Flink.NET Content:* Examples of potential applications for Flink.NET.
    *   **Community & Contribution**
        *   *Flink.NET Content:* How to get involved, contribution guidelines.

## 2. Core Concepts (Flink.NET Implementation)
    *   **Architecture Overview** (links to Readme section)
        *   JobManager
            *   *Flink.NET Content:* Role in Flink.NET, HA setup (planned).
            *   *Apache Flink Ref:* Link to Flink''s "JobManager" and "High Availability" documentation.
        *   TaskManagers
            *   *Flink.NET Content:* Role in Flink.NET, execution of tasks, pod structure.
            *   *Apache Flink Ref:* Link to Flink''s "TaskManager" and "Task Execution" documentation.
        *   Dataflow & JobGraph
            *   *Flink.NET Content:* How logical plans are translated and executed.
            *   *Apache Flink Ref:* Link to Flink''s "Dataflow Programming Model" and "Job Execution".
    *   **Stream Processing Model**
        *   Streams, Events, Transformations (briefly)
        *   *Apache Flink Ref:* Link to Flink''s "DataStream API Programming Guide" for these fundamental ideas.
    *   **State Management**
        *   *Flink.NET Content:* Overview of state in Flink.NET (`IValueState`, `IListState`, `IMapState`), keyed state, state descriptors. Brief mention of planned state backends (in-memory for testing, durable options like Redis + snapshots).
        *   *Apache Flink Ref:* Link to Flink''s "Working with State" documentation.
    *   **Checkpointing & Fault Tolerance**
        *   *Flink.NET Content:* Overview of Flink.NET''s checkpointing mechanism (barriers, snapshots, recovery), exactly-once semantics. Role of `CheckpointMetadata` and `OperatorStateMetadata`.
        *   *Apache Flink Ref:* Link to Flink''s "Checkpointing" and "Fault Tolerance" documentation.
    *   **Exactly-Once Semantics** (dedicated section, cross-referencing)
        *   *Flink.NET Content:* How Flink.NET aims to achieve this through checkpointing, transactional sinks, and idempotent processing.
        *   *Apache Flink Ref:* Link to Flink''s "Fault Tolerance Guarantees."

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
        *   `IWindowOperator` (placeholder, to be expanded with `WindowAssigners`, `Triggers`, `ProcessWindowFunction`-like concepts)
        *   *For each:*
            *   *Flink.NET Content:* C# interface details, simple usage examples.
            *   *Apache Flink Ref:* Link to the corresponding Flink `Function` (e.g., `MapFunction`, `RichMapFunction`).
    *   **Using `IRuntimeContext`**
        *   *Flink.NET Content:* Accessing job/task info, (later) accessing state, using accumulators/broadcast vars (planned).
    *   **Working with State (Detailed)**
        *   Using `ValueStateDescriptor`, `ListStateDescriptor`, `MapStateDescriptor`.
        *   Examples of using `IValueState`, `IListState`, `IMapState` in a Rich operator.
        *   State TTL, cleanup (planned).
        *   *Apache Flink Ref:* Link to Flink''s "Working with State" (keyed state, state types).
    *   **Windowing API (Future - based on `IWindowOperator` evolution)**
        *   *Flink.NET Content:* Event Time vs. Processing Time, Tumbling, Sliding, Session Windows, Triggers, Evictors.
        *   *Apache Flink Ref:* Link to Flink''s "Windowing" documentation.

## 4. Connectors
    *   **Overview**
        *   *Flink.NET Content:* Concept of sources and sinks in Flink.NET.
    *   **Source Connectors**
        *   (Planned) Kafka Connector
        *   (Planned) File Source Connector
        *   *Flink.NET Content:* Configuration, usage, checkpointing integration.
        *   *Apache Flink Ref:* Link to Flink''s "Connectors" documentation (e.g., Kafka connector).
    *   **Sink Connectors**
        *   (Planned) Console Sink
        *   (Planned) File Sink
        *   (Planned) Transactional Sinks (`ITwoPhaseCommitSink`)
        *   *Flink.NET Content:* Configuration, usage, guarantees.
        *   *Apache Flink Ref:* Link to Flink''s "Connectors" and `TwoPhaseCommitSinkFunction`.

## 5. JobManager API Reference
    *   **REST API**
        *   *Flink.NET Content:* Endpoints, request/response DTOs for `JobManagerController`.
        *   *Apache Flink Ref:* (Optional) Comparison to Flink''s Monitoring REST API if alignment is pursued.
    *   **Internal gRPC API**
        *   *Flink.NET Content:* Overview of `JobManagerInternalService` and its methods/messages (for developers of Flink.NET itself or advanced users).

## 6. Deployment
    *   **Kubernetes Deployment**
        *   *Flink.NET Content:* Overview of deploying JobManager and TaskManagers on Kubernetes, HA considerations (planned), configuration.
        *   *Apache Flink Ref:* Link to Flink''s "Kubernetes Deployment" documentation.
    *   **Local Development/Testing**
        *   *Flink.NET Content:* How to run/test Flink.NET applications locally (will evolve, potentially with Microsoft Aspire).

## 7. Advanced Topics (Placeholders for future content)
    *   **Serialization**
    *   **Metrics & Monitoring**
    *   **Performance Tuning**
    *   **Security**
