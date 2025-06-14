# System Design Overview

Flink.NET is architected as a distributed stream processing system designed for scalability, fault tolerance, and exactly-once processing semantics. It draws inspiration from FlinkDotnet's robust architecture, adapting its core components to the .NET ecosystem and a Kubernetes-native deployment model.

## Key Architectural Components

The system comprises several key interacting components:

*   **[JobManager](./Core-Concepts-JobManager.md) (Singleton/Leader Election):** This central component orchestrates job execution, manages checkpoints for fault tolerance, detects failures, and coordinates recovery processes. In a high-availability setup, leader election ensures there's always one active JobManager.
*   **[TaskManagers](./Core-Concepts-TaskManager.md) (Worker Nodes):** These are distributed .NET applications, typically running as Kubernetes Pods. TaskManagers are responsible for:
    *   Consuming input data streams.
    *   Executing the actual data processing logic (operators defined by the user).
    *   Managing local state for stateful operations (see [State Management Overview](./Core-Concepts-State-Management-Overview.md)).
    *   Interacting with external data sinks.
*   **Connectors (Sources & Sinks):** These are specialized components or libraries responsible for interfacing with external data systems. Sources read data from systems like Apache Kafka, Azure Event Hubs, etc., while Sinks write processed data to databases, APIs, or other messaging systems.
*   **Durable State Backend:** A persistent, scalable, and highly available storage solution is used to store all processing state and checkpoint metadata. This is crucial for achieving fault tolerance and exactly-once semantics, as detailed in the [Checkpointing Overview](./Core-Concepts-Checkpointing-Overview.md). For cloud-native deployments, inspiration is drawn from concepts like FlinkDotnet 2.0's Disaggregated State Management, which decouples state storage (often using Distributed File Systems like S3 or HDFS, managed by backends like Flink's ForSt) from compute resources and utilizes asynchronous state access. This approach aims to enhance scalability, resource efficiency, and recovery speed, guiding Flink.NET's strategy for robust state handling.
*   **[Memory Management](./Core-Concepts-Memory-Overview.md):** Understanding how Flink.NET manages memory for JobManagers, TaskManagers, network buffers, and state is crucial for performance and stability. This includes configurations for Kubernetes and local deployments, tuning, and troubleshooting.
*   **Job Submission & Management API:** A RESTful (and potentially gRPC) API exposed by the JobManager allows users and external systems to submit new processing jobs, monitor their status, manage their lifecycle (e.g., stop, cancel, scale), and inspect checkpoint information.

## Architectural Diagram

The following diagram illustrates the high-level interaction between these components within a Kubernetes environment:

```mermaid
graph TD
    subgraph Kubernetes Cluster
        JM[Job Manager] --> CM(Checkpoint Coordinator)
        JM -- Manages --> TM1[Task Manager 1]
        JM -- Manages --> TM2[Task Manager 2]
        JM -- Manages --> TM3[Task Manager 3]

        TM1 -- Processes Data --> OperatorA1
        TM1 -- Processes Data --> OperatorB1
        TM2 -- Processes Data --> OperatorA2
        TM2 -- Processes Data --> OperatorB2
        TM3 -- Processes Data --> OperatorA3
        TM3 -- Processes Data --> OperatorB3

        SourceConnector[Source Connector] --> InputStream(Input Stream: e.g., Kafka/Event Hubs)
        InputStream --> TM1
        InputStream --> TM2
        InputStream --> TM3

        OperatorA1 --> OperatorB1
        OperatorA2 --> OperatorB2
        OperatorA3 --> OperatorB3

        OperatorB1 --> SinkConnector[Sink Connector]
        OperatorB2 --> SinkConnector
        OperatorB3 --> SinkConnector

        SinkConnector --> OutputStream(Output Stream: e.g., DB/API)

        CM -- Coordinates Checkpoints --> TM1
        CM -- Coordinates Checkpoints --> TM2
        CM -- Coordinates Checkpoints --> TM3

        TM1 -- Persists State --> DSB[Durable State Backend]
        TM2 -- Persists State --> DSB
        TM3 -- Persists State --> DSB
        CM -- Stores Checkpoint Metadata --> DSB

        User[User/Product Team] --> API(Job Management API)
        API -- Submits Jobs --> JM
    end
```

This architecture is designed to enable parallel processing of data streams across multiple TaskManagers, coordinated by the JobManager, with robust state management and fault tolerance mechanisms ensuring data integrity and continuous operation.

---
Previous: [Business Requirements](./Business-Requirements.md)
[Home](https://github.com/devstress/FLINK.NET/blob/main/docs/wiki/Wiki-Structure-Outline.md)
Next: [Core Concepts - Memory Overview](./Core-Concepts-Memory-Overview.md)
