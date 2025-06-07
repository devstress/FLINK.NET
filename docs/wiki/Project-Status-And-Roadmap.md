# Project Status and Roadmap

Flink.NET is currently in an **alpha/foundational development stage**. Key architectural components like the JobManager, TaskManager, and basic stream processing APIs are in place. A .NET Aspire-based simulation environment has been set up to facilitate testing and development (see [Samples and Tutorials](./Sample-Local-High-Throughput-Test.md)).

**Current Focus & Next Steps:**
The immediate focus is on implementing **Phase 1: Core Functionality for Exactly-Once FIFO Processing**. This includes:
*   **Barrier-Based Checkpointing:** Design and initial implementation stages for fault tolerance and exactly-once semantics. A preliminary design for checkpoint barriers has been documented ([Core Concepts: Checkpointing - Barriers](./Core-Concepts-Checkpointing-Barriers.md)).
*   **High-Performance Default Serializers & Robust Custom Serializer Support:** Addressing current bottlenecks with POCO serialization is a priority.
*   **Full Keyed Processing Logic:** Implementing robust support for keyed streams, including state management per key.

**Implemented So Far (Highlights):**
*   Core JobManager and TaskManager services with gRPC communication.
*   Basic stream processing API (`StreamExecutionEnvironment`, `DataStream`, simple operators like Map, Sink, Source).
*   JobGraph model and submission from client to JobManager.
*   .NET Aspire project for local cluster simulation, including Redis and Kafka integration for samples.
*   Initial observability setup using OpenTelemetry, viewable in the Aspire Dashboard.
*   Proof-of-concept for client-side send throttling.

**Future Phases (Long-Term Vision):**
*   **Phase 2: Performance Optimization for High Throughput** (e.g., operator chaining, advanced flow control).
*   **Phase 3: Robustness and Advanced Features** (e.g., production-ready state backends, incremental checkpointing, memory optimizations).

The project welcomes contributions, especially for the core features outlined in these phases.

---
Previous: [Advanced Security](./Advanced-Security.md)
Next: [Wiki Structure Outline](./Wiki-Structure-Outline.md)
