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
*   JobManagerController now exposes endpoints for job submission, scaling, restarting and DLQ handling.
*   Experimental DisaggregatedStateBackend stores snapshots on the local filesystem.
*   MonotonicWatermarkGenerator and SlidingEventTimeWindows provide basic watermarking and windowing support.
*   HighAvailabilityCoordinator prototype enables simple leader election for tests.

**Future Phases (Long-Term Vision):**
*   **Phase 2: Performance Optimization for High Throughput** (e.g., operator chaining, advanced flow control).
*   **Phase 3: Robustness and Advanced Features** (e.g., production-ready state backends, incremental checkpointing, memory optimizations).

The project welcomes contributions, especially for the core features outlined in these phases.

## Flink 2.0 Compatibility

This section tracks how the project aligns with features introduced in Apache Flink 2.0. The goal is eventual parity, but the implementation is still early.

### Implemented Features
- **Networking Buffer Pool** – basic `NetworkBufferPool` and `LocalBufferPool` for efficient buffer reuse.
- **Checkpoint Barriers** – JobManager and TaskManager prototypes support checkpoint barrier messages.
- **Windowing API Skeleton** – simple window API with triggers and `KeyedWindowProcessor`.
- **Disaggregated State Backend Prototype** – local filesystem-based `DisaggregatedStateBackend`.
- **Transactional Sink Interface** – connectors can implement exactly-once via two-phase commit.
- **Watermark Generator and Sliding Windows** – `MonotonicWatermarkGenerator` and `SlidingEventTimeWindows` for watermark propagation.
- **High Availability Coordinator** – minimal leader election provided by `HighAvailabilityCoordinator`.
- **Unified Source API** – `IUnifiedSource<T>` for stream-batch unification.

### Planned Features
- Scalable state backends and remote storage options.
- Transactional sources and sinks.
- Watermark alignment across tasks.
- Robust leader election and recovery.
- Additional connectors using the unified API.

### Status
The project remains experimental. Achieving full feature parity with Apache Flink 2.0 will require substantial additional development.
---
Previous: [Advanced Security](./Advanced-Security.md)
[Home](https://github.com/devstress/FLINK.NET/blob/main/docs/wiki/Wiki-Structure-Outline.md)
Next: [Wiki Structure Outline](./Wiki-Structure-Outline.md)
