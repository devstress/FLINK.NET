# Flink.NET Enhancements, Simulation, and Observability Report

This document summarizes the investigations, proof-of-concept enhancements, and the setup of an Aspire simulation environment for Flink.NET.

## 1. Apache Flink Learnings (incorporating Flink 2.0 Insights)

Based on general knowledge of Apache Flink's architecture, including recent insights from Apache Flink 2.0:

### 1.1. Flink's Network Stack
*   **Core Component:** Uses Netty for high-performance, asynchronous network communication.
*   **Buffer Management:** Employs sophisticated buffer management with pooled memory (on-heap and off-heap/managed) to reduce GC and manage data efficiently in the network pipeline.
*   **Credit-Based Flow Control (Backpressure):** Implements a robust, application-level credit-based flow control. Receiving tasks grant credits to sending tasks, preventing overload and ensuring system stability. This is more granular than default gRPC HTTP/2 flow control.
*   **Channels & Partitioning:** Data streams are partitioned and exchanged via logical channels, with mechanisms for multiplexing multiple logical channels over single TCP connections.

**Key Lessons for Flink.NET:**
*   The most significant area for improvement in Flink.NET's networking is the implementation of an explicit, Flink-like credit-based backpressure system.
*   Advanced memory management for network buffers could be a future optimization.
*   Consider Flink 2.0's asynchronous execution model for disaggregated state, which implies non-blocking network I/O for remote state access, potentially influencing Flink.NET's own network interactions if similar state architectures are pursued.

### 1.2. Flink's Serialization
*   **`TypeSerializer<T>` Framework:** Central abstraction for serializing, deserializing, and copying objects.
*   **Automatic Serializer Generation:** For POJOs, Flink analyzes class structure and generates efficient serializers, avoiding reflection.
*   **Kryo as Fallback:** Uses Kryo for types where Flink cannot generate a specific serializer.
*   **Avro Support:** Good integration with Apache Avro.
*   **State Snapshots:** Serializers are vital for state checkpointing, with mechanisms (`TypeSerializerSnapshot`) for schema evolution compatibility.

**Key Lessons for Flink.NET:**
*   Implementing source generation for POCO serializers in C# would be a major performance benefit for Flink.NET, moving away from reflection-based or general-purpose JSON serializers in critical paths.
*   Integrating a high-performance binary serializer (like MemoryPack or MessagePack) as a general-purpose option or fallback.
*   Ensuring robust state serializer evolution for long-running applications.
*   Note Flink 2.0's enhancements, such as more efficient built-in serializers for collection types and an upgraded Kryo version (5.6), as examples of ongoing serialization optimization in the Flink ecosystem.

### 1.3. Flink's Ordering/Watermarking
*   **Event Time Processing:** Core support for processing data based on event timestamps, allowing for consistent results with out-of-order data.
*   **Watermarks:** Mechanism to track the progress of event time. A Watermark `W(t)` signals that no more events with timestamp `t' <= t` are expected.
*   **`WatermarkStrategy`:** Allows users to define how timestamps are extracted and watermarks are generated.
*   **Windowing:** Event time windowing relies heavily on watermarks to determine when windows are complete.

**Key Lessons for Flink.NET:**
*   Flink.NET currently lacks explicit event time and watermarking. Introducing these concepts is essential for advanced ordered processing and handling out-of-order streams correctly.

### 1.4. Other Notable Flink 2.0 Directions

Apache Flink 2.0 also introduces significant advancements in other areas, indicating the broader evolution of the Flink ecosystem. While not immediate implementation goals for Flink.NET's current phase, awareness of these trends is valuable:

*   **Stream-Batch Unification:** Flink 2.0 strengthens this with features like Materialized Tables (supporting schema/query updates and YARN/Kubernetes submission) and Adaptive Batch Execution (e.g., Adaptive Broadcast Join, Join Skew Optimization). This aims to simplify pipelines that manage both real-time and historical data.
*   **Streaming Lakehouse:** Deeper integration with Apache Paimon for real-time data freshness in lakehouse architectures is a key theme, with performance enhancements for such scenarios.
*   **AI Integration:** Flink 2.0 is increasingly supporting AI/LLM use cases, for example, by enabling dynamic AI model invocation within Flink CDC transforms and introducing specialized SQL syntax for AI models.

### 1.5. Flink 2.0 API and Configuration Notes

It's important to acknowledge that Apache Flink 2.0 introduced significant breaking changes, underscoring that Flink is an evolving project:

*   **API Removals:** Major APIs like DataSet, Scala DataStream/DataSet, SourceFunction, SinkFunction (and Sink V1), TableSource/TableSink were removed, requiring migration to newer APIs (DataStream API, Table API/SQL, Source/Sink V2, DynamicTableSource/Sink).
*   **Configuration Changes:** The legacy `flink-conf.yaml` is no longer supported (replaced by `config.yaml`), and many old configuration options were removed.
*   **Implications for Flink.NET:** While Flink.NET draws inspiration from Apache Flink, it has its own distinct .NET-native API and will manage its own evolution. However, the types of changes seen in Flink 2.0 (e.g., modernization of source/sink interfaces) provide context for long-term API design considerations in any stream processing system. Flink.NET's current custom interfaces like `ITwoPhaseCommitSink` are examples of its independent API design.

## 2. Implemented Enhancements (Proof-of-Concept - Step 3)

### 2.1. Backpressure PoC (Client-Side Throttling)
*   **Objective:** To illustrate a basic mechanism for controlling send rates and preventing a fast sender from overwhelming a receiver or its own network buffers.
*   **Implementation:**
    *   Modified `FlinkDotNet.TaskManager.TaskExecutor.NetworkedCollector`.
    *   Added a `SemaphoreSlim` to limit the number of outstanding send operations (i.e., `WriteAsync` calls on the gRPC stream that have not yet "completed" from the client's perspective).
    *   The `MaxOutstandingSends` is currently hardcoded (e.g., to 100).
    *   The semaphore is acquired before attempting to send a record and released after the `WriteAsync` operation is initiated.
*   **Nature:** This is client-side self-throttling, not a full server-driven credit-based backpressure system like in Apache Flink. In this PoC, the permit is released immediately after the write is initiated by the client, rather than waiting for an explicit acknowledgement or credit from the server.
*   **File:** `FlinkDotNet/FlinkDotNet.TaskManager/TaskExecutor.cs`

### 2.2. Serialization PoC
*   This was optional and was not implemented due to the complexity of creating a new high-performance binary serializer from scratch in the current environment without specific bottleneck data.
*   The recommendation from the Flink investigation (source-generated serializers, integrating libraries like Protobuf/MemoryPack) stands.

## 3. Aspire Simulation Project (Step 4)

A .NET Aspire project (`FlinkDotNetAspire`) has been created to facilitate simulation and testing of Flink.NET components.

### 3.1. Structure
*   **`FlinkDotNetAspire.AppHost`:** Orchestrates the different Flink.NET services and the simulator.
    *   Launches `FlinkDotNet.JobManager`.
    *   Launches one instance of `FlinkDotNet.TaskManager` (`taskmanager1`).
    *   Launches `FlinkJobSimulator`.
    *   Configures service discovery (e.g., provides JobManager gRPC endpoint to TaskManager and Simulator).
*   **`FlinkDotNetAspire.ServiceDefaults`:** A shared project providing common service configurations, notably:
    *   OpenTelemetry setup (metrics, traces, logging).
    *   Default health checks.
*   **`FlinkJobSimulator`:** A console application that:
    *   Defines a simple Flink.NET streaming job (e.g., In-Memory Source -> Map -> Console Sink).
    *   Uses `StreamExecutionEnvironment` to build a `JobGraph`.
    *   Serializes the `JobGraph` to Protobuf.
    *   Submits the job to the `JobManager` via gRPC.
    *   Includes simple `SimpleInMemorySourceFunction` and `SimpleToUpperMapOperator` for the PoC job.

### 3.2. How to Run
1.  Ensure you have .NET 8 and Aspire workload installed.
2.  Navigate to the `FlinkDotNetAspire/FlinkDotNetAspire.AppHost` directory.
3.  Run the AppHost: `dotnet run`
4.  Leave this process running to keep the simulated cluster alive.
5.  This will launch the JobManager, TaskManager, and the FlinkJobSimulator.
6.  The Aspire Dashboard should also launch (typically `http://localhost:18888`), allowing observation of services.
7.  The Flink.NET Web UI is also launched automatically at `http://localhost:5020`.

### 3.3. Interpreting Observability Data (Step 5 Setup)
*   **Metrics Added:**
    *   `flinkdotnet.taskmanager.records_sent` (Counter): Number of records sent by a TaskManager's `NetworkedCollector`.
    *   `flinkdotnet.taskmanager.records_received` (Counter): Number of records received by a TaskManager's `DataExchangeServiceImpl`.
*   **Aspire Dashboard:**
    *   When the Aspire solution is running, navigate to the Aspire Dashboard.
    *   Under the "Metrics" tab, you should be able to find and select the `taskmanager1` service.
    *   The custom counters (`flinkdotnet.taskmanager.records_sent`, `flinkdotnet.taskmanager.records_received`) should be listed.
    *   You can observe their values increase as the `FlinkJobSimulator` runs its job and data flows through the TaskManager.
*   **Throughput:**
    *   Basic throughput can be inferred by observing the change in these counters over time.
    *   For precise "per second" rates, the Aspire dashboard might offer some views, or the data would need to be exported to a system like Prometheus/Grafana where rate queries can be performed. The current setup focuses on making the raw counters available.

## 4. System Scalability and FIFO Ordering (Preliminary Summary)

This section addresses the original issue's requirements regarding achieving 1 million messages per second (msg/sec) with ordered FIFO, based on the current Flink.NET implementation and the PoCs/investigations performed.

### 4.1. FIFO Ordering

*   **Current Guarantees:**
    *   Flink.NET provides strong FIFO guarantees for messages flowing between a single upstream subtask and a single downstream subtask due to gRPC's inherent stream ordering.
    *   In a simple chain of operators (Source -> Op1 -> Op2 -> Sink) where each operator has a parallelism of 1, end-to-end FIFO is maintained.
    *   When using `KeyBy`, FIFO is maintained for records *of the same key* that originate *from the same upstream parallel subtask* and are processed by the *same downstream parallel subtask*.

*   **Limitations (Where Global FIFO is Not Guaranteed):**
    *   **Parallelism:** When an operator's parallelism > 1, and data is distributed (e.g., round-robin, broadcast, or non-keyed distribution), global FIFO is lost.
    *   **`KeyBy` (Global Perspective):**
        *   Across different keys: No global order.
        *   For the same key from *different parallel upstream subtasks*: While all records for that key go to the same downstream subtask, their interleaving at the receiver from different gRPC streams is not globally ordered.
    *   **Merging Streams (`union`):** Global FIFO is lost when multiple distinct streams are merged.
    *   **Event Time Ordering:** Flink.NET currently lacks event time processing and watermarking, which are essential for achieving meaningful ordered processing on out-of-order data based on when events actually occurred.

*   **Achieving "Ordered FIFO" for 1 Million Msg/Sec:**
    *   If "ordered FIFO" means strict, global end-to-end ordering across an entire complex job (with parallelism, shuffles, merges) at 1 million msg/sec, **the current Flink.NET implementation cannot guarantee this.** This level of ordering in a distributed system is a significant challenge and typically requires:
        *   Assigning globally unique, ordered sequence IDs at the source.
        *   Implementing stateful re-ordering operators at merge points or where parallelism changes.
        *   Or, relying on event-time semantics with watermarks (which Flink.NET does not yet have).
    *   If "ordered FIFO" refers to FIFO within logical partitions (e.g., per key after a `KeyBy`), Flink.NET provides this for data from the same upstream instance. Stronger per-key FIFO across parallel upstreams would need enhancements.

### 4.2. Scalability to 1 Million Messages/Second

Achieving 1 million msg/sec is a significant benchmark. The current Flink.NET is a foundational implementation.

*   **Potential Bottlenecks:**
    *   **Serialization:** The current reliance on `JsonPocoSerializer` for complex types is likely a major performance bottleneck. String operations and JSON processing are CPU-intensive.
    *   **Backpressure:** The lack of a robust, server-driven backpressure mechanism can lead to instability, buffer overflows, or `OutOfMemoryError`s under high load if downstream operators cannot keep up. The implemented client-side throttling PoC is a very basic first step and not a complete solution.
    *   **Networking (gRPC):** While gRPC is generally performant, achieving 1M msg/sec for small messages might push its limits without careful tuning of message batching, payload sizes, and gRPC configurations. Flink's direct Netty usage with custom buffer management is highly optimized.
    *   **Operator Efficiency:** The performance of user-defined functions (UDFs) and internal operator logic will be critical. Inefficient operators will limit throughput regardless of framework capabilities.
    *   **State Management:** For stateful operations at 1M msg/sec, the performance of state backends (reads/writes, serialization/deserialization of state) would be paramount. (This aspect was not deeply investigated in this phase).
    *   **Garbage Collection:** High object churn (e.g., from inefficient serialization or temporary objects in operators) can lead to GC pauses, impacting throughput and latency.

*   **Current Status:**
    *   **Unlikely to achieve 1M msg/sec reliably across diverse jobs** with the current implementation, especially with complex objects or stateful operations.
    *   For very simple jobs (e.g., byte array pass-through with minimal processing and P=1), the limits would be closer to raw gRPC throughput, but this doesn't represent typical stream processing workloads.

### 4.3. Further Work Required for High Throughput & Stronger Ordering

1.  **Serialization Overhaul (Critical for Performance):**
    *   Implement source-generation for POCO serializers to avoid reflection.
    *   Integrate high-performance binary serialization libraries (e.g., MemoryPack, MessagePack) as defaults or options.
    *   Optimize built-in serializers for common types.
2.  **Robust Backpressure (Critical for Stability & Performance):**
    *   Implement a Flink-like credit-based backpressure mechanism integrated with network buffers. This requires changes to the data exchange protocol (e.g., bidirectional gRPC streaming).
3.  **Event Time Processing & Watermarking:**
    *   Introduce `WatermarkStrategy`, timestamp assignment, and watermark propagation.
    *   Update windowing operators and other relevant components to support event time.
4.  **Advanced Network Buffer Management:**
    *   Explore more direct control over memory for network buffers, potentially using off-heap memory for very large deployments, similar to Flink.
5.  **Efficient State Backends:**
    *   Ensure state backends are highly performant and scalable (e.g., RocksDB integration, optimized in-memory stores).
6.  **Benchmarking & Profiling:**
    *   Systematically benchmark the system with various workloads to identify and address bottlenecks.
7.  **Global Ordering Mechanisms (If Strict Global FIFO is a Hard Requirement):**
    *   Design and implement strategies for global sequence numbering and reordering if required for specific use cases.

**Conclusion:**
Flink.NET has a foundational architecture. The Aspire simulation project provides a good basis for further development and testing. However, significant enhancements, particularly in serialization and backpressure, are needed to approach the goal of 1 million messages/second reliably. True ordered FIFO across parallel, distributed operations would require implementing event-time semantics or dedicated global ordering logic.
