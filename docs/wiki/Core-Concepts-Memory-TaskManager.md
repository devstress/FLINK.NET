# Core Concepts: TaskManager Memory (Flink.NET)

[Back to Main Outline](./Wiki-Structure-Outline.md)

### Table of Contents
- [TaskManager Memory Components](#taskmanager-memory-components)
- [Configuring TaskManager Memory](#configuring-taskmanager-memory)
  - [Kubernetes Deployment](#kubernetes-deployment)
  - [Local Execution](#local-execution)
- [Relationship Between Parallelism, Slots, and Memory (Conceptual)](#relationship-between-parallelism-slots-and-memory-conceptual)
- [Default Values and Recommendations](#default-values-and-recommendations)

---

The Flink.NET TaskManager is responsible for executing the tasks (operators, user-defined functions) of a dataflow. Proper memory configuration for TaskManagers is critical for performance, as this is where the actual data processing happens.

Reference: [Memory Overview](./Core-Concepts-Memory-Overview.md)

## TaskManager Memory Components

TaskManager memory is divided among several key components:

*   **.NET Runtime Heap:** This is a significant portion of TaskManager memory, used for:
    *   Running user-defined functions (UDFs) and operator logic. Efficient UDFs that minimize object allocations are crucial for high throughput and reduced GC pressure.
    *   Storing objects that represent data records being processed.
    *   Buffers used by operators during computation (e.g., for sorting, hash tables in joins).
    *   The Flink.NET framework code running within the TaskManager.
*   **Network Buffers (Managed Off-Heap or On-Heap - TBD):**
    *   Essential for data shuffling between tasks (TaskManagers). Network buffers are used to send data upstream and receive data downstream.
    *   Flink.NET may manage these as a dedicated pool, potentially off the .NET GC heap for better performance and predictability (common in Java Flink), or it might use byte arrays on the managed heap. The exact Flink.NET strategy will be detailed here once finalized and is a key factor for high-performance networking.
    *   Insufficient network memory can lead to backpressure and degraded performance.
*   **Managed State Backends:**
    *   If using a memory-based state backend (e.g., for testing or small state scenarios), this state will reside in the TaskManager's memory, typically on the .NET heap.
    *   For larger state, disk-based or distributed state backends (like RocksDB, not directly in TM memory) are recommended. These might still use some TaskManager memory for caching (e.g., RocksDB's block cache) or JNI-equivalent overhead if integrated via C# bindings with native code.
*   **Framework Overhead:** Memory used by the .NET runtime and the Flink.NET TaskManager framework itself (e.g., for task management, communication with JobManager, metrics reporting).

## Configuring TaskManager Memory

Configuration depends on the deployment environment:

### Kubernetes Deployment

*   **Kubernetes Pod Resources:**
    *   Define `resources.requests.memory` and `resources.limits.memory` in the TaskManager's Kubernetes Deployment or Job definition.
    *   These settings control the total memory available to each TaskManager pod.
*   **Flink.NET Internal Configurations (Planned):**
    *   Flink.NET aims to provide parameters to partition the pod's memory. Illustrative examples (actual parameters TBD by Flink.NET developers):
        *   `taskmanager.process.memory.size`: Total memory for the TaskManager process. Should be set slightly less than the K8s pod limit to account for system overhead within the pod.
        *   `taskmanager.heap.memory.size`: Target size for the .NET runtime heap.
        *   `taskmanager.network.memory.size`: Total memory dedicated to network buffers.
        *   `taskmanager.state.memory.size`: (If applicable) Memory for managed state backends or caches for disk-based state backends.
        *   `taskmanager.framework.memory.size`: (Conceptual) Memory reserved for Flink framework components outside of heap and network buffers.
    *   These settings help optimize resource usage (e.g., balancing heap vs. network memory) and tune performance.

**Diagram: TaskManager Hybrid Memory Management in Kubernetes**
```ascii
+---------------------------------------------------------------------------------+
| Kubernetes Node                                                                 |
|                                                                                 |
|  +----------------------------------------------------------------------------+ |
|  | TaskManager Pod                                                            | |
|  | (K8s: resources.requests.memory: "X Gi", limits.memory: "Y Gi")            | |
|  |                                                                            | |
|  |  +------------------------------------------------------------------------+ | |
|  |  | Flink.NET TaskManager Process                                          | | |
|  |  |                                                                        | | |
|  |  |  +-------------------------+ Flink.NET Internal Memory Configs          | | |
|  |  |  | .NET Runtime Heap       | (e.g., taskmanager.heap.memory.size)     | | |
|  |  |  +-------------------------+                                          | | |
|  |  |  | Network Buffers         | (e.g., taskmanager.network.memory.size)  | | |
|  |  |  +-------------------------+                                          | | |
|  |  |  | Managed State (if any)  | (e.g., taskmanager.state.memory.size)    | | |
|  |  |  +-------------------------+                                          | | |
|  |  |  | Framework / Other       | (e.g., taskmanager.framework.memory.size)| | |
|  |  |  +-------------------------+                                          | | |
|  |  |                                                                        | | |
|  |  +------------------------------------------------------------------------+ | |
|  +----------------------------------------------------------------------------+ |
|                                                                                 |
+---------------------------------------------------------------------------------+
```

### Local Execution

*   Memory usage is primarily governed by Flink.NET internal configurations (e.g., via `appsettings.json`).
*   Key settings would be illustrative parameters like `taskmanager.process.memory.size`, `taskmanager.heap.memory.size`, `taskmanager.network.memory.size`, etc.
*   Careful configuration is needed to avoid OOM errors.

## Relationship Between Parallelism, Slots, and Memory (Conceptual)

*   **(Conceptual - Subject to Flink.NET's final resource management design)**
*   **Apache Flink's Slot Concept:** In Apache Flink, a TaskManager is divided into "task slots." Each slot represents a fixed portion of the TaskManager's resources and can run one parallel pipeline of operators. The number of slots typically corresponds to the number of CPU cores.
*   **Flink.NET's Approach (TBD):** It's yet to be detailed if Flink.NET will adopt an identical slotting mechanism or a different model for managing concurrent task execution and resource isolation within a TaskManager (e.g., based on .NET's threading capabilities like `ThreadPool` or `Task Parallel Library`, potentially without strict slot-based subdivision).
*   **Impact of Parallelism on Memory:** Regardless of the exact slotting model, increasing the parallelism of operators running within a single TaskManager will generally increase its memory requirements:
    *   **.NET Runtime Heap:** Each parallel task instance will have its own operator objects, UDF instances, and any data buffered within those operators (e.g., for sorting, internal collections). If UDFs are stateful (using Flink.NET's state abstractions), the working set for that state might also contribute to heap pressure, although bulk state is ideally in a state backend.
    *   **Network Buffers:** Higher parallelism often means more logical network connections (input and output channels) between this TaskManager and others. Each channel requires a certain number of network buffers.
    *   **State Backend Caches:** If using disk-based state backends like RocksDB, increased parallelism might lead to more concurrent accesses and potentially a larger working set for any in-memory caches (like RocksDB's block cache).
*   **Resource Sharing:** The extent of memory sharing (e.g., for framework components, common read-only data) versus per-task allocations will depend on Flink.NET's internal architecture.
*   This section will be updated as Flink.NET's TaskManager resource scheduling and execution model are finalized.

### Default Values and Recommendations

*   **(Under Development)** Specific default values for Flink.NET TaskManager memory configurations (e.g., `taskmanager.heap.memory.size`, `taskmanager.network.memory.size`) and detailed sizing formulas will be provided as the framework matures and undergoes performance characterization.
*   **General Guidance (Current):**
    *   TaskManager memory is highly application-dependent. Key factors include:
        *   **User-Defined Function (UDF) Complexity:** Memory consumed by your operator logic, internal data structures, and object allocations.
        *   **State Size:** If using stateful operators, the amount of state managed per key and the chosen state backend (in-memory vs. disk-based with caches).
        *   **Data Volume & Throughput:** Higher data volumes may require more network buffering and potentially larger internal buffers in operators.
        *   **Parallelism:** The number of parallel task instances running on the TaskManager.
        *   **Network Intensity:** Jobs with many shuffle operations (keyBy, rebalance, joins) will have higher network memory demands.
    *   **Initial Recommendation:** TaskManagers typically require significantly more memory than JobManagers. For Kubernetes deployments, start with a generous allocation (e.g., 4GiB or more per TaskManager pod for moderately complex jobs) and monitor closely.
    *   **Monitoring:** Pay close attention to:
        *   .NET GC statistics (collections, pause times, heap size) using `dotnet-counters`.
        *   Network buffer metrics (availability, usage - see [Network Memory Tuning](./Core-Concepts-Memory-Network.md)).
        *   State backend metrics (cache hit rates, memory usage if applicable).
        *   Overall pod memory usage in Kubernetes.
    *   Adjust configurations iteratively based on observed performance and stability.
*   **Future Content will include:**
    *   Default values for planned Flink.NET TaskManager memory parameters.
    *   Detailed formulas or heuristics for estimating memory requirements based on job characteristics (e.g., data types, state size, parallelism).
    *   Guidance on tuning .NET GC for streaming workloads.
    *   Troubleshooting common TaskManager memory issues (OOMs, excessive GC, backpressure related to memory).

---

*Further Reading:*
*   [Core Concepts: JobManager Memory (Flink.NET)](./Core-Concepts-Memory-JobManager.md)
*   [Core Concepts: Memory Tuning (Flink.NET)](./Core-Concepts-Memory-Tuning.md)
*   [Core Concepts: Memory Troubleshooting (Flink.NET)](./Core-Concepts-Memory-Troubleshooting.md)
*   [Core Concepts: Network Memory Tuning (Flink.NET)](./Core-Concepts-Memory-Network.md)

---
**Navigation**
*   Previous: [JobManager Memory](./Core-Concepts-Memory-JobManager.md)
*   Next: [Network Memory Tuning](./Core-Concepts-Memory-Network.md)
