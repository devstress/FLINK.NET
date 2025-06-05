# Core Concepts: TaskManager Memory (Flink.NET)

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

*   **(Placeholder)** This section will discuss how the number of task slots per TaskManager (if this concept is used in Flink.NET similarly to Java Flink, where a slot is a unit of resource isolation) and operator parallelism can influence memory requirements.
*   Generally, more slots or higher parallelism per TaskManager might require more memory, as each slot/task needs its share of resources (heap for its UDFs and operator state, network buffers for its connections). Flink.NET's threading model (e.g., whether threads are shared across parallel tasks of the same operator within a TM, or dedicated) and resource sharing mechanisms within a TaskManager will determine the specifics of this relationship.

### Default Values and Recommendations

*   **(Placeholder)** This section will be filled in as Flink.NET matures and default configurations are established.
*   TaskManager memory is highly application-dependent (state size, UDF complexity, data volume, chosen state backend, network intensity).
*   Start with a reasonable allocation (e.g., 4Gi+ per TaskManager pod in K8s for moderately complex jobs) and tune based on application performance, GC behavior, network metrics, and state backend metrics (if applicable).

---

*Further Reading:*
*   [Core Concepts: JobManager Memory (Flink.NET)](./Core-Concepts-Memory-JobManager.md)
*   [Core Concepts: Memory Tuning (Flink.NET)](./Core-Concepts-Memory-Tuning.md)
*   [Core Concepts: Memory Troubleshooting (Flink.NET)](./Core-Concepts-Memory-Troubleshooting.md)
*   [Core Concepts: Network Memory Tuning (Flink.NET)](./Core-Concepts-Memory-Network.md)
