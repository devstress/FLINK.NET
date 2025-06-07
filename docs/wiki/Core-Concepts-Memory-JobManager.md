# Core Concepts: JobManager Memory (Flink.NET)

The Flink.NET JobManager is responsible for coordinating job execution, managing resources, and overseeing checkpoints and recovery. Its memory requirements must be configured properly for a stable and performant Flink.NET cluster.

Reference: [Memory Overview](./Core-Concepts-Memory-Overview.md)

## JobManager Memory Components

The JobManager's memory is primarily utilized by:

*   **.NET Runtime Heap:** This is the main memory pool for the JobManager. It's used for:
    *   Storing job metadata (JobGraphs, ExecutionGraphs).
    *   Managing task lifecycles and tracking their states.
    *   Coordinating checkpointing and recovery operations.
    *   Running the JobManager's internal services (e.g., REST API endpoints, RPC services for communication with TaskManagers).
    *   User code that might be part of job submission or cluster management utilities, if any run within the JobManager process.
*   **Network Communication:** While typically less intensive than TaskManagers, the JobManager still requires some memory for network buffers to communicate with TaskManagers (e.g., sending deployment instructions, receiving heartbeats and status updates). This is usually a smaller, fixed amount.
*   **Framework Overhead:** General memory used by the .NET runtime and the Flink.NET framework itself.

## Configuring JobManager Memory

Configuration depends on the deployment environment:

### Kubernetes Deployment

In Kubernetes, JobManager memory is managed by a combination of Kubernetes pod specifications and (planned) Flink.NET internal configurations.

*   **Kubernetes Pod Resources:**
    *   Define `resources.requests.memory` and `resources.limits.memory` in the JobManager's Kubernetes StatefulSet or Deployment definition.
    *   `requests.memory`: Guarantees the memory for the JobManager pod. This should be set to a value that comfortably allows the JobManager to operate under normal load.
    *   `limits.memory`: Enforces a hard cap. If the JobManager exceeds this, Kubernetes might terminate the pod (OOMKilled). This should be set higher than the request, allowing for some headroom, but not excessively high to waste resources.
*   **Flink.NET Internal Configurations (Planned):**
    *   Flink.NET aims to provide specific parameters to control memory allocation *within* the pod's total memory. Examples (illustrative names, actual parameters TBD by Flink.NET developers):
        *   `jobmanager.process.memory.size`: Specifies the total amount of memory the Flink.NET JobManager process will attempt to use. This should typically align closely with the Kubernetes limit, accounting for any small sidecars or system overhead within the pod.
        *   `jobmanager.heap.memory.size`: Suggests a target size for the .NET runtime heap.
        *   `jobmanager.offheap.memory.size`: (If Flink.NET uses significant off-heap memory in JobManager for specific components like network buffers or custom state stores) Defines the size for these off-heap components.
    *   These internal settings help Flink.NET structure its memory usage efficiently. For example, the .NET heap size will influence GC behavior.

**Diagram: Hybrid Memory Management in Kubernetes**
```ascii
+--------------------------------------------------------------------------+
| Kubernetes Node                                                          |
|                                                                          |
|   +----------------------------------------------------------------------+
|   | JobManager Pod                                                       |
|   | (K8s: resources.requests.memory: "X Mi", limits.memory: "Y Mi")      |
|   |                                                                      |
|   |   +----------------------------------------------------------------+ |
|   |   | Flink.NET JobManager Process                                   | |
|   |   |                                                                | |
|   |   |   +-----------------------+  Flink.NET Internal Memory Configs | |
|   |   |   | .NET Runtime Heap     |  (e.g., jobmanager.heap.memory.size)| |
|   |   |   +-----------------------+                                    | |
|   |   |   | Network Buffers       |  (e.g., jobmanager.network.memory.size) | |
|   |   |   +-----------------------+                                    | |
|   |   |   | Other Components      |                                    | |
|   |   |   +-----------------------+                                    | |
|   |   |                                                                | |
|   |   +----------------------------------------------------------------+ |
|   +----------------------------------------------------------------------+
|                                                                          |
+--------------------------------------------------------------------------+
```

*(This diagram illustrates how Kubernetes provides overall memory limits, while Flink.NET configurations (planned) will manage the internal partitioning.)*

### Local Execution

When running the JobManager locally (e.g., for development or testing without Kubernetes):

*   Memory usage is primarily governed by the Flink.NET internal configurations loaded via `appsettings.json`, environment variables, or command-line arguments.
    *   `jobmanager.process.memory.size` (or equivalent planned parameter) would be the main setting to control overall memory usage.
    *   `jobmanager.heap.memory.size` (or equivalent planned parameter) would guide the .NET runtime heap.
*   It's crucial to set these appropriately, as there's no external orchestrator like Kubernetes imposing hard limits, increasing the risk of `OutOfMemoryException` if not configured well.

**Diagram: Memory Management for Local Execution**
```ascii
+---------------------------------------------------------------------+
| Flink.NET JobManager Process (Local Execution Mode)                 |
|                                                                     |
|   +-----------------------+  Flink.NET Internal Memory Configs      |
|   | .NET Runtime Heap     |  (e.g., jobmanager.heap.memory.size,    |
|   +-----------------------+   loaded from appsettings.json, env vars)|
|   | Network Buffers       |                                         |
|   +-----------------------+                                         |
|   | Other Components      |                                         |
|   +-----------------------+                                         |
|                                                                     |
+---------------------------------------------------------------------+
```

### Default Values and Recommendations

*   **(Under Development)** This section will be updated with specific default values and detailed sizing recommendations for Flink.NET JobManager memory as the framework matures and more performance benchmarks become available.
*   **General Guidance (Current):**
    *   JobManager memory requirements typically scale with factors such as the number of concurrently running jobs, the complexity of their `JobGraph`s (number of vertices and edges), the configured parallelism of operators, and the frequency and size of checkpoint metadata being managed.
    *   Compared to TaskManagers, JobManagers generally require less memory, as they do not execute user code or buffer large amounts of in-flight data. However, ensuring sufficient heap space for managing job state and cluster coordination is vital.
    *   **Initial Recommendation:** When deploying, start with a conservative estimate (e.g., for a Kubernetes pod, consider 1-2 GiB as a starting point, similar to JVM-based Flink, but monitor closely as .NET memory characteristics will differ). Observe actual memory usage and .NET GC performance under load.
    *   **Monitoring:** Use .NET counters (`dotnet-counters`) for GC and heap analysis, and Kubernetes metrics (if applicable) to track pod memory usage. Adjust configurations based on these observations.
*   **Future Content will include:**
    *   Default values for planned Flink.NET specific configurations (e.g., `jobmanager.heap.memory.size`).
    *   Guidance on how to estimate memory based on workload characteristics.
    *   Troubleshooting common JobManager memory issues.

## Considerations for High Availability (HA)

In an HA setup, JobManager memory should also account for any state that needs to be kept for leader election and recovery purposes, though this state is typically managed via external durable storage rather than consuming large amounts of JobManager heap.

---

*Further Reading:*
*   [Core Concepts: TaskManager Memory (Flink.NET)](./Core-Concepts-Memory-TaskManager.md)
*   [Core Concepts: Memory Tuning (Flink.NET)](./Core-Concepts-Memory-Tuning.md)
*   [Core Concepts: Memory Troubleshooting (Flink.NET)](./Core-Concepts-Memory-Troubleshooting.md)
*   [Core Concepts: Network Memory Tuning (Flink.NET)](./Core-Concepts-Memory-Network.md)

---
[Home](https://github.com/devstress/FLINK.NET/blob/main/docs/wiki/Wiki-Structure-Outline.md)
