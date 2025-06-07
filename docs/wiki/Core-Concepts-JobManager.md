# Core Concepts: JobManager

### Table of Contents
- [Key Responsibilities](#key-responsibilities)
- [Deployment & High Availability (HA)](#deployment--high-availability-ha)
- [Memory Management](#memory-management)
  - [Memory Management in Kubernetes (Hybrid Model)](#memory-management-in-kubernetes-hybrid-model)
  - [Memory Management for Local Execution](#memory-management-for-local-execution)
- [Disclaimer](#disclaimer)

The JobManager is the central coordinating component of a Flink.NET deployment. It is responsible for the orchestration of job execution, resource management (including memory and CPU, in conjunction with the underlying cluster manager like Kubernetes), checkpoint coordination, and failure recovery. In a high-availability setup, there can be multiple JobManager instances, but only one is active as the leader at any given time.

See also: [System Design Overview in Readme.md](../../../Readme.md#system-design-overview)

*(Apache Flink Ref: [Setup JobManager](https://nightlies.apache.org/flink/flink-docs-release-1.17/docs/deployment/memory/mem_setup_jobmanager/))*

## Key Responsibilities:

1.  **Job Submission & Lifecycle Management:**
    *   Receives job submissions from clients (e.g., via the REST API). A job is typically submitted as a `JobGraph`, which is a description of the dataflow topology, operators, user code, and configuration.
    *   Manages the overall lifecycle of a job: deploying, monitoring, cancelling, stopping, and restarting (current implementation provides API endpoints; full orchestration logic is under development).

2.  **Task Scheduling & Resource Allocation:**
    *   Translates the `JobGraph` into an `ExecutionGraph`, which represents the physical execution plan (conceptual, full implementation under development).
    *   Requests resources (e.g., TaskManager pods with specific CPU/memory allocations) from the cluster manager (Kubernetes is the target architecture, integration is under development).
    *   Assigns individual tasks (parallel instances of operators) to available [TaskManagers](./Core-Concepts-TaskManager.md) (conceptual, full implementation under development).

3.  **Checkpoint Coordination:**
    *   The JobManager hosts the **Checkpoint Coordinator** (conceptual, current implementation includes gRPC stubs for communication and API for metadata retrieval).
    *   Triggers periodic checkpoints for running jobs (under development).
    *   Receives acknowledgements from TaskManagers when they complete their part of a checkpoint (gRPC stubs exist).
    *   Manages metadata about completed checkpoints (currently via `InMemoryJobRepository`, planned for durable storage).
    *   Initiates recovery procedures using the last successfully completed checkpoint in case of failures (under development).
    *   *(See Wiki Page: [Core Concepts: Checkpointing Overview](./Core-Concepts-Checkpointing-Overview.md) for more details)*

4.  **Failure Detection & Recovery:**
    *   Monitors TaskManagers via heartbeats (using the internal gRPC API, current implementation is a stub).
    *   Detects failures of TaskManagers or individual tasks (under development).
    *   Coordinates the recovery process (under development).

5.  **Metadata & API Hosting:**
    *   Maintains metadata about jobs, tasks, checkpoints, etc. (currently via `InMemoryJobRepository`).
    *   Exposes the Job Management REST API and Internal gRPC API.

## Deployment & High Availability (HA)

*   In Flink.NET, the JobManager is designed to be deployable on Kubernetes, with ongoing development to fully leverage its orchestration capabilities for HA and resource management (including CPU and memory).
*   Production environments require a High Availability (HA) setup, which is a key area of ongoing development.

*(Apache Flink Ref: [JobManager High Availability (HA)](https://nightlies.apache.org/flink/flink-docs-master/docs/deployment/ha/overview/))*

### Current Status and Planned HA
Currently, the Flink.NET JobManager utilizes an `InMemoryJobRepository` for storing all job and checkpoint metadata. This means that state is not durable across JobManager restarts, which is a fundamental limitation for achieving true High Availability. If the JobManager instance fails or is restarted, all running job states and metadata will be lost.

Key features required for robust HA on Kubernetes, such as leader election among multiple JobManager instances and automated failover, are planned for future development.

### General HA Principles on Kubernetes (Future Direction)
Achieving High Availability for the JobManager on Kubernetes will involve several standard patterns:

*   **Leader Election:** A distributed consensus mechanism will be necessary to elect a single active JobManager (leader) from a pool of instances. This can be achieved by leveraging native Kubernetes leader election capabilities (e.g., using Lease objects) or by integrating libraries like `Kubernetes.LeaderElection` or similar coordination primitives.
*   **Durable State Storage:** Critical for HA is the persistence of job metadata (JobGraphs, ExecutionGraphs, application state, operator states) and checkpoint information. This state must be stored in a fault-tolerant manner, accessible to any JobManager instance that might become the leader. Apache Flink 2.0's continued focus on robust and scalable state management, including concepts like disaggregated state, further underscores the critical need for durable and consistent storage of JobManager metadata for true high availability. Options include:
    *   **Persistent Volumes (PVs):** Using Kubernetes PVs with appropriate ReadWriteMany access modes if shared, or ReadWriteOnce with mechanisms to re-attach to a new leader.
    *   **Databases:** Employing a relational or NoSQL database deployed in a fault-tolerant configuration.
    *   **Distributed Stores:** Utilizing systems like etcd. While Apache Flink often uses ZooKeeper, a lighter-weight or more .NET-idiomatic solution might be preferred for Flink.NET if it meets the consistency and durability requirements.
*   **Liveness and Readiness Probes:** Properly configured liveness and readiness probes for JobManager pods will allow Kubernetes to manage their lifecycle effectively, restarting unhealthy instances or routing traffic only to ready ones. This contributes to overall service availability.

The JobManager is the "brain" of a Flink.NET application, and making it resilient is crucial for production deployments.

## Memory Management

Flink.NET JobManager's memory configuration strategy adapts to its deployment environment.

### Memory Management in Kubernetes (Hybrid Model)

When deploying the Flink.NET JobManager on Kubernetes, a hybrid approach to memory management is envisioned:

1.  **Kubernetes Layer (Overall Allocation):**
    *   The *total memory available to the JobManager pod* is defined and controlled by Kubernetes. This is done using standard Kubernetes manifest specifications within the `resources` section of the container spec:
        *   `resources.requests.memory`: Specifies the amount of memory Kubernetes guarantees for the pod.
        *   `resources.limits.memory`: Defines the maximum memory the pod can consume. Exceeding this can lead to the pod being OOMKilled by Kubernetes.
    *   This layer aligns with cloud-native practices, delegating overall resource entitlement and enforcement to the orchestrator.

2.  **Flink.NET Layer (Internal Partitioning - Planned):**
    *   *Within the memory allocated by Kubernetes*, Flink.NET aims to provide its own set of internal configuration options. These options (e.g., analogous to Apache Flink's `jobmanager.memory.process.size`, `jobmanager.memory.heap.size`, `jobmanager.memory.off-heap.size` but tailored for .NET) would allow for more granular partitioning of the pod's memory for different internal components of the JobManager.
    *   Potential internal areas for such configuration could include:
        *   **.NET Runtime Heap:** Memory for core JobManager logic, object graphs, task metadata, etc.
        *   **Network Buffers:** Dedicated memory for network communication (e.g., with TaskManagers).
        *   **State Backends (if memory-based components exist in JobManager):** Memory for any in-memory state stores or caches directly within the JobManager.
    *   This internal partitioning will help optimize performance and predictability for different workloads by ensuring critical components have sufficient dedicated memory, preventing contention within the overall pod memory.

**Benefits of the Hybrid Model:**
*   **Control:** Administrators gain control at two levels: overall pod resource consumption (via Kubernetes) and internal JobManager behavior/performance (via Flink.NET configurations).
*   **Resource Efficiency:** Allows for better resource management by sizing the pod appropriately and then fine-tuning internal allocations.
*   **Monitoring:** The total pod memory usage can be monitored using standard Kubernetes tools (`kubectl top pod`, Prometheus, etc.). Internal Flink.NET metrics (once implemented) would provide insights into the usage of internally defined memory pools.
*   **Alignment with Modern Flink Architectures:** Concepts from Apache Flink 2.0, such as Disaggregated State Management, aim to optimize resource utilization and could influence how Flink.NET's JobManager interacts with state and memory. For instance, offloading more state-related concerns to a specialized backend could reduce direct memory pressure on the JobManager, aligning with efficient cloud-native operations.

As Flink.NET matures, these internal configuration options and best practices for setting them in conjunction with Kubernetes requests/limits will be further developed and documented.

The following diagram illustrates this hybrid memory model:
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
|   |   |   | .NET Runtime Heap     |  (e.g., jobmanager.memory.heap.size)| |
|   |   |   +-----------------------+                                    | |
|   |   |   | Network Buffers       |  (e.g., jobmanager.memory.network.buffers) | |
|   |   |   +-----------------------+                                    | |
|   |   |   | State Backends (mem)  |                                    | |
|   |   |   +-----------------------+                                    | |
|   |   |   | Other Components      |                                    | |
|   |   |   +-----------------------+                                    | |
|   |   |                                                                | |
|   |   +----------------------------------------------------------------+ |
|   +----------------------------------------------------------------------+
|                                                                          |
+--------------------------------------------------------------------------+
```

### Memory Management for Local Execution

When the Flink.NET JobManager runs in a local mode (e.g., directly on a developer's machine for testing, or on a single server without Kubernetes orchestration), its memory usage is primarily governed by the planned Flink.NET internal memory configuration options.

*   **Configuration Source:** Users would set these Flink.NET-specific memory parameters (e.g., total process memory, heap size for .NET runtime, network buffer sizes) through mechanisms such as:
    *   Settings in `appsettings.json` or other configuration files.
    *   Environment variables.
    *   Command-line arguments.
    These configurations would be loaded and interpreted by the Flink.NET JobManager application at startup.
*   **Absence of External Limits:** In this local mode, there isn't an external orchestrator like Kubernetes imposing hard memory limits on the process. Therefore, correctly configuring Flink.NET's internal memory settings is crucial.
*   **Risk of OOM:** If these internal settings are too high for the available system memory, or if components consume more than allocated due to misconfiguration or leaks, the JobManager process could suffer from `OutOfMemoryException` or general system instability.
*   **Guidance:** It will be important for Flink.NET to provide clear guidance on default values and how to adjust these settings for typical local development or testing scenarios.

This mode offers flexibility for local development but places more responsibility on the user to manage memory settings appropriately for their environment. This model is illustrated below:
```ascii
+---------------------------------------------------------------------+
| Flink.NET JobManager Process (Local Execution Mode)                 |
|                                                                     |
|   +-----------------------+  Flink.NET Internal Memory Configs      |
|   | .NET Runtime Heap     |  (e.g., jobmanager.memory.heap.size,    |
|   +-----------------------+   loaded from appsettings.json, env vars)|
|   | Network Buffers       |                                         |
|   +-----------------------+                                         |
|   | State Backends (mem)  |                                         |
|   +-----------------------+                                         |
|   | Other Components      |                                         |
|   +-----------------------+                                         |
|                                                                     |
+---------------------------------------------------------------------+
```

## Disclaimer

Note: The Flink.NET JobManager is under active development. Some concepts, particularly around High Availability and fine-grained memory management, are evolving and may currently differ from the mature implementations found in Apache Flink. This documentation reflects the current design, observed behavior based on recent code analysis, and planned future enhancements. For definitive Apache Flink concepts, please refer to the official Apache Flink documentation.

---
Previous: [Core Concepts: Memory Overview](./Core-Concepts-Memory-Overview.md)
Next: [Core Concepts: TaskManager](./Core-Concepts-TaskManager.md)
